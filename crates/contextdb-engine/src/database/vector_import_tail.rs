//! Persist only an import's uncovered vector suffix in the native tail.
use super::*;
use crate::persistence::vector_paged::{
    MUTABLE_PAGE_SET, PagedBuildReason, PagedBuildRecord, PagedGenerationMeta, ProgressRecord,
    import_tail::NativeImportTail,
};
use contextdb_vector::hnsw::paged_builder::{
    PagedVectorPhase, PagedVectorProgress, PagedVectorTail,
};
use redb::ReadableDatabase;

impl Database {
    /// Import completion seam: completed routes have durable graph coverage.
    /// Replacement work remains owned by the ordinary maintenance scheduler.
    #[doc(hidden)]
    pub fn persist_import_vector_tails(&self) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        let mut operation = self.assert_open_operation();
        operation.release_schema_publication_for_online_maintenance();
        let _guard = self.pruning_guard.lock();
        self.maintenance_context()
            .persist_import_vector_tails(read_memory)
    }
}

impl MaintenanceContext {
    fn persist_import_vector_tails(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        let Some(persistence) = &self.persistence else {
            return Ok(true);
        };
        let mut copy_owner = contextdb_vector::VectorCopyAdmission::new(
            self.finalization.accountant.vector_budget()?,
        );
        let routes = vector_partition_routes(&self.vector, &mut copy_owner)?;
        let mut complete = true;
        for partition in routes {
            let Some(info) = self.vector.try_partition_info(
                &partition.index,
                &partition.partition_key,
                &mut copy_owner,
            )?
            else {
                continue;
            };
            if info.live_rows == 0 {
                continue;
            }
            let Some(mut catalog) = persistence.load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            else {
                complete = false;
                continue;
            };
            if catalog.change.is_none() {
                let mut base_workspace = self.vector.reserve_workspace(
                    Arc::clone(&self.finalization.vector_accountant),
                    128 * 1024,
                    "prepare_native_import_base",
                    "Release idle writer state before preparing native graph pages.",
                )?;
                let prepared = self.publish_direct_metadata(|| {
                    persistence.prepare_native_import_base(&catalog, |bytes, disk_bytes| {
                        base_workspace.resize(
                            bytes,
                            "prepare_native_import_base",
                            "Release idle writer state before copying native graph pages.",
                        )?;
                        self.admit_generation_disk(disk_bytes)
                    })
                })?;
                let Some(prepared) = prepared else {
                    complete = false;
                    continue;
                };
                if prepared != catalog {
                    catalog = prepared;
                    base_workspace.resize(
                        64 * 1024,
                        "prepare_native_import_base",
                        "Release idle writer state.",
                    )?;
                    self.install_published_vector_catalog(&catalog, read_memory.clone())?;
                }
            }
            if catalog.change.is_some_and(|change| {
                change.generation_id & crate::persistence::vector_paged::MUTABLE_STORAGE == 0
            }) {
                let mut workspace = self.vector.reserve_workspace(
                    Arc::clone(&self.finalization.vector_accountant),
                    128 * 1024,
                    "prepare_native_import_change",
                    "Release idle writer state before adopting saved change pages.",
                )?;
                let prepared = self.publish_direct_metadata(|| {
                    persistence.prepare_native_import_change(&catalog, |bytes, disk_bytes| {
                        workspace.resize(
                            bytes,
                            "prepare_native_import_change",
                            "Release idle writer state before copying saved change pages.",
                        )?;
                        self.admit_generation_disk(disk_bytes)
                    })
                })?;
                let Some(prepared) = prepared else {
                    complete = false;
                    continue;
                };
                catalog = prepared;
                workspace.resize(
                    64 * 1024,
                    "prepare_native_import_change",
                    "Release idle writer state.",
                )?;
                self.install_published_vector_catalog(&catalog, read_memory.clone())?;
            }
            let covered = catalog.change.unwrap_or(catalog.base).covered_lsn;
            let count = persistence.vector_partition_tail_entry_count(
                &partition.index,
                &partition.partition_key,
                covered,
            )?;
            let uncovered = persistence
                .vector_partition_has_uncovered_journal_records_with_admission(
                    &partition.index,
                    &partition.partition_key,
                    covered,
                    |bytes| copy_owner.reserve(bytes),
                )?;
            if !uncovered {
                continue;
            }
            let layout = contextdb_vector::scoped_index_layout(
                &self.vector,
                &partition.index,
                &mut copy_owner,
            )?;
            let mut policy = layout.resolve_policy(info.live_rows, 1);
            let mut declaration = vector_paged_build::declaration_for(&layout, &policy)?;
            let saved = catalog
                .change
                .map(|change| {
                    persistence
                        .load_vector_partition_change_generation(
                            &partition.index,
                            &partition.partition_key,
                            catalog.base.generation_id,
                            change.generation_id,
                        )
                        .and_then(|record| {
                            let meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
                            persistence.with_db(|db| {
                                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                                crate::persistence::vector_paged::import_tail::resolve_native_change_backing(&read, &meta)
                            })
                        })
                })
                .transpose()?;
            if let Some(meta) = &saved {
                if meta.page_set.pages != MUTABLE_PAGE_SET
                    || meta.declaration.dimension as usize != layout.dimension
                    || meta.declaration.quantization != layout.quantization
                {
                    complete = false;
                    continue;
                }
                // Append using the serving graph's construction declaration.
                // Maintenance still resolves the current policy and owns any
                // required topology replacement while this route serves.
                declaration = meta.declaration.clone();
                policy.hnsw_m = declaration.hnsw_m as usize;
                policy.hnsw_ef_construction = declaration.hnsw_ef_construction as usize;
                policy.hnsw_ef_search = declaration.hnsw_ef_search as usize;
                policy.policy_revision = declaration.policy_revision;
            }
            let body_bytes = count
                .checked_mul(
                    layout
                        .dimension
                        .saturating_mul(4)
                        .saturating_add(std::mem::size_of::<VectorEntry>())
                        .saturating_add(256),
                )
                .ok_or_else(|| Error::Other("import vector suffix size overflow".into()))?;
            let mut workspace = self.vector.reserve_workspace(
                Arc::clone(&self.finalization.vector_accountant),
                body_bytes.saturating_add(128 * 1024),
                "persist_import_vector_tail",
                "Release idle writer state before persisting this import's vector suffix.",
            )?;
            let ((mut entries, through_lsn), through_tx, _pin) =
                self.tx_mgr.with_commit_lock(|| {
                    let through_tx = self.tx_mgr.current_tx_max();
                    let pin = self.snapshot_registry.register(SnapshotId(through_tx.0));
                    let entries = persistence.load_vector_partition_tail_entries(
                        &partition.index,
                        &partition.partition_key,
                        covered,
                    )?;
                    Ok::<_, Error>((entries, through_tx, pin))
                })?;
            if entries.len() > count {
                return Err(Error::Other(
                    "import suffix grew beyond its admitted frontier".into(),
                ));
            }
            let storage = match &saved {
                Some(meta) => meta.storage_id,
                None => self
                    .tx_mgr
                    .with_commit_lock(|| persistence.allocate_native_tail_storage())?,
            };
            persistence.native_tail_entries_not_stored(storage, &mut entries, saved.as_ref())?;
            let mut progress = saved
                .as_ref()
                .map(PagedGenerationMeta::progress)
                .transpose()?
                .unwrap_or(PagedVectorProgress::new(
                    storage,
                    declaration.digest()?,
                    catalog.base.covered_tx,
                    catalog.base.covered_lsn,
                    0,
                ));
            progress.generation_id = storage;
            progress.phase = PagedVectorPhase::Inserting;
            let record = PagedBuildRecord {
                storage_id: storage,
                previous_generation_id: catalog.change.unwrap_or(catalog.base).generation_id,
                reason: PagedBuildReason::NewChanges,
                declaration: declaration.clone(),
                progress: ProgressRecord::from_progress(&progress),
                schema_binding: persistence.vector_schema_binding(&partition.index)?,
            };
            let backend = Arc::new(NativeImportTail {
                persistence: Arc::clone(persistence),
                partition: partition.clone(),
                storage_id: storage,
                declaration: declaration.clone(),
                schema_binding: record.schema_binding,
                catalog: parking_lot::Mutex::new(catalog),
                hooks: vector_paged_build::paged_build_hooks(
                    &self.vector,
                    &self.tx_mgr,
                    &self.resource_closed,
                    &self.publication_interrupted,
                    &partition,
                    &record,
                )?,
            });
            let initial_meta = saved.unwrap_or(PagedGenerationMeta::decode(
                &RedbPersistence::encode_paged_generation_meta(
                    storage,
                    &declaration,
                    &progress,
                    record.schema_binding,
                    crate::persistence::vector_paged::PageSetRoot {
                        pages: MUTABLE_PAGE_SET,
                        bytes: 0,
                        digest: [0; 32],
                    },
                )?,
            )?);
            let source = backend.source(&initial_meta)?;
            let mut tail = PagedVectorTail::new(
                partition.clone(),
                progress,
                policy,
                layout.dimension,
                layout.quantization,
                backend.clone(),
                source,
                self.finalization.accountant.vector_budget()?,
                Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            )?;
            let mut plan = vector_paged_build::paged_batch_plan(&declaration)?;
            let writes_per_point = plan.max_node_writes / plan.max_points;
            plan.max_points = entries.len().max(1);
            plan.max_node_writes = plan
                .max_points
                .checked_mul(writes_per_point)
                .ok_or_else(|| Error::Other("import tail write bound overflow".into()))?;
            self.admit_generation_disk(native_tail_write_bound(plan)?)?;
            self.publish_direct_metadata(|| {
                tail.append_committed_cohort(&entries, plan, through_tx, through_lsn)
            })?;
            #[cfg(feature = "test-seams")]
            crate::writer_commit_test_seams::reached(
                crate::writer_commit_test_seams::CommitBoundary::GenerationAfterRedb,
            );
            let published = backend.catalog.lock().clone();
            drop(tail);
            drop(backend);
            drop(entries);
            workspace.resize(
                64 * 1024,
                "persist_import_vector_tail",
                "Release idle writer state.",
            )?;
            self.install_published_vector_catalog(&published, read_memory.clone())?;
        }
        Ok(complete)
    }
}

impl MaintenanceContext {
    /// Absorb one committed cohort into the completed replacement, leaving
    /// every later cohort for a future finite scheduler wake.
    pub(super) fn catch_up_native_import_candidate(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        record: &PagedBuildRecord,
        progress: &PagedVectorProgress,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        use crate::persistence::vector_paged::import_tail::NativeImportCandidate;
        let Some(through_lsn) =
            persistence.native_candidate_first_uncovered_lsn(partition, progress.frontier_lsn)?
        else {
            return Ok(false);
        };
        let count = persistence.vector_partition_tail_entry_count_through(
            &partition.index,
            &partition.partition_key,
            progress.frontier_lsn,
            Some(through_lsn),
        )?;
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
        let bytes = count
            .checked_mul(
                layout
                    .dimension
                    .saturating_mul(4)
                    .saturating_add(std::mem::size_of::<VectorEntry>())
                    .saturating_add(256),
            )
            .ok_or_else(|| Error::Other("native candidate cohort admission overflow".into()))?;
        let _workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            bytes.saturating_add(128 * 1024),
            "catch_up_native_import_candidate",
            "Release idle writer state before advancing this finite cohort.",
        )?;
        let (mut entries, frontier_tx, _pin) = self.tx_mgr.with_commit_lock(|| {
            let tx = self.tx_mgr.current_tx_max();
            let pin = self.snapshot_registry.register(SnapshotId(tx.0));
            let (entries, loaded_through) = persistence
                .load_vector_partition_tail_entries_through(
                    &partition.index,
                    &partition.partition_key,
                    progress.frontier_lsn,
                    Some(through_lsn),
                )?;
            if loaded_through != through_lsn || entries.len() > count {
                return Err(Error::Other(
                    "native candidate cohort changed during admission".into(),
                ));
            }
            Ok::<_, Error>((entries, tx, pin))
        })?;
        let page_set = persistence.paged_storage_page_set_root(record.storage_id)?;
        let meta = PagedGenerationMeta::decode(&RedbPersistence::encode_paged_generation_meta(
            record.storage_id,
            &record.declaration,
            progress,
            record.schema_binding,
            page_set,
        )?)?;
        persistence.native_tail_entries_not_stored(record.storage_id, &mut entries, Some(&meta))?;
        let catalog = persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            .ok_or_else(|| Error::Other("native candidate has no serving catalog".into()))?;
        let backend = Arc::new(NativeImportCandidate {
            source_owner: NativeImportTail {
                persistence: Arc::clone(persistence),
                partition: partition.clone(),
                storage_id: record.storage_id,
                declaration: record.declaration.clone(),
                schema_binding: record.schema_binding,
                catalog: parking_lot::Mutex::new(catalog),
                hooks: vector_paged_build::paged_build_hooks(
                    &self.vector,
                    &self.tx_mgr,
                    &self.resource_closed,
                    &self.publication_interrupted,
                    partition,
                    record,
                )?,
            },
        });
        let mut next = progress.clone();
        next.phase = PagedVectorPhase::Inserting;
        let policy = layout.resolve_policy(progress.total_points, 1);
        let source = backend.source_owner.source(&meta)?;
        let mut tail = PagedVectorTail::new(
            partition.clone(),
            next,
            policy,
            layout.dimension,
            layout.quantization,
            backend,
            source,
            self.finalization.accountant.vector_budget()?,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )?;
        let mut plan = vector_paged_build::paged_batch_plan(&record.declaration)?;
        let writes = plan.max_node_writes / plan.max_points;
        plan.max_points = entries.len().max(1);
        plan.max_node_writes = plan
            .max_points
            .checked_mul(writes)
            .ok_or_else(|| Error::Other("native candidate write quota overflow".into()))?;
        self.admit_generation_disk(native_tail_write_bound(plan)?)?;
        self.vector.forget_partition_paged_build(partition);
        self.publish_direct_metadata(|| {
            tail.append_committed_cohort(&entries, plan, frontier_tx, through_lsn)
        })?;
        drop(tail);
        let saved = persistence
            .load_paged_build_record(partition)?
            .ok_or_else(|| Error::Other("native catch-up lost its restart record".into()))?;
        vector_paged_build::register_paged_build(
            &self.vector,
            &self.tx_mgr,
            persistence,
            &self.finalization.accountant,
            &self.resource_closed,
            &self.publication_interrupted,
            partition,
            &saved,
            copy_owner,
        )?;
        Ok(true)
    }
}

fn native_tail_write_bound(
    plan: contextdb_vector::hnsw::paged_builder::PagedVectorBatchPlan,
) -> Result<usize> {
    plan.max_node_writes
        .checked_mul(plan.max_node_bytes.saturating_add(128))
        .and_then(|bytes| bytes.checked_add(plan.max_points.saturating_mul(4096)))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| Error::Other("native vector disk write admission overflow".into()))
}
