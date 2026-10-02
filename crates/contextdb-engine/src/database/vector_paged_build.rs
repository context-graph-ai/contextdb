//! File-backed vector construction through finite, restartable paged builds.
//!
//! A base generation (initial build, replacement or topology change) is built
//! by the vector crate's `PagedGenerationBuilder` against native storage. The
//! build's frontier record is durable before its first batch and its frontier
//! snapshot is held as a construction floor until publication, both in this
//! process and, after a restart, from the durable record before any cleanup.
//! Sealed change generations over a published base keep their existing
//! suffix-sized path.
use super::vector_maintenance_inventory::MaintenanceBuildCandidate;
use super::*;
use crate::persistence::vector_paged::{
    NativePagedGeneration, PagedBuildHooks, PagedBuildReason, PagedBuildRecord, PagedDeclaration,
    PagedPublication, VECTOR_GRAPH_PAGED_FORMAT_VERSION,
};
use contextdb_vector::hnsw::paged_builder::{
    PagedEdge, PagedVectorBatchPlan, PagedVectorPhase, PagedVectorPoint, PagedVectorProgress,
};

/// Source points one construction batch may take. A batch's working set does
/// not grow with this count; it bounds the length of one write transaction and
/// the rejected-history window scanned beside it.
const PAGED_BATCH_POINTS: usize = 16;
/// Batches one maintenance cycle gives a partition under a tight budget; the
/// cycle's source work stays at PAGED_BATCH_POINTS * this count.
const PAGED_MIN_BATCHES_PER_CYCLE: usize = 4;
/// Upper bound on consecutive batches one maintenance cycle gives one
/// partition when the writer budget has room for many batch workspaces.
const PAGED_MAX_BATCHES_PER_CYCLE: usize = 64;
/// Retained serving controls of one published paged generation: the tail
/// handle, its progress record and the captured-root source descriptor.
pub(super) const PAGED_GENERATION_RESIDENT_BYTES: u64 = 4096;

/// Exact batch plan for one declaration. Every term is the vector crate's own
/// validation minimum, independent of the partition's population.
pub(super) fn paged_batch_plan(declaration: &PagedDeclaration) -> Result<PagedVectorBatchPlan> {
    let m = declaration.hnsw_m as usize;
    let ef_construction = declaration.hnsw_ef_construction as usize;
    let dimension = declaration.dimension as usize;
    let degree_square = (2 * m + 1).saturating_mul(2 * m + 1);
    let scratch_points = degree_square.max(ef_construction).saturating_add(1);
    let value_bytes = match declaration.quantization {
        VectorQuantization::F32 => dimension.saturating_mul(4),
        VectorQuantization::SQ8 => dimension.saturating_add(32),
        VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(32),
    };
    let max_layer = 16usize;
    let max_node_bytes = (max_layer - 1)
        .saturating_mul(m)
        .saturating_add(2 * m)
        .saturating_mul(std::mem::size_of::<PagedEdge>())
        .saturating_add(max_layer.saturating_mul(std::mem::size_of::<Vec<PagedEdge>>()))
        .saturating_add(value_bytes)
        .saturating_add(std::mem::size_of::<PagedVectorPoint>())
        // Encoded framing: origin, identity, per-layer lengths and the value
        // length prefix of `encode_paged_point_into`.
        .saturating_add(256);
    let source_key_bytes = 32 + 24;
    Ok(PagedVectorBatchPlan {
        max_points: PAGED_BATCH_POINTS,
        scratch_points,
        max_node_bytes,
        max_node_writes: PAGED_BATCH_POINTS.saturating_mul(1 + 2 * m * max_layer),
        // One encoded page buffer plus one decoded source body.
        dirty_page_bytes: max_node_bytes
            .saturating_add(value_bytes)
            .saturating_add(256),
        source_key_bytes,
        progress_bytes: std::mem::size_of::<PagedVectorProgress>()
            .saturating_add(2 * source_key_bytes)
            .saturating_add(256),
    })
}

fn declaration_for(
    layout: &VectorIndexLayout,
    policy: &contextdb_vector::store::ResolvedVectorPolicy,
) -> Result<PagedDeclaration> {
    let narrow = |value: usize, name: &str| {
        u32::try_from(value)
            .map_err(|_| Error::Other(format!("vector {name} exceeds the durable range")))
    };
    Ok(PagedDeclaration {
        dimension: narrow(layout.dimension, "dimension")?,
        quantization: layout.quantization,
        hnsw_m: narrow(policy.hnsw_m, "HNSW M")?,
        hnsw_ef_construction: narrow(policy.hnsw_ef_construction, "HNSW EF_CONSTRUCTION")?,
        hnsw_ef_search: narrow(policy.hnsw_ef_search, "HNSW EF_SEARCH")?,
        policy_revision: policy.policy_revision,
    })
}

fn progress_state(reason: PagedBuildReason) -> &'static str {
    match reason {
        PagedBuildReason::InitialBuild | PagedBuildReason::PolicyChange => "building",
        PagedBuildReason::Tombstones | PagedBuildReason::NewChanges => "compacting",
    }
}

/// Engine hooks shared by every batch of one registered build.
pub(super) fn paged_build_hooks(
    vector: &Arc<VectorStore>,
    tx_mgr: &Arc<TransactionManager<DynStore>>,
    resource_closed: &Arc<AtomicBool>,
    publication_interrupted: &Arc<AtomicBool>,
    partition: &VectorPartitionRef,
    record: &PagedBuildRecord,
) -> Result<PagedBuildHooks> {
    let progress = record.progress()?;
    let closed = Arc::clone(resource_closed);
    let interrupted = Arc::clone(publication_interrupted);
    let report_vector = Arc::downgrade(vector);
    let report_partition = partition.clone();
    let reason = record.reason;
    let total = progress.total_points;
    let frontier = progress.frontier_tx;
    let gate_manager = Arc::downgrade(tx_mgr);
    Ok(PagedBuildHooks {
        checkpoint: Arc::new(move || {
            if interrupted.load(Ordering::SeqCst) {
                return Err(Error::PublicationInterrupted);
            }
            if closed.load(Ordering::SeqCst) {
                return Err(Error::ReadCancelled);
            }
            Ok(())
        }),
        inserted: Arc::new(move |done| {
            if let Some(vector) = report_vector.upgrade() {
                vector.report_partition_paged_build_progress(
                    &report_partition,
                    progress_state(reason),
                    reason.as_str(),
                    total,
                    done,
                    frontier,
                );
            }
        }),
        gate: Arc::new(move |work| {
            let manager = gate_manager
                .upgrade()
                .ok_or_else(|| Error::Other("vector build outlived its writer".into()))?;
            manager.with_commit_lock(work)
        }),
    })
}

/// Register one durable build with the vector store and publish its durable
/// progress as the partition's reported maintenance progress.
pub(super) fn register_paged_build(
    vector: &Arc<VectorStore>,
    tx_mgr: &Arc<TransactionManager<DynStore>>,
    persistence: &Arc<RedbPersistence>,
    accountant: &Arc<MemoryAccountant>,
    resource_closed: &Arc<AtomicBool>,
    publication_interrupted: &Arc<AtomicBool>,
    partition: &VectorPartitionRef,
    record: &PagedBuildRecord,
) -> Result<()> {
    let progress = record.progress()?;
    let hooks = paged_build_hooks(
        vector,
        tx_mgr,
        resource_closed,
        publication_interrupted,
        partition,
        record,
    )?;
    let native = Arc::new(NativePagedGeneration::new(
        Arc::clone(persistence),
        partition.clone(),
        record,
        hooks,
    ));
    let plan = paged_batch_plan(&record.declaration)?;
    vector.register_partition_paged_builder(
        partition,
        progress.clone(),
        native,
        plan,
        accountant.clone(),
    )?;
    report_durable_progress(vector, partition, record.reason, &progress);
    Ok(())
}

fn report_durable_progress(
    vector: &VectorStore,
    partition: &VectorPartitionRef,
    reason: PagedBuildReason,
    progress: &PagedVectorProgress,
) {
    vector.report_partition_paged_build_progress(
        partition,
        progress_state(reason),
        reason.as_str(),
        progress.total_points,
        progress.graph.points,
        progress.frontier_tx,
    );
}

impl Database {
    /// Restore every unpublished paged build: its construction floor first,
    /// then its restartable builder. Runs during open, before any cleanup.
    pub(super) fn restore_paged_vector_builds(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        install_paged_root_capture(persistence, &self.tx_mgr);
        for (partition, frontier) in persistence.load_construction_floors()? {
            self.snapshot_registry
                .hold_construction_floor(&partition, SnapshotId::from_tx(frontier));
        }
        for (partition, record) in persistence.load_paged_build_records()? {
            let progress = record.progress()?;
            self.snapshot_registry
                .hold_construction_floor(&partition, SnapshotId::from_tx(progress.frontier_tx));
            let registered = self.vector_store.contains_index(&partition.index)
                && self
                    .vector_store
                    .partition_info(&partition.index, &partition.partition_key)
                    .is_some();
            if !registered {
                // The route no longer exists; nothing can publish this build.
                // Its pages are queued for cleanup by the next maintenance
                // cycle once the floor has been released here.
                self.snapshot_registry
                    .release_construction_floor(&partition);
                persistence.discard_paged_build(&partition)?;
                continue;
            }
            register_paged_build(
                &self.vector_store,
                &self.tx_mgr,
                persistence,
                &self.accountant,
                &self.resource_closed,
                &self.publication_interrupted,
                &partition,
                &record,
            )?;
        }
        Ok(())
    }
}

/// Serving readers of published paged generations capture the writer's
/// current published root; they never begin a fresh storage read.
pub(super) fn install_paged_root_capture(
    persistence: &RedbPersistence,
    tx_mgr: &Arc<TransactionManager<DynStore>>,
) {
    let manager = Arc::downgrade(tx_mgr);
    persistence.set_paged_root_capture(Arc::new(move || {
        let manager = manager
            .upgrade()
            .ok_or_else(|| Error::Other("paged vector reader outlived its writer".into()))?;
        writer_read::durable_root(manager.capture_published_read().handle)?
            .ok_or_else(|| Error::Other("writer has no durable published root".into()))
    }));
}

impl MaintenanceContext {
    /// Advance this partition's paged base build, starting one when the
    /// maintained route needs a new base. `None` leaves the partition to the
    /// existing sealed-change and repair paths.
    pub(super) fn advance_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<Option<VectorPartitionPublicationOutcome>> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(None);
        };
        if self.vector.partition_route_quarantine(partition).is_some() {
            return Ok(None);
        }
        install_paged_root_capture(persistence, &self.tx_mgr);
        let recorded = persistence.load_paged_build_record(partition)?;
        if recorded.is_none()
            && (!self.paged_base_build_applies(partition, persistence)?
                || self.whole_build_fits(partition)?)
        {
            return Ok(None);
        }
        // Admission first: a budget that cannot fund one construction batch
        // refuses with the established typed generation refusal before any
        // durable build record, floor or registration exists.
        {
            let layout = self.vector.index_layout(&partition.index)?;
            let live = self
                .vector
                .partition_info(&partition.index, &partition.partition_key)
                .map(|info| info.live_rows)
                .unwrap_or(0);
            let declaration = declaration_for(&layout, &layout.resolve_policy(live.max(1), 1))?;
            let batch = paged_batch_plan(&declaration)?.construction_workspace_bytes()?;
            drop(self.vector.reserve_workspace(
                self.accountant.clone(),
                batch,
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before compacting this maintained route.",
            )?);
        }
        let record = match recorded {
            Some(record) => {
                if self
                    .vector
                    .partition_paged_build_progress(partition)
                    .is_none()
                {
                    register_paged_build(
                        &self.vector,
                        &self.tx_mgr,
                        persistence,
                        &self.accountant,
                        &self.resource_closed,
                        &self.publication_interrupted,
                        partition,
                        &record,
                    )?;
                }
                record
            }
            None => match self.start_paged_vector_build(partition, persistence)? {
                Some(record) => record,
                None => return Ok(None),
            },
        };
        // A changed declaration cannot finish this frontier.
        let layout = self.vector.index_layout(&partition.index)?;
        let progress = record.progress()?;
        let current = declaration_for(&layout, &layout.resolve_policy(progress.total_points, 1))?;
        if current != record.declaration {
            let mut checkpoint = || self.build_checkpoint();
            self.vector
                .abandon_partition_paged_build(partition, &mut checkpoint)?;
            self.snapshot_registry.release_construction_floor(partition);
            self.vector.clear_partition_paged_build_progress(partition);
            return Ok(Some(VectorPartitionPublicationOutcome::default()));
        }
        let plan = paged_batch_plan(&record.declaration)?;
        let batch_bytes = plan.construction_workspace_bytes()?;
        // One cycle gives this partition one batch workspace per sixteenth of
        // the declared writer budget: a tight budget advances one finite batch
        // per cycle, a roomy one finishes small partitions in a single cycle.
        // Free headroom is not used: it swings with resident caches.
        let budget = self.accountant.usage().limit.unwrap_or(usize::MAX);
        let batches = (budget / batch_bytes.saturating_mul(16).max(1))
            .clamp(PAGED_MIN_BATCHES_PER_CYCLE, PAGED_MAX_BATCHES_PER_CYCLE * 4);
        for _ in 0..batches {
            let progress = self
                .vector
                .partition_paged_build_progress(partition)
                .ok_or_else(|| Error::Other("paged vector build lost its registration".into()))?;
            match progress.phase {
                PagedVectorPhase::Inserting | PagedVectorPhase::Encoding => {
                    #[cfg(feature = "test-seams")]
                    if progress.phase == PagedVectorPhase::Inserting && progress.graph.points == 0 {
                        contextdb_vector::hnsw::observe_paged_hnsw_build();
                    }
                    let mut checkpoint = || self.build_checkpoint();
                    let stepped = self.vector.step_partition_paged_build_with_plan(
                        partition,
                        plan,
                        &mut checkpoint,
                    );
                    let durable = self
                        .vector
                        .partition_paged_build_progress(partition)
                        .unwrap_or(progress);
                    report_durable_progress(&self.vector, partition, record.reason, &durable);
                    if !stepped? {
                        break;
                    }
                }
                PagedVectorPhase::Complete => {
                    return self
                        .publish_paged_vector_build(
                            partition,
                            persistence,
                            &record,
                            &progress,
                            plan,
                        )
                        .map(Some);
                }
                _ => {
                    return Err(Error::Other(
                        "registered paged vector build is not publishable".into(),
                    ));
                }
            }
        }
        Ok(Some(VectorPartitionPublicationOutcome::default()))
    }

    /// A paged build replaces or creates a BASE generation over live rows.
    /// Sealed changes over a base without one, delete-only routes and empty
    /// routes keep their existing suffix-sized publication.
    fn paged_base_build_applies(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
    ) -> Result<bool> {
        let Some(info) = self
            .vector
            .partition_info(&partition.index, &partition.partition_key)
        else {
            return Ok(false);
        };
        if info.live_rows == 0 {
            return Ok(false);
        }
        let layout = self.vector.index_layout(&partition.index)?;
        let policy = layout.resolve_policy(info.live_rows, 1);
        let topology_replacement = info.graph_available
            && info.serving_policy.is_none_or(|serving| {
                serving.hnsw_m != policy.hnsw_m
                    || serving.hnsw_ef_construction != policy.hnsw_ef_construction
            });
        let publish_change = persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            .is_some_and(|catalog| catalog.change.is_none())
            && !topology_replacement;
        Ok(!publish_change)
    }

    /// Whether the resident whole build's complete construction workspace fits
    /// the writer's current headroom. Only a build that cannot be funded whole
    /// moves to finite paged batches.
    fn whole_build_fits(&self, partition: &VectorPartitionRef) -> Result<bool> {
        let Some(info) = self
            .vector
            .partition_info(&partition.index, &partition.partition_key)
        else {
            return Ok(true);
        };
        let layout = self.vector.index_layout(&partition.index)?;
        let policy = layout.resolve_policy(info.live_rows, 1);
        let mut plan = VectorGenerationWorkspacePlan::new(
            info.live_rows,
            info.live_rows,
            &layout,
            partition,
            policy,
            0,
        )?;
        plan.assign_raw_decode_owner(true, false);
        let needed = plan.build_phase_bytes();
        Ok(self
            .accountant
            .usage()
            .available
            .is_none_or(|available| available >= needed))
    }

    /// Persist a new frontier record and hold its floor, under the commit
    /// gate. Returns `None` when the existing sealed-change path owns this
    /// partition's next publication.
    fn start_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
    ) -> Result<Option<PagedBuildRecord>> {
        let Some(info) = self
            .vector
            .partition_info(&partition.index, &partition.partition_key)
        else {
            return Ok(None);
        };
        let layout = self.vector.index_layout(&partition.index)?;
        let current = persistence.load_vector_partition_generation_catalog_for(
            &partition.index,
            &partition.partition_key,
        )?;
        let policy = layout.resolve_policy(info.live_rows, 1);
        let topology_replacement = info.graph_available
            && info.serving_policy.is_none_or(|serving| {
                serving.hnsw_m != policy.hnsw_m
                    || serving.hnsw_ef_construction != policy.hnsw_ef_construction
            });
        let publish_change = current
            .as_ref()
            .is_some_and(|catalog| catalog.change.is_none())
            && !topology_replacement;
        if publish_change {
            return Ok(None);
        }
        let reason = if current.is_none() {
            PagedBuildReason::InitialBuild
        } else if topology_replacement {
            PagedBuildReason::PolicyChange
        } else if info.tombstones != 0 {
            PagedBuildReason::Tombstones
        } else {
            PagedBuildReason::NewChanges
        };
        let previous_generation = match current.as_ref() {
            Some(catalog) => catalog
                .change
                .map(|change| change.generation_id)
                .unwrap_or(catalog.base.generation_id)
                .max(catalog.base.generation_id),
            None => 0,
        };
        let highest = persistence.highest_vector_partition_generation_id(
            &partition.index,
            &partition.partition_key,
            |_| Ok(()),
        )?;
        let generation_id = previous_generation
            .max(highest)
            .checked_add(1)
            .ok_or_else(|| {
                Error::Other(format!(
                    "vector generation identity exhausted for {}.{}",
                    partition.index.table, partition.index.column
                ))
            })?;
        let record = self
            .tx_mgr
            .with_commit_lock(|| -> Result<Option<PagedBuildRecord>> {
                let frontier_tx = self.tx_mgr.current_tx_max();
                let frontier_lsn = self.tx_mgr.current_lsn();
                let total = persistence.vector_partition_live_membership_count(partition)?;
                if total == 0 {
                    // An empty route keeps the existing delete-only publication.
                    return Ok(None);
                }
                let declaration = declaration_for(&layout, &layout.resolve_policy(total, 1))?;
                let record = persistence.begin_paged_build(
                    partition,
                    declaration,
                    reason,
                    generation_id,
                    previous_generation,
                    frontier_tx,
                    frontier_lsn,
                    total,
                )?;
                self.snapshot_registry
                    .hold_construction_floor(partition, SnapshotId::from_tx(frontier_tx));
                Ok(Some(record))
            })?;
        let Some(record) = record else {
            return Ok(None);
        };
        register_paged_build(
            &self.vector,
            &self.tx_mgr,
            persistence,
            &self.accountant,
            &self.resource_closed,
            &self.publication_interrupted,
            partition,
            &record,
        )?;
        Ok(Some(record))
    }

    fn publish_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        record: &PagedBuildRecord,
        progress: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
    ) -> Result<VectorPartitionPublicationOutcome> {
        let meta = RedbPersistence::encode_paged_generation_meta(
            record.storage_id,
            &record.declaration,
            progress,
        )?;
        let durable_bytes = u64::try_from(meta.len())
            .map_err(|_| Error::Other("paged generation metadata exceeds u64".into()))?;
        let state_digest = *blake3::hash(&meta).as_bytes();
        let descriptor = VectorPartitionGenerationDescriptor {
            generation_id: progress.generation_id,
            covered_tx: progress.frontier_tx,
            covered_lsn: progress.frontier_lsn,
            resident_bytes: PAGED_GENERATION_RESIDENT_BYTES,
            durable_bytes,
            policy_revision: record.declaration.policy_revision,
            hnsw_m: record.declaration.hnsw_m,
            hnsw_ef_construction: record.declaration.hnsw_ef_construction,
        };
        let candidate = VectorPartitionGenerationCandidate {
            base: VectorPartitionBaseGenerationRecord {
                index: partition.index.clone(),
                partition_key: partition.partition_key.clone(),
                generation_id: progress.generation_id,
                base_tx: progress.frontier_tx,
                base_lsn: progress.frontier_lsn,
                highest_included_tx: progress.frontier_tx,
                highest_included_lsn: progress.frontier_lsn,
                dimension: record.declaration.dimension,
                quantization: record.declaration.quantization,
                algorithm_version: VECTOR_GRAPH_ALGORITHM_VERSION,
                graph_format_version: VECTOR_GRAPH_PAGED_FORMAT_VERSION,
                graph: VectorPartitionGraphBytes::verified(meta),
                state_digest,
            },
            change: None,
            catalog: VectorPartitionGenerationCatalogRecord {
                index: partition.index.clone(),
                partition_key: partition.partition_key.clone(),
                base: descriptor,
                change: None,
                published_tx: progress.frontier_tx,
                published_lsn: progress.frontier_lsn,
            },
        };
        let prepared = persistence.prepare_vector_partition_generation_candidate(&candidate)?;
        self.admit_generation_disk(prepared.write_reservation_bytes())?;
        persistence.stage_paged_publication(
            partition,
            PagedPublication {
                prepared,
                previous_generation_id: record.previous_generation_id,
            },
        );
        let mut checkpoint = || self.build_checkpoint();
        let published = self.vector.publish_partition_paged_build_with_plan(
            partition,
            progress.frontier_lsn,
            plan,
            &mut checkpoint,
        );
        persistence.clear_paged_publication(partition);
        published?;
        #[cfg(feature = "test-seams")]
        crate::writer_commit_test_seams::reached(
            crate::writer_commit_test_seams::CommitBoundary::GenerationAfterRedb,
        );
        self.snapshot_registry.release_construction_floor(partition);
        self.vector.clear_partition_paged_build_progress(partition);
        #[cfg(feature = "test-seams")]
        self.vector.observe_durable_base_publication(
            partition,
            progress.generation_id,
            progress.frontier_tx,
        )?;
        let published_catalog = candidate.catalog;
        drop(candidate.base);
        let mut workspace = self.vector.reserve_workspace(
            self.accountant.clone(),
            RedbPersistence::maintenance_vector_catalog_heap_bytes(&published_catalog),
            "truncate_vector_partition_journal",
            "Raise MEMORY_LIMIT before cleaning this maintained route.",
        )?;
        let truncated = match persistence
            .truncate_vector_partition_journal_through_catalog_with_checkpoint(
                &published_catalog,
                &mut workspace,
                || self.build_checkpoint(),
            ) {
            Ok(truncated) => truncated,
            Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                return Err(error);
            }
            Err(error) => {
                return self.retain_published_vector_cleanup_failure(partition, &error);
            }
        };
        drop(workspace);
        if !truncated {
            let cleanup_retry = self.retry_vector_journal_truncations()?;
            return Ok(VectorPartitionPublicationOutcome {
                published: true,
                cleanup_failure: cleanup_retry.first_failure_details,
            });
        }
        match self.install_published_vector_catalog(&published_catalog) {
            Ok(()) => {}
            Err(
                error @ (Error::MemoryBudgetExceeded { .. } | Error::DiskBudgetExceeded { .. }),
            ) => {
                return self.retain_published_vector_cleanup_failure(partition, &error);
            }
            Err(error) => return Err(error),
        }
        Ok(VectorPartitionPublicationOutcome::published())
    }

    /// Finite page cleanup for retired or abandoned paged generations.
    pub(super) fn reclaim_paged_vector_storage(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        if !persistence.has_paged_cleanup()? {
            return Ok(());
        }
        let _workspace = self.vector.reserve_workspace(
            self.accountant.clone(),
            PAGED_CLEANUP_KEYS.saturating_mul(128),
            "reclaim_paged_vector_generation",
            "Raise MEMORY_LIMIT before reclaiming retired vector pages.",
        )?;
        self.build_checkpoint()?;
        persistence.paged_cleanup_step(PAGED_CLEANUP_KEYS)?;
        Ok(())
    }
}

/// Pages one cleanup transaction removes.
const PAGED_CLEANUP_KEYS: usize = 4096;

/// Persistence of a published paged generation: sealed, so a tail append is
/// refused rather than silently writing into a generation readers share.
pub(super) struct SealedPagedGeneration;

impl contextdb_vector::hnsw::paged_builder::PagedVectorPersistence for SealedPagedGeneration {
    fn commit_batch(
        &self,
        _previous: &PagedVectorProgress,
        _plan: PagedVectorBatchPlan,
        _batch: &mut dyn FnMut(
            &mut dyn contextdb_vector::hnsw::paged_builder::PagedVectorTransaction,
        ) -> Result<PagedVectorProgress>,
    ) -> Result<contextdb_vector::hnsw::paged_builder::PagedVectorCommittedBatch> {
        Err(Error::Other(
            "a published paged vector generation is sealed".into(),
        ))
    }
}

impl MaintenanceContext {
    /// Routes with a durable unpublished build that this cycle's sampled
    /// inventory did not select. One small control record per pending build.
    pub(super) fn unselected_paged_builds(
        &self,
        selected: &[MaintenanceBuildCandidate],
    ) -> Result<Vec<VectorPartitionRef>> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(persistence
            .load_paged_build_records()?
            .into_iter()
            .map(|(partition, _)| partition)
            .filter(|partition| {
                !selected
                    .iter()
                    .any(|candidate| candidate.partition == *partition)
            })
            .collect())
    }
}

/// Releases a whole build's durable and in-process construction floor when
/// its maintenance call returns by any path other than process death.
pub(super) struct WholeBuildFloor<'a> {
    context: &'a MaintenanceContext,
    partition: &'a VectorPartitionRef,
}

impl<'a> WholeBuildFloor<'a> {
    pub(super) fn new(context: &'a MaintenanceContext, partition: &'a VectorPartitionRef) -> Self {
        Self { context, partition }
    }
}

impl Drop for WholeBuildFloor<'_> {
    fn drop(&mut self) {
        let Some(persistence) = self.context.persistence.as_ref() else {
            return;
        };
        if let Err(error) = persistence.clear_construction_floor(self.partition) {
            tracing::warn!(%error, "could not clear a whole vector build's durable floor");
            return;
        }
        self.context
            .snapshot_registry
            .release_construction_floor(self.partition);
    }
}
