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
    PagedPublication, RESIDENT_EXPORT_MARKER, VECTOR_GRAPH_PAGED_FORMAT_VERSION,
};
use contextdb_vector::hnsw::paged_builder::{
    PagedEdge, PagedGenerationPersistence, PagedVectorBatchPlan, PagedVectorPhase,
    PagedVectorPoint, PagedVectorProgress,
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
pub(crate) const PAGED_GENERATION_RESIDENT_BYTES: u64 = 4096;

/// Exact batch plan for one declaration. Every term is the vector crate's own
/// validation minimum, independent of the partition's population.
pub(crate) fn paged_batch_plan(declaration: &PagedDeclaration) -> Result<PagedVectorBatchPlan> {
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

pub(crate) fn declaration_for(
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
    _vector: &Arc<VectorStore>,
    tx_mgr: &DatabaseTransactionHost,
    resource_closed: &Arc<AtomicBool>,
    publication_interrupted: &Arc<AtomicBool>,
    _partition: &VectorPartitionRef,
    record: &PagedBuildRecord,
) -> Result<PagedBuildHooks> {
    record.progress()?;
    let closed = Arc::clone(resource_closed);
    let interrupted = Arc::clone(publication_interrupted);
    let gate_manager = tx_mgr.downgrade();
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
        // A failed batch rolls back its inserted points. Report only the
        // committed restart record in advance_paged_vector_build, so a retry
        // cannot make this build's reported completed count move backward.
        inserted: Arc::new(|_| {}),
        gate: Arc::new(move |work| {
            let manager = gate_manager
                .upgrade()
                .ok_or_else(|| Error::Other("vector build outlived its writer".into()))?;
            // Publication already owns this manager's commit boundary on this
            // thread. Inherit that guard instead of locking its mutex twice;
            // ordinary construction batches still acquire their own guard.
            if manager.commit_boundary_held_by_current_thread() {
                work()
            } else {
                manager.with_commit_lock(work)
            }
        }),
    })
}

/// Register one durable build with the vector store and publish its durable
/// progress as the partition's reported maintenance progress.
pub(super) fn register_paged_build(
    vector: &Arc<VectorStore>,
    tx_mgr: &DatabaseTransactionHost,
    persistence: &Arc<RedbPersistence>,
    accountant: &crate::memory_accounting::AccountantHandle,
    resource_closed: &Arc<AtomicBool>,
    publication_interrupted: &Arc<AtomicBool>,
    partition: &VectorPartitionRef,
    record: &PagedBuildRecord,
    copy_owner: &mut contextdb_vector::VectorCopyAdmission,
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
        accountant.vector_budget()?,
        copy_owner,
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
    pub(super) fn restore_paged_vector_builds(
        &self,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        install_paged_root_capture(persistence, &self.tx_mgr);
        for (partition, frontier) in persistence.load_construction_floors()? {
            self.snapshot_registry
                .hold_construction_floor(&partition, SnapshotId::from_tx(frontier));
        }
        for (partition, record) in persistence.load_paged_build_records()? {
            let mut record_copy_owner = copy_owner.scratch();
            let copy_owner = &mut record_copy_owner;
            let progress = record.progress()?;
            if progress.last_source_key.as_deref() == Some(RESIDENT_EXPORT_MARKER) {
                // The resident graph died with its process. Partial export
                // links may name not-yet-exported points, so never register
                // this as a resumable paged construction or serving graph.
                persistence.discard_paged_build(&partition)?;
                persistence.clear_construction_floor(&partition)?;
                self.snapshot_registry
                    .release_construction_floor(&partition);
                continue;
            }
            let retention = if record.storage_id & crate::persistence::vector_paged::MUTABLE_STORAGE
                != 0
            {
                persistence.native_candidate_retention_floor(&partition, progress.frontier_tx)?
            } else {
                progress.frontier_tx
            };
            self.snapshot_registry
                .hold_construction_floor(&partition, SnapshotId::from_tx(retention));
            let registered = self.vector_store.contains_index(&partition.index)
                && self
                    .vector_store
                    .try_partition_info(&partition.index, &partition.partition_key, copy_owner)?
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
                copy_owner,
            )?;
        }
        Ok(())
    }
}

/// Serving readers of published paged generations capture the writer's
/// captured operation root, or the published root outside a read scope; they
/// never begin a fresh storage read.
pub(super) fn install_paged_root_capture(
    persistence: &RedbPersistence,
    tx_mgr: &DatabaseTransactionHost,
) {
    let manager = tx_mgr.downgrade();
    let scoped_manager = manager.clone();
    persistence.set_paged_root_capture(
        Arc::new(move || {
            let manager = manager
                .upgrade()
                .ok_or_else(|| Error::Other("paged vector reader outlived its writer".into()))?;
            if let Some(root) = writer_read::scoped_root_for_manager(&manager) {
                return Ok(root);
            }
            writer_read::durable_root(manager.capture_published_read().handle)?
                .ok_or_else(|| Error::Other("writer has no durable published root".into()))
        }),
        Arc::new(move || {
            let manager = scoped_manager
                .upgrade()
                .ok_or_else(|| Error::Other("vector reader outlived its writer".into()))?;
            Ok(writer_read::scoped_root_for_manager(&manager))
        }),
    );
}

impl MaintenanceContext {
    /// Peak of a whole generation load, including the encoded record and
    /// decoder/route workspace. Inputs are authenticated metadata or the
    /// existing codec capacity bound; this never visits vector bodies.
    fn whole_reader_load_bytes(
        partition: &VectorPartitionRef,
        resident: usize,
        durable: usize,
    ) -> Result<usize> {
        let metadata = vector_route_metadata_workspace(partition)?.max(64 * 1024);
        Ok(resident
            .saturating_add(durable.saturating_add(metadata).saturating_mul(2))
            .saturating_add(metadata))
    }

    pub(super) fn captured_whole_reader_serving_fits(
        &self,
        partition: &VectorPartitionRef,
        count: usize,
        layout: &VectorIndexLayout,
        policy: contextdb_vector::store::ResolvedVectorPolicy,
        prior_base: Option<&crate::persistence::VectorPartitionGenerationDescriptor>,
    ) -> Result<bool> {
        let resident = HnswIndex::estimated_resident_bytes_with_m(
            count,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        )
        .saturating_add(prior_base.map_or(0, |base| base.resident_bytes as usize));
        let durable = HnswIndex::durable_generation_capacity_bound(
            count,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        )?
        .saturating_add(prior_base.map_or(0, |base| base.durable_bytes as usize));
        Ok(
            Self::whole_reader_load_bytes(partition, resident, durable)? as u128
                <= self.reader_memory_limit as u128,
        )
    }

    pub(super) fn whole_reader_serving_fits(
        &self,
        partition: &VectorPartitionRef,
        info: &contextdb_vector::store::VectorPartitionInfo,
        layout: &VectorIndexLayout,
    ) -> Result<bool> {
        // Live and retained rows already cover the raw identities. Pending
        // inserts and tombstones describe work on those same identities, not
        // additional vector payloads. The captured-frontier check separately
        // prices the actual selected graph and any retained base or carriers.
        let count = info.live_rows.saturating_add(info.retained_rows);
        let policy = layout.resolve_policy(info.live_rows, 1);
        let resident = HnswIndex::estimated_resident_bytes_with_m(
            count,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        );
        let durable = HnswIndex::durable_generation_capacity_bound(
            count,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        )?;
        if Self::whole_reader_load_bytes(partition, resident, durable)? as u128
            > self.reader_memory_limit as u128
        {
            return Ok(false);
        }
        // An oversized selected base must be replaced, rather than retaining
        // it underneath a small sealed change that would still be unservable.
        let catalog = match self.persistence.as_ref() {
            Some(persistence) => persistence.load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?,
            None => None,
        };
        Ok(!self.reader_serving_replacement_needed(partition, catalog.as_ref())?)
    }

    /// A selected whole base/change that exceeds the store's reader declaration
    /// is a replacement need even when ordinary consolidation is not due.
    /// Native paged catalogs have only their small serving metadata here.
    pub(super) fn reader_serving_replacement_needed(
        &self,
        partition: &VectorPartitionRef,
        catalog: Option<&VectorPartitionGenerationCatalogRecord>,
    ) -> Result<bool> {
        let Some(catalog) = catalog else {
            return Ok(false);
        };
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(false);
        };
        // Metadata only, under an admitted route workspace. A native base
        // stays native regardless of a small reader's per-request limit:
        // it is never repeatedly rebuilt for whole-graph serving feasibility.
        let _metadata = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            vector_route_metadata_workspace(partition)?.max(64 * 1024),
            "prepare_vector_generation",
            "Raise MEMORY_LIMIT before inspecting generation format.",
        )?;
        if persistence.vector_partition_base_graph_format(partition, catalog.base.generation_id)?
            == VECTOR_GRAPH_PAGED_FORMAT_VERSION
        {
            return Ok(false);
        }
        let mut resident = catalog.base.resident_bytes;
        let mut durable = catalog.base.durable_bytes;
        if let Some(change) = catalog.change {
            resident = resident.saturating_add(change.resident_bytes);
            durable = durable.saturating_add(change.durable_bytes);
        }
        let resident = DurableVectorGenerationLoader::descriptor_usize(
            resident,
            "reader serving resident declaration",
        )?;
        let durable = DurableVectorGenerationLoader::descriptor_usize(
            durable,
            "reader serving durable declaration",
        )?;
        Ok(
            Self::whole_reader_load_bytes(partition, resident, durable)? as u128
                > self.reader_memory_limit as u128,
        )
    }
    /// Advance this partition's paged base build, starting one when the
    /// maintained route needs a new base. `None` leaves the partition to the
    /// existing sealed-change and repair paths.
    pub(super) fn advance_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        force_paged: bool,
    ) -> Result<Option<VectorPartitionPublicationOutcome>> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(None);
        };
        // A quarantined route leaves this path for repair before anything
        // reads its retired generations: that inspection reads generation
        // records, which repair admits as source pages of its own.
        if self.vector.partition_route_quarantine(partition).is_some() {
            return Ok(None);
        }
        // Route selection owns keys and decoded catalog metadata before its
        // first source read. This phase ends before a batch owns its workspace.
        let (recorded, base_build_applies, force_paged) = {
            let inventory_bytes = partition
                .partition_key
                .estimated_bytes()
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(1024)
                .saturating_mul(64);
            let _inventory = self.vector.reserve_workspace(
                Arc::clone(&self.finalization.vector_accountant),
                inventory_bytes,
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before inspecting this maintained route.",
            )?;
            // Retired generations are read only once this inspection is funded.
            let retired_without_catalog =
                match persistence.retired_vector_rebuild_pending(partition) {
                    Ok(pending) => pending,
                    Err(Error::StoreCorrupted { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };
            let force_paged = force_paged || retired_without_catalog;
            install_paged_root_capture(persistence, &self.tx_mgr);
            let mut recorded = persistence.load_paged_build_record(partition)?;
            if recorded
                .as_ref()
                .map(|record| record.progress())
                .transpose()?
                .is_some_and(|progress| {
                    progress.last_source_key.as_deref() == Some(RESIDENT_EXPORT_MARKER)
                })
            {
                self.publish_direct_metadata(|| persistence.discard_paged_build(partition))?;
                self.vector.forget_partition_paged_build(partition);
                self.snapshot_registry.release_construction_floor(partition);
                self.vector.clear_partition_paged_build_progress(partition);
                recorded = None;
            }
            let applies = force_paged
                || recorded.is_some()
                || self.paged_base_build_applies(partition, persistence, copy_owner)?;
            (recorded, applies, force_paged)
        };
        if recorded.is_none()
            && !force_paged
            && (!base_build_applies || self.whole_build_fits(partition, persistence, copy_owner)?)
        {
            return Ok(None);
        }
        // Admission first: a budget that cannot fund one construction batch
        // refuses with the established typed generation refusal before any
        // durable build record, floor or registration exists.
        {
            let layout =
                contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
            let live = self
                .vector
                .try_partition_info(&partition.index, &partition.partition_key, copy_owner)?
                .map(|info| info.live_rows)
                .unwrap_or(0);
            let declaration = declaration_for(&layout, &layout.resolve_policy(live.max(1), 1))?;
            let batch = paged_batch_plan(&declaration)?.construction_workspace_bytes()?;
            drop(self.vector.reserve_workspace(
                Arc::clone(&self.finalization.vector_accountant),
                batch,
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before compacting this maintained route.",
            )?);
        }
        if recorded.is_none()
            && self
                .vector
                .partition_paged_build_progress(partition)
                .is_some()
        {
            // Erasure removed this partition's build durably; forget the
            // in-process registration, its floor and its reported progress.
            self.vector.forget_partition_paged_build(partition);
            self.snapshot_registry.release_construction_floor(partition);
            self.vector.clear_partition_paged_build_progress(partition);
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
                        &self.finalization.accountant,
                        &self.resource_closed,
                        &self.publication_interrupted,
                        partition,
                        &record,
                        copy_owner,
                    )?;
                }
                record
            }
            None => match self.start_paged_vector_build(
                partition,
                persistence,
                copy_owner,
                force_paged,
            )? {
                Some(record) => record,
                None => return Ok(None),
            },
        };
        // A changed declaration cannot finish this frontier.
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
        let progress = record.progress()?;
        let current = declaration_for(&layout, &layout.resolve_policy(progress.total_points, 1))?;
        if current != record.declaration
            || persistence.vector_schema_binding(&partition.index)? != record.schema_binding
        {
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
        let budget = self
            .finalization
            .accountant
            .usage()
            .limit
            .unwrap_or(usize::MAX);
        let batches = (budget / batch_bytes.saturating_mul(16).max(1))
            .clamp(PAGED_MIN_BATCHES_PER_CYCLE, PAGED_MAX_BATCHES_PER_CYCLE * 4);
        for _ in 0..batches {
            let progress = self
                .vector
                .partition_paged_build_progress(partition)
                .ok_or_else(|| Error::Other("paged vector build lost its registration".into()))?;
            match progress.phase {
                PagedVectorPhase::Inserting | PagedVectorPhase::Encoding => {
                    #[cfg(any(test, feature = "test-seams"))]
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
                    if record.storage_id & crate::persistence::vector_paged::MUTABLE_STORAGE != 0
                        && self.catch_up_native_import_candidate(
                            partition,
                            persistence,
                            &record,
                            &progress,
                            copy_owner,
                        )?
                    {
                        return Ok(Some(VectorPartitionPublicationOutcome::default()));
                    }
                    return self
                        .publish_paged_vector_build(
                            partition,
                            persistence,
                            &record,
                            &progress,
                            plan,
                            read_memory.clone(),
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
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        let Some(info) = self.vector.try_partition_info(
            &partition.index,
            &partition.partition_key,
            copy_owner,
        )?
        else {
            return Ok(false);
        };
        if info.live_rows == 0 {
            return Ok(false);
        }
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
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
            && !self.vector.partition_has_paged_storage(partition)
            && !topology_replacement;
        Ok(!self.whole_reader_serving_fits(partition, &info, &layout)?
            || !publish_change
            || !self.change_build_fits(partition, persistence, copy_owner)?)
    }

    /// Whether the resident whole build's complete construction workspace fits
    /// the writer's current headroom. Only a build that cannot be funded whole
    /// moves to finite paged batches.
    fn whole_build_fits(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        let Some(info) = self.vector.try_partition_info(
            &partition.index,
            &partition.partition_key,
            copy_owner,
        )?
        else {
            return Ok(true);
        };
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
        if self.whole_reader_serving_fits(partition, &info, &layout)?
            && let Some(plan) = self.change_build_plan(partition, persistence, copy_owner)?
        {
            return Ok(self.workspace_plan_fits(plan));
        }
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
        let mut needed = plan.complete_peak_bytes();
        if !self.captured_whole_reader_serving_fits(
            partition,
            info.live_rows,
            &layout,
            policy,
            None,
        )? {
            let declaration = declaration_for(&layout, &policy)?;
            let export_bytes = paged_batch_plan(&declaration)?.workspace_bytes()?;
            needed = needed.max(plan.resident_export_phase_bytes(export_bytes));
        }
        Ok(self
            .finalization
            .accountant
            .usage()
            .available
            .is_none_or(|available| available >= needed))
    }

    /// Price the same suffix and prior-base owners as resident publication.
    /// A base larger than free headroom must not make every suffix retry fail.
    fn change_build_plan(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<VectorGenerationWorkspacePlan>> {
        let Some(info) = self.vector.try_partition_info(
            &partition.index,
            &partition.partition_key,
            copy_owner,
        )?
        else {
            return Ok(None);
        };
        let Some((base, change)) = persistence.load_vector_partition_generation_descriptors_for(
            &partition.index,
            &partition.partition_key,
        )?
        else {
            return Ok(None);
        };
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
        let policy = layout.resolve_policy(info.live_rows, 1);
        let topology_replacement = info.graph_available
            && info.serving_policy.is_none_or(|serving| {
                serving.hnsw_m != policy.hnsw_m
                    || serving.hnsw_ef_construction != policy.hnsw_ef_construction
            });
        if change.is_some()
            || self.vector.partition_has_paged_storage(partition)
            || topology_replacement
        {
            return Ok(None);
        }
        let (raw_count, mut graph_count) = self.vector.partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            SnapshotId(self.tx_mgr.current_tx_max().0),
            Some(base.covered_lsn),
            copy_owner,
        )?;
        let delete_carriers = graph_count == 0;
        if delete_carriers {
            let mut identities = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                0,
                "vector",
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before counting delete-only graph carriers.",
            )?;
            let count = persistence.vector_partition_uncovered_tombstone_entry_count(
                partition,
                base.covered_lsn,
                self.tx_mgr.current_lsn(),
                |count| {
                    let needed = count.saturating_mul(128);
                    identities.try_grow_for(
                        needed.saturating_sub(identities.bytes()),
                        "vector",
                        "prepare_vector_generation",
                        "Raise MEMORY_LIMIT before counting delete-only graph carriers.",
                    )
                },
            )?;
            graph_count = if count == 1 { 2 } else { count };
        }
        let mut plan = VectorGenerationWorkspacePlan::new(
            if delete_carriers {
                raw_count
            } else {
                graph_count
            },
            graph_count,
            &layout,
            partition,
            policy,
            usize::try_from(base.durable_bytes)
                .map_err(|_| Error::Other("vector base byte count exceeds address space".into()))?,
        )?;
        plan.assign_raw_decode_owner(true, delete_carriers);
        if delete_carriers {
            plan.carrier_metadata_bytes = graph_count.saturating_mul(
                partition
                    .partition_key
                    .estimated_bytes()
                    .saturating_add(partition.index.table.len())
                    .saturating_add(partition.index.column.len())
                    .saturating_add(512),
            );
        }
        Ok(Some(plan))
    }

    pub(super) fn workspace_plan_fits(&self, plan: VectorGenerationWorkspacePlan) -> bool {
        let needed = plan.complete_peak_bytes();
        self.finalization
            .accountant
            .usage()
            .available
            .is_none_or(|available| available >= needed)
    }

    fn change_build_fits(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        Ok(self
            .change_build_plan(partition, persistence, copy_owner)?
            .is_none_or(|plan| self.workspace_plan_fits(plan)))
    }

    /// Persist a new frontier record and hold its floor, under the commit
    /// gate. Returns `None` when the existing sealed-change path owns this
    /// partition's next publication.
    pub(super) fn start_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        force_paged: bool,
    ) -> Result<Option<PagedBuildRecord>> {
        let schema_binding = persistence.vector_schema_binding(&partition.index)?;
        let Some(info) = self.vector.try_partition_info(
            &partition.index,
            &partition.partition_key,
            copy_owner,
        )?
        else {
            return Ok(None);
        };
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
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
            && !self.vector.partition_has_paged_storage(partition)
            && !topology_replacement;
        if publish_change
            && !force_paged
            && self.whole_reader_serving_fits(partition, &info, &layout)?
            && self.change_build_fits(partition, persistence, copy_owner)?
        {
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
        let highest = if current.is_none() && persistence.has_vector_repair_fence()? {
            persistence.highest_retired_vector_generation_key(partition)?
        } else {
            persistence.highest_vector_partition_generation_id(
                &partition.index,
                &partition.partition_key,
                |_| Ok(()),
            )?
        };
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
                    schema_binding,
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
            &self.finalization.accountant,
            &self.resource_closed,
            &self.publication_interrupted,
            partition,
            &record,
            copy_owner,
        )?;
        Ok(Some(record))
    }

    /// Export a funded whole build into unpublished native storage, then use
    /// the ordinary native publication and writer-root installation. The
    /// frontier/floor were captured by the caller before any body was copied.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn publish_resident_paged_generation(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        layout: &VectorIndexLayout,
        policy: contextdb_vector::store::ResolvedVectorPolicy,
        reason: PagedBuildReason,
        generation_id: u64,
        previous_generation_id: u64,
        covered_tx: TxId,
        covered_lsn: Lsn,
        schema_binding: crate::persistence::VectorSchemaBinding,
        graph: HnswIndex,
        entries: Vec<VectorEntry>,
        workspace: &mut contextdb_vector::VectorWorkspaceReservation,
        workspace_plan: VectorGenerationWorkspacePlan,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VectorPartitionPublicationOutcome> {
        let declaration = declaration_for(layout, &policy)?;
        let plan = paged_batch_plan(&declaration)?;
        workspace.resize(
            workspace_plan.resident_export_phase_bytes(plan.workspace_bytes()?),
            "prepare_vector_generation",
            "Free writer memory before exporting this graph.",
        )?;
        self.build_checkpoint()?;
        let row_count = entries
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .count();
        if row_count != graph.len() {
            return Err(Error::Other(
                "resident export row count differs from its graph".into(),
            ));
        }
        let mut rows = Vec::with_capacity(row_count);
        rows.extend(entries.iter().filter(|entry| entry.deleted_tx.is_none()));
        rows.sort_unstable_by_key(|entry| entry.row_id);
        if rows.windows(2).any(|pair| pair[0].row_id == pair[1].row_id) {
            return Err(Error::Other(
                "resident export has duplicate frontier rows".into(),
            ));
        }
        let record = self
            .tx_mgr
            .with_commit_lock(|| -> Result<Option<PagedBuildRecord>> {
                self.build_checkpoint()?;
                let current_layout = contextdb_vector::scoped_index_layout(
                    &self.vector,
                    &partition.index,
                    copy_owner,
                )?;
                if declaration_for(
                    &current_layout,
                    &current_layout.resolve_policy(rows.len(), 1),
                )? != declaration
                {
                    return Ok(None);
                }
                // The existing publication transaction rechecks predecessor and
                // empty-route/erasure validity; every export batch CASes this record.
                persistence
                    .begin_resident_paged_export(
                        partition,
                        declaration.clone(),
                        reason,
                        generation_id,
                        previous_generation_id,
                        covered_tx,
                        covered_lsn,
                        rows.len(),
                        schema_binding,
                    )
                    .map(Some)
            })?;
        let Some(record) = record else {
            return Ok(VectorPartitionPublicationOutcome::default());
        };
        let hooks = paged_build_hooks(
            &self.vector,
            &self.tx_mgr,
            &self.resource_closed,
            &self.publication_interrupted,
            partition,
            &record,
        )?;
        let native =
            NativePagedGeneration::new(Arc::clone(persistence), partition.clone(), &record, hooks);
        // Export streams one point at a time without navigation scratch.
        // A larger finite transaction amortizes commits; its admitted byte
        // workspace and the writer's fixed dirty-page cache stay unchanged.
        let export_plan = PagedVectorBatchPlan {
            max_points: 256,
            max_node_writes: 256usize.saturating_mul(1 + 2 * policy.hnsw_m * 16),
            ..plan
        };
        let mut progress = record.progress()?;
        while progress.phase != PagedVectorPhase::Complete {
            self.build_checkpoint()?;
            let next =
                native.commit_generation_batch(&progress, export_plan, &mut |transaction| {
                    self.build_checkpoint()?;
                    self.vector.export_resident_paged_batch(
                        &graph,
                        &rows,
                        &progress,
                        export_plan,
                        transaction,
                    )
                })?;
            progress = next;
            self.build_checkpoint()?;
        }
        drop(rows);
        drop(entries);
        drop(graph);
        drop(native);
        // There is no complete graph left while serving controls and candidate
        // copies are prepared. Keep metadata admitted until registration ends.
        workspace.resize(
            workspace_plan
                .durable_metadata_bytes
                .saturating_add(64 * 1024),
            "prepare_vector_generation",
            "Free writer memory before publishing this graph.",
        )?;
        let completed = persistence
            .load_paged_build_record(partition)?
            .ok_or_else(|| Error::Other("resident export lost its complete build record".into()))?;
        register_paged_build(
            &self.vector,
            &self.tx_mgr,
            persistence,
            &self.finalization.accountant,
            &self.resource_closed,
            &self.publication_interrupted,
            partition,
            &completed,
            copy_owner,
        )?;
        self.publish_paged_vector_build(
            partition,
            persistence,
            &completed,
            &progress,
            plan,
            read_memory,
        )
    }

    pub(super) fn publish_paged_vector_build(
        &self,
        partition: &VectorPartitionRef,
        persistence: &Arc<RedbPersistence>,
        record: &PagedBuildRecord,
        progress: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VectorPartitionPublicationOutcome> {
        let metadata_bytes = vector_route_metadata_workspace(partition)?
            .saturating_mul(8)
            .saturating_add(64 * 1024);
        let mut candidate_workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            metadata_bytes,
            "prepare_vector_generation",
            "Free writer memory before preparing paged publication metadata.",
        )?;
        // The build is complete: its last page write has committed, so this
        // root is final for the page set the publication names.
        let page_set = persistence.paged_storage_page_set_root(record.storage_id)?;
        let meta = if record.storage_id & crate::persistence::vector_paged::MUTABLE_STORAGE != 0 {
            persistence.native_base_meta_bytes(partition, record, progress, page_set)?
        } else {
            RedbPersistence::encode_paged_generation_meta(
                record.storage_id,
                &record.declaration,
                progress,
                record.schema_binding,
                page_set,
            )?
        };
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
        let prepared = persistence.prepare_vector_partition_generation_candidate_with_admission(
            &candidate,
            |scan_bytes| {
                candidate_workspace.resize(
                    metadata_bytes.saturating_add(scan_bytes),
                    "prepare_vector_generation",
                    "Free writer memory before preparing paged publication metadata.",
                )
            },
        )?;
        candidate_workspace.resize(
            metadata_bytes.saturating_add(prepared.stale_metadata_bytes()),
            "prepare_vector_generation",
            "Free writer memory before publishing paged metadata.",
        )?;
        self.admit_generation_disk(prepared.write_reservation_bytes())?;
        #[cfg(any(test, feature = "test-seams"))]
        let published_pending = Arc::new(parking_lot::Mutex::new(None));
        persistence.stage_paged_publication(
            partition,
            PagedPublication {
                prepared,
                previous_generation_id: record.previous_generation_id,
                #[cfg(any(test, feature = "test-seams"))]
                published_pending: Arc::clone(&published_pending),
            },
        );
        let mut checkpoint = || self.build_checkpoint();
        let mut publication_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let published = self.publish_direct_metadata(|| {
            self.vector
                .with_partition_generation_publication(partition, || -> Result<_> {
                    let current_layout = contextdb_vector::scoped_index_layout(
                        &self.vector,
                        &partition.index,
                        &mut publication_copy_owner,
                    )?;
                    if declaration_for(
                        &current_layout,
                        &current_layout.resolve_policy(progress.total_points, 1),
                    )? != record.declaration
                    {
                        return Ok(None);
                    }
                    self.vector
                        .publish_partition_paged_build_with_plan(
                            partition,
                            progress.frontier_lsn,
                            plan,
                            &mut checkpoint,
                        )
                        .map(Some)
                })
        });
        persistence.clear_paged_publication(partition);
        let Some(published) = published? else {
            // Abandon outside the commit/partition gates: it can wait for a
            // finite in-flight operation before acquiring the abandon CAS.
            self.vector
                .abandon_partition_paged_build(partition, &mut checkpoint)?;
            self.snapshot_registry.release_construction_floor(partition);
            self.vector.clear_partition_paged_build_progress(partition);
            return Ok(VectorPartitionPublicationOutcome::default());
        };
        if published.phase == PagedVectorPhase::Abandoned {
            self.snapshot_registry.release_construction_floor(partition);
            self.vector.clear_partition_paged_build_progress(partition);
            // No catalog was installed. Its journal and retained generation
            // must not be changed or reported as a successful publication.
            return Ok(VectorPartitionPublicationOutcome::default());
        }
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_commit_test_seams::reached(
            crate::writer_commit_test_seams::CommitBoundary::GenerationAfterRedb,
        );
        self.snapshot_registry.release_construction_floor(partition);
        self.vector.clear_partition_paged_build_progress(partition);
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(pending) = published_pending.lock().take() {
            self.vector.observe_durable_base_publication(
                partition,
                progress.generation_id,
                progress.frontier_tx,
                pending,
            );
        }
        let published_catalog = candidate.catalog;
        drop(candidate.base);
        let mut workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            RedbPersistence::maintenance_vector_catalog_heap_bytes(&published_catalog),
            "truncate_vector_partition_journal",
            "Raise MEMORY_LIMIT before cleaning this maintained route.",
        )?;
        let truncated = match self.publish_direct_metadata(|| {
            // Each covered record the cleanup decodes is admitted in its
            // writer workspace and dropped before the next is read; its
            // decode is charged to this writer for that one record, not
            // kept by a private reader at the shipped default.
            let decode = crate::read_image_memory::ImageMemory::operation_writer_per_call(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                0,
            )?;
            let _decode = decode.enter();
            persistence.truncate_vector_partition_journal_through_catalog_with_checkpoint(
                &published_catalog,
                &mut workspace,
                || self.build_checkpoint(),
            )
        }) {
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
            let cleanup_retry = self.retry_vector_journal_truncations(read_memory.clone())?;
            return Ok(VectorPartitionPublicationOutcome {
                published: true,
                cleanup_failure: cleanup_retry.first_failure_details,
            });
        }
        match self.install_published_vector_catalog(&published_catalog, read_memory) {
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
        // One exit guard starts before the first fallible read/admission.
        // An intentional UNKNOWN result retains its admitted progress; every
        // unexpected error/cancellation, including early reads, releases it.
        struct CursorExit<'a> {
            persistence: &'a crate::persistence::RedbPersistence,
            keep: bool,
        }
        impl Drop for CursorExit<'_> {
            fn drop(&mut self) {
                if !self.keep {
                    (*self.persistence).abandon_vector_repair_audit_cursor();
                }
            }
        }
        let mut cursor_exit = CursorExit {
            persistence,
            keep: false,
        };
        self.build_checkpoint()?;
        let work_pending = vector_source_page_admission::with_preparation_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || Ok(persistence.has_vector_repair_fence()? || persistence.has_paged_cleanup()?),
        )?;
        if !work_pending {
            return Ok(());
        }
        let has_fence = vector_source_page_admission::with_preparation_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || persistence.has_vector_repair_fence(),
        )?;
        let audit_scratch = if has_fence { 256 * 1024 } else { 0 };
        let mut workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            audit_scratch,
            "reclaim_paged_vector_generation",
            "Free writer memory before reclaiming retired vector pages.",
        )?;
        let attempt = (|| -> Result<_> {
            self.build_checkpoint()?;
            self.tx_mgr.with_maintenance_publication(|publication| {
                self.build_checkpoint()?;
                vector_source_page_admission::with_preparation_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || {
                        publication.reserve()?;
                        let status =
                            persistence.repair_orphan_audit_step(|| self.build_checkpoint())?;
                        publication.complete();
                        if persistence.has_paged_cleanup()? {
                            publication.reserve()?;
                            persistence.paged_cleanup_step(PAGED_CLEANUP_KEYS, |bytes| {
                        workspace.resize(audit_scratch + bytes, "reclaim_paged_vector_generation",
                            "Free writer memory before reclaiming retired vector pages.")
                    }, || self.build_checkpoint())?;
                            publication.complete();
                        }
                        Ok(status)
                    },
                )
            })
        })();
        let status = attempt?;
        cursor_exit.keep = true;
        if matches!(
            status,
            crate::persistence::vector_paged::RepairAuditStatus::Blocked
        ) {
            return Err(Error::Other("vector page ownership reclamation is stalled by an unauthenticated retained reference; inspect the damaged index".into()));
        }
        Ok(())
    }
    pub(super) fn prepare_vector_authoritative_erasure(&self) -> Result<()> {
        match self.reclaim_paged_vector_storage() {
            Ok(()) => {}
            Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                return Err(error);
            }
            Err(error) => {
                if !self
                    .persistence
                    .as_ref()
                    .is_some_and(|p| p.repair_orphan_audit_blocked())
                {
                    return Err(error);
                }
                // UNKNOWN ownership is still a durable erasure fence. The
                // existing typed fence refusal below is the PURGE answer.
            }
        }
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        vector_source_page_admission::with_preparation_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || {
                if persistence.has_vector_repair_fence()? || persistence.has_paged_cleanup()? {
                    return Err(crate::persistence::vector_paged::corruption(
                        "vector ownership proof or page cleanup is incomplete; run maintenance before authoritative erasure",
                    ));
                }
                Ok(())
            },
        )
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
