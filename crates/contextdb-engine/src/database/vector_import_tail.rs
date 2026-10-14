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

/// Share of the writer's effective memory limit one import cohort's decoded
/// vectors may take: the paged build's own batch share (one batch workspace
/// per sixteenth of the writer budget, `vector_paged_build.rs`). The
/// effective limit is the declared one, or, when none is declared, the
/// inherited process ceiling every writer admission already enforces. A
/// tight budget absorbs an import in more, smaller cohorts instead of
/// refusing it; an undeclared limit bounds cohorts exactly as a declared one
/// of that size would.
const IMPORT_COHORT_BUDGET_SHARE: usize = 16;

/// What the next cohort of a route's uncovered suffix, up to the import's
/// fixed target, can be.
enum CohortPlan {
    /// No uncovered commit at or before the target.
    Covered,
    /// The first uncovered commit alone exceeds the cohort share: a
    /// replacement build absorbs it in bounded batches.
    Rebuild,
    /// Whole committed LSNs, oldest first, through `through`, while they fit.
    Retain { vectors: usize, through: Lsn },
}

/// What one pass over a partition's uncovered suffix did.
enum ImportCohort {
    /// One committed cohort was absorbed and published; more may follow.
    Absorbed,
    /// Nothing is left to absorb on this route.
    Covered,
    /// The route cannot take a native tail now.
    Incomplete,
    /// The next commit is absorbed by a replacement build instead.
    RebuildRoute,
}

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

impl Database {
    /// Per route of `table.column`: the serving coverage's LSN and
    /// transaction, the commit transaction the commit index records at that
    /// LSN, and the inserts its maintained counters still hold pending at the
    /// coverage's transaction -- what a reader's captured suffix is admitted
    /// from.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn vector_serving_pending_inserts_for_test(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Vec<(Lsn, TxId, Option<TxId>, usize)>> {
        let _operation = self.assert_open_operation();
        let context = self.maintenance_context();
        let Some(persistence) = &context.persistence else {
            return Ok(Vec::new());
        };
        let _decode = context.writer_maintenance_decode()?;
        let mut copy_owner = contextdb_vector::VectorCopyAdmission::new(
            context.finalization.accountant.vector_budget()?,
        );
        let mut pending = Vec::new();
        for partition in vector_partition_routes(&context.vector, &mut copy_owner)? {
            if partition.index.table != table || partition.index.column != column {
                continue;
            }
            let Some(catalog) = persistence.load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            else {
                continue;
            };
            let serving = catalog.change.unwrap_or(catalog.base);
            let inserts = persistence.with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                RedbPersistence::vector_suffix_saved_inserts_in_read(
                    &read,
                    &partition,
                    serving.covered_tx,
                )
            })?;
            let commit_tx = self
                .tx_mgr
                .try_commit_index_floor(std::ops::Bound::Included(serving.covered_lsn))?
                .filter(|(lsn, _)| *lsn == serving.covered_lsn)
                .map(|(_, tx)| tx);
            pending.push((serving.covered_lsn, serving.covered_tx, commit_tx, inserts));
        }
        Ok(pending)
    }
}

impl Database {
    /// Per route of `table.column` with a durable replacement build: its
    /// phase, inserted points and frontier LSN.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn vector_build_progress_for_test(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Vec<(PagedVectorPhase, usize, Lsn)>> {
        let _operation = self.assert_open_operation();
        let context = self.maintenance_context();
        let Some(persistence) = &context.persistence else {
            return Ok(Vec::new());
        };
        let _decode = context.writer_maintenance_decode()?;
        let mut copy_owner = contextdb_vector::VectorCopyAdmission::new(
            context.finalization.accountant.vector_budget()?,
        );
        let mut builds = Vec::new();
        for partition in vector_partition_routes(&context.vector, &mut copy_owner)? {
            if partition.index.table != table || partition.index.column != column {
                continue;
            }
            if let Some(record) = persistence.load_paged_build_record(&partition)? {
                let progress = record.progress()?;
                builds.push((progress.phase, progress.graph.points, progress.frontier_lsn));
            }
        }
        Ok(builds)
    }

    /// Per route of `table.column`: the maintained pending counters as
    /// `(covered_tx, inserts, tombstones)`, serving and replacement candidate.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub fn vector_pending_counters_for_test(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Vec<((TxId, u64, u64), Option<(TxId, u64, u64)>)>> {
        let _operation = self.assert_open_operation();
        let context = self.maintenance_context();
        let Some(persistence) = &context.persistence else {
            return Ok(Vec::new());
        };
        let _decode = context.writer_maintenance_decode()?;
        let mut copy_owner = contextdb_vector::VectorCopyAdmission::new(
            context.finalization.accountant.vector_budget()?,
        );
        let mut counters = Vec::new();
        for partition in vector_partition_routes(&context.vector, &mut copy_owner)? {
            if partition.index.table == table && partition.index.column == column {
                counters.push(persistence.vector_pending_counters_for_test(&partition)?);
            }
        }
        Ok(counters)
    }
}

impl MaintenanceContext {
    /// Import-tail work is writer maintenance, so every stored value it
    /// decodes -- partition metadata, catalogs, counts, the uncovered suffix
    /// and the publication that follows -- is funded by this writer's
    /// declared memory limit. Without an active owner, persistence opens a
    /// private reader at the shipped default reader memory and refuses any
    /// suffix whose vectors exceed it, whatever the consumer declared.
    fn writer_maintenance_decode(&self) -> Result<crate::read_image_memory::ImageMemoryScope> {
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        )
    }

    /// Load one cohort's stored vectors. The vectors it keeps are already
    /// admitted in this cohort's writer workspace (`kept_vector_bytes` of
    /// it is their decoded f32 bodies), so the decode owner adopts that
    /// credit for each kept vector instead of charging it again. Everything
    /// else a stored value's decode needs -- its record, names, decode
    /// control and any quantized scratch -- is charged to the declared writer
    /// limit by a per-call owner, live for that one value and returned when
    /// the next value's decode begins, never accumulated across values.
    fn load_import_cohort(
        &self,
        site: &'static str,
        cohort_vectors: usize,
        cohort_through: Option<Lsn>,
        workspace_bytes: usize,
        kept_vector_bytes: usize,
        load: impl FnOnce() -> Result<(Vec<VectorEntry>, Lsn)>,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        let owner = crate::read_image_memory::ImageMemory::operation_writer_per_call(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            kept_vector_bytes,
        )?;
        #[cfg(feature = "test-seams")]
        let observed = std::sync::Arc::downgrade(&owner);
        let loaded = {
            let _decode = owner.enter();
            #[cfg(feature = "test-seams")]
            self.observe_import_cohort_load(
                site,
                crate::vector_observations::ImportTailLoadBoundaryForTest::Enter,
                cohort_vectors,
                cohort_through,
                workspace_bytes,
                Some(&owner),
            )?;
            load()
        };
        #[cfg(feature = "test-seams")]
        let adopted_output = owner.adopted_output();
        drop(owner);
        #[cfg(feature = "test-seams")]
        {
            let boundary = match &loaded {
                Ok((entries, _)) => {
                    crate::vector_observations::ImportTailLoadBoundaryForTest::Loaded {
                        vectors: entries.len(),
                        adopted_output,
                    }
                }
                Err(error) => crate::vector_observations::ImportTailLoadBoundaryForTest::Refused {
                    error: error.to_string(),
                },
            };
            let survivor = observed.upgrade();
            self.observe_import_cohort_load(
                site,
                boundary,
                cohort_vectors,
                cohort_through,
                workspace_bytes,
                survivor.as_ref(),
            )?;
        }
        #[cfg(not(feature = "test-seams"))]
        let _ = (site, cohort_vectors, cohort_through, workspace_bytes);
        loaded
    }

    #[cfg(feature = "test-seams")]
    fn observe_import_cohort_load(
        &self,
        site: &'static str,
        boundary: crate::vector_observations::ImportTailLoadBoundaryForTest,
        cohort_vectors: usize,
        cohort_through: Option<Lsn>,
        workspace_bytes: usize,
        owner: Option<&Arc<crate::read_image_memory::ImageMemory>>,
    ) -> Result<()> {
        if !crate::vector_observations::import_tail_load_observed() {
            return Ok(());
        }
        let usage = self.finalization.accountant.usage();
        let event = crate::vector_observations::ImportTailLoadForTest {
            site,
            boundary,
            writer_used: usage.used,
            writer_limit: usage.limit,
            cohort_vectors,
            cohort_through,
            workspace_bytes,
            decode_owner: owner.map_or(("", ""), |owner| owner.labels()),
            decode_owner_funded_by_writer: owner
                .is_some_and(|owner| owner.funded_by_writer(&self.finalization.accountant)),
            decode_owner_per_call: owner.is_some_and(|owner| owner.is_per_call()),
            decode_held: owner.map_or(0, |owner| owner.held_with_call()),
        };
        if let Some(limit) = crate::vector_observations::emit_import_tail_load_for_test(&event) {
            self.finalization.accountant.set_budget(limit)?;
        }
        Ok(())
    }

    /// Bytes of decoded vectors one import cohort may retain: a sixteenth
    /// of the declared writer limit (unlimited without one).
    fn import_cohort_share(&self) -> usize {
        let usage = self.finalization.accountant.usage();
        // Undeclared: the whole-process ceiling is this writer's effective
        // limit (an undeclared writer is not a member of the process
        // aggregate, so its own `available` does not carry that ceiling).
        usage
            .limit
            .unwrap_or_else(crate::memory_accounting::effective_process_ceiling)
            / IMPORT_COHORT_BUDGET_SHARE
    }

    /// The transaction the commit index records at exactly `lsn`.
    fn commit_tx_at(&self, lsn: Lsn) -> Result<TxId> {
        match self
            .tx_mgr
            .try_commit_index_floor(std::ops::Bound::Included(lsn))?
        {
            Some((found, tx)) if found == lsn => Ok(tx),
            _ => Err(Error::Other(
                "an import cohort's last commit is missing from the commit index".into(),
            )),
        }
    }

    /// The next cohort of a route's uncovered suffix after `covered`, never
    /// past the import's fixed `target`: whole committed LSNs, oldest first,
    /// while their decoded vectors fit this writer's cohort share.
    fn import_cohort_plan(
        &self,
        persistence: &RedbPersistence,
        partition: &VectorPartitionRef,
        covered: Lsn,
        target: Lsn,
        entry_bytes: usize,
    ) -> Result<CohortPlan> {
        let share = self.import_cohort_share();
        let fits = |vectors: usize| {
            vectors
                .checked_mul(entry_bytes)
                .is_some_and(|bytes| bytes <= share)
        };
        Ok(
            match persistence
                .vector_partition_journal_cohort_by_key(partition, covered, target, fits)?
            {
                None => CohortPlan::Covered,
                Some((0, _)) => CohortPlan::Rebuild,
                Some((vectors, through)) => CohortPlan::Retain { vectors, through },
            },
        )
    }

    /// Drive this route's replacement build toward the import's fixed
    /// target until a publication moves its coverage. Returns false when the
    /// route cannot advance now (quarantined, no build possible, no durable
    /// progress, or superseded twice): the import reports it incomplete.
    fn rebuild_route_to_target(
        &self,
        persistence: &Arc<RedbPersistence>,
        partition: &VectorPartitionRef,
        read_memory: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        target: (TxId, Lsn),
    ) -> Result<bool> {
        let mut superseded = false;
        loop {
            {
                // Every decode of this one step -- the build record before
                // and after it and the step's own inspection -- is charged
                // to a writer owner of the step alone, returned when the
                // step's records are dropped below. The import's own decode
                // owner never accumulates a charge per step.
                let step = crate::read_image_memory::ImageMemory::operation_writer(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                )?;
                let _step = step.enter();
                let before = persistence
                    .load_paged_build_record(partition)?
                    .map(|record| record.progress())
                    .transpose()?;
                let outcome = self.advance_paged_vector_build_toward(
                    partition,
                    copy_owner,
                    Arc::clone(read_memory),
                    target,
                )?;
                let Some(outcome) = outcome else {
                    return Ok(false);
                };
                if outcome.published {
                    return Ok(true);
                }
                let after = persistence
                    .load_paged_build_record(partition)?
                    .map(|record| record.progress())
                    .transpose()?;
                match (&before, &after) {
                    // Durable progress moved: source cursor, phase, encoding
                    // cursor or a catch-up frontier, each finite.
                    (_, Some(after)) if before.as_ref() != Some(after) => {}
                    // The build vanished without a publication: superseded.
                    (Some(_), None) if !superseded => superseded = true,
                    _ => return Ok(false),
                }
            }
            #[cfg(feature = "test-seams")]
            crate::vector_observations::emit_import_rebuild_step_for_test(
                crate::read_image_memory::active_owner().map_or(0, |owner| owner.held_with_call()),
            );
        }
    }

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
        // The import's fixed target: every commit at or before it is covered
        // before this call reports complete; later commits are never chased.
        // Its snapshot stays pinned so a build can source exactly it.
        let (target, _target_pin) = self.tx_mgr.with_commit_lock(|| {
            let target = (self.tx_mgr.current_tx_max(), self.tx_mgr.current_lsn());
            let pin = self.snapshot_registry.register(SnapshotId(target.0.0));
            Ok::<_, Error>((target, pin))
        })?;
        let routes = vector_partition_routes(&self.vector, &mut copy_owner)?;
        let mut complete = true;
        for partition in routes {
            // One committed cohort per pass, each published on its own, so a
            // suffix larger than this writer's cohort share is absorbed in
            // finite steps instead of refused.
            loop {
                let _decode = self.writer_maintenance_decode()?;
                match self.persist_import_vector_tail_cohort(
                    persistence,
                    &partition,
                    &read_memory,
                    &mut copy_owner,
                    target,
                )? {
                    ImportCohort::Absorbed => {}
                    ImportCohort::RebuildRoute => {
                        if !self.rebuild_route_to_target(
                            persistence,
                            &partition,
                            &read_memory,
                            &mut copy_owner,
                            target,
                        )? {
                            complete = false;
                            break;
                        }
                    }
                    ImportCohort::Covered => break,
                    ImportCohort::Incomplete => {
                        complete = false;
                        break;
                    }
                }
            }
        }
        Ok(complete)
    }

    fn persist_import_vector_tail_cohort(
        &self,
        persistence: &Arc<RedbPersistence>,
        partition: &VectorPartitionRef,
        read_memory: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        target: (TxId, Lsn),
    ) -> Result<ImportCohort> {
        let Some(info) = self.vector.try_partition_info(
            &partition.index,
            &partition.partition_key,
            copy_owner,
        )?
        else {
            return Ok(ImportCohort::Covered);
        };
        if info.live_rows == 0 {
            return Ok(ImportCohort::Covered);
        }
        let Some(mut catalog) = persistence.load_vector_partition_generation_catalog_for(
            &partition.index,
            &partition.partition_key,
        )?
        else {
            return Ok(ImportCohort::Incomplete);
        };
        // Route before anything is prepared: a commit this writer cannot
        // retain whole goes to a replacement build, and no serving generation
        // that build would retire is copied first.
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector, &partition.index, copy_owner)?;
        let entry_bytes = layout
            .dimension
            .saturating_mul(4)
            .saturating_add(std::mem::size_of::<VectorEntry>())
            .saturating_add(256);
        match self.import_cohort_plan(
            persistence,
            partition,
            catalog.change.unwrap_or(catalog.base).covered_lsn,
            target.1,
            entry_bytes,
        )? {
            CohortPlan::Rebuild => return Ok(ImportCohort::RebuildRoute),
            // A covered route still adopts a saved change or prepares its
            // native base, exactly as before; only the rebuild is decided
            // first.
            CohortPlan::Covered | CohortPlan::Retain { .. } => {}
        }
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
                return Ok(ImportCohort::Incomplete);
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
                return Ok(ImportCohort::Incomplete);
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
        let CohortPlan::Retain {
            vectors: count,
            through: cohort_through,
        } = self.import_cohort_plan(persistence, partition, covered, target.1, entry_bytes)?
        else {
            // Nothing at or before the target is uncovered, or preparation
            // moved the coverage onto a commit only a rebuild can take.
            return Ok(
                match self.import_cohort_plan(
                    persistence,
                    partition,
                    covered,
                    target.1,
                    entry_bytes,
                )? {
                    CohortPlan::Rebuild => ImportCohort::RebuildRoute,
                    _ => ImportCohort::Covered,
                },
            );
        };
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
                return Ok(ImportCohort::Incomplete);
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
            .checked_mul(entry_bytes)
            .ok_or_else(|| Error::Other("import vector suffix size overflow".into()))?;
        let workspace_bytes = body_bytes.saturating_add(128 * 1024);
        let mut workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            workspace_bytes,
            "persist_import_vector_tail",
            "Release idle writer state before persisting this import's vector suffix.",
        )?;
        let ((mut entries, through_lsn), through_tx, _pin) =
            self.tx_mgr.with_commit_lock(|| {
                // Coverage names the commit the cohort ends at, never a
                // commit it did not absorb.
                let through_tx = self.commit_tx_at(cohort_through)?;
                let pin = self.snapshot_registry.register(SnapshotId(through_tx.0));
                let entries = self.load_import_cohort(
                    "persist_import_vector_tail",
                    count,
                    Some(cohort_through),
                    workspace_bytes,
                    count.saturating_mul(layout.dimension.saturating_mul(4)),
                    || {
                        persistence.load_vector_partition_tail_entries_through(
                            &partition.index,
                            &partition.partition_key,
                            covered,
                            Some(cohort_through),
                        )
                    },
                )?;
                Ok::<_, Error>((entries, through_tx, pin))
            })?;
        if entries.len() > count || through_lsn != cohort_through {
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
                partition,
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
        if published.change.map(|change| change.covered_lsn) <= Some(covered) {
            return Err(Error::Other(
                "import cohort publication did not advance its coverage".into(),
            ));
        }
        self.install_published_vector_catalog(&published, read_memory.clone())?;
        Ok(ImportCohort::Absorbed)
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
        catch_up_bound: Option<Lsn>,
    ) -> Result<bool> {
        let _decode = self.writer_maintenance_decode()?;
        use crate::persistence::vector_paged::import_tail::NativeImportCandidate;
        let Some(through_lsn) =
            persistence.native_candidate_first_uncovered_lsn(partition, progress.frontier_lsn)?
        else {
            return Ok(false);
        };
        // A commit after the caller's fixed target is not this call's to
        // absorb; the complete candidate publishes at its truthful frontier.
        if catch_up_bound.is_some_and(|bound| through_lsn > bound) {
            return Ok(false);
        }
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
        // A cohort larger than this writer's cohort share is never retained
        // whole: the candidate publishes at its truthful frontier and a fresh
        // replacement absorbs that commit in bounded batches.
        if bytes > self.import_cohort_share() {
            return Ok(false);
        }
        let workspace_bytes = bytes.saturating_add(128 * 1024);
        let _workspace = self.vector.reserve_workspace(
            Arc::clone(&self.finalization.vector_accountant),
            workspace_bytes,
            "catch_up_native_import_candidate",
            "Release idle writer state before advancing this finite cohort.",
        )?;
        let (mut entries, frontier_tx, _pin) = self.tx_mgr.with_commit_lock(|| {
            // The candidate's coverage names the absorbed commit, not the
            // store's newest transaction.
            let tx = self.commit_tx_at(through_lsn)?;
            let pin = self.snapshot_registry.register(SnapshotId(tx.0));
            let (entries, loaded_through) = self.load_import_cohort(
                "catch_up_native_import_candidate",
                count,
                Some(through_lsn),
                workspace_bytes,
                count.saturating_mul(layout.dimension.saturating_mul(4)),
                || {
                    persistence.load_vector_partition_tail_entries_through(
                        &partition.index,
                        &partition.partition_key,
                        progress.frontier_lsn,
                        Some(through_lsn),
                    )
                },
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

#[cfg(all(test, feature = "test-seams"))]
mod undeclared_limit_tests {
    use crate::{Database, DatabaseOpenOptions, MaintenancePolicy};
    use contextdb_core::{Value, Wallclock};
    use std::collections::HashMap;

    const DIMENSION: usize = 384;
    const BASE_ROWS: i64 = 16;
    const COMMITS: i64 = 64;
    const PER_COMMIT: i64 = 1024;
    /// The child's whole-process ceiling: the undeclared writer's fixed
    /// 64 MiB page cache plus 64 MiB, below the suffix's decoded vectors
    /// (65,536 x 1,536 B = 96 MiB) held as one cohort, far above a cohort
    /// bounded by a sixteenth of it.
    const PROCESS_CEILING: usize = 128 * 1024 * 1024;
    const CHILD_PATH: &str = "S7_UNDECLARED_LIMIT_PATH";

    fn options() -> DatabaseOpenOptions {
        // No writer memory limit is declared. The reader is roomy: opening
        // a store with this history is not what this test is about.
        let mut options = DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        };
        options.owner_reads.limits.limits.memory = 1 << 30;
        options
    }

    fn vector_for(id: i64) -> Vec<f32> {
        let mut values: Vec<f32> = (0..DIMENSION)
            .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.25)
            .collect();
        values[(id as usize) % DIMENSION] += 4.0;
        values
    }

    fn pending(db: &Database) -> usize {
        let routes = db
            .vector_serving_pending_inserts_for_test("points", "embedding")
            .unwrap();
        assert_eq!(routes.len(), 1, "{routes:?}");
        routes[0].3
    }

    #[test]
    fn an_undeclared_writer_limit_bounds_import_cohorts_by_the_process_ceiling() {
        let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
        if let Ok(path) = std::env::var(CHILD_PATH) {
            crate::memory_accounting::set_process_ceiling_for_test(Some(PROCESS_CEILING));
            let db = Database::open_with_options(&path, options()).unwrap();
            assert_eq!(db.memory_limit(), None, "no writer limit is declared");
            // Each serving-tail cohort's admitted vectors, as it enters.
            let cohorts = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let sink = cohorts.clone();
            let observation = crate::vector_observations::observe_import_tail_loads_for_test(
                std::sync::Arc::new(move |event| {
                    if event.site == "persist_import_vector_tail"
                        && matches!(
                            event.boundary,
                            crate::vector_observations::ImportTailLoadBoundaryForTest::Enter
                        )
                    {
                        sink.lock().push(event.cohort_vectors);
                    }
                    None
                }),
            );
            assert_eq!(
                db.persist_import_vector_tails().unwrap(),
                true,
                "the suffix is absorbed in cohorts bounded by the process ceiling"
            );
            drop(observation);
            let cohorts = std::mem::take(&mut *cohorts.lock());
            let high_water = db.write_memory_high_water_for_test();
            eprintln!(
                "undeclared limit: {} cohorts, largest {:?} vectors, writer high water {high_water}",
                cohorts.len(),
                cohorts.iter().max()
            );
            assert!(cohorts.len() > 1, "the suffix is split: {cohorts:?}");
            assert!(
                cohorts
                    .iter()
                    .all(|vectors| vectors * DIMENSION * 4 <= PROCESS_CEILING / 16),
                "no cohort's decoded vectors exceed a sixteenth of the effective limit: {cohorts:?}"
            );
            assert!(
                high_water <= PROCESS_CEILING,
                "the writer stays within the process ceiling: {high_water}"
            );
            assert_eq!(pending(&db), 0);
            db.close().unwrap();
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("undeclared.db");
        {
            let db = Database::open_with_options(&path, options()).unwrap();
            db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32) CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
            for id in 0..BASE_ROWS {
                db.execute(
                    "INSERT INTO points VALUES ($id, 1, $v)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vector_for(id))),
                    ]),
                )
                .unwrap();
            }
            while db
                .run_maintenance_cycle()
                .unwrap()
                .vector
                .remaining_partitions
                != 0
            {}
            for commit in 0..COMMITS {
                let first = 1_000_000 + commit * PER_COMMIT;
                let mut sql = String::from("INSERT INTO points VALUES ");
                let mut params = HashMap::new();
                for row in 0..PER_COMMIT {
                    if row > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format!("($i{row}, 1, $v{row})"));
                    params.insert(format!("i{row}"), Value::Int64(first + row));
                    params.insert(format!("v{row}"), Value::Vector(vector_for(first + row)));
                }
                db.execute(&sql, &params).unwrap();
            }
            assert_eq!(pending(&db), (COMMITS * PER_COMMIT) as usize);
            db.close().unwrap();
        }
        let test = concat!(
            module_path!(),
            "::an_undeclared_writer_limit_bounds_import_cohorts_by_the_process_ceiling"
        )
        .trim_start_matches("contextdb_engine::");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(test)
            .args(["--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_PATH, &path)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "child failed:\n{}\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
        let db = Database::open_with_options(&path, options()).unwrap();
        assert_eq!(pending(&db), 0, "the whole suffix is covered");
        let nearest = db
            .execute(
                "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
                &HashMap::from([("q".into(), Value::Vector(vector_for(1_000_000 + 40_000)))]),
            )
            .unwrap()
            .rows;
        assert_eq!(nearest, vec![vec![Value::Int64(1_000_000 + 40_000)]]);
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod build_record_refusal_tests {
    use crate::memory_accounting::MemoryAccountant;
    use crate::read_image_memory::ImageMemory;
    use crate::{Database, DatabaseOpenOptions, MaintenancePolicy};
    use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey, Wallclock};
    use contextdb_vector::VectorPartitionRef;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const DIMENSION: usize = 384;

    fn options(limit: usize) -> DatabaseOpenOptions {
        let mut options = DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(limit),
            ..DatabaseOpenOptions::default()
        };
        options.owner_reads.limits.limits.memory = 1 << 30;
        options
    }

    fn vector_for(id: i64) -> Vec<f32> {
        let mut values: Vec<f32> = (0..DIMENSION)
            .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.25)
            .collect();
        values[(id as usize) % DIMENSION] += 4.0;
        values
    }

    /// One decode under a writer owner that has no headroom at all.
    fn refused<T: std::fmt::Debug>(decode: impl FnOnce() -> contextdb_core::Result<T>) -> Error {
        let tiny: crate::memory_accounting::AccountantHandle =
            Arc::new(MemoryAccountant::with_budget(1)).into();
        let owner =
            ImageMemory::charging_for(tiny.clone(), "writer_source", "decode_stored_value", "test");
        let error = {
            let _scope = owner.enter();
            decode().expect_err("a decode with no headroom refuses")
        };
        assert_eq!(owner.held(), 0, "a refused decode returns its charge");
        drop(owner);
        assert_eq!(tiny.usage().used, 0, "nothing stays charged");
        error
    }

    #[test]
    fn a_refused_build_record_decode_is_the_typed_refusal_and_leaves_the_build_intact() {
        let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("build-record-refusal.db");
        {
            let db = Database::open_with_options(&path, options(512 * 1024 * 1024)).unwrap();
            db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32) CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
            for id in 0..16 {
                db.execute(
                    "INSERT INTO points VALUES ($id, 1, $v)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vector_for(id))),
                    ]),
                )
                .unwrap();
            }
            while db
                .run_maintenance_cycle()
                .unwrap()
                .vector
                .remaining_partitions
                != 0
            {}
            // One commit larger than the 16 MiB writer's cohort share.
            let mut sql = String::from("INSERT INTO points VALUES ");
            let mut params = HashMap::new();
            for row in 0..2048i64 {
                if row > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("($i{row}, 1, $v{row})"));
                params.insert(format!("i{row}"), Value::Int64(1_000_000 + row));
                params.insert(
                    format!("v{row}"),
                    Value::Vector(vector_for(1_000_000 + row)),
                );
            }
            db.execute(&sql, &params).unwrap();
            db.close().unwrap();
        }
        let db = Arc::new(Database::open_with_options(&path, options(16 * 1024 * 1024)).unwrap());
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("points", "embedding"),
            VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap(),
        );
        // Between two rebuild steps a healthy durable build exists.
        let checked = Arc::new(AtomicBool::new(false));
        let observed = (Arc::clone(&db), partition.clone(), Arc::clone(&checked));
        let _steps = crate::vector_observations::observe_import_rebuild_steps_for_test(Arc::new(
            move |_| {
                let (db, partition, checked) = &observed;
                if checked.swap(true, Ordering::SeqCst) {
                    return;
                }
                let persistence = db.persistence.as_ref().unwrap();
                let roomy = || {
                    let owner = ImageMemory::operation_writer(
                        crate::memory_accounting::AccountantHandle::clone(
                            &db.finalization.accountant,
                        ),
                    )
                    .unwrap();
                    let _scope = owner.enter();
                    persistence
                        .load_paged_build_record(partition)
                        .unwrap()
                        .expect("a durable build exists between steps")
                        .progress()
                        .unwrap()
                };
                let before = roomy();
                for error in [
                    refused(|| persistence.load_paged_build_record(partition)),
                    refused(|| persistence.next_paged_build_record_after(None)),
                ] {
                    assert!(
                        matches!(error, Error::MemoryBudgetExceeded { .. }),
                        "a valid record's refused decode is the typed refusal, never corruption: {error:?}"
                    );
                }
                assert_eq!(roomy(), before, "the durable progress is unchanged");
            },
        ));
        assert!(
            db.persist_import_vector_tails().unwrap(),
            "the retry with headroom completes"
        );
        assert!(checked.load(Ordering::SeqCst), "the refusal was exercised");
        assert!(
            db.persistence
                .as_ref()
                .unwrap()
                .load_paged_build_record(&partition)
                .unwrap()
                .is_none(),
            "the build published"
        );
    }
}
