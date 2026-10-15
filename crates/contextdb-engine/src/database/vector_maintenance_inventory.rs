//! Admitted maintenance metadata is separate from passive startup inspection.
//! A cursor pins one committed catalog root; refused singleton bodies remain
//! deferred while navigation continues over the already admitted parent.

use super::*;
use crate::memory_accounting::OwnedMemoryReservation;
use contextdb_vector::VectorWorkspaceReservation;
use contextdb_vector::store::{IndexState, VectorPartitionSourceMaintenanceMetadata};

const PREPARE: &str = "prepare_vector_generation";
const CLEANUP: &str = "truncate_vector_partition_journal";
const HINT: &str = "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before inspecting this maintained route.";

pub(super) struct AdmittedMaintenanceCatalog {
    pub(super) catalog: VectorPartitionGenerationCatalogRecord,
    pub(super) workspace: VectorWorkspaceReservation,
    pub(super) pending_cleanup: bool,
    source: Option<usize>,
}

#[derive(Default)]
pub(super) struct MaintenanceMetadataFailures {
    pub(super) first: Option<VectorMaintenanceFailureDetails>,
}

pub(super) struct MaintenanceCatalogInventory {
    pub(super) complete: bool,
    pub(super) catalogs: Vec<AdmittedMaintenanceCatalog>,
    pub(super) routes: Vec<MaintenanceBuildCandidate>,
    pub(super) failures: MaintenanceMetadataFailures,
    _memory: OwnedMemoryReservation,
}

#[derive(Default)]
pub(super) struct FailedMaintenancePartitions {
    pub(super) inventory: Option<MaintenanceCatalogInventory>,
}

impl FailedMaintenancePartitions {
    pub(super) fn contains(&self, route: &VectorPartitionRef) -> bool {
        self.inventory.as_ref().is_some_and(|inventory| {
            inventory.catalogs.iter().any(|entry| {
                entry.catalog.index == route.index
                    && entry.catalog.partition_key == route.partition_key
            })
        })
    }
}

impl PartialEq<HashSet<VectorPartitionRef>> for FailedMaintenancePartitions {
    fn eq(&self, other: &HashSet<VectorPartitionRef>) -> bool {
        self.inventory
            .as_ref()
            .map_or(0, |inventory| inventory.catalogs.len())
            == other.len()
            && other.iter().all(|route| self.contains(route))
    }
}

impl std::fmt::Debug for FailedMaintenancePartitions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries = formatter.debug_set();
        if let Some(inventory) = &self.inventory {
            for entry in &inventory.catalogs {
                entries.entry(&(&entry.catalog.index, &entry.catalog.partition_key));
            }
        }
        entries.finish()
    }
}

pub(super) struct MaintenanceBuildCandidates {
    pub(super) candidates: Vec<MaintenanceBuildCandidate>,
    pub(super) failures: MaintenanceMetadataFailures,
    _memory: OwnedMemoryReservation,
}

pub(super) struct MaintenanceBuildCandidate {
    pub(super) partition: VectorPartitionRef,
    cold: bool,
    _workspace: VectorWorkspaceReservation,
}

/// Heap scratch used by stable sort in the pinned Rust 1.94 toolchain.
/// `core/slice/sort/stable/mod.rs` uses insertion sort through 20 items,
/// then a 4 KiB stack buffer or exactly this many heap slots. Its small-sort
/// scratch floor is 32 + 16 slots (`shared/smallsort.rs`). Recheck this bound
/// when changing rust-toolchain.toml; the sort's allocation is not a Rust API.
pub(super) fn candidate_sort_heap_bytes(len: usize) -> usize {
    let size = std::mem::size_of::<MaintenanceBuildCandidate>();
    if len <= 20 {
        return 0;
    }
    let slots = (len - len / 2).max(len.min(8_000_000 / size)).max(48);
    if slots <= 4096 / size {
        0
    } else {
        slots.saturating_mul(size)
    }
}

struct SourceObservation {
    index: usize,
    state: Arc<IndexState>,
    metadata: VectorPartitionSourceMaintenanceMetadata,
    failure: Option<VectorMaintenanceFailureDetails>,
    quarantine: Option<VectorRouteQuarantineReason>,
    failed: bool,
}

struct InventoryPhase {
    complete: bool,
    indexes: MaintenanceIndexes,
    sources: Vec<SourceObservation>,
    catalogs: Vec<AdmittedMaintenanceCatalog>,
    misplaced: Vec<(VectorPartitionRef, VectorWorkspaceReservation)>,
    failures: MaintenanceMetadataFailures,
    source_memory: OwnedMemoryReservation,
    memory: OwnedMemoryReservation,
}

struct MaintenanceIndexes {
    indexes: Vec<VectorIndexRef>,
    _memory: OwnedMemoryReservation,
}

impl MaintenanceContext {
    fn maintenance_indexes(&self) -> Result<MaintenanceIndexes> {
        let mut memory = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
        let mut indexes = Vec::new();
        let metadata = self.relational.table_meta.read();
        for (table, meta) in metadata.iter() {
            for column in &meta.columns {
                if !matches!(column.column_type, ColumnType::Vector(_)) {
                    continue;
                }
                memory.try_grow_for(
                    table
                        .len()
                        .saturating_add(column.name.len())
                        .saturating_add(std::mem::size_of::<VectorIndexRef>()),
                    "vector_maintenance",
                    PREPARE,
                    HINT,
                )?;
                indexes.reserve_exact(1);
                let index = VectorIndexRef::new(table.clone(), column.name.clone());
                if self.vector.contains_index(&index) {
                    indexes.push(index);
                }
            }
        }
        Ok(MaintenanceIndexes {
            indexes,
            _memory: memory,
        })
    }

    fn maintenance_workspace(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<VectorWorkspaceReservation> {
        self.vector
            .reserve_workspace(self.accountant.clone(), bytes, operation, HINT)
    }

    fn admit_inventory_slot<T>(
        &self,
        _slot: &'static str,
        memory: &mut OwnedMemoryReservation,
        slots: &mut Vec<T>,
    ) -> Result<()> {
        if slots.len() == slots.capacity() {
            #[cfg(all(test, feature = "test-seams"))]
            let _budget = vector_public_cycle_admission_tests::refuse_inventory_slot_for_test(
                &self.accountant,
                _slot,
            );
            memory.try_grow_for(
                std::mem::size_of::<T>(),
                "vector_maintenance",
                PREPARE,
                HINT,
            )?;
            slots.reserve_exact(1);
        }
        Ok(())
    }

    /// Caller holds the existing commit boundary. Strong state handles are
    /// consumed before it releases, so DDL cannot detach their directory owner.
    fn maintenance_inventory_phase_locked(&self) -> Result<InventoryPhase> {
        let indexes = match self.maintenance_indexes() {
            Ok(indexes) => indexes,
            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                return Ok(InventoryPhase {
                    complete: false,
                    indexes: MaintenanceIndexes {
                        indexes: Vec::new(),
                        _memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
                    },
                    sources: Vec::new(),
                    catalogs: Vec::new(),
                    misplaced: Vec::new(),
                    failures: MaintenanceMetadataFailures {
                        first: Some(VectorMaintenanceFailureDetails::from_error(&error)),
                    },
                    source_memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
                    memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
                });
            }
            Err(error) => return Err(error),
        };
        let mut phase = InventoryPhase {
            complete: true,
            indexes,
            sources: Vec::new(),
            catalogs: Vec::new(),
            misplaced: Vec::new(),
            failures: MaintenanceMetadataFailures::default(),
            source_memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
            memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
        };
        for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
            let sampled = self
                .vector
                .with_partition_sources(index, |view| -> Result<()> {
                    let count = view.len();
                    phase.source_memory.try_grow_for(
                        count.saturating_mul(std::mem::size_of::<SourceObservation>()),
                        "vector_maintenance",
                        PREPARE,
                        HINT,
                    )?;
                    phase.sources.reserve_exact(count);
                    view.visit(|_, state| -> Result<()> {
                        let metadata = state.source_maintenance_metadata();
                        phase.sources.push(SourceObservation {
                            index: index_position,
                            state: Arc::clone(state),
                            metadata,
                            failure: None,
                            quarantine: metadata.quarantine,
                            failed: false,
                        });
                        Ok(())
                    })
                });
            match sampled {
                Ok(()) => {}
                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                    return Ok(self.incomplete_inventory_phase(phase, &error));
                }
                Err(error) => return Err(error),
            }
        }
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(phase);
        };
        let scanned = vector_source_page_admission::with_preparation_source_page_admission(
            Arc::clone(&self.accountant),
            || {
                persistence.scan_vector_catalog_for_maintenance(|handle| {
                    self.build_checkpoint()?;
                    let mut physical_source = None;
                    let mut pending_workspace = None;
                    let mut pending_refusal = None;
                    for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
                        self.vector
                            .with_partition_sources(index, |view| -> Result<()> {
                                view.visit(|key, state| -> Result<()> {
                                    if physical_source.is_some() {
                                        return Ok(());
                                    }
                                    let mut frames =
                                        OwnedMemoryReservation::new(Arc::clone(&self.accountant));
                                    frames.try_grow_for(
                                        RedbPersistence::maintenance_catalog_key_frame_bytes(key),
                                        "vector_maintenance",
                                        PREPARE,
                                        HINT,
                                    )?;
                                    if !RedbPersistence::maintenance_catalog_key_matches(
                                        &handle, index, key,
                                    )? {
                                        return Ok(());
                                    }
                                    let position = phase
                                        .sources
                                        .iter()
                                        .position(|source| {
                                            source.index == index_position
                                                && Arc::ptr_eq(&source.state, state)
                                        })
                                        .expect("commit boundary preserves the sampled source");
                                    physical_source = Some(position);
                                    let pending = phase.sources[position]
                                        .metadata
                                        .failure
                                        .is_some_and(|failure| {
                                            failure.operation == CLEANUP
                                                && failure.message.contains("durable")
                                        });
                                    if pending {
                                        match self.maintenance_workspace(
                                            RedbPersistence::vector_journal_cleanup_key_reservation(
                                                index, key,
                                            ),
                                            CLEANUP,
                                        ) {
                                            Ok(workspace) => pending_workspace = Some(workspace),
                                            Err(error) => pending_refusal = Some(error),
                                        }
                                    }
                                    Ok(())
                                })
                            })?;
                        if physical_source.is_some() {
                            break;
                        }
                    }
                    if let Some(error) = pending_refusal {
                        Self::record_inventory_refusal(&mut phase, physical_source, &error, true);
                        return Ok(());
                    }
                    let materialized = handle.materialize().map_err(RedbPersistence::storage_error);
                    let (_key, value) = match materialized {
                        Ok(entry) => entry,
                        Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                            Self::record_inventory_refusal(
                                &mut phase,
                                physical_source,
                                &error,
                                pending_workspace.is_some(),
                            );
                            return Ok(());
                        }
                        Err(error) => return Err(error),
                    };
                    let pending = pending_workspace.is_some();
                    let mut workspace = match pending_workspace {
                        Some(workspace) => workspace,
                        None => self.maintenance_workspace(0, PREPARE)?,
                    };
                    // The pending reservation continues to own its existing key
                    // bound. It is resized, never duplicated, after authentication.
                    let key_bound = if pending {
                        let source = &phase.sources[physical_source.expect("pending source")];
                        self.vector.with_partition_sources(
                            &phase.indexes.indexes[source.index],
                            |view| -> Result<usize> {
                                let mut bound = 0;
                                view.visit(|route_key, state| -> Result<()> {
                                    if Arc::ptr_eq(state, &source.state) {
                                        bound =
                                            RedbPersistence::vector_journal_cleanup_key_reservation(
                                                &phase.indexes.indexes[source.index],
                                                route_key,
                                            );
                                    }
                                    Ok(())
                                })?;
                                Ok(bound)
                            },
                        )?
                    } else {
                        0
                    };
                    if let Err(error) = workspace.resize(
                        key_bound.saturating_add(
                            RedbPersistence::maintenance_vector_catalog_decode_bound(
                                value.value().len(),
                            ),
                        ),
                        if pending { CLEANUP } else { PREPARE },
                        HINT,
                    ) {
                        Self::record_inventory_refusal(
                            &mut phase,
                            physical_source,
                            &error,
                            pending,
                        );
                        return Ok(());
                    }
                    let catalog = match persistence.decode_maintenance_vector_catalog(value.value())
                    {
                        Ok(catalog) => catalog,
                        Err(Error::StoreCorrupted { .. }) => {
                            if let Some(position) = physical_source {
                                let reason =
                                    RedbPersistence::maintenance_vector_catalog_fault_reason(
                                        value.value(),
                                    );
                                Self::record_inventory_quarantine(
                                    &mut phase.sources[position],
                                    reason,
                                );
                            }
                            return Ok(());
                        }
                        Err(error) => return Err(error),
                    };
                    // The authenticated route is the sole payload attribution
                    // authority. Reject-only windows supply only physical equality.
                    let mut frames = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
                    frames.try_grow_for(
                        RedbPersistence::maintenance_catalog_key_frame_bytes(
                            &catalog.partition_key,
                        ),
                        "vector_maintenance",
                        PREPARE,
                        HINT,
                    )?;
                    let correct_key = RedbPersistence::maintenance_catalog_key_matches(
                        &handle,
                        &catalog.index,
                        &catalog.partition_key,
                    )?;
                    drop(frames);
                    workspace.resize(
                        key_bound.saturating_add(
                            RedbPersistence::maintenance_vector_catalog_heap_bytes(&catalog),
                        ),
                        if pending { CLEANUP } else { PREPARE },
                        HINT,
                    )?;
                    if !correct_key {
                        if let Some(position) = physical_source {
                            Self::record_inventory_quarantine(
                                &mut phase.sources[position],
                                VectorRouteQuarantineReason::CorruptBase,
                            );
                        } else {
                            self.admit_inventory_slot(
                                "misplaced",
                                &mut phase.memory,
                                &mut phase.misplaced,
                            )?;
                            phase.misplaced.push((
                                VectorPartitionRef::new(catalog.index, catalog.partition_key),
                                workspace,
                            ));
                        }
                    } else {
                        self.admit_inventory_slot(
                            "catalogs",
                            &mut phase.memory,
                            &mut phase.catalogs,
                        )?;
                        phase.catalogs.push(AdmittedMaintenanceCatalog {
                            catalog,
                            workspace,
                            pending_cleanup: pending,
                            source: physical_source,
                        });
                    }
                    Ok(())
                })
            },
        );
        match scanned {
            Ok(()) => {}
            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                // Navigation cannot resume without its indispensable owner.
                // End the whole sample, retaining only a truthful refusal.
                return Ok(self.incomplete_inventory_phase(phase, &error));
            }
            Err(error) => return Err(error),
        }
        // Resolve canonical-route quarantine from valid miskeyed payloads,
        // including those encountered after a canonical valid catalog.
        for (route, _) in &phase.misplaced {
            for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
                if *index != route.index {
                    continue;
                }
                self.vector
                    .with_partition_sources(index, |view| -> Result<()> {
                        view.visit(|key, state| -> Result<()> {
                            if *key == route.partition_key
                                && let Some(source) = phase.sources.iter_mut().find(|source| {
                                    source.index == index_position
                                        && Arc::ptr_eq(&source.state, state)
                                })
                            {
                                Self::record_inventory_quarantine(
                                    source,
                                    VectorRouteQuarantineReason::CorruptBase,
                                );
                            }
                            Ok(())
                        })
                    })?;
            }
        }
        phase.catalogs.retain(|entry| {
            entry
                .source
                .is_none_or(|position| phase.sources[position].quarantine.is_none())
                && !phase.misplaced.iter().any(|(route, _)| {
                    route.index == entry.catalog.index
                        && route.partition_key == entry.catalog.partition_key
                })
        });
        Ok(phase)
    }

    fn incomplete_inventory_phase(&self, phase: InventoryPhase, error: &Error) -> InventoryPhase {
        let mut failures = phase.failures;
        failures
            .first
            .get_or_insert_with(|| VectorMaintenanceFailureDetails::from_error(error));
        InventoryPhase {
            complete: false,
            indexes: MaintenanceIndexes {
                indexes: Vec::new(),
                _memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
            },
            sources: Vec::new(),
            catalogs: Vec::new(),
            misplaced: Vec::new(),
            failures,
            source_memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
            memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
        }
    }

    fn record_inventory_quarantine(
        source: &mut SourceObservation,
        reason: VectorRouteQuarantineReason,
    ) {
        if source.quarantine != Some(VectorRouteQuarantineReason::IncompatibleFormat)
            || reason == VectorRouteQuarantineReason::IncompatibleFormat
        {
            source.quarantine = Some(reason);
        }
    }

    fn record_inventory_refusal(
        phase: &mut InventoryPhase,
        source: Option<usize>,
        error: &Error,
        pending: bool,
    ) {
        let details = if pending {
            VectorMaintenanceFailureDetails::from_published_cleanup_error(error)
        } else {
            VectorMaintenanceFailureDetails::from_error(error)
        };
        phase.failures.first.get_or_insert(details);
        if let Some(position) = source {
            phase.sources[position].failure = Some(details);
            phase.sources[position].failed = true;
        }
    }

    fn finish_inventory_phase_locked(&self, phase: InventoryPhase) -> MaintenanceCatalogInventory {
        // Every source view has returned before the callback-free adapter takes
        // maintenance/registry guards. No retained state escapes this boundary.
        for source in &phase.sources {
            self.vector.apply_partition_source_maintenance_metadata(
                &phase.indexes.indexes[source.index],
                &source.state,
                source.failure,
                source.quarantine,
            );
        }
        let InventoryPhase {
            complete,
            catalogs,
            failures,
            memory,
            ..
        } = phase;
        MaintenanceCatalogInventory {
            complete,
            catalogs,
            routes: Vec::new(),
            failures,
            _memory: memory,
        }
    }

    pub(super) fn maintenance_catalog_inventory_locked(
        &self,
    ) -> Result<MaintenanceCatalogInventory> {
        let phase = self.maintenance_inventory_phase_locked()?;
        Ok(self.finish_inventory_phase_locked(phase))
    }

    pub(super) fn maintenance_catalog_inventory(&self) -> Result<MaintenanceCatalogInventory> {
        self.tx_mgr
            .with_commit_lock(|| self.maintenance_catalog_inventory_locked())
    }

    pub(super) fn record_catalog_metadata_failure(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
        details: VectorMaintenanceFailureDetails,
    ) {
        self.tx_mgr
            .with_commit_lock(|| self.record_catalog_metadata_failure_locked(catalog, details));
    }

    pub(super) fn record_catalog_metadata_failure_locked(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
        details: VectorMaintenanceFailureDetails,
    ) {
        if let Some(state) = self
            .vector
            .try_partition_state(&catalog.index, &catalog.partition_key)
        {
            self.vector.apply_partition_source_maintenance_metadata(
                &catalog.index,
                &state,
                Some(details),
                None,
            );
        }
    }

    pub(super) fn maintenance_closing_inventory_locked(
        &self,
    ) -> Result<MaintenanceCatalogInventory> {
        let mut phase = self.maintenance_inventory_phase_locked()?;
        if !phase.complete {
            return Ok(self.finish_inventory_phase_locked(phase));
        }
        let mut routes = Vec::new();
        let sampled = (|| -> Result<()> {
            for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
                self.vector
                    .with_partition_sources(index, |view| -> Result<()> {
                        view.visit(|key, state| -> Result<()> {
                            let position = phase
                                .sources
                                .iter()
                                .position(|source| {
                                    source.index == index_position
                                        && Arc::ptr_eq(&source.state, state)
                                })
                                .expect("sampled source");
                            if phase.sources[position].failed
                                || phase.sources[position].metadata.historical_only
                            {
                                return Ok(());
                            }
                            let heap = key
                                .estimated_bytes()
                                .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len());
                            let workspace = match self.maintenance_workspace(heap, PREPARE) {
                                Ok(workspace) => workspace,
                                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                                    let details =
                                        VectorMaintenanceFailureDetails::from_error(&error);
                                    phase.failures.first.get_or_insert(details);
                                    phase.sources[position].failure = Some(details);
                                    phase.sources[position].failed = true;
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            };
                            self.admit_inventory_slot(
                                "closing_routes",
                                &mut phase.memory,
                                &mut routes,
                            )?;
                            routes.push(MaintenanceBuildCandidate {
                                partition: VectorPartitionRef::new(index.clone(), key.clone()),
                                cold: false,
                                _workspace: workspace,
                            });
                            Ok(())
                        })
                    })?;
            }
            Ok(())
        })();
        match sampled {
            Ok(()) => {}
            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                drop(routes);
                return Ok(self.finish_inventory_phase_locked(
                    self.incomplete_inventory_phase(phase, &error),
                ));
            }
            Err(error) => return Err(error),
        }
        let mut inventory = self.finish_inventory_phase_locked(phase);
        inventory.routes = routes;
        Ok(inventory)
    }

    pub(super) fn reclaim_catalogs_locked(
        &self,
        inventory: &mut MaintenanceCatalogInventory,
    ) -> Result<usize> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(0);
        };
        let mut reclaimed = 0usize;
        for entry in &mut inventory.catalogs {
            let catalog = &entry.catalog;
            let heap = RedbPersistence::maintenance_vector_catalog_heap_bytes(catalog);
            let mut route_memory = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
            let result = route_memory
                .try_grow_for(heap, "vector_maintenance", PREPARE, HINT)
                .and_then(|()| {
                    let partition = VectorPartitionRef::new(
                        catalog.index.clone(),
                        catalog.partition_key.clone(),
                    );
                    let retained_count = self
                        .vector
                        .partition_graph_generation_status(&partition)
                        .map_or(0, |status| status.retained_generations);
                    route_memory.try_grow_for(
                        retained_count.saturating_mul(std::mem::size_of::<(u64, Option<u64>)>()),
                        "vector_maintenance",
                        PREPARE,
                        HINT,
                    )?;
                    let retained = self.vector.retained_partition_chain_ids(&partition);
                    entry.workspace.resize(
                        heap.saturating_add(
                            RedbPersistence::vector_journal_cleanup_key_reservation(
                                &catalog.index,
                                &catalog.partition_key,
                            ),
                        ),
                        PREPARE,
                        HINT,
                    )?;
                    vector_source_page_admission::with_catalog_reclamation_source_page_admission(
                        Arc::clone(&self.accountant),
                        || {
                            persistence
                                .reclaim_superseded_vector_partition_generations(catalog, &retained)
                        },
                    )
                });
            match result {
                Ok((count, _)) => reclaimed = reclaimed.saturating_add(count),
                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                    let details = VectorMaintenanceFailureDetails::from_error(&error);
                    inventory.failures.first.get_or_insert(details);
                    self.record_catalog_metadata_failure_locked(catalog, details);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(reclaimed)
    }

    pub(super) fn maintenance_build_candidates(&self) -> Result<MaintenanceBuildCandidates> {
        self.tx_mgr.with_commit_lock(|| {
            let mut phase = self.maintenance_inventory_phase_locked()?;
            if !phase.complete {
                let inventory = self.finish_inventory_phase_locked(phase);
                return Ok(MaintenanceBuildCandidates {
                    candidates: Vec::new(),
                    failures: inventory.failures,
                    _memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
                });
            }
            let mut candidates = Vec::new();
            let mut memory = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
            let sampled = (|| -> Result<()> {
            for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
                let layout = self.vector.index_layout(index)?;
                let mut sampled_routes = Vec::new();
                let mut sample_memory = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
                self.vector
                    .with_partition_sources(index, |view| -> Result<()> {
                        view.visit(|key, state| -> Result<()> {
                            let source_position = phase
                                .sources
                                .iter()
                                .position(|source| {
                                    source.index == index_position
                                        && Arc::ptr_eq(&source.state, state)
                                })
                                .expect("sampled source");
                            if phase.sources[source_position].metadata.historical_only
                                || phase.sources[source_position].failed
                            {
                                return Ok(());
                            }
                            let route_heap = key
                                .estimated_bytes()
                                .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len());
                            // The sampled route and Info route coexist with the
                            // journal prefix/canonical-key/validation-key owners.
                            let fixed = route_heap.saturating_mul(2).saturating_add(
                                RedbPersistence::vector_journal_cleanup_key_reservation(index, key),
                            );
                            let workspace = match self.maintenance_workspace(fixed, PREPARE) {
                                Ok(workspace) => workspace,
                                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                                    let details =
                                        VectorMaintenanceFailureDetails::from_error(&error);
                                    phase.failures.first.get_or_insert(details);
                                    phase.sources[source_position].failure = Some(details);
                                    phase.sources[source_position].failed = true;
                                    return Ok(());
                                }
                                Err(error) => return Err(error),
                            };
                            self.admit_inventory_slot("sampled_routes", &mut sample_memory, &mut sampled_routes)?;
                            sampled_routes.push((
                                VectorPartitionRef::new(index.clone(), key.clone()),
                                source_position,
                                route_heap,
                                fixed,
                                workspace,
                            ));
                            Ok(())
                        })
                    })?;
                // Source views are gone before ordinary route helpers acquire
                // their bulk/partition registry guards.
                for (route, source_position, route_heap, fixed, mut workspace) in sampled_routes {
                    let Some(mut info) = self
                        .vector
                        .partition_info(&route.index, &route.partition_key)
                    else {
                        continue;
                    };
                    info.quarantine_reason = phase.sources[source_position].quarantine;
                    let fresh = phase.sources[source_position]
                        .state
                        .graph_generation_status()
                        .fresh_tail_entries;
                    let covered_lsn = phase
                        .catalogs
                        .iter()
                        .find(|entry| {
                            entry.catalog.index == route.index
                                && entry.catalog.partition_key == route.partition_key
                        })
                        .map(|entry| {
                            entry
                                .catalog
                                .change
                                .unwrap_or(entry.catalog.base)
                                .covered_lsn
                        })
                        .unwrap_or(Lsn(0));
                    let uncovered = if info.quarantine_reason.is_some() {
                        false
                    } else if let Some(persistence) = self.persistence.as_ref() {
                        match vector_source_page_admission::with_preparation_source_page_admission(
                            Arc::clone(&self.accountant),
                            || {
                                persistence
                                    .vector_partition_has_uncovered_journal_records_with_admission(
                                        &route.index,
                                        &route.partition_key,
                                        covered_lsn,
                                        |decode| {
                                            workspace.resize(
                                                fixed.saturating_add(decode),
                                                PREPARE,
                                                HINT,
                                            )
                                        },
                                    )
                            },
                        ) {
                            Ok(uncovered) => uncovered,
                            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                                let details = VectorMaintenanceFailureDetails::from_error(&error);
                                phase.failures.first.get_or_insert(details);
                                phase.sources[source_position].failure = Some(details);
                                phase.sources[source_position].failed = true;
                                continue;
                            }
                            Err(Error::StoreCorrupted { .. }) => {
                                Self::record_inventory_quarantine(
                                    &mut phase.sources[source_position],
                                    VectorRouteQuarantineReason::CorruptChanges,
                                );
                                info.quarantine_reason = phase.sources[source_position].quarantine;
                                false
                            }
                            Err(error) => return Err(error),
                        }
                    } else {
                        false
                    };
                    if layout.maintenance_need(&info).is_some()
                        || (!info.graph_available && (fresh != 0 || uncovered))
                    {
                        let cold = info.base_generation.is_none()
                            && info.serving_policy.is_none()
                            && info.quarantine_reason.is_none();
                        drop(info);
                        workspace.resize(route_heap, PREPARE, HINT)?;
                        self.admit_inventory_slot("candidates", &mut memory, &mut candidates)?;
                        candidates.push(MaintenanceBuildCandidate {
                            partition: route,
                            cold,
                            _workspace: workspace,
                        });
                    }
                }
            }
            // Catalog-backed routes without a live indexed state still need
            // their uncovered journal represented before physical cleanup.
            // Retired raw history is such a route, just like an absent source.
            for entry in &mut phase.catalogs {
                let catalog = &entry.catalog;
                if entry.source.is_some_and(|position| {
                    !phase.sources[position].metadata.historical_only
                        || phase.sources[position].failed
                }) || !self.vector.contains_index(&catalog.index)
                {
                    continue;
                }
                let Some(persistence) = self.persistence.as_ref() else {
                    continue;
                };
                let heap = RedbPersistence::maintenance_vector_catalog_heap_bytes(catalog);
                let fixed =
                    heap.saturating_add(RedbPersistence::vector_journal_cleanup_key_reservation(
                        &catalog.index,
                        &catalog.partition_key,
                    ));
                let probe = entry.workspace.resize(fixed, PREPARE, HINT).and_then(|()| {
                    vector_source_page_admission::with_preparation_source_page_admission(
                        Arc::clone(&self.accountant),
                        || {
                            persistence
                                .vector_partition_has_uncovered_journal_records_with_admission(
                                    &catalog.index,
                                    &catalog.partition_key,
                                    catalog.change.unwrap_or(catalog.base).covered_lsn,
                                    |decode| {
                                        entry.workspace.resize(
                                            fixed.saturating_add(decode),
                                            PREPARE,
                                            HINT,
                                        )
                                    },
                                )
                        },
                    )
                });
                match probe {
                    Ok(true) | Err(Error::StoreCorrupted { .. }) => {
                        let mut workspace = match self.maintenance_workspace(heap, PREPARE) {
                            Ok(workspace) => workspace,
                            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                                phase.failures.first.get_or_insert_with(|| {
                                    VectorMaintenanceFailureDetails::from_error(&error)
                                });
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        // The catalog and candidate now own distinct route copies.
                        let partition = VectorPartitionRef::new(
                            catalog.index.clone(),
                            catalog.partition_key.clone(),
                        );
                        workspace.resize(heap, PREPARE, HINT)?;
                        self.admit_inventory_slot("candidates", &mut memory, &mut candidates)?;
                        candidates.push(MaintenanceBuildCandidate {
                            partition,
                            cold: false,
                            _workspace: workspace,
                        });
                    }
                    Ok(false) => {}
                    Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                        phase.failures.first.get_or_insert_with(|| {
                            VectorMaintenanceFailureDetails::from_error(&error)
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
                let sort_bytes = candidate_sort_heap_bytes(candidates.len());
                let mut sort_memory = OwnedMemoryReservation::new(Arc::clone(&self.accountant));
                if sort_bytes != 0 {
                    #[cfg(all(test, feature = "test-seams"))]
                    let _budget = vector_public_cycle_admission_tests::candidate_sort_budget_for_test(
                        &self.accountant,
                        candidates.len(),
                        sort_bytes,
                    );
                    sort_memory.try_grow_for(sort_bytes, "vector_maintenance", PREPARE, HINT)?;
                    #[cfg(all(test, feature = "test-seams"))]
                    vector_public_cycle_admission_tests::observe_candidate_sort_for_test(
                        &self.accountant,
                        candidates.iter().map(|candidate| (&candidate.partition, candidate.cold)),
                        false,
                    );
                    candidates.sort_by_key(|candidate| !candidate.cold);
                    #[cfg(all(test, feature = "test-seams"))]
                    vector_public_cycle_admission_tests::observe_candidate_sort_for_test(
                        &self.accountant,
                        candidates.iter().map(|candidate| (&candidate.partition, candidate.cold)),
                        true,
                    );
                } else {
                    candidates.sort_by_key(|candidate| !candidate.cold);
                }
                drop(sort_memory);
                Ok(())
            })();
            match sampled {
                Ok(()) => {}
                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                    drop(candidates);
                    drop(memory);
                    let inventory = self.finish_inventory_phase_locked(
                        self.incomplete_inventory_phase(phase, &error),
                    );
                    return Ok(MaintenanceBuildCandidates {
                        candidates: Vec::new(),
                        failures: inventory.failures,
                        _memory: OwnedMemoryReservation::new(Arc::clone(&self.accountant)),
                    });
                }
                Err(error) => return Err(error),
            }
            let inventory = self.finish_inventory_phase_locked(phase);
            Ok(MaintenanceBuildCandidates {
                candidates,
                failures: inventory.failures,
                _memory: memory,
            })
        })
    }
}
