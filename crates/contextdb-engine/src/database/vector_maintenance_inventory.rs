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
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    pub(super) routes: Vec<MaintenanceBuildCandidate>,
    pub(super) failures: MaintenanceMetadataFailures,
    _memory: OwnedMemoryReservation,
}

#[derive(Default)]
pub(super) struct FailedMaintenancePartitions {
    pub(super) inventory: Option<MaintenanceCatalogInventory>,
    pub(super) sources: Option<Arc<VectorStore>>,
}

impl FailedMaintenancePartitions {
    pub(super) fn contains(&self, route: &VectorPartitionRef) -> bool {
        self.inventory.as_ref().is_some_and(|inventory| {
            inventory.catalogs.iter().any(|entry| {
                entry.catalog.index == route.index
                    && entry.catalog.partition_key == route.partition_key
            })
        }) || self.sources.as_ref().is_some_and(|sources| {
            sources
                .try_partition_state(&route.index, &route.partition_key)
                .and_then(|state| state.source_maintenance_metadata().failure)
                .is_some_and(|failure| failure.operation == CLEANUP)
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

pub(super) struct MaintenanceCatalogStream {
    inventory: MaintenanceCatalogInventory,
    cursor: Option<redb::BoundedByteCursor<'static>>,
}

struct MaintenanceIndexes {
    indexes: Vec<VectorIndexRef>,
    _memory: OwnedMemoryReservation,
}

impl MaintenanceContext {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn maintenance_indexes(&self) -> Result<MaintenanceIndexes> {
        let mut memory = OwnedMemoryReservation::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn maintenance_workspace(
        &self,
        bytes: usize,
        operation: &'static str,
    ) -> Result<VectorWorkspaceReservation> {
        self.vector.reserve_workspace_static(
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            bytes,
            operation,
            HINT,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn admit_inventory_slot<T>(
        &self,
        _slot: &'static str,
        memory: &mut OwnedMemoryReservation,
        slots: &mut Vec<T>,
    ) -> Result<()> {
        if slots.len() == slots.capacity() {
            #[cfg(all(test, feature = "test-seams"))]
            let _budget = vector_public_cycle_admission_tests::refuse_inventory_slot_for_test(
                &self.finalization.accountant,
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn maintenance_inventory_phase_locked(&self) -> Result<InventoryPhase> {
        self.maintenance_inventory_phase_with_catalog_retention_locked(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn maintenance_inventory_phase_with_catalog_retention_locked(
        &self,
        retain_catalogs: bool,
    ) -> Result<InventoryPhase> {
        let indexes = match self.maintenance_indexes() {
            Ok(indexes) => indexes,
            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                return Ok(InventoryPhase {
                    complete: false,
                    indexes: MaintenanceIndexes {
                        indexes: Vec::new(),
                        _memory: OwnedMemoryReservation::new(
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                        ),
                    },
                    sources: Vec::new(),
                    catalogs: Vec::new(),
                    misplaced: Vec::new(),
                    failures: MaintenanceMetadataFailures {
                        first: Some(VectorMaintenanceFailureDetails::from_error(&error)),
                    },
                    source_memory: OwnedMemoryReservation::new(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                    ),
                    memory: OwnedMemoryReservation::new(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                    ),
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
            source_memory: OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            ),
            memory: OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(
                &self.finalization.accountant,
            )),
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
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
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
                                    let mut frames = OwnedMemoryReservation::new(
                                        crate::memory_accounting::AccountantHandle::clone(
                                            &self.finalization.accountant,
                                        ),
                                    );
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
                    if let Err(error) = workspace.resize_static(
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
                    let mut frames = OwnedMemoryReservation::new(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                    );
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
                    workspace.resize_static(
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
                    } else if retain_catalogs {
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
                _memory: OwnedMemoryReservation::new(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                ),
            },
            sources: Vec::new(),
            catalogs: Vec::new(),
            misplaced: Vec::new(),
            failures,
            source_memory: OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            ),
            memory: OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(
                &self.finalization.accountant,
            )),
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

    fn apply_inventory_source_outcomes(&self, phase: &InventoryPhase) {
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
    }

    fn finish_inventory_phase_locked(&self, phase: InventoryPhase) -> MaintenanceCatalogInventory {
        self.apply_inventory_source_outcomes(&phase);
        Self::release_inventory_sources(phase)
    }

    fn release_inventory_sources(phase: InventoryPhase) -> MaintenanceCatalogInventory {
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

    /// Capture the fixed catalog root after validating all source attribution.
    /// Decoded catalogs are released as validation proceeds; only the borrowed
    /// source outcomes remain beside the cursor through this finite pass.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn maintenance_catalog_stream_locked(&self) -> Result<MaintenanceCatalogStream> {
        let phase = self.maintenance_inventory_phase_with_catalog_retention_locked(false)?;
        // A durable cleanup failure stays visible until its retry completes.
        // Preserve that marker while releasing every sampled strong source
        // before callbacks can retire a registry's original directory owner.
        for source in &phase.sources {
            let pending = source.metadata.failure.filter(|failure| {
                failure.operation == CLEANUP && failure.message.contains("durable")
            });
            self.vector.apply_partition_source_maintenance_metadata(
                &phase.indexes.indexes[source.index],
                &source.state,
                source.failure.or(pending),
                source.quarantine,
            );
        }
        let cursor = if phase.complete {
            match self.persistence.as_ref() {
                Some(persistence) => {
                    vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        || persistence.maintenance_vector_catalog_cursor(),
                    )?
                }
                None => None,
            }
        } else {
            None
        };
        Ok(MaintenanceCatalogStream {
            inventory: Self::release_inventory_sources(phase),
            cursor,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn maintenance_catalog_stream(&self) -> Result<MaintenanceCatalogStream> {
        self.tx_mgr
            .with_commit_lock(|| self.maintenance_catalog_stream_locked())
    }

    /// Consume the captured root one catalog at a time. A callback retains only
    /// failed work; successful entries physically drop before the next decode.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_maintenance_catalog_stream(
        &self,
        mut stream: MaintenanceCatalogStream,
        mut visit: impl FnMut(&mut AdmittedMaintenanceCatalog) -> Result<bool>,
    ) -> Result<MaintenanceCatalogInventory> {
        let mut retained = Vec::new();
        let mut memory = OwnedMemoryReservation::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
        let mut failures = MaintenanceMetadataFailures {
            first: stream.inventory.failures.first,
        };
        if let (Some(persistence), Some(cursor)) =
            (self.persistence.as_ref(), stream.cursor.as_mut())
        {
            loop {
                self.build_checkpoint()?;
                let next = vector_source_page_admission::with_preparation_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || {
                        cursor
                            .next()
                            .transpose()
                            .map_err(RedbPersistence::storage_error)
                    },
                );
                let handle = match next {
                    Ok(Some(handle)) => handle,
                    Ok(None) => break,
                    Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                        failures.first.get_or_insert_with(|| {
                            VectorMaintenanceFailureDetails::from_error(&error)
                        });
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let inspected =
                    vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        || -> Result<Option<AdmittedMaintenanceCatalog>> {
                            let (_key, encoded) = handle
                                .materialize()
                                .map_err(RedbPersistence::storage_error)?;
                            let mut workspace = self.maintenance_workspace(
                                RedbPersistence::maintenance_vector_catalog_decode_bound(
                                    encoded.value().len(),
                                ),
                                PREPARE,
                            )?;
                            let catalog = match persistence
                                .decode_maintenance_vector_catalog(encoded.value())
                            {
                                Ok(catalog) => catalog,
                                Err(Error::StoreCorrupted { .. }) => return Ok(None),
                                Err(error) => return Err(error),
                            };
                            let mut frames = OwnedMemoryReservation::new(
                                crate::memory_accounting::AccountantHandle::clone(
                                    &self.finalization.accountant,
                                ),
                            );
                            frames.try_grow_for(
                                RedbPersistence::maintenance_catalog_key_frame_bytes(
                                    &catalog.partition_key,
                                ),
                                "vector_maintenance",
                                PREPARE,
                                HINT,
                            )?;
                            if !RedbPersistence::maintenance_catalog_key_matches(
                                &handle,
                                &catalog.index,
                                &catalog.partition_key,
                            )? {
                                return Ok(None);
                            }
                            drop(frames);
                            let metadata = self
                                .vector
                                .try_partition_state(&catalog.index, &catalog.partition_key)
                                .map(|state| state.source_maintenance_metadata());
                            if metadata.is_some_and(|metadata| metadata.quarantine.is_some()) {
                                return Ok(None);
                            }
                            let pending_cleanup = metadata
                                .and_then(|metadata| metadata.failure)
                                .is_some_and(|failure| {
                                    failure.operation == CLEANUP
                                        && failure.message.contains("durable")
                                });
                            workspace.resize_static(
                                RedbPersistence::maintenance_vector_catalog_heap_bytes(&catalog),
                                if pending_cleanup { CLEANUP } else { PREPARE },
                                HINT,
                            )?;
                            Ok(Some(AdmittedMaintenanceCatalog {
                                catalog,
                                workspace,
                                pending_cleanup,
                                source: None,
                            }))
                        },
                    );
                let mut entry = match inspected {
                    Ok(Some(entry)) => entry,
                    Ok(None) => continue,
                    Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                        failures.first.get_or_insert_with(|| {
                            VectorMaintenanceFailureDetails::from_error(&error)
                        });
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if visit(&mut entry)? {
                    match self.admit_inventory_slot("catalogs", &mut memory, &mut retained) {
                        Ok(()) => {}
                        Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                            failures.first.get_or_insert_with(|| {
                                VectorMaintenanceFailureDetails::from_error(&error)
                            });
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                    retained.push(entry);
                }
            }
        }
        Ok(MaintenanceCatalogInventory {
            complete: stream.inventory.complete,
            catalogs: retained,
            routes: Vec::new(),
            failures,
            _memory: memory,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn maintenance_catalog_inventory_locked(
        &self,
    ) -> Result<MaintenanceCatalogInventory> {
        let phase = self.maintenance_inventory_phase_locked()?;
        Ok(self.finish_inventory_phase_locked(phase))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    /// Reclaim selected source graphs one route at a time. Strong source
    /// handles are released before the caller retires registry states, and
    /// no list of unrelated owned routes occupies reclamation headroom.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_closing_source_graphs_locked(
        &self,
        registered_snapshots: &[SnapshotId],
    ) -> Result<(MaintenanceCatalogInventory, usize)> {
        let mut phase = self.maintenance_inventory_phase_with_catalog_retention_locked(false)?;
        if !phase.complete {
            return Ok((self.finish_inventory_phase_locked(phase), 0));
        }
        let mut reclaimed = 0usize;
        for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
            for position in 0..phase.sources.len() {
                let source = &phase.sources[position];
                if source.index != index_position
                    || source.failed
                    || source.metadata.historical_only
                {
                    continue;
                }
                let mut route = None;
                let captured = self
                    .vector
                    .with_partition_sources(index, |view| -> Result<()> {
                        view.visit(|key, state| -> Result<()> {
                            if !Arc::ptr_eq(state, &source.state) {
                                return Ok(());
                            }
                            let heap = key
                                .estimated_bytes()
                                .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len());
                            let workspace = self.maintenance_workspace(heap, PREPARE)?;
                            route = Some(MaintenanceBuildCandidate {
                                partition: VectorPartitionRef::new(index.clone(), key.clone()),
                                cold: false,
                                _workspace: workspace,
                            });
                            Ok(())
                        })
                    });
                match captured {
                    Ok(()) => {}
                    Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                        let details = VectorMaintenanceFailureDetails::from_error(&error);
                        phase.failures.first.get_or_insert(details);
                        phase.sources[position].failure = Some(details);
                        phase.sources[position].failed = true;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                if let Some(route) = route {
                    reclaimed = reclaimed.saturating_add(
                        self.vector
                            .reclaim_snapshot_free_partition_graph_generations(
                                &route.partition,
                                registered_snapshots,
                            ),
                    );
                }
            }
        }
        Ok((self.finish_inventory_phase_locked(phase), reclaimed))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_catalogs_locked(
        &self,
        inventory: &mut MaintenanceCatalogInventory,
        publication: &mut contextdb_tx::manager::MaintenancePublication<'_, DynStore>,
        registered_snapshots: &[SnapshotId],
    ) -> Result<usize> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(0);
        };
        let mut reclaimed = 0usize;
        let mut first_failure = None;
        let mut reclaim = |entry: &mut AdmittedMaintenanceCatalog| -> Result<bool> {
            let catalog = &entry.catalog;
            let heap = RedbPersistence::maintenance_vector_catalog_heap_bytes(catalog);
            let mut route_memory = OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            );
            let result = route_memory
                .try_grow_for(heap, "vector_maintenance", PREPARE, HINT)
                .and_then(|()| {
                    let partition = VectorPartitionRef::new(
                        catalog.index.clone(),
                        catalog.partition_key.clone(),
                    );
                    // The caller keeps its removal guard through the durable
                    // commit. Selection can still name the current serving
                    // chain when a replacement is durable but not installed.
                    // Admit the temporary HashSet and returned Vec before
                    // capture, with at most one tuple per sampled snapshot.
                    let capture_bound = registered_snapshots
                        .len()
                        .saturating_mul(
                            std::mem::size_of::<(u64, Option<u64>)>()
                                .saturating_mul(4)
                                .saturating_add(std::mem::size_of::<SnapshotId>()),
                        )
                        .saturating_add(4096);
                    route_memory.try_grow_for(
                        capture_bound,
                        "vector_maintenance",
                        PREPARE,
                        HINT,
                    )?;
                    let retained = self
                        .vector
                        .snapshot_retained_partition_chain_ids(&partition, registered_snapshots);
                    entry.workspace.resize_static(
                        heap.saturating_add(
                            RedbPersistence::vector_journal_cleanup_key_reservation(
                                &catalog.index,
                                &catalog.partition_key,
                            )
                            .saturating_mul(3)
                            .saturating_add(64 * 1024),
                        ),
                        PREPARE,
                        HINT,
                    )?;
                    vector_source_page_admission::with_catalog_reclamation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        || {
                            self.build_checkpoint()?;
                            publication.reserve()?;
                            let reclaimed = persistence
                                .reclaim_superseded_vector_partition_generations(
                                    catalog,
                                    &retained,
                                    || self.build_checkpoint(),
                                )?;
                            // Publish each completed catalog before a later
                            // catalog can fail under this same commit guard.
                            publication.complete();
                            Ok(reclaimed)
                        },
                    )
                });
            match result {
                Ok((count, _)) => reclaimed = reclaimed.saturating_add(count),
                Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                    let details = VectorMaintenanceFailureDetails::from_error(&error);
                    first_failure.get_or_insert(details);
                    publication.abandon();
                    self.record_catalog_metadata_failure_locked(catalog, details);
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
            Ok(false)
        };
        if inventory.catalogs.is_empty() {
            let stream = self.maintenance_catalog_stream_locked()?;
            let streamed = self.visit_maintenance_catalog_stream(stream, &mut reclaim)?;
            inventory.failures.first = inventory.failures.first.or(streamed.failures.first);
        } else {
            for entry in &mut inventory.catalogs {
                reclaim(entry)?;
            }
        }
        inventory.failures.first = inventory.failures.first.or(first_failure);
        Ok(reclaimed)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn maintenance_build_candidates(&self) -> Result<MaintenanceBuildCandidates> {
        let mut maintenance_vector_copy_owner = contextdb_vector::VectorCopyAdmission::new(
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
        );
        self.tx_mgr.with_commit_lock(|| {
            let mut phase = self.maintenance_inventory_phase_with_catalog_retention_locked(false)?;
            if !phase.complete {
                let inventory = self.finish_inventory_phase_locked(phase);
                return Ok(MaintenanceBuildCandidates {
                    candidates: Vec::new(),
                    failures: inventory.failures,
                    _memory: OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)),
                });
            }
            let mut candidates = Vec::new();
            let mut memory = OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant));
            let sampled = (|| -> Result<()> {
            for (index_position, index) in phase.indexes.indexes.iter().enumerate() {
                let layout = contextdb_vector::scoped_index_layout(&self.vector, index, &mut maintenance_vector_copy_owner)?;
                // Copy one borrowed route, then release its source view before
                // asking ordinary helpers for its metadata. Unrelated routes do
                // not retain preparation workspaces beside this one.
                for source_position in 0..phase.sources.len() {
                    if phase.sources[source_position].index != index_position
                        || phase.sources[source_position].metadata.historical_only
                        || phase.sources[source_position].failed
                    {
                        continue;
                    }
                    let mut sampled_route = None;
                    self.vector.with_partition_sources(index, |view| -> Result<()> {
                        view.visit(|key, state| -> Result<()> {
                            if !Arc::ptr_eq(&phase.sources[source_position].state, state) {
                                return Ok(());
                            }
                            let route_heap = key.estimated_bytes()
                                .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len());
                            let fixed = route_heap.saturating_mul(2).saturating_add(
                                RedbPersistence::vector_journal_cleanup_key_reservation(index, key),
                            );
                            let workspace = self.maintenance_workspace(fixed, PREPARE)?;
                            sampled_route = Some((
                                VectorPartitionRef::new(index.clone(), key.clone()),
                                route_heap, fixed, workspace,
                            ));
                            Ok(())
                        })
                    })?;
                    let Some((route, route_heap, fixed, mut workspace)) = sampled_route else {
                        continue;
                    };
                    // Inventory validation retained only source outcomes. Admit
                    // and decode this route's own catalog after enumeration has
                    // released every other catalog body.
                    let fixed = fixed.saturating_add(Self::vector_route_inventory_bytes(&route));
                    workspace.resize_static(fixed, PREPARE, HINT)?;
                    let catalog = match self.persistence.as_ref().filter(|_| phase.sources[source_position].quarantine.is_none()) {
                        Some(persistence) => vector_source_page_admission::with_preparation_source_page_admission(
                            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                            || persistence.load_vector_partition_generation_catalog_for(&route.index, &route.partition_key),
                        )?,
                        None => None,
                    };
                    let mut route_copy_owner = maintenance_vector_copy_owner.scratch();
                    let mut info = match vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                        || self.vector.try_partition_info(
                            &route.index,
                            &route.partition_key,
                            &mut route_copy_owner,
                        ),
                    ) {
                        Ok(Some(info)) => info,
                        Ok(None) => continue,
                        Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                            return Err(error);
                        }
                        Err(error) => {
                            let mut details = VectorMaintenanceFailureDetails::from_error(&error);
                            if matches!(error, Error::MemoryBudgetExceeded { .. }) {
                                // Candidate inspection is required generation preparation;
                                // retain the source refusal and normalize only its report.
                                details.operation = PREPARE;
                            }
                            phase.failures.first.get_or_insert(details);
                            phase.sources[source_position].failure = Some(details);
                            phase.sources[source_position].failed = true;
                            continue;
                        }
                    };
                    let retired_rebuild = if let Some(persistence) = self.persistence.as_ref() {
                        match vector_source_page_admission::with_preparation_source_page_admission(
                            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant), || persistence.retired_vector_rebuild_pending(&route),
                        ) {
                            Ok(pending) => pending,
                            Err(error @ (Error::MemoryBudgetExceeded { .. } | Error::StoreCorrupted { .. })) => {
                                let details = VectorMaintenanceFailureDetails::from_error(&error);
                                phase.failures.first.get_or_insert(details);
                                phase.sources[source_position].failure = Some(details);
                                phase.sources[source_position].failed = true;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    } else { false };

                    info.quarantine_reason = phase.sources[source_position].quarantine;
                    let fresh = phase.sources[source_position]
                        .state
                        .graph_generation_status()
                        .fresh_tail_entries;
                    let covered_lsn = catalog.as_ref()
                        .map(|catalog| catalog.change.unwrap_or(catalog.base).covered_lsn)
                        .unwrap_or(Lsn(0));
                    let uncovered = if retired_rebuild || info.quarantine_reason.is_some() {
                        false
                    } else if let Some(persistence) = self.persistence.as_ref() {
                        match vector_source_page_admission::with_preparation_source_page_admission(
                            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                            || {
                                persistence
                                    .vector_partition_has_uncovered_journal_records_with_admission(
                                        &route.index,
                                        &route.partition_key,
                                        covered_lsn,
                                        |decode| {
                                            workspace.resize_static(
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
                    let ordinary_need = layout.maintenance_need(&info).is_some();
                    let reader_need = if ordinary_need || info.quarantine_reason.is_some() {
                        false
                    } else {
                        match vector_source_page_admission::with_preparation_source_page_admission(
                            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                            || self.reader_serving_replacement_needed(&route, catalog.as_ref()),
                        ) {
                            Ok(needed) => needed,
                            Err(error @ Error::MemoryBudgetExceeded { .. }) => {
                                // Keep this source's existing partition-local
                                // preparation refusal; a later cycle retries it.
                                let details = VectorMaintenanceFailureDetails::from_error(&error);
                                phase.failures.first.get_or_insert(details);
                                phase.sources[source_position].failure = Some(details);
                                phase.sources[source_position].failed = true;
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    };
                    drop(catalog);
                    if retired_rebuild || ordinary_need
                        || reader_need
                        || (!info.graph_available && (fresh != 0 || uncovered))
                    {
                        let cold = info.base_generation.is_none()
                            && info.serving_policy.is_none()
                            && info.quarantine_reason.is_none();
                        drop(info);
                        workspace.resize_static(route_heap, PREPARE, HINT)?;
                        self.admit_inventory_slot("candidates", &mut memory, &mut candidates)?;
                        candidates.push(MaintenanceBuildCandidate {
                            partition: route,
                            cold,
                            _workspace: workspace,
                        });
                    }
                }
            }
            // Publish the sampled source outcomes before releasing their strong
            // handles. The orphan scan captures its own fixed source inventory;
            // retaining both snapshots needlessly consumes preparation credit.
            self.apply_inventory_source_outcomes(&phase);
            phase.sources = Vec::new();
            phase.source_memory = OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            );
            // Catalog-backed routes without a live indexed state still need
            // their uncovered journal represented before physical cleanup.
            // Retired raw history is such a route, just like an absent source.
            let stream = self.maintenance_catalog_stream_locked()?;
            let orphan_inventory = self.visit_maintenance_catalog_stream(stream, |entry| {
                let catalog = &entry.catalog;
                let source = self.vector.try_partition_state(&catalog.index, &catalog.partition_key);
                if source.as_ref().is_some_and(|state| {
                    let metadata = state.source_maintenance_metadata();
                    !metadata.historical_only || metadata.failure.is_some()
                }) || !self.vector.contains_index(&catalog.index)
                {
                    return Ok(false);
                }
                let Some(persistence) = self.persistence.as_ref() else {
                    return Ok(false);
                };
                let heap = RedbPersistence::maintenance_vector_catalog_heap_bytes(catalog);
                let fixed =
                    heap.saturating_add(RedbPersistence::vector_journal_cleanup_key_reservation(
                        &catalog.index,
                        &catalog.partition_key,
                    ));
                let probe = entry.workspace.resize_static(fixed, PREPARE, HINT).and_then(|()| {
                    vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                        || {
                            persistence
                                .vector_partition_has_uncovered_journal_records_with_admission(
                                    &catalog.index,
                                    &catalog.partition_key,
                                    catalog.change.unwrap_or(catalog.base).covered_lsn,
                                    |decode| {
                                        entry.workspace.resize_static(
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
                                return Ok(false);
                            }
                            Err(error) => return Err(error),
                        };
                        // The catalog and candidate now own distinct route copies.
                        let partition = VectorPartitionRef::new(
                            catalog.index.clone(),
                            catalog.partition_key.clone(),
                        );
                        workspace.resize_static(heap, PREPARE, HINT)?;
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
                Ok(false)
            })?;
            phase.failures.first = phase.failures.first.or(orphan_inventory.failures.first);
                let sort_bytes = candidate_sort_heap_bytes(candidates.len());
                let mut sort_memory = OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant));
                if sort_bytes != 0 {
                    #[cfg(all(test, feature = "test-seams"))]
                    let _budget = vector_public_cycle_admission_tests::candidate_sort_budget_for_test(
                        &self.finalization.accountant,
                        candidates.len(),
                        sort_bytes,
                    );
                    sort_memory.try_grow_for(sort_bytes, "vector_maintenance", PREPARE, HINT)?;
                    #[cfg(all(test, feature = "test-seams"))]
                    vector_public_cycle_admission_tests::observe_candidate_sort_for_test(
                        &self.finalization.accountant,
                        candidates.iter().map(|candidate| (&candidate.partition, candidate.cold)),
                        false,
                    );
                    candidates.sort_by_key(|candidate| !candidate.cold);
                    #[cfg(all(test, feature = "test-seams"))]
                    vector_public_cycle_admission_tests::observe_candidate_sort_for_test(
                        &self.finalization.accountant,
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
                        _memory: OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)),
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
