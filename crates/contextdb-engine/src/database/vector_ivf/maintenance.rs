//! The existing maintenance publisher drives each declared native tuple.
use super::super::*;
impl MaintenanceContext {
    pub(crate) fn run_native_vector_cycle(
        &self,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<VectorMaintenanceReport> {
        let Some(persistence) = &self.persistence else {
            return Ok(VectorMaintenanceReport::default());
        };
        let mut copy_owner = crate::vector_read_memory::admission(memory.clone());
        let routes = super::super::maintenance_runtime::vector_partition_routes(
            &self.vector,
            &mut copy_owner,
        )?;
        let mut report = VectorMaintenanceReport::default();
        let mut indexes = HashSet::new();
        let mut pending = HashSet::new();
        let mut built = HashSet::new();
        for route in routes {
            self.build_checkpoint()?;
            let (meta, _workspace) = {
                let metadata = self.relational.table_meta.read();
                let Some(meta) = metadata.get(&route.index.table) else {
                    continue;
                };
                let workspace = self.vector.reserve_workspace_static(
                    self.finalization.accountant.vector_budget()?,
                    meta.metadata_bytes(),
                    "native_vector_declaration",
                    "Release retained vector state or raise MEMORY_LIMIT.",
                )?;
                (meta.clone(), workspace)
            };
            let Some(column) = meta.columns.iter().find(|c| c.name == route.index.column) else {
                continue;
            };
            // EXACT alone needs no maintained generation. A declared native
            // policy still gets its maintenance, including an explicit EXACT override.
            if column.search_mode == VectorSearchMode::Exact
                && column.search_target_accuracy_hundredths.is_none()
                && column.search_expected_vectors.is_none()
                && column.ivf_lists.is_none()
                && column.ivf_probes.is_none()
                && column.ivf_refine_factor_hundredths.is_none()
                && column.vector_engine_policy == Default::default()
            {
                continue;
            }
            report.nonempty_partitions += 1;
            indexes.insert(route.index.clone());
            if persistence.native_ivf_calibration_route().is_some()
                && !persistence
                    .native_ivf_calibration_is_selected(&route.index, &route.partition_key)
            {
                report.remaining_partitions += 1;
                pending.insert(route.index.clone());
                continue;
            }
            let outcome = self
                .tx_mgr
                .with_maintenance_publication(|publication| {
                    publication.reserve()?;
                    self.admit_generation_disk(0)?;
                    let removal = self
                        .snapshot_registry
                        .begin_vector_removal_pass(self.tx_mgr.current_tx_max());
                    let can_reclaim = if removal.registered_snapshots().is_empty() {
                        true
                    } else if let Some((generation, basis)) = persistence.native_ivf_cleanup_basis(
                        &route.index,
                        &route.partition_key,
                        memory.clone(),
                    )? {
                        removal.native_readers_allow_cleanup(generation, basis, |root| {
                            let root =
                                crate::persistence::vector_paged::CapturedPagedRoot::from_writer(
                                    root,
                                )?;
                            crate::persistence::vector_ivf::reclamation::selected_generation(
                                &root,
                                memory.clone(),
                                &route.index,
                                &route.partition_key,
                            )
                            .map(|selected| selected.map(|(generation, _)| generation))
                        })?
                    } else {
                        false
                    };
                    let progress = persistence.advance_native_ivf_partition(
                        &route.index,
                        &route.partition_key,
                        column,
                        can_reclaim,
                        &|| self.build_checkpoint(),
                    )?;
                    drop(removal);
                    publication.complete();
                    // Capture the publisher's real refreshed root before releasing
                    // the same boundary. All query work runs after this closure.
                    if progress.calibration {
                        persistence.capture_native_ivf_calibration(
                            &route.index,
                            &route.partition_key,
                            column,
                        )?;
                    }
                    Ok(progress)
                })
                .and_then(|progress| {
                    if progress.calibration {
                        let mut publish = |operation: &mut dyn FnMut() -> Result<()>| {
                            super::super::maintenance_runtime::publish_direct_metadata(
                                &self.tx_mgr,
                                operation,
                            )
                        };
                        persistence.advance_native_ivf_calibration(
                            &route.index,
                            &route.partition_key,
                            &|| self.build_checkpoint(),
                            &mut publish,
                        )?;
                    }
                    Ok(progress)
                });
            match outcome {
                Ok(progress) => {
                    if progress.in_flight {
                        self.snapshot_registry
                            .hold_construction_floor(&route, progress.source_snapshot);
                        report.remaining_partitions += 1;
                        pending.insert(route.index.clone());
                    } else {
                        self.snapshot_registry.release_construction_floor(&route);
                    }
                    if progress.ready {
                        report.ready_partitions += 1;
                    }
                    if progress.published {
                        report.built_partitions += 1;
                        built.insert(route.index.clone());
                    }
                }
                Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                    return Err(error);
                }
                Err(error) => {
                    #[cfg(feature = "test-seams")]
                    crate::vector_ivf_observations::disk::native_ivf_build_refused_for_test(
                        persistence
                            .native_ivf_physical_disk_owner_for_test()?
                            .identity(),
                        &route.index,
                        &route.partition_key,
                        &error,
                    );
                    report.remaining_partitions += 1;
                    pending.insert(route.index.clone());
                    report
                        .first_failure_details
                        .get_or_insert_with(|| VectorMaintenanceFailureDetails::from_error(&error));
                }
            }
        }
        report.nonempty_indexes = indexes.len();
        report.remaining_indexes = pending.len();
        report.ready_indexes = indexes.difference(&pending).count();
        report.built_indexes = built.len();
        report.first_failure = report.first_failure_details.map(|details| details.failure);
        Ok(report)
    }
}
