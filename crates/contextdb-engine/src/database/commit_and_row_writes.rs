use super::*;

/// The legacy manager destroys its detached write set before unwinding back to
/// this frame. Retire its recorded row invoices only after that destruction;
/// ordinary returns keep the existing success/failure cleanup as their owner.
struct DetachedRowUnwindRefund<'a> {
    accountant: &'a crate::memory_accounting::AccountantHandle,
    bytes: std::cell::Cell<usize>,
}

impl Drop for DetachedRowUnwindRefund<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() && self.bytes.get() != 0 {
            self.accountant.release(self.bytes.get());
        }
    }
}

/// Transaction metadata has no successful/failure return path during unwind.
/// Its lineage memo and row-graph journal still own real preparation invoices.
/// Declare this before commit locals so those locals drop before the metadata.
struct CommitMetadataUnwindCleanup<'a> {
    database: &'a Database,
    tx: TxId,
}
impl Drop for CommitMetadataUnwindCleanup<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.database.remove_original_terminal_stages(self.tx);
        }
    }
}

impl Database {
    pub(super) fn take_local_schema_stage(&self, lsn: Lsn) -> Option<LocalSchemaStage> {
        self.local_schema_stages.lock().remove(&lsn)
    }

    pub(super) fn publish_local_schema_stage(&self, mut stage: LocalSchemaStage) {
        self.relational_store
            .publish_table_projections_with_generation(
                stage.relational,
                stage.memory_swap.history.take(),
            );
        self.graph
            .publish_prepared_dag_edge_types(stage.dag_edge_types);
        self.publish_prepared_rank_formula_cache(stage.rank_formulas);
        if let Some(vector) = stage.vector {
            self.vector_store
                .publish_prepared_received_schema_tables(vector);
        }
        if let Some(publication) = stage.native_event_bus.get_mut().take() {
            let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
                unreachable!("owning schema publication host");
            };
            handle.with_host(|host| host.events.publish_owned_definition_only(publication));
        }
        if let Some(event_bus) = stage.event_bus {
            self.publish_prepared_event_bus_publication_under_queue_mutation_lease(
                event_bus,
                Vec::new(),
            );
        }
        if let Some(publication) = stage.native_trigger.take() {
            self.publish_original_trigger_publication(publication);
        } else {
            self.publish_prepared_trigger_publication(stage.trigger);
        }
        self.in_memory_table_generations
            .lock()
            .extend(stage.table_generations);
        self.publish_memory_schema_history(stage.memory_history.take());
        stage.memory_swap.commit_after_swap();
        self.clear_statement_cache();
        self.reconcile_maintenance_thread();
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_source_and_event_bus_ddl(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_with_source_and_sync_ddl(
            tx,
            source,
            event_bus_ddl,
            &[],
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_source_and_sync_ddl(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_with_source_and_sync_ddl_and_trigger_audit_projection(
            tx,
            source,
            event_bus_ddl,
            trigger_ddl,
            None,
            None,
            false,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_with_source_and_sync_ddl_and_trigger_audit_projection(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
        sync_pull_trigger_audit_projection: Option<&BTreeMap<String, TriggerDeclaration>>,
        sync_tombstones: Option<&[AcceptedSyncTombstone]>,
        sync_trigger_derived_rows_are_local: bool,
        mut delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let _metadata_unwind = CommitMetadataUnwindCleanup { database: self, tx };
        #[cfg(feature = "test-seams")]
        let _publication_admission = self.publication_admission_for_test(tx);
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Source reads in preparation use this operation's reader. Each actual
        // persistence write admits its own writer decode scope at that boundary.
        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory.clone(),
        )?
        .enter();
        let outcome = if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            handle.with_host(|host| {
                self.commit_owned_ordinary(
                    host,
                    tx,
                    source,
                    event_bus_ddl,
                    trigger_ddl,
                    sync_pull_trigger_audit_projection,
                    sync_trigger_derived_rows_are_local,
                    delivery_prepare,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })
        } else {
            (|| {
                let vector_copy_owner = std::cell::RefCell::new(vector_copy_owner);
                let detached_capture = std::cell::Cell::new(None::<bool>);
                let detached_rows_funding = std::cell::RefCell::new(
                    None::<(contextdb_core::read_memory::ReadMemoryOwner, usize)>,
                );

                let discard_selections = self
                    .pending_commit_metadata
                    .lock()
                    .get(&tx)
                    .map(|m| m.discard_selections.clone())
                    .unwrap_or_default();
                let mut discard_drain = if discard_selections.is_empty() {
                    None
                } else {
                    Some(self.event_bus.begin_authoritative_purge_queue_drain()?)
                };
                let discard_hashes = self.authoritative_purge_selected_blob_hashes(
                    &discard_selections,
                    read_memory.clone(),
                )?;
                let discard_blob_guard = if discard_selections.is_empty() {
                    None
                } else {
                    Some(
                        self.blob_repository
                            .acquire_exclusive_sorted(&discard_hashes)?,
                    )
                };
                let discard_publication = if discard_selections.is_empty() {
                    None
                } else {
                    let tables = discard_selections
                        .iter()
                        .map(|selection| selection.table.clone())
                        .collect();
                    Some(self.read_publication_gate.acquire_write(&tables))
                };
                let pending_trigger_audits = std::cell::RefCell::new(Vec::new());
                let sync_pull_pre_trigger_creation_keys =
                    std::cell::RefCell::new(HashSet::<String>::new());
                let sync_pull_pre_trigger_rows =
                    std::cell::RefCell::new(HashSet::<(String, RowId)>::new());
                let pending_trigger_active_guards =
                    std::cell::RefCell::new(Vec::<TriggerCallbackThreadGuard>::new());
                let mut pending_sink_events = Vec::new();
                // For a durable queue append this lease starts before the persistent
                // store consumes staged events and survives until their memory queue
                // publication. An authoritative purge's phase-two freeze therefore
                // cannot inventory a queue between those two halves of one commit.
                let pending_sink_queue_mutation =
                    std::cell::RefCell::new(None::<event_bus::QueueMutationLease>);
                let mut committed_trigger_audit_entries = Vec::new();
                let in_memory_lineage_delta = std::cell::RefCell::new(None);
                let prepared_delivery_memory = std::cell::RefCell::new(None);
                // The funding of the ownership keys custody retirement moves into
                // the write set. Declared before the write set (`ws` below), so it
                // is released only after the write set is dropped, committed or not.
                let retired_custody_keys = std::cell::RefCell::new(Vec::with_capacity(2));
                let committed_graph_arrivals =
                    std::cell::RefCell::new(Vec::<(GraphIdentity, FundedGraphArrival)>::new());
                let validation_noop_count = std::cell::Cell::new(0_u64);
                let pre_apply_index_maintenance_visits = std::cell::Cell::new(0_u64);
                #[cfg(any(test, feature = "test-seams"))]
                let apply_started = std::cell::Cell::new(None::<Instant>);
                let delete_release_bytes = std::cell::RefCell::new(DeleteReleaseBytes::default());
                let vector_snapshot_removal_guard =
                    std::cell::RefCell::new(None::<ActiveRemovalPassGuard>);
                // Held outside the callbacks so they outlive every one of them: each
                // removes its stage-map entry, and releases what that entry holds, on
                // an error return AND on a panic anywhere between registration and
                // the point the commit consumes the entry -- the case no `Result`
                // branch reaches. Ownership passes on once the durable apply has
                // consumed the snapshot sample and the erasure image, and once
                // publication has taken the local schema stage.
                let local_schema_registration =
                    std::cell::RefCell::new(None::<schema_stage_registry::LocalRegistration>);
                let vector_snapshot_registration =
                    std::cell::RefCell::new(None::<CommitStageRegistration<Arc<[SnapshotId]>>>);
                let local_erasure_registration =
                    std::cell::RefCell::new(None::<CommitStageRegistration<discard::Stage>>);
                // Armed just before the durable apply; see `PublicationInterruptionWatch`.
                let publication_watch =
                    std::cell::RefCell::new(None::<PublicationInterruptionWatch>);
                let sync_trigger_dispatch_required =
                    std::cell::Cell::new(if source == CommitSource::SyncPull {
                        self.has_matching_sync_trigger_for_tx(
                            tx,
                            sync_pull_trigger_audit_projection,
                        )?
                    } else {
                        false
                    });
                let detached_row_unwind_refund = DetachedRowUnwindRefund {
                    accountant: &self.finalization.accountant,
                    bytes: std::cell::Cell::new(0),
                };
                let (lsn, ws) = {
                    match self.tx_mgr.commit_with_lsn_precanonicalized_active_prepare_and_applied_mut(
                tx,
                read_memory.clone(),
                || {
                    // The manager holds its commit guard, but not the active-tx
                    // map, while projecting both families. Meaningful DDL joins
                    // the same atomic prepared apply as row mutations; unchanged
                    // DDL does not turn an otherwise empty transaction into a commit.
                    let event_bus_changes = if event_bus_ddl.is_empty() {
                        false
                    } else {
                        self.event_bus_definitions_with_ddl(None, event_bus_ddl)?
                            != *self.event_bus.definitions.lock()
                    };
                    let trigger_changes = if trigger_ddl.is_empty() {
                        false
                    } else {
                        self.apply_trigger_ddl_batch_projection(trigger_ddl)?
                            != *self.trigger.declarations.lock()
                            || Self::trigger_ddl_has_transient_lifecycle(trigger_ddl)
                    };
                    Ok(event_bus_changes || trigger_changes)
                },
                |lsn, visibility_tx| {
                    // Transaction-local absence is never an ordinary DELETE event.
                    if !discard_selections.is_empty() {
                        self.tx_mgr.with_write_set(tx, |ws| {
                            ws.relational_deletes.retain(|(table, id, _)| {
                                !discard_selections
                                    .iter()
                                    .any(|s| s.table == *table && s.local_row_id == *id)
                            });
                            for selected in &discard_selections {
                                ws.config_deletes.push(Self::durable_row_lineage_config_key(
                                    &selected.table,
                                    &selected.natural_key,
                                    selected.table_generation,
                                ));
                            }
                        })?;
                    }
                    *local_schema_registration.borrow_mut() =
                        self.prepare_and_register_local_schema_stage(tx, visibility_tx, lsn, read_memory.clone())?;
                    #[cfg(test)]
                    take_local_commit_stage_fault_for_test(
                        LocalCommitStageFaultPoint::AfterLocalSchemaStageRegistration,
                    );
                    // This observes the canonical staged write set before
                    // conditional-update revalidation can discard a stale
                    // descendant as a no-op. A purged lineage is a typed
                    // refusal, not an ordinary lost-update result.
                    self.tx_mgr
                        .with_write_set(tx, |ws| self.reject_purged_lineage_recreation(ws, read_memory.clone()))??;
                    if source != CommitSource::SyncPull {
                        let outcome = self.prepare_active_trigger_write_set_for_dispatch(tx, &mut vector_copy_owner.borrow_mut(), read_memory.clone())?;
                        Self::reject_user_conditional_update_conflicts(source, &outcome)?;
                        validation_noop_count.set(
                            validation_noop_count
                                .get()
                                .saturating_add(outcome.conditional_noop_count),
                        );
                        sync_trigger_dispatch_required
                            .set(self.has_matching_local_trigger_for_tx(tx, read_memory.clone())?);
                    }
                    if sync_trigger_dispatch_required.get() {
                        let outcome = match if source == CommitSource::SyncPull {
                            self.prepare_active_sync_trigger_write_set_for_dispatch(tx)
                        } else {
                            Ok(CommitValidationOutcome::default())
                        } {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                let pending = pending_trigger_audits.borrow();
                                let _ = self.append_rolled_back_trigger_audits(&pending, tx, &error);
                                return Err(error);
                            }
                        };
                        Self::reject_user_conditional_update_conflicts(source, &outcome)?;
                        validation_noop_count.set(
                            validation_noop_count
                                .get()
                                .saturating_add(outcome.conditional_noop_count),
                        );
                        if source == CommitSource::SyncPull {
                            let (creation_keys, rows) = self.tx_mgr.with_write_set(tx, |ws| {
                                let creation_keys = ws
                                    .config_writes
                                    .iter()
                                    .filter(|(key, _)| key.starts_with("sync_creation_lineage.v1."))
                                    .map(|(key, _)| key.clone())
                                    .collect();
                                let rows = ws
                                    .relational_inserts
                                    .iter()
                                    .map(|(table, row)| (table.clone(), row.row_id))
                                    .collect();
                                (creation_keys, rows)
                            })?;
                            *sync_pull_pre_trigger_creation_keys.borrow_mut() = creation_keys;
                            *sync_pull_pre_trigger_rows.borrow_mut() = rows;
                        }
                        let dispatch = if source == CommitSource::SyncPull {
                            self.dispatch_sync_triggers_for_tx(
                                tx,
                                sync_pull_trigger_audit_projection,
                             &mut vector_copy_owner.borrow_mut(), read_memory.clone())
                        } else {
                            self.dispatch_triggers_for_tx(tx, &mut vector_copy_owner.borrow_mut(), read_memory.clone())
                        };
                        match dispatch {
                            Ok(outcome) => {
                                *pending_trigger_audits.borrow_mut() = outcome.pending;
                                pending_trigger_active_guards
                                    .borrow_mut()
                                    .extend(outcome.active_guards);
                            }
                            Err(failure) => {
                                let failure = *failure;
                                pending_trigger_active_guards
                                    .borrow_mut()
                                    .extend(failure.active_guards);
                                return Err(failure.error);
                            }
                        }
                        if source == CommitSource::SyncPull && !sync_trigger_derived_rows_are_local
                        {
                            let pre_trigger_rows = sync_pull_pre_trigger_rows.borrow();
                            self.tx_mgr.with_write_set(tx, |ws| {
                                let derived_rows = ws
                                    .relational_inserts
                                    .iter()
                                    .filter(|(table, row)| {
                                        !pre_trigger_rows.contains(&(table.clone(), row.row_id))
                                    })
                                    .map(|(table, row)| (table.clone(), row.row_id))
                                    .collect::<Vec<_>>();
                                for (table, row_id) in derived_rows {
                                    ws.set_relational_insert_source_lsn(
                                        table,
                                        row_id,
                                        SYNC_SOURCE_LSN_OWN_COMMIT,
                                    );
                                }
                            })?;
                        }
                        let outcome = if source == CommitSource::SyncPull {
                            self.prepare_active_sync_trigger_write_set_for_dispatch(tx)?
                        } else {
                            self.prepare_active_trigger_write_set_for_dispatch(tx, &mut vector_copy_owner.borrow_mut(), read_memory.clone())?
                        };
                        Self::reject_user_conditional_update_conflicts(source, &outcome)?;
                        validation_noop_count.set(
                            validation_noop_count
                                .get()
                                .saturating_add(outcome.conditional_noop_count),
                        );
                    }
                    // The pass parks every read that arrives at a snapshot at or
                    // before this commit's watermark until the apply is over, so
                    // it is begun only when it can have an effect -- see
                    // `write_set_could_empty_a_vector_partition`.
                    let can_remove_vector_state =
                        self.write_set_could_empty_a_vector_partition(tx)?;
                    if can_remove_vector_state {
                        let watermark = self.tx_mgr.current_tx_max();
                        let committing_statement_registration =
                            COMMITTING_STATEMENT_SNAPSHOT_REGISTRATION.with(std::cell::Cell::get);
                        *vector_snapshot_removal_guard.borrow_mut() =
                            Some(self.snapshot_registry.begin_removal_pass_excluding(
                                watermark,
                                committing_statement_registration,
                            ));
                    }
                    Ok(())
                },
                |ws| {
                    if !ws.is_empty() {
                        if source == CommitSource::SyncPull && sync_trigger_dispatch_required.get() {
                            self.rewrite_txid_placeholders_for_rows_not_in(
                                tx,
                                ws,
                                &sync_pull_pre_trigger_rows.borrow(),
                            );
                        }
                        if source == CommitSource::SyncPull {
                            // A received row has only the authenticated wire
                            // sidecar staged by sync apply. Never leave local
                            // unbound creation evidence behind: after a later
                            // sidecar loss it could be rebound as a false
                            // local author.
                            let received_creation_keys =
                                sync_pull_pre_trigger_creation_keys.borrow();
                            ws.config_writes.retain(|(key, _)| {
                                if !key.starts_with("sync_creation_lineage.v1.") {
                                    return true;
                                }
                                sync_trigger_derived_rows_are_local
                                    && !received_creation_keys.contains(key)
                                    && !self.private_source_creation_key(tx,key)
                            });
                            // An authoritative hub owns callback-derived rows,
                            // so only that role retains and stamps post-snapshot
                            // creation evidence. An edge materialization stays
                            // Pulled and owns no new fleet creation identity.
                            self.stamp_unbound_creation_lineages_at_commit(ws)?;
                        } else {
                            self.stamp_unbound_creation_lineages_at_commit(ws)?;
                        }
                        if source != CommitSource::SyncPull {
                            self.rewrite_txid_placeholders(tx, ws)?;
                        }
                        self.reject_purged_lineage_recreation(ws, read_memory.clone())?;
                        let final_validation = self.commit_validate(tx, ws, &mut vector_copy_owner.borrow_mut(), read_memory.clone())?;
                        Self::reject_user_conditional_update_conflicts(source, &final_validation)?;
                        validation_noop_count.set(
                            validation_noop_count
                                .get()
                                .saturating_add(final_validation.conditional_noop_count),
                        );
                        let registered_snapshots = vector_snapshot_removal_guard
                            .borrow()
                            .as_ref()
                            .map(|guard| guard.registered_snapshots.clone())
                            .unwrap_or_default();
                        if let Some(lsn) = ws.commit_lsn
                            && self
                                .vector_partition_snapshot_stages
                                .lock()
                                .insert(lsn, registered_snapshots)
                                .is_some()
                        {
                            return Err(Error::Other((
                                "vector snapshot stage already exists for commit LSN".to_string()).into()));
                        }
                        if let Some(lsn) = ws.commit_lsn {
                            *vector_snapshot_registration.borrow_mut() =
                                Some(CommitStageRegistration::new(
                                    &self.vector_partition_snapshot_stages,
                                    lsn,
                                ));
                        }
                        #[cfg(test)]
                        take_local_commit_stage_fault_for_test(
                            LocalCommitStageFaultPoint::AfterVectorSnapshotStageRegistration,
                        );
                        *committed_graph_arrivals.borrow_mut() = self.stage_pulled_graph_arrivals(
                            ws,
                            source == CommitSource::SyncPull
                                && !sync_trigger_derived_rows_are_local,
                        )?;
                        if let Some(lsn) = ws.commit_lsn {
                            let trigger_ddl_start =
                                self.stage_event_bus_ddl_for_commit(lsn, event_bus_ddl, read_memory.clone())?;
                            if trigger_ddl_start != 0
                                && pending_sink_queue_mutation.borrow().is_none()
                            {
                                *pending_sink_queue_mutation.borrow_mut() =
                                    Some(self.event_bus.begin_queue_mutation());
                            }
                            self.stage_trigger_ddl_for_commit(lsn, trigger_ddl, trigger_ddl_start, read_memory.clone())?;
                        }
                        self.plugin.pre_commit(ws, source)?;
                        let registrations = self
                            .pending_commit_metadata
                            .lock()
                            .get(&tx)
                            .map(|m| m.delivery_registrations.clone())
                            .unwrap_or_default();
                        for registration in registrations {
                            for record in registration.prepare(self, ws, read_memory.clone())? {
                                record.verify()?;
                                ws.config_writes.push((
                                    record.key()?,
                                    crate::custody::records::encode(&record)?,
                                ));
                            }
                        }
                        if let Some(prepare) = delivery_prepare.as_mut() {
                            for record in prepare.prepare(self, ws)? {
                                record.verify()?;
                                ws.config_writes.push((
                                    record.key()?,
                                    crate::custody::records::encode(&record)?,
                                ));
                            }
                        }
                        *prepared_delivery_memory.borrow_mut() = self.prepare_custody_commit(ws, read_memory.clone(), &mut retired_custody_keys.borrow_mut())?;
                        #[cfg(any(test, feature = "test-seams"))]
                        let projection_started = Instant::now();
                        let prepared_sink_events = self
                            .prepare_sink_events_for_write_set_with_event_bus_ddl(
                                ws,
                                event_bus_ddl,
                                read_memory.clone(),
                            )?;
                        if !prepared_sink_events.is_empty() {
                            if pending_sink_queue_mutation.borrow().is_none() {
                                *pending_sink_queue_mutation.borrow_mut() =
                                    Some(self.event_bus.begin_queue_mutation());
                            }
                            self.event_bus.ensure_queue_capacity(&prepared_sink_events)?;
                        }
                        if self.persistence.is_some() && !prepared_sink_events.is_empty() {
                            ws.config_writes.push((
                                event_bus::NEXT_ACTION_ID_CONFIG_KEY.to_string(),
                                RedbPersistence::encode_config_value(
                                    &self.event_bus.next_action_id(),
                                )?,
                            ));
                        }
                        if self.persistence.is_some()
                            && let Some(lsn) = ws.commit_lsn
                        {
                            self.event_bus
                                .stage_sink_events_for_persistence(lsn, prepared_sink_events);
                        } else {
                            pending_sink_events = prepared_sink_events;
                        }
                        if let Some(lsn) = ws.commit_lsn {
                            committed_trigger_audit_entries = if source == CommitSource::SyncPull {
                                let projected_declarations =
                                    self.staged_trigger_declarations_for_commit(lsn);
                                let audit_projection = sync_pull_trigger_audit_projection
                                    .or(projected_declarations.as_ref());
                                let mut entries = self
                                    .committed_sync_pull_trigger_audits_for_write_set(
                                        ws,
                                        lsn,
                                        audit_projection,
                                    read_memory.clone(),)?;
                                entries.extend(self.committed_trigger_audits_for_pending(
                                    &pending_trigger_audits.borrow(),
                                    ws,
                                    lsn,
                                ));
                                entries
                            } else {
                                let pending = pending_trigger_audits.borrow();
                                self.committed_trigger_audits_for_pending(&pending, ws, lsn)
                            };
                            self.stage_trigger_audits_for_persistence(
                                lsn,
                                &committed_trigger_audit_entries,
                            );
                        }
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            let delta = Instant::now()
                                .duration_since(projection_started)
                                .as_nanos()
                                .min(u128::from(u64::MAX))
                                as u64;
                            let mut stage_wall_nanos = [0; 7];
                            stage_wall_nanos[6] = delta;
                            self.add_commit_stage_stats(&CommitStageStats {
                                stage_wall_nanos: Some(stage_wall_nanos),
                                ..Default::default()
                            });
                        }
                        *delete_release_bytes.borrow_mut() =
                            self.delete_release_bytes_for_write_set(ws, &mut vector_copy_owner.borrow_mut(), read_memory.clone())?;
                        if let Some(drain) = discard_drain.take() {
                            drop(pending_sink_queue_mutation.borrow_mut().take());
                            *local_erasure_registration.borrow_mut() =
                                Some(self.prepare_discard_commit(
                                    ws,
                                    &discard_selections,
                                    drain.freeze_for_commit_locked_purge(),
                                    discard_blob_guard.as_ref().expect("discard hash fence"),
                                    discard_publication.as_ref().expect("discard read publication fence"),
                                 &mut vector_copy_owner.borrow_mut(), read_memory.clone())?);
                            #[cfg(test)]
                            take_local_commit_stage_fault_for_test(
                                LocalCommitStageFaultPoint::AfterLocalErasureStageRegistration,
                            );
                            *prepared_delivery_memory.borrow_mut() = self.prepare_custody_commit(
                                ws,
                                read_memory.clone(),
                                &mut retired_custody_keys.borrow_mut(),
                            )?;
                        }
                        if self.persistence.is_none() {
                            *in_memory_lineage_delta.borrow_mut() =
                                Some(self.prepare_in_memory_lineage_delta(&ws.config_writes)?);
                        }
                        pre_apply_index_maintenance_visits
                            .set(self.relational_store.index_maintenance_visits());
                        #[cfg(any(test, feature = "test-seams"))]
                        if ws
                            .commit_lsn
                            .is_some_and(|lsn| self.local_schema_stages.lock().contains_key(&lsn))
                        {
                            self.local_schema_pre_commit_pause.maybe_pause();
                        }
                        #[cfg(any(test, feature = "test-seams"))]
                        apply_started.set(Some(Instant::now()));
                    }
                    if source == CommitSource::SyncPull
                        && self.capture_detached_sync_write_set.load(Ordering::SeqCst)
                        && ws.config_writes.iter().any(|(key,_)|
                            key == "__contextdb.detached_capture_owner.v1")
                    {
                        // Nested DDL commits belong to the private planner,
                        // not to the outer adjudicated receipt transaction.
                        let refused = ws.config_writes.iter().any(|(key, _)|
                            key == "__contextdb.detached_refusal_capture.v1");
                        ws.config_writes.retain(|(key,_)|key != "__contextdb.detached_capture_owner.v1"
                            && key != "__contextdb.detached_refusal_capture.v1");
                        self.capture_detached_sync_write_set.store(false,Ordering::SeqCst);
                        // The final owning write set is returned below after
                        // the private commit hooks. Capture intent only here;
                        // cloning the complete row/vector body image is needless.
                        if self.relational_store.uses_disk_backed_committed_bodies()
                            && crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?.is_some() {
                            let owner=self.private_replay_owner(tx).ok_or_else(||Error::Other(
                                "paged private capture has no original preparation owner".into()))?;
                            let bytes=ws.relational_inserts.iter().try_fold(0usize,|bytes,(table,row)| {
                                let admitted=ws.admitted_bytes_for_staged_row(table,row.row_id).ok_or_else(||
                                    Error::Other("paged private capture lost its row admission record".into()))?;
                                bytes.checked_add(admitted).ok_or_else(||Error::Other("private captured row invoice overflow".into()))
                            })?;
                            // Do not adopt until commit succeeds. Failure
                            // disposal still owns the active staged debits.
                            *detached_rows_funding.borrow_mut()=Some((owner,bytes));
                        }
                        detached_capture.set(Some(refused));
                    }
                    if matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_))
                        && self.persistence.is_some()
                        && self.relational_store.uses_disk_backed_committed_bodies()
                    {
                        let bytes = ws.relational_inserts.iter()
                            .filter_map(|(table, row)| ws.admitted_bytes_for_staged_row(table, row.row_id))
                            .sum();
                        detached_row_unwind_refund.bytes.set(bytes);
                    }
                    *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(
                        &self.publication_interrupted,
                    ));
                    Ok(())
                },
                |lsn, ws| {
                    // The durable apply has consumed this commit's snapshot
                    // sample and erasure image; what they held is its now.
                    if let Some(registration) = vector_snapshot_registration.borrow_mut().as_mut() {
                        registration.ownership_transferred();
                    }
                    if let Some(registration) = local_erasure_registration.borrow_mut().as_mut() {
                        registration.ownership_transferred();
                    }
                    if self.graph_store.is_source_backed() {
                        self.finalization.accountant.release(ws.adj_inserts.iter().map(standalone_edge_retained_bytes).sum());
                    }
                    if !ws.is_empty() {
                        if let Some(stage) = self.take_local_schema_stage(lsn) {
                            if let Some(registration) =
                                local_schema_registration.borrow_mut().as_mut()
                            {
                                registration.ownership_transferred();
                            }
                            #[cfg(any(test, feature = "test-seams"))]
                            self.local_schema_pre_publish_pause.maybe_pause();
                            #[cfg(test)]
                            take_local_commit_stage_fault_for_test(
                                LocalCommitStageFaultPoint::AfterDurableCommitBeforeLocalPublication,
                            );
                            self.publish_local_schema_stage(stage);
                            // Between two of this commit's own publication
                            // components: the schema image is live, the
                            // custody and lineage deltas below are not.
                            #[cfg(any(test, feature = "test-seams"))]
                            self.local_schema_between_publication_pause.maybe_pause();
                        }
                        // Publish only the admitted custody keys.
                        if let Some(delta) = prepared_delivery_memory.borrow_mut().take() {
                            self.publish_custody_delta(delta);
                        }
                        if let Some(delta) = in_memory_lineage_delta.borrow_mut().take() {
                            self.publish_in_memory_lineage_delta(delta);
                        }
                        if self.persistence.is_none() && !committed_graph_arrivals.borrow().is_empty() {
                            self.sync_graph_arrivals
                                .write()
                                .extend(committed_graph_arrivals.borrow_mut().drain(..));
                        }
                        let index_maintenance_visits = self
                            .relational_store
                            .index_maintenance_visits()
                            .saturating_sub(pre_apply_index_maintenance_visits.get());
                        #[cfg(any(test, feature = "test-seams"))]
                        let mut stats = CommitStageStats {
                            index_maintenance_visits,
                            ..Default::default()
                        };
                        #[cfg(not(any(test, feature = "test-seams")))]
                        let stats = CommitStageStats {
                            index_maintenance_visits,
                            ..Default::default()
                        };
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Some(started) = apply_started.get() {
                            let delta = Instant::now()
                                .duration_since(started)
                                .as_nanos()
                                .min(u128::from(u64::MAX))
                                as u64;
                            let mut stage_wall_nanos = [0; 7];
                            stage_wall_nanos[5] = delta;
                            stats.stage_wall_nanos = Some(stage_wall_nanos);
                        }
                        self.add_commit_stage_stats(&stats);
                        if pending_sink_queue_mutation.borrow().is_some() {
                            self.publish_staged_event_bus_ddl_commit_under_queue_mutation_lease(
                                lsn,
                            );
                        } else {
                            self.publish_staged_event_bus_ddl_commit(lsn);
                        }
                        self.publish_staged_trigger_ddl_commit(lsn);
                        if let Some(sync_tombstones) = sync_tombstones {
                            let mut tombstones = self.sync_tombstone_arrivals.write();
                            for (table, natural_key, arrival, kind, row_id) in sync_tombstones {
                                tombstones.insert(
                                    Self::sync_tombstone_key(table, natural_key),
                                    SyncTombstoneArrival {
                                        arrival: arrival.unwrap_or(lsn),
                                        delete_lsn: lsn,
                                        row_id: Some(*row_id),
                                        kind: *kind,
                                        accepted_local_order_invalidated: false,
                                    },
                                );
                            }
                        }
                        // DDL metadata mutation also needs the commit mutex.
                        // Release delete-side accounting here so DROP/RENAME
                        // cannot remove vector state before cleanup observes it.
                        self.release_delete_allocations_from_bytes(&delete_release_bytes.borrow());
                    }
                },
            ) {
                Ok(committed) => {
                    detached_row_unwind_refund.bytes.set(0);
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    committed
                }
                Err(mut failure) => {
                    detached_row_unwind_refund.bytes.set(0);
                    // An ordinary failure: nothing was published, and the
                    // durable apply either did not happen or reported its own
                    // error. A refusal before the storage commit keeps serving.
                    // A storage commit that failed cannot abort, so the storage
                    // layer refuses later writers until a reopen recovers.
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    if let Some(ws) = failure.write_set.as_deref() {
                        self.plugin.commit_failed(ws, source, failure.error.engine_diagnostic());
                    }
                    if let Some(lsn) = failure.write_set.as_ref().and_then(|ws| ws.commit_lsn) {
                        // A received-schema stage is registered during
                        // active_prepare, before the ordinary mutable prepare
                        // callback. Drop it on either later prepare failure or
                        // Redb apply failure; PersistentCompositeStore also
                        // removes it on its own flush error.
                        let _ = self.take_received_schema_stage(lsn);
                        let _ = self.take_local_schema_stage(lsn);
                        self.local_erasure_stages.lock().remove(&lsn);
                        self.vector_partition_snapshot_stages.lock().remove(&lsn);
                        let _ = self.event_bus.take_staged_sink_events_for_persistence(lsn);
                        self.discard_staged_event_bus_ddl_commit(lsn);
                        self.discard_staged_trigger_ddl_commit(lsn);
                        self.discard_staged_trigger_audits_for_persistence(lsn);
                    }
                    if self.private_received_rows_hold_delete_invoices() {
                        if let Some(ws) = failure.write_set.take() {
                            // Cleanup refusal must not replace the failed commit.
                            let _ = self.dispose_private_abandoned_write_set(*ws,&mut vector_copy_owner.borrow_mut());
                        }
                    } else if let Some(ws) = failure.write_set.take() {
                        let _ = self.release_insert_allocations(&ws,&mut vector_copy_owner.borrow_mut());
                        drop(ws);
                    }
                    self.pending_commit_metadata.lock().remove(&tx);
                    self.pending_local_schema_stages.lock().remove(&tx);
                    // Audit preparation is secondary. It may refuse without
                    // skipping stage release or changing the primary failure.
                    let _ = self.append_rolled_back_trigger_audits(
                        &pending_trigger_audits.borrow(), tx, &failure.error,
                    );
                    return Err(failure.error);
                }
            }
                };
                #[cfg(feature = "test-seams")]
                self.publication_committed_for_test(tx, lsn);
                self.pending_commit_metadata.lock().remove(&tx);

                if !ws.is_empty() {
                    self.release_committed_row_body_allocations(&ws);
                    self.plugin.post_commit(&ws, source);
                    let sink_events_to_publish = if self.persistence.is_some() {
                        ws.commit_lsn
                            .and_then(|lsn| {
                                self.event_bus.take_staged_sink_events_for_persistence(lsn)
                            })
                            .map(EventBusState::materialize_staged_sink_events)
                            .unwrap_or_default()
                    } else {
                        pending_sink_events
                    };
                    if pending_sink_queue_mutation.borrow().is_some() {
                        self.publish_prepared_sink_events_to_memory_under_queue_mutation_lease(
                            sink_events_to_publish,
                        );
                    } else {
                        self.publish_prepared_sink_events_to_memory(sink_events_to_publish);
                    }
                    drop(pending_sink_queue_mutation.borrow_mut().take());
                    self.retain_private_final_audits(
                        tx,
                        std::mem::take(&mut *pending_trigger_audits.borrow_mut()),
                    )?;
                    self.append_trigger_audits_to_memory(committed_trigger_audit_entries);
                    self.publish_commit_event_if_subscribers(&ws, source, lsn);
                }

                let committed_tx = ws.finalized_tx;
                if let Some(refused) = detached_capture.get() {
                    *self.detached_sync_write_set.lock() =
                        Some(connected_replay::DetachedSyncCapture {
                            write_set: ws,
                            refused,
                            prefix: None,
                            rows_invoice: detached_rows_funding.borrow_mut().take().map(
                                |(owner, bytes)| {
                                    contextdb_core::read_memory::ReadCredit::from_prepaid(
                                        owner, bytes,
                                    )
                                },
                            ),
                        });
                }
                Ok(CommitValidationOutcome {
                    committed_tx,
                    conditional_noop_count: validation_noop_count.get(),
                    conditional_conflict_count: 0,
                })
            })()
        };
        #[cfg(any(test, feature = "test-seams"))]
        if matches!(&outcome, Ok(outcome) if outcome.committed_tx.is_some()) {
            // The terminal write set and callback-local invoices have dropped;
            // retained snapshot and adapter owners remain visible in the sum.
            self.emit_writer_memory_ownership_receipt_for_test("commit");
        }
        outcome
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reject_user_conditional_update_conflicts(
        source: CommitSource,
        outcome: &CommitValidationOutcome,
    ) -> Result<()> {
        if source == CommitSource::User && outcome.conditional_conflict_count > 0 {
            return Err(Error::ConditionalUpdateConflict {
                count: outcome.conditional_conflict_count,
            });
        }
        Ok(())
    }

    pub(super) fn build_commit_event(
        ws: &contextdb_tx::WriteSet,
        source: CommitSource,
        lsn: Lsn,
    ) -> CommitEvent {
        let mut tables_changed: Vec<String> = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table.clone())
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(table, _, _)| table.clone()),
            )
            .chain(
                ws.vector_inserts
                    .iter()
                    .map(|entry| entry.index.table.clone()),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(index, _, _)| index.table.clone()),
            )
            .chain(
                ws.vector_moves
                    .iter()
                    .map(|(index, _, _, _)| index.table.clone()),
            )
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        tables_changed.sort();

        let relational_row_count = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| (table.clone(), row.row_id))
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(table, row_id, _)| (table.clone(), *row_id)),
            )
            .collect::<HashSet<_>>()
            .len();

        CommitEvent {
            source,
            lsn,
            tables_changed,
            row_count: relational_row_count
                + ws.adj_inserts.len()
                + ws.adj_deletes.len()
                + ws.vector_inserts.len()
                + ws.vector_deletes.len()
                + ws.vector_moves.len(),
        }
    }

    pub(super) fn publish_commit_event_if_subscribers(
        &self,
        ws: &contextdb_tx::WriteSet,
        source: CommitSource,
        lsn: Lsn,
    ) {
        let mut subscriptions = self.subscriptions.lock();
        if subscriptions.subscribers.is_empty() {
            return;
        }
        let event = Self::build_commit_event(ws, source, lsn);
        let subscribers = std::mem::take(&mut subscriptions.subscribers);
        let mut live_subscribers = Vec::with_capacity(subscribers.len());

        for sender in subscribers {
            match sender.try_send(event.clone()) {
                Ok(()) => {
                    subscriptions.events_sent += 1;
                    live_subscribers.push(sender);
                }
                Err(TrySendError::Full(_)) => {
                    subscriptions.events_dropped += 1;
                    live_subscribers.push(sender);
                }
                Err(TrySendError::Disconnected(_)) => {}
            }
        }

        subscriptions.subscribers = live_subscribers;
    }

    pub(super) fn stop_pruning_thread(&self) {
        let handle = {
            let mut runtime = self.pruning_runtime.lock();
            let handle = runtime.request_shutdown_and_take_handle();
            runtime.shutdown = Arc::new(AtomicBool::new(false));
            handle
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn execute_statement_with_plan(
        &self,
        stmt: &Statement,
        sql: &str,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        cached_plan: Option<&PhysicalPlan>,
        mut preopened_autocommit_tx: Option<TxId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Err(error) = self.plugin.on_query(sql) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            return Err(error);
        }

        let started = Instant::now();
        // Authenticated sync applies already run against their own private
        // received-schema image and intentionally combine one source DDL
        // vector with its rows. Keep that established atomic path in the
        // ordinary executor; this local authoring stage is only for caller
        // transactions.
        if let Some(tx) = tx
            && Self::is_staged_local_schema_statement(stmt)
            && !self.sync_apply_local_schema_bypass_active(tx)
        {
            let result = self.stage_local_schema_statement(tx, stmt, sql, read_memory.clone());
            let outcome = query_outcome_from_result(&result);
            self.plugin.post_query(sql, started.elapsed(), &outcome);
            return result;
        }
        if let Some(change) = self.ddl_change_for_statement(stmt, tx).as_ref()
            && let Err(error) = self.plugin.on_ddl(change)
        {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            return Err(error);
        }
        if let Some(tx) = tx
            && Self::is_local_table_schema_statement(stmt)
            && !self.sync_apply_local_schema_bypass_active(tx)
        {
            let result = Err(Error::Other((
                "transactional local schema staging does not support this schema operation; commit or roll back before using it"
                    .to_string()).into()));
            let outcome = query_outcome_from_result(&result);
            self.plugin.post_query(sql, started.elapsed(), &outcome);
            return result;
        }
        if let Some(result) = self.execute_event_bus_statement(stmt, tx, read_memory.clone()) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            let outcome = query_outcome_from_result(&result);
            self.plugin.post_query(sql, started.elapsed(), &outcome);
            return result;
        }

        // Handle INSERT INTO GRAPH / __edges as a virtual table routing to the graph store.
        if let Statement::Insert(ins) = stmt
            && (ins.table.eq_ignore_ascii_case("GRAPH")
                || ins.table.eq_ignore_ascii_case("__edges"))
        {
            return self.execute_graph_insert(
                ins,
                params,
                tx,
                preopened_autocommit_tx.take(),
                vector_copy_owner,
                read_memory.clone(),
            );
        }

        let result = (|| {
            if let Some(plan) = cached_plan {
                #[cfg(any(test, feature = "test-seams"))]
                observe_read_execution_convergence(ReadExecutionConvergenceEvent::PlanReady);
                let skip_static_dml_validation =
                    matches!(plan, PhysicalPlan::Update(_) | PhysicalPlan::Delete(_));
                return self.run_planned_statement(
                    stmt,
                    plan,
                    params,
                    tx,
                    preopened_autocommit_tx.take(),
                    skip_static_dml_validation,
                    vector_copy_owner,
                    read_memory.clone(),
                    native_trace_refund,
                );
            }

            let (stmt, plan) = {
                // Pre-resolve InSubquery expressions with CTE context before planning.
                let stmt = self.pre_resolve_cte_subqueries(
                    stmt,
                    params,
                    tx,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                let plan = contextdb_planner::plan(&stmt)?;
                #[cfg(any(test, feature = "test-seams"))]
                observe_read_execution_convergence(ReadExecutionConvergenceEvent::PlanReady);
                self.cache_statement_if_eligible(sql, &stmt, &plan);
                (stmt, plan)
            };
            self.run_planned_statement(
                &stmt,
                &plan,
                params,
                tx,
                preopened_autocommit_tx.take(),
                false,
                vector_copy_owner,
                read_memory.clone(),
                native_trace_refund,
            )
        })();
        if result.is_err() {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
        }
        let duration = started.elapsed();
        let outcome = query_outcome_from_result(&result);
        self.plugin.post_query(sql, duration, &outcome);
        result.map(strip_internal_row_id)
    }

    pub(super) fn execute_event_bus_statement(
        &self,
        stmt: &Statement,
        tx: Option<TxId>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Option<Result<OwnedQueryResult>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !matches!(
            stmt,
            Statement::CreateEventType { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
                | Statement::DropRoute { .. }
        ) {
            return None;
        }
        let change = self
            .ddl_change_for_statement(stmt, tx)
            .expect("event bus statement has DDL change");
        Some(match tx {
            Some(tx) => self
                .stage_event_bus_ddl_in_tx(tx, change)
                .map(|()| OwnedQueryResult::empty()),
            None => self
                .apply_event_bus_ddl_from_user(vec![change], read_memory.clone())
                .map(|()| OwnedQueryResult::empty()),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_planned_statement(
        &self,
        stmt: &Statement,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        preopened_autocommit_tx: Option<TxId>,
        skip_static_dml_validation: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        native_trace_refund: Option<&crate::executor::bounded::NativeTraceRefund>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(tx) = tx
            && matches!(
                plan,
                PhysicalPlan::Insert(_)
                    | PhysicalPlan::Delete(_)
                    | PhysicalPlan::Update(_)
                    | PhysicalPlan::Discard(_)
            )
            && self.pending_local_schema_stages.lock().contains_key(&tx)
        {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
            return Err(Error::Other((
                "mixed DML and transactional local schema DDL is not supported; commit or roll back before writing rows"
                    .to_string()).into()));
        }
        if !skip_static_dml_validation && let Err(error) = validate_dml(plan, self, params) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
            return Err(error);
        }
        // Catalog validation for every column the plan names, before any row is
        // read. Sits with the existing static DML check so both plan-time
        // refusals happen at one seam.
        if let Err(error) = validate_plan_columns(self, plan) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
            return Err(error);
        }
        let result = match tx {
            Some(tx) => {
                let _write_gate = if self.trigger_callback_tx_bound_matches(tx) {
                    None
                } else {
                    Some(self.enter_sql_write_control_bypass(tx))
                };
                let snapshot = self.snapshot_for_read();
                let checkpoint = matches!(
                    plan,
                    PhysicalPlan::Insert(_) | PhysicalPlan::Update(_) | PhysicalPlan::Delete(_)
                )
                .then(|| self.write_set_checkpoint(tx))
                .transpose()?;
                let result = self.with_snapshot_override(snapshot, || {
                    self.run_read_plan(
                        plan,
                        params,
                        Some(tx),
                        snapshot,
                        vector_copy_owner,
                        read_memory.clone(),
                        native_trace_refund,
                    )
                });
                if let Some(checkpoint) = checkpoint {
                    if result.is_err() {
                        let _ = self.restore_write_set_checkpoint(tx, checkpoint);
                    } else {
                        checkpoint.settle();
                    }
                }
                result
            }
            None => self.execute_autocommit(
                plan,
                params,
                preopened_autocommit_tx,
                vector_copy_owner,
                read_memory.clone(),
                native_trace_refund,
            ),
        };
        if result.is_ok()
            && let Statement::CreateTable(ct) = stmt
            && !ct.dag_edge_types.is_empty()
        {
            self.graph.register_dag_edge_types(&ct.dag_edge_types);
        }
        result
    }

    /// Handle `INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES (...)`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn execute_graph_insert(
        &self,
        ins: &contextdb_parser::ast::Insert,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        preopened_autocommit_tx: Option<TxId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::executor::resolve_expr;

        let col_index = |name: &str| {
            ins.columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
        };
        let source_idx = col_index("source_id")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires source_id column".into()))?;
        let target_idx = col_index("target_id")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires target_id column".into()))?;
        let edge_type_idx = col_index("edge_type")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires edge_type column".into()))?;

        let auto_commit = tx.is_none();
        let tx = match tx {
            Some(tx) => tx,
            None => match preopened_autocommit_tx {
                Some(tx) => tx,
                None => self.begin_for_public_autocommit_write()?,
            },
        };
        let checkpoint = (!auto_commit)
            .then(|| self.write_set_checkpoint(tx))
            .transpose()?;
        let result = {
            let _write_gate = self.enter_sql_write_control_bypass(tx);
            (|| {
                let mut count = 0u64;
                for row_exprs in &ins.values {
                    let source = resolve_expr(&row_exprs[source_idx], params)?;
                    let target = resolve_expr(&row_exprs[target_idx], params)?;
                    let edge_type = resolve_expr(&row_exprs[edge_type_idx], params)?;

                    let source_uuid = match &source {
                        Value::Uuid(u) => *u,
                        Value::Text(t) => uuid::Uuid::parse_str(t).map_err(|e| {
                            Error::PlanError(format!("invalid source_id uuid: {e}"))
                        })?,
                        _ => return Err(Error::PlanError("source_id must be UUID".into())),
                    };
                    let target_uuid = match &target {
                        Value::Uuid(u) => *u,
                        Value::Text(t) => uuid::Uuid::parse_str(t).map_err(|e| {
                            Error::PlanError(format!("invalid target_id uuid: {e}"))
                        })?,
                        _ => return Err(Error::PlanError("target_id must be UUID".into())),
                    };
                    let edge_type_str = match &edge_type {
                        Value::Text(t) => t.clone(),
                        _ => return Err(Error::PlanError("edge_type must be TEXT".into())),
                    };

                    self.insert_edge_with_read_memory(
                        tx,
                        source_uuid,
                        target_uuid,
                        edge_type_str,
                        Default::default(),
                        read_memory.clone(),
                    )?;
                    count += 1;
                }
                Ok(count)
            })()
        };

        let count = match result {
            Ok(count) => count,
            Err(error) => {
                if let Some(checkpoint) = checkpoint {
                    let _ = self.restore_write_set_checkpoint(tx, checkpoint);
                }
                if auto_commit {
                    let _ = self.rollback_without_callback_tx_control(tx);
                }
                return Err(error);
            }
        };

        if let Some(checkpoint) = checkpoint {
            checkpoint.settle();
        }
        if auto_commit {
            self.with_current_statement_snapshot_excluded_from_terminal_commit(|| {
                self.commit_with_source(
                    tx,
                    CommitSource::AutoCommit,
                    vector_copy_owner,
                    read_memory.clone(),
                )
            })?;
        }

        Ok(OwnedQueryResult::empty_with_affected(count))
    }

    pub(super) fn ddl_change_for_statement(
        &self,
        stmt: &Statement,
        tx: Option<TxId>,
    ) -> Option<DdlChange> {
        match stmt {
            Statement::CreateTable(ct) => {
                crate::executor::validate_create_table_vector_declarations(&ct.name, ct).ok()?;
                Some(ddl_change_from_create_table(ct))
            }
            Statement::DropTable(dt) => Some(DdlChange::DropTable {
                name: dt.name.clone(),
            }),
            Statement::AlterTable(at) => {
                let mut meta = self.table_meta(&at.table).unwrap_or_default();
                // A rename or a drop carries the author's own marker, so a
                // receiver never has to guess either from the column list.
                let rename_marker = match &at.action {
                    AlterAction::RenameColumn { from, to } => {
                        sync_rename_marker_for_column(&meta, from, to)
                    }
                    AlterAction::DropColumn { column, .. } => {
                        Some(sync_column_drop_constraint(column))
                    }
                    _ => None,
                };
                // Simulate the alter action on a cloned meta to get post-alteration columns
                match &at.action {
                    AlterAction::AddColumn(col) => {
                        let rank_policy = col
                            .rank_policy
                            .as_deref()
                            .map(crate::executor::map_rank_policy);
                        let column =
                            crate::executor::core_column_from_ast(&at.table, col, rank_policy)
                                .ok()?;
                        meta.columns.push(column);
                        if col.expires {
                            meta.expires_column = Some(col.name.clone());
                        }
                    }
                    AlterAction::DropColumn {
                        column: name,
                        cascade: _,
                    } => {
                        meta.columns.retain(|c| c.name != *name);
                        if meta.expires_column.as_deref() == Some(name.as_str()) {
                            meta.expires_column = None;
                        }
                    }
                    AlterAction::RenameColumn { from, to } => {
                        if let Some(c) = meta.columns.iter_mut().find(|c| c.name == *from) {
                            c.name = to.clone();
                        }
                        if meta.expires_column.as_deref() == Some(from.as_str()) {
                            meta.expires_column = Some(to.clone());
                        }
                    }
                    AlterAction::SetVectorMaxPartitions {
                        column,
                        max_partitions,
                    } => {
                        crate::executor::set_vector_max_partitions(
                            &at.table,
                            column,
                            max_partitions,
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorSearchMode {
                        column,
                        search_mode,
                    } => {
                        crate::executor::set_vector_search_mode(
                            &at.table,
                            column,
                            *search_mode,
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorAutoIndexAt {
                        column,
                        auto_index_at,
                    } => {
                        crate::executor::set_vector_auto_index_at(
                            &at.table,
                            column,
                            auto_index_at.as_deref(),
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorAutoExpansionCeiling {
                        column,
                        auto_expansion_ceiling,
                    } => {
                        crate::executor::set_vector_auto_expansion_ceiling(
                            &at.table,
                            column,
                            auto_expansion_ceiling.as_deref(),
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorFlatBelow { column, flat_below } => {
                        crate::executor::set_vector_flat_below(
                            &at.table,
                            column,
                            flat_below.as_deref(),
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorSearch { .. }
                    | AlterAction::SetVectorIvf { .. }
                    | AlterAction::SetVectorWith { .. }
                    | AlterAction::ResetVectorWith { .. } => {
                        crate::executor::set_clustered_vector_policy(
                            &at.table, &at.action, &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetVectorConsolidation {
                        column,
                        consolidation,
                    } => {
                        crate::executor::set_vector_consolidation(
                            &at.table,
                            column,
                            consolidation.as_ref(),
                            &mut meta,
                        )
                        .ok()?;
                    }
                    AlterAction::SetRetain {
                        duration_seconds,
                        sync_safe,
                        declared_unit,
                    } => {
                        // The execution path refuses a delivery promise the
                        // table's direction could never keep, so the projection
                        // must not hand a plugin a statement about to be
                        // rejected.
                        if crate::executor::refuse_promise_with_no_delivery(
                            &at.table,
                            *sync_safe,
                            Some(crate::executor::effective_sync_direction(&meta)),
                        )
                        .is_err()
                        {
                            return None;
                        }
                        meta.default_ttl_seconds = Some(*duration_seconds);
                        meta.sync_safe = *sync_safe;
                        meta.retain_declared_unit = Some(*declared_unit);
                    }
                    AlterAction::DropRetain => {
                        meta.default_ttl_seconds = None;
                        meta.sync_safe = false;
                        meta.retain_declared_unit = None;
                    }
                    AlterAction::SetSyncDirection(direction) => {
                        if crate::executor::refuse_promise_with_no_delivery(
                            &at.table,
                            meta.sync_safe,
                            Some(*direction),
                        )
                        .is_err()
                        {
                            return None;
                        }
                        meta.sync_direction = Some(*direction);
                    }
                    AlterAction::SetHistory(policy) => {
                        meta.history_policy = Some(*policy);
                    }
                    AlterAction::SetSyncConflict(policy) => {
                        meta.conflict_policy = Some(*policy);
                    }
                    AlterAction::SetMaintainedCounts(declared) => {
                        meta.maintained_counts = match declared {
                            contextdb_parser::ast::MaintainedCountsDecl::Default => None,
                            other => Some(
                                contextdb_planner::planner::map_maintained_counts_declaration(
                                    other,
                                )
                                .ok()?,
                            ),
                        };
                    }
                }
                crate::executor::validate_vector_partition_declarations(&at.table, &meta).ok()?;
                Some(DdlChange::AlterTable {
                    name: at.table.clone(),
                    columns: meta
                        .columns
                        .iter()
                        .map(|c| {
                            (
                                c.name.clone(),
                                sql_type_for_meta_column(c, &meta.propagation_rules),
                            )
                        })
                        .collect(),
                    constraints: create_table_constraints_from_meta(&meta)
                        .into_iter()
                        .chain(rename_marker)
                        .collect(),
                    foreign_keys: single_column_foreign_keys_from_meta(&meta, &HashSet::new()),
                    composite_foreign_keys: meta.composite_foreign_keys.clone(),
                    composite_unique: meta.unique_constraints.clone(),
                })
            }
            Statement::CreateEventType { name, when, table } => Some(DdlChange::CreateEventType {
                name: name.clone(),
                trigger: match when {
                    contextdb_parser::ast::EventTypeTrigger::Insert => "INSERT",
                    contextdb_parser::ast::EventTypeTrigger::Update => "UPDATE",
                    contextdb_parser::ast::EventTypeTrigger::Delete => "DELETE",
                }
                .to_string(),
                table: table.clone(),
            }),
            Statement::CreateTrigger {
                name,
                table,
                on_events,
                including_sync,
            } => {
                let on_events = on_events
                    .iter()
                    .map(|event| match event {
                        contextdb_parser::ast::TriggerEvent::Insert => "INSERT",
                        contextdb_parser::ast::TriggerEvent::Update => "UPDATE",
                        contextdb_parser::ast::TriggerEvent::Delete => "DELETE",
                    })
                    .map(str::to_string)
                    .collect();
                Some(if *including_sync {
                    DdlChange::CreateTriggerIncludingSync {
                        name: name.clone(),
                        table: table.clone(),
                        on_events,
                    }
                } else {
                    DdlChange::CreateTrigger {
                        name: name.clone(),
                        table: table.clone(),
                        on_events,
                    }
                })
            }
            Statement::DropTrigger { name } => Some(DdlChange::DropTrigger { name: name.clone() }),
            Statement::CreateSink {
                name,
                sink_type,
                url,
            } => Some(DdlChange::CreateSink {
                name: name.clone(),
                sink_type: match sink_type {
                    contextdb_parser::ast::SinkType::Webhook => "WEBHOOK",
                    contextdb_parser::ast::SinkType::Callback => "CALLBACK",
                }
                .to_string(),
                url: url.clone(),
            }),
            Statement::CreateRoute {
                name,
                event_type,
                sink,
                where_in,
            } => Some(DdlChange::CreateRoute {
                name: name.clone(),
                event_type: event_type.clone(),
                sink: sink.clone(),
                table: self
                    .event_bus_table_for_event_type_with_pending(tx, event_type)
                    .unwrap_or_default(),
                where_in: where_in
                    .as_ref()
                    .map(|where_in| (where_in.column.clone(), where_in.values.clone())),
            }),
            Statement::DropRoute { name } => Some(DdlChange::DropRoute {
                name: name.clone(),
                table: self
                    .event_bus_table_for_route_with_pending(tx, name)
                    .unwrap_or_default(),
            }),
            _ => None,
        }
    }

    /// Pre-resolve InSubquery expressions within SELECT statements that have CTEs.
    /// This allows CTE-backed subqueries in WHERE clauses to be evaluated before planning.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn pre_resolve_cte_subqueries(
        &self,
        stmt: &Statement,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Statement> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Statement::Select(sel) = stmt
            && !sel.ctes.is_empty()
            && sel.body.where_clause.is_some()
        {
            use crate::executor::resolve_in_subqueries_with_ctes;
            let resolved_where = sel
                .body
                .where_clause
                .as_ref()
                .map(|expr| {
                    resolve_in_subqueries_with_ctes(
                        self,
                        expr,
                        params,
                        tx,
                        &sel.ctes,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                })
                .transpose()?;
            let mut new_body = sel.body.clone();
            new_body.where_clause = resolved_where;
            Ok(Statement::Select(contextdb_parser::ast::SelectStatement {
                ctes: sel.ctes.clone(),
                body: new_body,
            }))
        } else {
            Ok(stmt.clone())
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn insert_row(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<RowId> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.insert_row_with_read_memory(tx, table, values, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_with_read_memory(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        crate::executor::apply_missing_column_defaults(self, table, &mut values, Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let admission = self.admit_retained_row_owned(
            table,
            &values,
            &meta,
            "relational",
            "staged_index_posting",
            "Reduce row size or raise MEMORY_LIMIT.",
        )?;
        self.insert_row_with_admission(
            tx,
            table,
            values,
            Some(admission),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Carry the SQL producer's original reservation into the row caller.
    /// Host selection preserves the Legacy caller and routes Owned through
    /// the paired original producer before any native factory is enabled.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_prepaid_owned(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        admission: OwnedMemoryReservation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let original = OriginalRowValues { values, admission };
        self.insert_row_with_admission(
            tx,
            table,
            original.values,
            Some(original.admission),
            vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_row_with_admission(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        admission: Option<OwnedMemoryReservation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut original = OriginalOptionalRowValues { values, admission };
        self.ensure_no_mixed_local_schema_dml(tx)?;
        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        let _operation = if trigger_callback_bound {
            None
        } else {
            Some(self.open_operation_after_write_control_wait(tx, "insert_row")?)
        };
        if !trigger_callback_bound || !self.trigger.ready.load(Ordering::SeqCst) {
            self.ensure_trigger_table_ready(table, "insert_row")?;
        }
        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(_) => self.insert_row_within_operation(
                tx,
                table,
                original.values,
                trigger_callback_bound,
                &mut original.admission,
                vector_copy_owner,
                read_memory.clone(),
            ),
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                self.insert_row_within_operation_owned(
                    host,
                    tx,
                    table,
                    original.values,
                    trigger_callback_bound,
                    &mut original.admission,
                    read_memory.clone(),
                )
            }),
        };
        if result.is_err() {
            let _ = self.restore_write_set_checkpoint(tx, checkpoint);
        } else {
            checkpoint.settle();
        }
        result
    }

    /// The ordinary Owned row producer consumes the caller's existing debit.
    /// Row lineage uses its own original auxiliary producer. Vector placement
    /// remains a prerequisite; refusal restores the coupled checkpoint.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_row_within_operation_owned(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        trigger_callback_bound: bool,
        admission: &mut Option<OwnedMemoryReservation>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_primary_key_values_present(table, &values)?;
        let creation_key = self
            .table_meta(table)
            .and_then(|meta| natural_key_from_row_values(&meta, &values));
        if !trigger_callback_bound {
            self.validate_row_constraints(tx, table, &values, None, read_memory.clone())?;
        }
        if !self.row_graph_insert_allowed(tx, table, &values, read_memory.clone())? {
            return Ok(RowId(0));
        }
        let assume_no_state_machine =
            if trigger_callback_bound && !self.has_access_constraints_for_query() {
                !self.trigger_insert_table_has_state_machine(table)?
            } else {
                false
            };
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(
            table,
            row_id,
            &values,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        let inserted = self.insert_original_relational_row(
            host,
            tx,
            table,
            row_id,
            values,
            self.snapshot_for_read(),
            assume_no_state_machine.then(|| self.trigger_callback_wallclock()),
            assume_no_state_machine,
            admission,
            read_memory.clone(),
        )?;
        if let Some(key) = creation_key {
            self.stage_local_creation_lineage(tx, table, key, inserted, read_memory)?;
        }
        Ok(inserted)
    }

    /// Construct genuine independent vector originals before any row slot is
    /// blanked or staged. The selected companion admits every target first.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_original_row_vectors(
        &self,
        tx: TxId,
        table: &str,
        row: RowId,
        values: &HashMap<ColName, Value>,
        meta: &TableMeta,
    ) -> Result<OriginalDatabaseVectors> {
        self.with_preparation_original(|original| {
            let count = meta
                .columns
                .iter()
                .filter(|column| {
                    matches!(column.column_type, ColumnType::Vector(_))
                        && matches!(values.get(&column.name), Some(Value::Vector(_)))
                })
                .count();
            let mut bytes = std::alloc::Layout::array::<OriginalDatabaseVector>(count)
                .map_err(|_| Error::Other("original vector control size overflow".into()))?
                .size();
            for column in &meta.columns {
                if column.quantization == VectorQuantization::F32
                    && matches!(column.column_type, ColumnType::Vector(_))
                    && matches!(values.get(&column.name), Some(Value::Vector(_)))
                {
                    bytes = bytes.checked_add(column.name.len()).ok_or_else(|| {
                        Error::Other("original vector control size overflow".into())
                    })?;
                }
            }
            let control = crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "vector",
                "original_row_vector_controls",
                "Reduce staged vectors or raise MEMORY_LIMIT.",
            )?;
            let mut result = OriginalDatabaseVectors {
                entries: Vec::with_capacity(count),
                control,
            };
            if result.entries.capacity() != count {
                return Err(Error::Other("source native capacity mismatch".into()));
            }
            for column in &meta.columns {
                if !matches!(column.column_type, ColumnType::Vector(_)) {
                    continue;
                }
                let Some(Value::Vector(vector)) = values.get(&column.name) else {
                    continue;
                };
                let admission = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    crate::memory_accounting::preparation_vector_values_copy_bytes(
                        table,
                        &column.name,
                        vector.len(),
                    )?,
                    "vector",
                    "original_row_vector_body",
                    "Reduce staged vectors or raise MEMORY_LIMIT.",
                )?;
                let body = OriginalDatabaseVector {
                    entry: crate::memory_accounting::preparation_copy_vector_values(
                        table,
                        &column.name,
                        row,
                        vector.as_slice(),
                        tx,
                    )?,
                    scalar: column.quantization == VectorQuantization::F32,
                    cancellation: None,
                    admission,
                };
                self.vector_store
                    .validate_vector(&body.entry.index, body.entry.vector.len())?;
                result.entries.push(body);
            }
            Ok(result)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_original_row_vectors(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        mut vectors: OriginalDatabaseVectors,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Inputs have all passed validation. Preserve the existing final
        // placement for duplicate index/row inputs, retiring superseded actual
        // bodies before their invoices; array capacity keeps its original owner.
        let mut position = 0;
        while position < vectors.entries.len() {
            let index = &vectors.entries[position].entry.index;
            let row = vectors.entries[position].entry.row_id;
            if vectors.entries[position + 1..]
                .iter()
                .any(|later| later.entry.index == *index && later.entry.row_id == row)
            {
                drop(vectors.entries.remove(position));
            } else {
                position += 1;
            }
        }
        // Epoch observation stays outside the active guard. The caller's
        // paired frame retains/restores these metadata mutations on refusal.
        let mut read_owner = crate::vector_read_memory::admission(read_memory);
        for original in &mut vectors.entries {
            self.record_vector_schema_epoch(tx, &original.entry.index)?;
            let live = self.vector_store.has_live_entry_for_row(
                &original.entry.index,
                original.entry.row_id,
                self.snapshot_for_read(),
                &mut read_owner,
            )?;
            original.cancellation = Some(self.prepare_original_vector_cancellation(
                tx,
                &original.entry.index,
                original.entry.row_id,
                live,
            )?);
        }
        // Keep array/name controls outside the iterator and all lower calls.
        let _control = vectors.control;
        let entries = vectors.entries;
        host.transactions.with_transactions(|lower| {
            lower.with_engine(tx, |guard| {
                guard.with_tx_checkpoint(|guard| {
                    for mut original in entries {
                        let cancellation = original
                            .cancellation
                            .take()
                            .expect("prepared vector cancellation");
                        cancellation.with_value(|plan| {
                            self.publish_original_vector_cancellation(
                                guard,
                                &original.entry.index,
                                original.entry.row_id,
                                plan,
                            )
                        })?;
                        let scalar = original.scalar.then(|| {
                            (
                                original.entry.row_id,
                                original.entry.index.column.as_str().to_owned(),
                            )
                        });
                        let invoice = original
                            .admission
                            .into_source_vector_invoice_for(&host.funder)?;
                        let mutation = guard.prepare_vector(original.entry, scalar, invoice)?;
                        guard.publish(mutation);
                    }
                    Ok(())
                })
            })
        })
    }

    /// Identity and time stay at the original caller. Only publication changes:
    /// the original invoice is adopted before the hidden relational producer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_original_relational_row(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        table: &str,
        row_id: RowId,
        mut values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        created_at: Option<Wallclock>,
        assume_no_state_machine: bool,
        admission: &mut Option<OwnedMemoryReservation>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !assume_no_state_machine
            && let Some(sm) = &meta.state_machine
            && let Some(Value::Text(new_status)) = values.get(&sm.column)
            && let Some(id @ Value::Uuid(_)) = values.get("id")
            && let Some(existing) = self.relational_point_with_tx(
                Some(tx),
                table,
                "id",
                id,
                snapshot,
                read_memory.clone(),
            )?
        {
            let old_status = existing
                .values
                .get(&sm.column)
                .and_then(Value::as_text)
                .unwrap_or("");
            if !RelationalStore::validate_state_transition_for_meta(
                &meta, &sm.column, old_status, new_status,
            ) {
                return Err(Error::InvalidStateTransition(format!(
                    "{old_status} -> {new_status}"
                )));
            }
        }
        let vectors = self.prepare_original_row_vectors(tx, table, row_id, &values, &meta)?;
        self.blank_quantized_vector_values(table, &mut values);
        let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
        let result = (|| {
            self.stage_row_graph_insert(tx, table, row_id, &values, read_memory.clone())?;
            let row = VersionedRow {
                row_id,
                values,
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
                created_at: Some(created_at.unwrap_or_else(Wallclock::now)),
            };
            let invoice = admission
                .take()
                .ok_or_else(|| DatabaseTransactionHost::producer_gap("original row reservation"))?;
            let inserted =
                host.stage_original_row(self.relational_store.clone(), tx, table, row, invoice)?;
            self.stage_original_row_vectors(host, tx, vectors, read_memory.clone())?;
            Ok(inserted)
        })();
        if result.is_err() || result.as_ref().is_ok_and(|id| *id == RowId(0)) {
            self.restore_write_set_checkpoint(tx, checkpoint)?;
        } else {
            checkpoint.settle();
        }
        result
    }

    /// SQL keeps an unused reservation on its duplicate-UNIQUE no-op path.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_with_unique_noop_owned(
        &self,
        tx: TxId,
        table: &str,
        mut values: HashMap<ColName, Value>,
        admission: &mut Option<OwnedMemoryReservation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<InsertRowResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
            return self.insert_row_with_unique_noop(
                tx,
                table,
                values,
                vector_copy_owner,
                read_memory,
            );
        };
        self.ensure_trigger_table_ready(table, "insert_row")?;
        self.complete_insert_access_values(table, &mut values)?;
        let allow_duplicate_unique_noop =
            !self.table_meta(table).is_some_and(|meta| meta.immutable);
        match self.check_row_constraints(
            tx,
            table,
            &values,
            None,
            allow_duplicate_unique_noop,
            read_memory.clone(),
        )? {
            RowConstraintCheck::DuplicateUniqueNoOp => Ok(InsertRowResult::NoOp),
            RowConstraintCheck::Valid => {
                let row_id = self.relational_store.new_row_id();
                self.assert_row_write_allowed(
                    table,
                    row_id,
                    &values,
                    self.snapshot_for_read(),
                    read_memory.clone(),
                )?;
                let natural_key = self
                    .table_meta(table)
                    .and_then(|meta| natural_key_from_row_values(&meta, &values));
                let inserted = handle.with_host(|host| {
                    self.insert_original_relational_row(
                        host,
                        tx,
                        table,
                        row_id,
                        values,
                        self.snapshot_for_read(),
                        None,
                        false,
                        admission,
                        read_memory.clone(),
                    )
                })?;
                if let Some(key) = natural_key {
                    self.stage_local_creation_lineage(tx, table, key, inserted, read_memory)?;
                }
                Ok(InsertRowResult::Inserted(inserted))
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_row_within_operation(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        trigger_callback_bound: bool,
        admission: &mut Option<OwnedMemoryReservation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Statement-scoped bound: `Value::TxId(n)` must satisfy
        // `n <= max(committed_watermark, tx)` so writes inside an active
        // transaction can reference their own allocated TxId. The error,
        // when fired, still reports `committed_watermark` per plan B7.
        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_primary_key_values_present(table, &values)?;
        let creation_key = self
            .table_meta(table)
            .and_then(|meta| natural_key_from_row_values(&meta, &values));
        if !trigger_callback_bound {
            self.validate_row_constraints(tx, table, &values, None, read_memory.clone())?;
        }
        if !self.row_graph_insert_allowed(tx, table, &values, read_memory.clone())? {
            return Ok(RowId(0));
        }
        // Taken before the values are handed to the relational store, which
        // moves them; the store that owns vectors is written the moment the
        // row exists to carry them.
        let row_vectors_source = values.clone();
        self.blank_quantized_vector_values(table, &mut values);
        let staged_before = self.staged_row_count(tx);
        if trigger_callback_bound && !self.has_access_constraints_for_query() {
            let has_state_machine = self.trigger_insert_table_has_state_machine(table)?;
            let row_id = self.relational_store.new_row_id();
            self.assert_row_write_allowed(
                table,
                row_id,
                &values,
                self.snapshot_for_read(),
                read_memory.clone(),
            )?;
            if !has_state_machine {
                let inserted = self.insert_relational_row_from_read(
                    tx,
                    table,
                    row_id,
                    values,
                    self.snapshot_for_read(),
                    Some(self.trigger_callback_wallclock()),
                    read_memory.clone(),
                )?;
                if let Some(admission) = admission.take() {
                    self.record_owned_row_admission(tx, table, inserted, admission);
                }
                self.write_row_vectors_or_unstage(
                    tx,
                    table,
                    inserted,
                    &row_vectors_source,
                    staged_before,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if let Some(key) = creation_key {
                    self.stage_local_creation_lineage(
                        tx,
                        table,
                        key,
                        inserted,
                        read_memory.clone(),
                    )?;
                }
                return Ok(inserted);
            }
            let inserted = self.insert_relational_row_from_read(
                tx,
                table,
                row_id,
                values,
                self.snapshot_for_read(),
                None,
                read_memory.clone(),
            )?;
            if let Some(admission) = admission.take() {
                self.record_owned_row_admission(tx, table, inserted, admission);
            }
            self.write_row_vectors_or_unstage(
                tx,
                table,
                inserted,
                &row_vectors_source,
                staged_before,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            if let Some(key) = creation_key {
                self.stage_local_creation_lineage(tx, table, key, inserted, read_memory.clone())?;
            }
            return Ok(inserted);
        }
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(
            table,
            row_id,
            &values,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        let inserted = self.insert_relational_row_from_read(
            tx,
            table,
            row_id,
            values,
            self.snapshot_for_read(),
            None,
            read_memory.clone(),
        )?;
        if let Some(admission) = admission.take() {
            self.record_owned_row_admission(tx, table, inserted, admission);
        }
        self.write_row_vectors_or_unstage(
            tx,
            table,
            inserted,
            &row_vectors_source,
            staged_before,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        if let Some(key) = creation_key {
            self.stage_local_creation_lineage(tx, table, key, inserted, read_memory.clone())?;
        }
        Ok(inserted)
    }

    /// UPDATE-aware insert: the UPDATE path first deletes the old row and
    /// then re-inserts. The constraint probe must skip the old row_id so the
    /// same PK does not self-collide. The old row's index entry still looks
    /// visible at the committed-watermark snapshot because its `deleted_tx`
    /// equals the current (uncommitted) `tx`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_replacing(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        old_row_id: RowId,
        place_vectors: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.ensure_trigger_table_ready(table, "insert_row_replacing")?;
        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_primary_key_values_present(table, &values)?;
        if !self.trigger_callback_tx_bound_matches(tx) {
            self.validate_row_constraints(
                tx,
                table,
                &values,
                Some(old_row_id),
                read_memory.clone(),
            )?;
        }
        self.assert_row_write_allowed(
            table,
            old_row_id,
            &values,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        // The UPDATE path places every vector column itself -- an assigned one
        // is deleted and re-inserted, one the statement did not touch is moved
        // to the new row identity -- so it asks for none to be placed here and
        // each is stored once. An upsert that resolves onto an existing row
        // has no such handling of its own and asks for them.
        let row_vectors = if place_vectors {
            self.row_vector_values(table, &values)
        } else {
            Vec::new()
        };
        self.blank_quantized_vector_values(table, &mut values);
        let inserted = self.replace_row_after_update_validation(
            tx,
            table,
            old_row_id,
            values,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        self.write_row_vectors(
            tx,
            inserted,
            row_vectors,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(inserted)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_replacing_owned(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        old_row_id: RowId,
        place_vectors: bool,
        admission: OwnedMemoryReservation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let original = OriginalRowValues { values, admission };
        let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
            let inserted = self.insert_row_replacing(
                tx,
                table,
                original.values,
                old_row_id,
                place_vectors,
                vector_copy_owner,
                read_memory,
            )?;
            self.record_owned_row_admission(tx, table, inserted, original.admission);
            return Ok(inserted);
        };
        self.ensure_trigger_table_ready(table, "insert_row_replacing")?;
        let mut values = self.coerce_row_for_insert(
            table,
            original.values,
            Some(self.committed_watermark()),
            Some(tx),
        )?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_primary_key_values_present(table, &values)?;
        if !self.trigger_callback_tx_bound_matches(tx) {
            self.validate_row_constraints(
                tx,
                table,
                &values,
                Some(old_row_id),
                read_memory.clone(),
            )?;
        }
        self.assert_row_write_allowed(
            table,
            old_row_id,
            &values,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        // UPDATE owns its vector leg separately; an upsert asks this producer
        // to place every supplied vector exactly once, as the ordinary path does.
        let vectors = place_vectors
            .then(|| self.prepare_original_row_vectors(tx, table, old_row_id, &values, &meta))
            .transpose()?;
        self.blank_quantized_vector_values(table, &mut values);
        let original = OriginalRowValues {
            values,
            admission: original.admission,
        };
        handle.with_host(|host| {
            // Capture complete physical owners, not independent body/credit
            // fields. A refused checkpoint drops these paired owners intact.
            #[allow(
                clippy::redundant_locals,
                reason = "capture the complete body-and-invoice owner before fallible work"
            )]
            let original = original;
            #[allow(
                clippy::redundant_locals,
                reason = "capture the complete vector-and-control owner before fallible work"
            )]
            let vectors = vectors;
            let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
            let result = (|| {
                #[allow(
                    clippy::redundant_locals,
                    reason = "keep closure capture custody structural"
                )]
                let original = original;
                let inserted = self.replace_row_after_update_validation_owned(
                    tx,
                    table,
                    old_row_id,
                    original.values,
                    self.snapshot_for_read(),
                    original.admission,
                    read_memory.clone(),
                )?;
                if let Some(vectors) = vectors {
                    self.stage_original_row_vectors(host, tx, vectors, read_memory.clone())?;
                }
                Ok(inserted)
            })();
            if result.is_err() {
                self.restore_write_set_checkpoint(tx, checkpoint)?;
            } else {
                checkpoint.settle();
            }
            result
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_row_after_update_validation_owned(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        admission: OwnedMemoryReservation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let original = OriginalRowValues { values, admission };
        let DatabaseTransactionHost::Owned(_) = &self.tx_mgr else {
            let inserted = self.replace_row_after_update_validation(
                tx,
                table,
                row_id,
                original.values,
                snapshot,
                read_memory,
            )?;
            self.record_owned_row_admission(tx, table, inserted, original.admission);
            return Ok(inserted);
        };
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !self.trigger_callback_tx_bound_matches(tx) {
            self.validate_row_constraints(
                tx,
                table,
                &original.values,
                Some(row_id),
                read_memory.clone(),
            )?;
        }
        let committed_row_exists = self
            .committed_row_at(table, row_id, snapshot, read_memory)?
            .is_some();
        self.replace_row_after_update_validation_counted_owned(
            tx,
            table,
            row_id,
            original.values,
            UpdateReplacementContext {
                meta: &meta,
                committed_row_exists,
                created_at: Wallclock::now(),
            },
            original.admission,
        )
        .map(|(row, _, _)| row)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_row_after_update_validation_counted_owned(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        context: UpdateReplacementContext<'_>,
        admission: OwnedMemoryReservation,
    ) -> Result<(RowId, WriteSetCounts, WriteSetCounts)> {
        let original = OriginalRowValues { values, admission };
        match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(_) => {
                let outcome = self.replace_row_after_update_validation_counted(
                    tx,
                    table,
                    row_id,
                    original.values,
                    context,
                )?;
                self.record_owned_row_admission(tx, table, outcome.0, original.admission);
                Ok(outcome)
            }
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                self.replace_original_row_counted(host, tx, table, row_id, original, context)
            }),
        }
    }

    /// Sequential replacement keeps every inverse in the existing paired
    /// checkpoint. No empty-current candidate or bare WriteSet is imported.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_original_row_counted(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        table: &str,
        row_id: RowId,
        mut original: OriginalRowValues,
        context: UpdateReplacementContext<'_>,
    ) -> Result<(RowId, WriteSetCounts, WriteSetCounts)> {
        self.validate_primary_key_values_present(table, &original.values)?;
        if context.meta.immutable {
            return Err(Error::ImmutableTable(table.to_string()));
        }
        // Keep the existing source coordinates. Only the obsolete numeric
        // admission hint is removed; native row invoices own the actual body.
        let sidecars = self.tx_mgr.with_read(tx, |read| {
            (
                read.relational_insert_source_lsns
                    .get(table)
                    .and_then(|rows| rows.get(&row_id))
                    .copied(),
                read.relational_insert_source_kinds
                    .get(table)
                    .and_then(|rows| rows.get(&row_id))
                    .copied(),
                read.relational_insert_admitted_bytes
                    .get(table)
                    .is_some_and(|rows| rows.contains_key(&row_id)),
            )
        })?;
        if sidecars.2 && sidecars.0.is_some() != sidecars.1.is_some() {
            return Err(DatabaseTransactionHost::producer_gap(
                "replacement provenance sidecar successor",
            ));
        }
        self.blank_quantized_vector_values(table, &mut original.values);
        let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
        let result = (|| {
            let before = self.write_set_counts(tx)?;
            let read_memory = self.declared_reader_memory()?;
            self.retire_row_graph(tx, table, row_id, read_memory.clone())?;
            self.stage_row_graph_insert(tx, table, row_id, &original.values, read_memory)?;
            host.transactions.with_transactions(|transactions| {
                transactions.with_engine(tx, |guard| {
                    let removed = guard.prepare_remove(table, row_id)?;
                    guard.publish(removed);
                    if sidecars.2 {
                        let coordinates = sidecars.0.zip(sidecars.1);
                        let successor = guard.prepare_sidecars(table, row_id, coordinates, None)?;
                        guard.publish_tx(successor);
                    }
                    Ok(())
                })
            })?;
            if context.committed_row_exists {
                self.stage_original_relational_delete(host, tx, table, row_id)?;
            }
            let row = VersionedRow {
                row_id,
                values: original.values,
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
                created_at: Some(context.created_at),
            };
            let inserted = host.stage_original_row(
                self.relational_store.clone(),
                tx,
                table,
                row,
                original.admission,
            )?;
            let after = self.write_set_counts(tx)?;
            Ok((inserted, before, after))
        })();
        if result.is_err() {
            self.restore_write_set_checkpoint(tx, checkpoint)?;
        } else {
            checkpoint.settle();
        }
        result
    }

    /// The deletion's own original string is admitted before allocation; the
    /// guarded splice also admits its controls before native publication.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_original_relational_delete(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        table: &str,
        row_id: RowId,
    ) -> Result<()> {
        if host.transactions.with_transactions(|lower| {
            lower.with_read(tx, |read| {
                read.relational_deletes
                    .iter()
                    .any(|(name, id, _)| name == table && *id == row_id)
            })
        })? {
            return Ok(());
        }
        struct OriginalDelete {
            body: (TableName, RowId, TxId),
            admission: OwnedMemoryReservation,
        }
        // Lend the authentic companion and finish input admission before the
        // active-map guard. An unused pair retires after that guard closes.
        let mut original = host.events.with_owned_persistence_original(|original| {
            let admission = crate::memory_accounting::reserve_preparation_from_original(
                original,
                table.len(),
                "relational",
                "staged_row_delete",
                "Reduce staged deletions or raise MEMORY_LIMIT.",
            )?;
            Ok(OriginalDelete {
                body: (table.to_owned(), row_id, tx),
                admission,
            })
        })?;
        let invoice = original
            .admission
            .into_source_aux_invoice_for(&host.funder)?;
        let mut input = Some((original.body, invoice));
        host.transactions.with_transactions(|transactions| {
            transactions.with_engine(tx, |guard| {
                let read = guard.read();
                if read
                    .relational_deletes
                    .iter()
                    .any(|(name, id, _)| name == table && *id == row_id)
                {
                    return Ok(());
                }
                let at = read.relational_deletes.len();
                let (body, invoice) = input.take().expect("one original delete input");
                let batch = guard.prepare_owned_aux(
                    contextdb_tx::staged_source::AuxBody::RelationalDelete(body),
                    invoice,
                )?;
                let prepared = guard.prepare_aux_splice(at, &[], batch)?;
                guard.publish_tx(prepared);
                Ok(())
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_row_after_update_validation(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !self.trigger_callback_tx_bound_matches(tx) {
            self.validate_row_constraints(tx, table, &values, Some(row_id), read_memory.clone())?;
        }
        let committed_row_exists = self
            .committed_row_at(table, row_id, snapshot, read_memory.clone())?
            .is_some();
        self.replace_row_after_update_validation_counted(
            tx,
            table,
            row_id,
            values,
            UpdateReplacementContext {
                meta: &meta,
                committed_row_exists,
                created_at: Wallclock::now(),
            },
        )
        .map(|(row, _, _)| row)
    }

    /// A relational delete discards a same-transaction insert before commit.
    /// Return that insert's admission now, since commit/rollback will no
    /// longer find it in the write set to settle the charge.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_relational_row_releasing_staged(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = (|| {
            self.retire_row_graph(tx, table, row_id, read_memory.clone())?;
            self.delete_relational_row_releasing_staged_inner(tx, table, row_id, read_memory)
        })();
        if result.is_err() {
            self.restore_write_set_checkpoint(tx, checkpoint)?;
        } else {
            checkpoint.settle();
        }
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_relational_row_releasing_staged_inner(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                self.note_private_pending_row_replacement(tx, table, row_id)?;
                let committed = self
                    .committed_row_at(table, row_id, self.snapshot_for_read(), read_memory)?
                    .is_some();
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        let removal = guard.prepare_remove(table, row_id)?;
                        guard.publish(removal);
                        Ok(())
                    })
                })?;
                if committed {
                    self.stage_original_relational_delete(host, tx, table, row_id)?;
                }
                Ok(())
            });
        }
        let meta = self.relational_store.table_meta(table);
        self.note_private_pending_row_replacement(tx, table, row_id)?;
        #[cfg(any(test, feature = "test-seams"))]
        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            if let Some(hook) = slot.borrow().as_ref() {
                (hook.observe)(StagedDeleteRacePointForTest::BeforeRetirement);
            }
        });
        #[cfg(any(test, feature = "test-seams"))]
        if STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            slot.borrow()
                .as_ref()
                .is_some_and(|hook| hook.mode == StagedDeleteRaceModeForTest::SplitLock)
        }) {
            return self.delete_relational_row_split_lock_for_test(
                tx,
                table,
                row_id,
                meta.as_ref(),
            );
        }
        let mut transferred_control = Some(
            crate::memory_accounting::TransferredRowReservation::prepare_control(
                &self.finalization.accountant,
            )?,
        );
        let charge = self.relational.delete_retiring_staged_charge_with_undo(
            tx,
            table,
            row_id,
            self.committed_row_at(table, row_id, self.snapshot_for_read(), read_memory.clone())?
                .is_some(),
            |row, admitted| {
                #[cfg(any(test, feature = "test-seams"))]
                STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
                    if let Some(hook) = slot.borrow().as_ref() {
                        (hook.observe)(StagedDeleteRacePointForTest::AdmissionReadObserved);
                    }
                });
                admitted.unwrap_or_else(|| {
                    retained_row_bytes_with_optional_meta(table, row, meta.as_ref())
                })
            },
            |bytes| {
                Some(
                    crate::memory_accounting::TransferredRowReservation::from_admitted_with_control(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        bytes,
                        transferred_control
                            .take()
                            .expect("one selected staged-row transfer"),
                    ) as Arc<dyn contextdb_tx::StagedPostingCharge>,
                )
            },
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            if let Some(hook) = slot.borrow().as_ref() {
                (hook.observe)(StagedDeleteRacePointForTest::RetiredCharge(charge));
            }
        });
        if charge != 0 {
            self.finalization.accountant.release(charge);
        }
        Ok(())
    }

    /// The former three-lock sequence, reachable only through the test hook.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn delete_relational_row_split_lock_for_test(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        meta: Option<&TableMeta>,
    ) -> Result<()> {
        let (removed, charge) = self.tx_mgr.with_write_set(tx, |ws| {
            let staged = ws
                .relational_inserts
                .iter()
                .filter(|(staged_table, row)| staged_table == table && row.row_id == row_id);
            staged.fold((0usize, 0usize), |(count, bytes), (_, row)| {
                let admitted = ws
                    .admitted_bytes_for_staged_row(table, row_id)
                    .unwrap_or_else(|| retained_row_bytes_with_optional_meta(table, row, meta));
                (count + 1, bytes.saturating_add(admitted))
            })
        })?;
        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            if let Some(hook) = slot.borrow().as_ref() {
                (hook.observe)(StagedDeleteRacePointForTest::AdmissionReadObserved);
            }
        });
        self.relational.delete(tx, table, row_id)?;
        if removed != 0 {
            self.finalization.accountant.release(charge);
            self.tx_mgr.with_write_set(tx, |ws| {
                ws.forget_relational_insert_admitted_bytes(table, row_id);
            })?;
        }
        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            if let Some(hook) = slot.borrow().as_ref() {
                (hook.observe)(StagedDeleteRacePointForTest::RetiredCharge(charge));
            }
        });
        Ok(())
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn write_set_lock_is_held_for_test(&self) -> bool {
        self.tx_mgr.write_set_lock_is_held_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn delete_relational_row_releasing_staged_for_test(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.delete_relational_row_releasing_staged(tx, table, row_id, read_memory.clone())
    }

    /// Observe the real delete path at the two sides of its write-set lock.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn delete_relational_row_with_race_hook_for_test(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        mode: StagedDeleteRaceModeForTest,
        hook: impl Fn(StagedDeleteRacePointForTest) + 'static,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(StagedDeleteRaceHookForTest {
                mode,
                observe: Box::new(hook),
            });
        });
        let result =
            self.delete_relational_row_releasing_staged(tx, table, row_id, read_memory.clone());
        STAGED_DELETE_RACE_HOOK_FOR_TEST.with(|slot| {
            slot.borrow_mut().take();
        });
        result
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn staged_row_id_for_test(&self, tx: TxId, table: &str) -> Result<Option<RowId>> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts
                .iter()
                .find(|(staged_table, _)| staged_table == table)
                .map(|(_, row)| row.row_id)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_row_after_update_validation_counted(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        context: UpdateReplacementContext<'_>,
    ) -> Result<(RowId, WriteSetCounts, WriteSetCounts)> {
        let checkpoint = self.write_set_checkpoint(tx)?;
        let before = self.write_set_counts(tx)?;
        let result = (|| {
            let read_memory = self.declared_reader_memory()?;
            self.retire_row_graph(tx, table, row_id, read_memory.clone())?;
            self.stage_row_graph_insert(tx, table, row_id, &values, read_memory)?;
            let (id, _, _) = self.replace_row_after_update_validation_counted_inner(
                tx, table, row_id, values, context,
            )?;
            Ok((id, before, self.write_set_counts(tx)?))
        })();
        if result.is_err() {
            self.restore_write_set_checkpoint(tx, checkpoint)?;
        } else {
            checkpoint.settle();
        }
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_row_after_update_validation_counted_inner(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        context: UpdateReplacementContext<'_>,
    ) -> Result<(RowId, WriteSetCounts, WriteSetCounts)> {
        self.validate_primary_key_values_present(table, &values)?;
        if context.meta.immutable {
            return Err(Error::ImmutableTable(table.to_string()));
        }
        let mut values = values;
        self.blank_quantized_vector_values(table, &mut values);
        let table_name = table.to_string();
        let row = VersionedRow {
            row_id,
            values,
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(0),
            created_at: Some(context.created_at),
        };
        let (outcome, retired_charge) = self.tx_mgr.with_write_set(tx, |ws| {
            self.relational.stage_ordered_row_from_descriptor(
                ws,
                table,
                &row,
                ws.relational_inserts.len(),
                context.meta,
            )?;
            let before = current_write_set_counts(ws);
            let retired_charge = ws
                .relational_inserts
                .iter()
                .filter(|(staged_table, staged_row)| {
                    staged_table == &table_name && staged_row.row_id == row_id
                })
                .map(|(_, staged_row)| {
                    ws.admitted_bytes_for_staged_row(table, staged_row.row_id)
                        .unwrap_or_else(|| {
                            retained_row_bytes_for_meta(
                                table,
                                &staged_row.values,
                                context.meta,
                                true,
                            )
                        })
                })
                .sum::<usize>();
            let prior_admitted = ws.admitted_bytes_for_staged_row(table, row_id);
            let deferred = if ws.ordered_staged_rows.checkpoint_active() {
                let transferred = (ws.ordered_staged_rows.contains_row(table, row_id)
                    && ws
                        .relational_inserts
                        .iter()
                        .any(|(name, row)| name == table && row.row_id == row_id))
                .then(|| {
                    crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        retired_charge,
                    )
                    .map(|owner| owner as Arc<dyn contextdb_tx::StagedPostingCharge>)
                })
                .transpose()?;
                ws.ordered_staged_rows.retain_displaced(
                    &mut ws.relational_inserts,
                    table,
                    row_id,
                    prior_admitted,
                    transferred,
                )
            } else {
                ws.relational_inserts.retain(|(staged_table, staged_row)| {
                    !(staged_table == &table_name && staged_row.row_id == row_id)
                });
                false
            };
            if retired_charge != 0 {
                ws.forget_relational_insert_admitted_bytes(table, row_id);
            }
            if context.committed_row_exists
                && !ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == &table_name && *deleted_row_id == row_id
                    })
            {
                ws.relational_deletes.push((table_name.clone(), row_id, tx));
            }
            ws.relational_inserts.push((table_name, row));
            let after = current_write_set_counts(ws);
            ws.rebind_ordered_staged_rows();
            Ok::<_, Error>((
                (row_id, before, after),
                if deferred { 0 } else { retired_charge },
            ))
        })??;
        self.finalization.accountant.release(retired_charge);
        Ok(outcome)
    }

    /// Internal variant used by sync-apply: skips the TXID bound check because
    /// peer TxIds may legitimately exceed the local watermark. Still enforces
    /// wrong-variant + reverse-direction TXID column rules.
    /// Insert a row that arrived over sync. `created_at` is the birth time the
    /// row carried from the node that wrote it: retention judges a row by that
    /// stamp against the holder's own clock, so a replicated row keeps its age
    /// instead of being handed a fresh window on arrival. `None` (an origin
    /// that recorded no stamp) falls back to stamping the row here.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        created_at: Option<Wallclock>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let values = self.coerce_row_for_insert(table, values, None, None)?;
        self.validate_row_constraints(tx, table, &values, None, read_memory.clone())?;
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(
            table,
            row_id,
            &values,
            self.snapshot(),
            read_memory.clone(),
        )?;
        let mut values = values;
        let row_vectors = self.row_vector_values(table, &values);
        self.blank_quantized_vector_values(table, &mut values);
        let inserted = self.insert_synced_row_at(
            tx,
            table,
            row_id,
            values,
            self.snapshot(),
            created_at,
            read_memory.clone(),
        )?;
        self.write_row_vectors(
            tx,
            inserted,
            row_vectors,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(inserted)
    }

    /// Upsert a row that arrived over sync. See [`Self::insert_row_for_sync`]
    /// for what `created_at` carries and why.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn upsert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        natural_key: &NaturalKey,
        values: HashMap<ColName, Value>,
        created_at: Option<Wallclock>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<UpsertResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let values = self.coerce_row_for_insert(table, values, None, None)?;
        let snapshot = self.snapshot_for_read();
        // Resolve the existing row on the WHOLE natural key — every key column —
        // so a composite identity is matched in full rather than collapsed onto a
        // leading-column probe that resolves the wrong row and drops the update.
        // A single-column key routes through the same point lookup as before.
        let key_columns = natural_key.key_columns();
        let key_values: Option<Vec<Value>> = key_columns
            .iter()
            .map(|column| values.get(column).cloned())
            .collect();
        let existing_row = match key_values {
            Some(key_values) => self.conflict_lookup_in_tx(
                tx,
                table,
                &key_columns,
                &key_values,
                snapshot,
                read_memory.clone(),
            )?,
            None => None,
        };
        let existing_row_id = existing_row.as_ref().map(|row| row.row_id);

        if let (Some(existing), Some(meta)) = (existing_row.as_ref(), self.table_meta(table)) {
            for col_def in meta.columns.iter().filter(|c| c.immutable) {
                let Some(incoming) = values.get(&col_def.name) else {
                    continue;
                };
                let existing_value = existing.values.get(&col_def.name);
                if existing_value != Some(incoming) {
                    return Err(Error::ImmutableColumn {
                        table: table.to_string(),
                        column: col_def.name.clone(),
                    });
                }
            }
        }

        self.validate_row_constraints(tx, table, &values, existing_row_id, read_memory.clone())?;

        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let meta = self.table_meta(table);
        let new_state = meta
            .as_ref()
            .and_then(|m| m.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        if let Some(existing) = existing_row.as_ref() {
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &existing.values,
                snapshot,
                read_memory.clone(),
            )?;
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &values,
                snapshot,
                read_memory.clone(),
            )?;
            let changed = values
                .iter()
                .any(|(column, value)| existing.values.get(column) != Some(value));
            if !changed {
                return Ok(UpsertResult::NoOp);
            }
            // Sync carries a complete row, including its current state.  A
            // scalar-only change must not be treated as an explicit
            // `state = state` transition: local SQL keeps that stricter
            // contract and validates an assigned state column separately.
            let state_text_pair = meta
                .as_ref()
                .and_then(|table_meta| table_meta.state_machine.as_ref())
                .map(|state_machine| {
                    (
                        existing
                            .values
                            .get(&state_machine.column)
                            .and_then(Value::as_text),
                        values.get(&state_machine.column).and_then(Value::as_text),
                    )
                });
            let carried_state_unchanged_text = matches!(
                state_text_pair,
                Some((Some(old_state), Some(incoming_state))) if old_state == incoming_state
            );
            let state_changed_text = matches!(
                state_text_pair,
                Some((Some(old_state), Some(incoming_state))) if old_state != incoming_state
            );
            // An absent or malformed state is never a no-op: it must retain
            // the normal validator/constraint path. Only two equal text
            // values mean sync carried the existing state unchanged.
            if !carried_state_unchanged_text {
                self.validate_commit_time_upsert_state_transition(table, existing, &values)?;
            }
            self.delete_relational_row_releasing_staged(
                tx,
                table,
                existing.row_id,
                read_memory.clone(),
            )?;
            self.insert_synced_row_at(
                tx,
                table,
                existing.row_id,
                values,
                snapshot,
                created_at,
                read_memory.clone(),
            )?;
            if state_changed_text
                && let (Some(uuid), Some(state), Some(_meta)) =
                    (row_uuid, new_state.as_deref(), meta.as_ref())
            {
                self.propagate_state_change_if_needed(
                    tx,
                    table,
                    Some(uuid),
                    Some(state),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            return Ok(UpsertResult::Updated);
        }

        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, snapshot, read_memory.clone())?;
        self.insert_synced_row_at(
            tx,
            table,
            row_id,
            values,
            snapshot,
            created_at,
            read_memory.clone(),
        )?;
        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
        {
            self.propagate_state_change_if_needed(
                tx,
                table,
                Some(uuid),
                Some(state),
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        Ok(UpsertResult::Inserted)
    }

    /// Stage a synced row, honouring the birth time it carried when it has
    /// one and stamping locally when it does not.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_synced_row_at(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        created_at: Option<Wallclock>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RowId> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_primary_key_values_present(table, &values)?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        let row_bytes = self.admit_retained_row_bytes(
            table,
            &values,
            &meta,
            "sync",
            "insert_received_row",
            "Reduce sync batch size or raise MEMORY_LIMIT before receiving rows.",
        )?;
        let inserted = match created_at {
            Some(created_at) => self.insert_relational_row_from_read(
                tx,
                table,
                row_id,
                values,
                snapshot,
                Some(created_at),
                read_memory.clone(),
            ),
            None => self.insert_relational_row_from_read(
                tx,
                table,
                row_id,
                values,
                snapshot,
                None,
                read_memory.clone(),
            ),
        };
        match &inserted {
            Ok(row_id) => {
                self.record_staged_row_admission(tx, table, *row_id, row_bytes);
                self.note_private_source_row_change(tx, table, *row_id)?;
            }
            Err(_) => self.finalization.accountant.release(row_bytes),
        }
        inserted
    }

    /// Whether this table's row for `natural_key` currently holds a version
    /// that ARRIVED over sync rather than one written here. The marker is set
    /// when a sync apply stages the row and cleared by any local write to it
    /// (see `sync_source_lsn_updates`), so it describes the CURRENT version,
    /// never the row's history: a pulled row that was then edited locally
    /// reads `false`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn row_version_arrived_by_sync(
        &self,
        table: &str,
        natural_key: &NaturalKey,
    ) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.row_version_arrived_by_sync_with_read_memory(table, natural_key, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn row_version_arrived_by_sync_with_read_memory(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let Some(row) = self.visible_row_by_natural_key(
            table,
            natural_key,
            self.snapshot_for_read(),
            &HashSet::new(),
            read_memory.clone(),
        )?
        else {
            return Ok(false);
        };
        let (_, kind) = self.relational_sync_source_at_read_root(table, row.row_id, read_memory)?;
        Ok(kind.is_some_and(|kind| {
            matches!(kind, contextdb_relational::store::SyncSourceKind::Pulled)
        }))
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Mark one existing live row as a pulled arrival without constructing a
    /// synthetic wire apply. Production-dead: destination-move tests use this
    /// to isolate echo selection from the independently authenticated lineage
    /// contract exercised by the transport journey.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn mark_row_arrived_by_sync_for_test(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        arrival_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let row_id = self
            .row_id_for_natural_key_full(table, natural_key, self.snapshot_for_read(), read_memory.clone())?
            .ok_or_else(|| {
                Error::SyncError((format!(
                    "cannot mark missing {table} row as a pulled sync arrival"
                )).into())
            })?;
        self.relational_store.set_sync_source_lsns([(
            table.to_string(),
            row_id,
            arrival_lsn,
        )]);
        self.relational_store.set_sync_source_kinds([(
            table.to_string(),
            row_id,
            contextdb_relational::store::SyncSourceKind::Pulled,
        )]);
        Ok(())
    }

    }

    /// Whether this exact outgoing row change was supplied by sync. Live rows
    /// use their relational provenance sidecar; deletes use their current
    /// tombstone marker or the durable accepted ancestry of a received delete.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn row_change_arrived_by_sync(&self, row: &RowChange) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.row_change_arrived_by_sync_with_read_memory(row, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn row_change_arrived_by_sync_with_read_memory(
        &self,
        row: &RowChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.checked_row_change_arrived_by_sync(row, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn checked_row_change_arrived_by_sync(
        &self,
        row: &RowChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        if !row.deleted {
            // The sidecar describes the row's CURRENT version, so it can only
            // answer for a change that carries that same version. A change at
            // some other position — a surviving row republished by a later
            // commit, whose stored version keeps its original one — is not what
            // the sidecar speaks about. The vector half of the same group
            // already refuses to answer there (`vector_change_arrived_by_sync`
            // requires this identical match), and a row and its vectors must be
            // classified as ONE group: dropping the row while its vector rides
            // strands that vector at a peer which never received the row it
            // belongs to.
            // A dropped generation has no current provenance projection. Its
            // retained authored occurrence must still travel with its DDL.
            if self.table_meta(&row.table).is_none() {
                return Ok(false);
            }
            let snapshot = self.snapshot_for_read();
            let current = self.visible_row_by_natural_key(
                &row.table,
                &row.natural_key,
                snapshot,
                &HashSet::new(),
                read_memory.clone(),
            )?;
            let Some(current) = current else {
                return Ok(false);
            };
            if current.lsn != row.lsn {
                return Ok(false);
            }
            return Ok(matches!(
                self.relational_sync_source_at_read_root(
                    &row.table,
                    current.row_id,
                    read_memory.clone()
                )?
                .1,
                Some(contextdb_relational::store::SyncSourceKind::Pulled)
            ));
        }
        if self
            .sync_tombstone_arrivals
            .read()
            .get(&Self::sync_tombstone_key(&row.table, &row.natural_key))
            .is_some_and(|marker| {
                matches!(
                    marker.kind,
                    contextdb_relational::store::SyncSourceKind::Pulled
                ) && (marker.delete_lsn == row.lsn
                    || marker.delete_lsn == SYNC_SOURCE_LSN_OWN_COMMIT)
            })
        {
            return Ok(true);
        }
        // Reopen does not restore the session's arrival-order guard. The
        // received delete's durable lineage still identifies its exact local
        // occurrence, so its old row history must not become pending work.
        let generation = self.durable_lineage_table_generation(&row.table, read_memory.clone())?;
        let key = Self::durable_lineage_config_key(&row.table, &row.natural_key, generation);
        let record = self.load_lineage_record(&self.lineage_state_lock.lock(), &key)?;
        let received_delete = record.is_some_and(|record| {
            record.table == row.table
                && record.natural_key == row.natural_key
                && record.table_generation == generation
                && record.delete_lsn == row.lsn.0
                && record.delete_obligation == DurableDeleteObligation::Accepted
                && record.accepted_hub_lsn.is_none()
                && !record.locally_created
        });
        // A new same-key row retires the old session marker; an older durable
        // delete must not hide that local work.
        if !received_delete {
            return Ok(false);
        }
        Ok(self
            .visible_row_by_natural_key(
                &row.table,
                &row.natural_key,
                self.snapshot_for_read(),
                &HashSet::new(),
                read_memory.clone(),
            )?
            .is_none())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn graph_arrival_config_key(record: &DurableGraphArrival) -> Result<String> {
        let identity = rmp_serde::to_vec(&(record.source, record.target, &record.edge_type))
            .map_err(|error| {
                Error::SyncError((format!("graph arrival identity encode failed: {error}")).into())
            })?;
        Ok(format!(
            "{SYNC_GRAPH_ARRIVAL_PREFIX}{}",
            blake3::hash(&identity).to_hex()
        ))
    }

    pub(super) fn graph_arrival_key(record: &DurableGraphArrival) -> GraphIdentity {
        (record.source, record.target, record.edge_type.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_graph_arrival_memory(
        &self,
        record: &DurableGraphArrival,
    ) -> Result<(GraphIdentity, FundedGraphArrival)> {
        // Both owned strings, the immutable Arc root and hash-table slack
        // are funded before either the key or the payload is cloned.
        let credit = contextdb_core::read_memory::ReadCredit::try_new(
            self.finalization.accountant.read_budget()?,
            record.edge_type.len().saturating_mul(2).saturating_add(
                std::mem::size_of::<(GraphIdentity, FundedGraphArrival)>()
                    .saturating_mul(4)
                    .saturating_add(std::mem::size_of::<GraphArrivalRoot>())
                    .saturating_add(128),
            ),
            "sync",
            "graph_arrival_memory",
            "Raise the declared memory limit to retain received graph provenance.",
        )?;
        let key = Self::graph_arrival_key(record);
        Ok((
            key,
            FundedGraphArrival(Arc::new(GraphArrivalRoot {
                record: record.clone(),
                _credit: credit,
            })),
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_pulled_graph_arrivals(
        &self,
        ws: &mut WriteSet,
        pulled: bool,
    ) -> Result<Vec<(GraphIdentity, FundedGraphArrival)>> {
        let Some(local_lsn) = ws.commit_lsn else {
            return Ok(Vec::new());
        };
        let mut records = ws
            .adj_deletes
            .iter()
            .map(|(source, edge_type, target, _)| DurableGraphArrival {
                source: *source,
                target: *target,
                edge_type: edge_type.clone(),
                local_lsn,
                deleted: true,
                pulled,
            })
            .collect::<Vec<_>>();
        // Storage applies deletes before inserts, so a same-commit
        // delete+reinsert is live. Insert markers must win that exact tie.
        records.extend(ws.adj_inserts.iter().map(|edge| DurableGraphArrival {
            source: edge.source,
            target: edge.target,
            edge_type: edge.edge_type.clone(),
            local_lsn,
            deleted: false,
            pulled,
        }));
        let markers = if self.persistence.is_none() {
            records
                .iter()
                .map(|record| self.prepare_graph_arrival_memory(record))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        for record in records {
            ws.config_writes.push((
                Self::graph_arrival_config_key(&record)?,
                RedbPersistence::encode_config_value(&record)?,
            ));
        }
        Ok(markers)
    }

    /// Whether this exact receiver-local graph occurrence came from sync.
    /// Insert and delete are distinct so a later local operation on the same
    /// `(source,target,type)` is never mistaken for an echo.
    /// A durable arrival lookup refusal is returned to the caller.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn graph_change_arrived_by_sync(&self, edge: &EdgeChange) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.graph_change_arrived_by_sync_with_read_memory(edge, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn graph_change_arrived_by_sync_with_read_memory(
        &self,
        edge: &EdgeChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let deleted = matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
        self.with_graph_arrival(
            &(edge.source, edge.target, edge.edge_type.clone()),
            |marker| {
                Ok(marker.is_some_and(|marker| {
                    marker.pulled && marker.local_lsn == edge.lsn && marker.deleted == deleted
                }))
            },
            read_memory.clone(),
        )
    }

    /// Whether this exact vector-owner mutation arrived by sync. Vector
    /// RowIds are local-store identities, so outbound echo filtering must use
    /// the owner plus its local commit LSN rather than a table/LSN group.
    /// Delete provenance is intentionally session-local, matching the current
    /// tombstone retention boundary.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn vector_change_arrived_by_sync(&self, vector: &VectorChange) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.vector_change_arrived_by_sync_with_read_memory(vector, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_change_arrived_by_sync_with_read_memory(
        &self,
        vector: &VectorChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.checked_vector_change_arrived_by_sync(vector, read_memory)
    }

    /// Checked form of [`Self::vector_change_arrived_by_sync`]: a durable
    /// history read failure is returned, never read as "not synced".
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn checked_vector_change_arrived_by_sync(
        &self,
        vector: &VectorChange,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        if !vector.vector.is_empty() {
            let current = self.committed_row_at(
                &vector.index.table,
                vector.row_id,
                self.snapshot_for_read(),
                read_memory.clone(),
            )?;
            let Some(row) = current else {
                return Ok(false);
            };
            if row.lsn != vector.lsn {
                return Ok(false);
            }
            return Ok(matches!(
                self.relational_sync_source_at_read_root(
                    &vector.index.table,
                    vector.row_id,
                    read_memory.clone()
                )?
                .1,
                Some(contextdb_relational::store::SyncSourceKind::Pulled)
            ));
        }
        Ok(self
            .find_history_change(
                vector.lsn,
                vector.lsn,
                |entry| {
                    let ChangeLogEntry::RowDelete {
                        table,
                        row_id,
                        natural_key,
                        lsn,
                    } = entry
                    else {
                        return None;
                    };
                    (table == &vector.index.table
                        && *row_id == vector.row_id
                        && *lsn == vector.lsn
                        && self
                            .sync_tombstone_arrivals
                            .read()
                            .get(&Self::sync_tombstone_key(table, natural_key))
                            .is_some_and(|marker| {
                                matches!(
                                    marker.kind,
                                    contextdb_relational::store::SyncSourceKind::Pulled
                                ) && marker.delete_lsn == *lsn
                                    && marker.row_id == Some(*row_id)
                            }))
                    .then_some(())
                },
                read_memory.clone(),
            )?
            .is_some())
    }

    pub(super) fn sync_tombstone_key(table: &str, natural_key: &NaturalKey) -> (String, String) {
        (table.to_string(), format!("{:?}", natural_key.pairs()))
    }

    pub(crate) const DURABLE_LINEAGE_CONFIG_PREFIX: &'static str = "sync_lineage.v1.record.";

    /// A key's lifecycle record describes its current life. When a later life
    /// replaces a purged record there, the purged life keeps its permanent
    /// tombstone under this prefix, one entry per purged lineage root, written
    /// in the same durable write as the replacement.
    pub(crate) const DURABLE_PURGED_LINEAGE_HISTORY_PREFIX: &'static str =
        "sync_lineage.v1.purged.";

    /// The `{generation}.{identity}` component shared by a key's lifecycle
    /// record, row sidecar, and purged-life history.
    pub(super) fn durable_lineage_key_suffix(
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> String {
        format!(
            "{generation:016x}.{}",
            Self::hex_component(&sync_identity_key(table, natural_key))
        )
    }

    pub(super) fn durable_purged_lineage_history_key(record: &DurableLineageRecord) -> String {
        format!(
            "{}{}.{}",
            Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX,
            Self::durable_lineage_key_suffix(
                &record.table,
                &record.natural_key,
                record.table_generation
            ),
            blake3::hash(record.lineage_root.as_bytes()).to_hex()
        )
    }

    /// Every purged life of the key whose lifecycle suffix is `suffix`: the
    /// current record when it is purged, then every earlier purged life.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purged_lineage_records_for_suffix(
        &self,
        state: &InMemoryLineageState,
        suffix: &str,
    ) -> Result<Vec<DurableLineageRecord>> {
        let mut records = self
            .load_lineage_record(
                state,
                &format!("{}{suffix}", Self::DURABLE_LINEAGE_CONFIG_PREFIX),
            )?
            .filter(|record| record.delete_obligation == DurableDeleteObligation::Purged)
            .into_iter()
            .collect::<Vec<_>>();
        let prefix = format!("{}{suffix}.", Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX);
        match &self.persistence {
            Some(persistence) => records.extend(
                persistence
                    .load_config_values_with_prefix::<DurableLineageRecord>(&prefix)?
                    .into_iter()
                    .map(|(_, record)| record),
            ),
            None => records.extend(
                state
                    .purged_history
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(&prefix))
                    .map(|(_, record)| record.clone()),
            ),
        }
        Ok(records)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purged_lineage_records(
        &self,
        state: &InMemoryLineageState,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> Result<Vec<DurableLineageRecord>> {
        self.purged_lineage_records_for_suffix(
            state,
            &Self::durable_lineage_key_suffix(table, natural_key, generation),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purged_lineage_records_at_generation(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> Result<Vec<DurableLineageRecord>> {
        let state = self.lineage_state_lock.lock();
        self.purged_lineage_records(&state, table, natural_key, generation)
    }

    /// Every purged life that a later life has displaced from its key's
    /// lifecycle record.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn list_purged_lineage_history(
        &self,
        state: &InMemoryLineageState,
    ) -> Result<Vec<DurableLineageRecord>> {
        match &self.persistence {
            Some(persistence) => Ok(persistence
                .load_config_values_with_prefix::<DurableLineageRecord>(
                    Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX,
                )?
                .into_iter()
                .map(|(_, record)| record)
                .collect()),
            None => Ok(state.purged_history.values().cloned().collect()),
        }
    }

    /// For lifecycle writes applied in order, return the permanent history
    /// entry of every purged record a write replaces with another life. The
    /// caller persists these entries in the same durable write, so a purged
    /// life's tombstone is never overwritten.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn purged_lives_displaced_by(
        &self,
        writes: &[(String, DurableLineageRecord)],
    ) -> Result<Vec<(String, DurableLineageRecord)>> {
        let state = self.lineage_state_lock.lock();
        let mut current = HashMap::<&str, DurableLineageRecord>::new();
        let mut displaced = Vec::new();
        for (key, incoming) in writes {
            let slot = match current.get(key.as_str()) {
                Some(record) => Some(record.clone()),
                None => self.load_lineage_record(&state, key)?,
            };
            if let Some(slot) = slot
                && slot.delete_obligation == DurableDeleteObligation::Purged
                && slot.lineage_root != incoming.lineage_root
            {
                displaced.push((Self::durable_purged_lineage_history_key(&slot), slot));
            }
            current.insert(key.as_str(), incoming.clone());
        }
        Ok(displaced)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn encoded_purged_lives_displaced_by(
        &self,
        writes: &[(String, Vec<u8>, DurableLineageRecord)],
    ) -> Result<Vec<(String, Vec<u8>, DurableLineageRecord)>> {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::encoded_purged_lives_displaced_by");
        self.purged_lives_displaced_by(
            &writes
                .iter()
                .map(|(key, _, record)| (key.clone(), record.clone()))
                .collect::<Vec<_>>(),
        )?
        .into_iter()
        .map(|(key, record)| {
            RedbPersistence::encode_config_value(&record).map(|bytes| (key, bytes, record))
        })
        .collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_unbound_creation_lineage(
        &self,
        state: &InMemoryLineageState,
        key: &str,
    ) -> Result<Option<DurableUnboundCreationLineage>> {
        match &self.persistence {
            Some(persistence) => persistence.load_config_value(key),
            None => Ok(state.unbound_creations.get(key).cloned()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_row_lineage_sidecar(
        &self,
        state: &InMemoryLineageState,
        key: &str,
    ) -> Result<Option<DurableRowLineageSidecar>> {
        match &self.persistence {
            Some(persistence) => persistence.load_config_value(key),
            None => Ok(state.row_sidecars.get(key).cloned()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn store_row_lineage_sidecar(
        &self,
        state: &mut InMemoryLineageState,
        key: &str,
        record: &DurableRowLineageSidecar,
    ) -> Result<()> {
        match &self.persistence {
            Some(persistence) => {
                self.publish_direct_metadata(|| persistence.flush_config_value(key, record))
            }
            None => {
                state.row_sidecars.insert(key.to_string(), record.clone());
                Ok(())
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_lineage_record(
        &self,
        state: &InMemoryLineageState,
        key: &str,
    ) -> Result<Option<DurableLineageRecord>> {
        match &self.persistence {
            Some(persistence) => persistence.load_config_value(key),
            None => Ok(state.records.get(key).cloned()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn store_lineage_record(
        &self,
        state: &mut InMemoryLineageState,
        key: &str,
        record: &DurableLineageRecord,
    ) -> Result<()> {
        match &self.persistence {
            Some(persistence) => {
                self.publish_direct_metadata(|| persistence.flush_config_value(key, record))
            }
            None => {
                let pending = self.prepare_resident_pending_record(state, key, Some(record))?;
                state.records.insert(key.to_string(), record.clone());
                pending.publish();
                Ok(())
            }
        }
    }

    /// Publish a delete record and its exact local-row sidecar as one durable
    /// unit. The caller holds `lineage_state_lock`, which is also the memory
    /// backend's atomicity boundary.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn store_lineage_record_and_row_sidecar(
        &self,
        state: &mut InMemoryLineageState,
        record_key: &str,
        record: &DurableLineageRecord,
        sidecar_key: &str,
        sidecar: &DurableRowLineageSidecar,
    ) -> Result<()> {
        match &self.persistence {
            Some(persistence) => {
                let encoded_record = RedbPersistence::encode_config_value(record)?;
                let encoded_sidecar = RedbPersistence::encode_config_value(sidecar)?;
                self.publish_direct_metadata(|| {
                    persistence.flush_encoded_config_values(vec![
                        (record_key, encoded_record),
                        (sidecar_key, encoded_sidecar),
                    ])
                })
            }
            None => {
                let pending =
                    self.prepare_resident_pending_record(state, record_key, Some(record))?;
                state.records.insert(record_key.to_string(), record.clone());
                state
                    .row_sidecars
                    .insert(sidecar_key.to_string(), sidecar.clone());
                pending.publish();
                Ok(())
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn list_lineage_records(
        &self,
        state: &InMemoryLineageState,
    ) -> Result<Vec<(String, DurableLineageRecord)>> {
        match &self.persistence {
            Some(persistence) => {
                persistence.load_config_values_with_prefix(Self::DURABLE_LINEAGE_CONFIG_PREFIX)
            }
            None => Ok(state
                .records
                .iter()
                .map(|(key, record)| (key.clone(), record.clone()))
                .collect()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn local_delete_sidecar_from_record(
        record: &DurableLineageRecord,
    ) -> Result<DurableRowLineageSidecar> {
        let local_row_id = record.local_row_id.ok_or_else(|| {
            Error::SyncError(
                ("locally created pending delete lacks the pinned local row id".to_string()).into(),
            )
        })?;
        let author_node_id = record.author_node_id.clone().ok_or_else(|| {
            Error::SyncError(
                ("locally created pending delete lacks its creation lineage".to_string()).into(),
            )
        })?;
        let author_database_incarnation = record
            .author_database_incarnation
            .as_deref()
            .and_then(Incarnation::from_hex)
            .ok_or_else(|| {
                Error::SyncError(
                    ("locally created pending delete has an invalid author incarnation"
                        .to_string())
                    .into(),
                )
            })?;
        if record.lineage_attestation.is_empty() {
            return Err(Error::SyncError(
                ("locally created pending delete lacks its lineage attestation".to_string()).into(),
            ));
        }
        Ok(DurableRowLineageSidecar {
            author_node_id,
            author_database_incarnation,
            author_local_mutation_position: Lsn(record.author_local_mutation_position),
            table_generation: record.table_generation,
            lineage_root: record.lineage_root.clone(),
            lineage_attestation: record.lineage_attestation.clone(),
            local_row_id: Some(local_row_id),
            locally_created: true,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn local_delete_sidecar_slot_is_eligible(
        &self,
        state: &InMemoryLineageState,
        sidecar_key: &str,
        local_row_id: RowId,
    ) -> Result<bool> {
        Ok(match self.load_row_lineage_sidecar(state, sidecar_key)? {
            None => true,
            Some(existing) => {
                existing.local_row_id == Some(local_row_id) && existing.locally_created
            }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_in_memory_lineage_delta(
        &self,
        config_writes: &[(String, Vec<u8>)],
    ) -> Result<InMemoryLineageDelta> {
        let mut delta = InMemoryLineageDelta::default();
        for (key, value) in config_writes {
            if key.starts_with(SYNC_PUSH_OUTCOME_PREFIX) {
                delta
                    .push_outcomes
                    .insert(key.clone(), RedbPersistence::decode_config_value(value)?);
            } else if key.starts_with("sync_creation_lineage.v1.") {
                delta
                    .unbound_creations
                    .insert(key.clone(), RedbPersistence::decode_config_value(value)?);
            } else if key.starts_with("sync_row_lineage.v1.") {
                delta
                    .row_sidecars
                    .insert(key.clone(), RedbPersistence::decode_config_value(value)?);
            } else if key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                delta
                    .records
                    .insert(key.clone(), RedbPersistence::decode_config_value(value)?);
            } else if key.starts_with(Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX) {
                delta
                    .purged_history
                    .insert(key.clone(), RedbPersistence::decode_config_value(value)?);
            }
        }
        delta.connected_membership = self.prepare_connected_membership_delta(config_writes)?;
        delta.pending = self.prepare_resident_pending_delta(&delta.records)?;
        Ok(delta)
    }

    pub(super) fn publish_in_memory_lineage_delta(&self, mut delta: InMemoryLineageDelta) {
        if self.persistence.is_some() {
            return;
        }
        if let Some(pending) = delta.pending.take() {
            pending.publish(delta);
        } else {
            resident_pending::install(&mut self.lineage_state_lock.lock(), delta);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_graph_arrival<T>(
        &self,
        identity: &GraphIdentity,
        visit: impl FnOnce(Option<&DurableGraphArrival>) -> Result<T>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            let key_record = DurableGraphArrival {
                source: identity.0,
                target: identity.1,
                edge_type: identity.2.clone(),
                local_lsn: Lsn(0),
                deleted: false,
                pulled: false,
            };
            let key = Self::graph_arrival_config_key(&key_record)?;
            let record = root.config_point::<DurableGraphArrival>(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                &key,
            )?;
            if let Some(record) = &record
                && Self::graph_arrival_key(&record.value) != *identity
            {
                return Err(crate::funded_error::corruption_text(
                    Ok(read_memory.clone()),
                    &key,
                    "graph-arrival key disagrees with its canonical record".into(),
                ));
            }
            return visit(record.as_ref().map(|record| &record.value));
        }
        visit(
            self.sync_graph_arrivals
                .read()
                .get(identity)
                .map(|record| &**record),
        )
    }

    pub(super) fn durable_lineage_config_key(
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> String {
        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::durable_lineage_config_key");
        format!(
            "{}{}",
            Self::DURABLE_LINEAGE_CONFIG_PREFIX,
            Self::durable_lineage_key_suffix(table, natural_key, generation)
        )
    }

    pub(crate) fn durable_lineage_table_generation_key(table: &str) -> String {
        format!(
            "{}{}",
            Self::DURABLE_LINEAGE_TABLE_GENERATION_PREFIX,
            Self::hex_component(table.as_bytes())
        )
    }

    pub(crate) const DURABLE_LINEAGE_TABLE_GENERATION_PREFIX: &str =
        "sync_lineage.v1.table_generation.";

    /// A received-schema stage adjudicates against a private in-memory
    /// database.  Its generation map must therefore be a complete snapshot of
    /// the real owner's authoritative state, including names whose tables were
    /// dropped but whose next same-name CREATE still depends on their prior
    /// generation.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn authoritative_table_generations_for_detached_working(
        &self,
    ) -> Result<HashMap<String, u64>> {
        let _operation = self.open_operation()?;
        let Some(persistence) = &self.persistence else {
            return Ok(self.in_memory_table_generations.lock().clone());
        };

        let mut generations = HashMap::new();
        for (key, raw_generation) in persistence
            .load_config_values_raw_with_prefix(Self::DURABLE_LINEAGE_TABLE_GENERATION_PREFIX)?
        {
            let suffix = key
                .strip_prefix(Self::DURABLE_LINEAGE_TABLE_GENERATION_PREFIX)
                .ok_or_else(|| Error::StoreCorrupted {
                    path: key.clone().into(),
                    reason: "table-generation key lacks its canonical prefix"
                        .to_string()
                        .into(),
                })?;
            if suffix.is_empty()
                || suffix.len() % 2 != 0
                || !suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            {
                return Err(Error::StoreCorrupted {
                    path: key.into(),
                    reason:
                        "table-generation key has a malformed canonical lowercase hex table suffix"
                            .to_string()
                            .into(),
                });
            }
            let table_bytes = suffix
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let high = (pair[0] as char).to_digit(16).expect("validated hex") as u8;
                    let low = (pair[1] as char).to_digit(16).expect("validated hex") as u8;
                    (high << 4) | low
                })
                .collect::<Vec<_>>();
            let table = String::from_utf8(table_bytes).map_err(|_| Error::StoreCorrupted {
                path: key.clone().into(),
                reason: "table-generation key table suffix is not UTF-8"
                    .to_string()
                    .into(),
            })?;
            let generation = RedbPersistence::decode_config_value_exact::<u64>(&raw_generation)
                .map_err(|err| Error::StoreCorrupted {
                    path: key.clone().into(),
                    reason: format!("table-generation value is not an exact canonical u64: {err}")
                        .into(),
                })?;
            let canonical_generation =
                RedbPersistence::encode_config_value(&generation).map_err(|err| {
                    Error::StoreCorrupted {
                        path: key.clone().into(),
                        reason: format!(
                            "table-generation value cannot be canonically encoded: {err}"
                        )
                        .into(),
                    }
                })?;
            if generation == 0
                || canonical_generation != raw_generation
                || Self::durable_lineage_table_generation_key(&table) != key
                || generations.insert(table, generation).is_some()
            {
                return Err(Error::StoreCorrupted {
                    path: key.into(),
                    reason: "table-generation key or value is zero, duplicate, or noncanonical"
                        .to_string()
                        .into(),
                });
            }
        }
        for table in self.relational_store.table_meta.read().keys() {
            if !generations.contains_key(table) {
                return Err(Error::StoreCorrupted {
                    path: Self::durable_lineage_table_generation_key(table).into(),
                    reason: format!(
                        "table {table} has schema metadata but no authoritative generation"
                    )
                    .into(),
                });
            }
        }
        Ok(generations)
    }

    pub(super) fn durable_ddl_generation_sidecar_key(lsn: Lsn, ordinal: u32) -> String {
        format!("sync_ddl_generation.v1.{:020}.{ordinal:06}", lsn.0)
    }

    pub(super) const DURABLE_RECEIVED_DDL_ARRIVAL_CONFIG_PREFIX: &str =
        "sync_ddl_received_arrival.v1.";

    pub(super) fn durable_received_ddl_arrival_key(
        local_lsn: Lsn,
        persisted_ordinal: u32,
    ) -> String {
        format!(
            "{}{:020}.{persisted_ordinal:06}",
            Self::DURABLE_RECEIVED_DDL_ARRIVAL_CONFIG_PREFIX,
            local_lsn.0
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn has_received_ddl_arrivals(&self) -> Result<bool> {
        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            RedbPersistence::config_prefix_exists_at_root(
                &root,
                Self::DURABLE_RECEIVED_DDL_ARRIVAL_CONFIG_PREFIX,
            )
        } else {
            Ok(!self.received_ddl_arrivals.read().is_empty())
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_received_ddl_arrivals(
        &self,
        mut visit: impl FnMut((Lsn, u32), &DurableReceivedDdlArrival) -> Result<()>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            let mut cursor = root.config_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Self::DURABLE_RECEIVED_DDL_ARRIVAL_CONFIG_PREFIX,
            )?;
            while let Some(window) = cursor.next(|_, bytes| {
                RedbPersistence::decode_config_value_exact::<DurableReceivedDdlArrival>(bytes)
            })? {
                let record = &window.value;
                let key = Self::durable_received_ddl_arrival_key(
                    record.local_lsn,
                    record.persisted_ordinal,
                );
                if window.position() != key.as_bytes() {
                    return Err(crate::funded_error::corruption_text(
                        Ok(read_memory.clone()),
                        &key,
                        "received DDL arrival key does not bind one exact local occurrence".into(),
                    ));
                }
                visit((record.local_lsn, record.persisted_ordinal), record)?;
            }
            return Ok(());
        }
        let arrivals = self.received_ddl_arrivals.read();
        let mut records = arrivals.iter().collect::<Vec<_>>();
        records.sort_unstable_by_key(|(key, _)| **key);
        for (key, record) in records {
            visit(*key, record)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_received_ddl_arrival<T>(
        &self,
        occurrence: (Lsn, u32),
        visit: impl FnOnce(Option<&DurableReceivedDdlArrival>) -> Result<T>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            return self.with_received_ddl_arrival_at_root(&root, occurrence, read_memory, visit);
        }
        visit(self.received_ddl_arrivals.read().get(&occurrence))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_received_ddl_arrival_at_root<T>(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        occurrence: (Lsn, u32),
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        visit: impl FnOnce(Option<&DurableReceivedDdlArrival>) -> Result<T>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        RedbPersistence::validate_ddl_occurrence_ordinal(occurrence.1)?;
        let key = Self::durable_received_ddl_arrival_key(occurrence.0, occurrence.1);
        let record = root.config_point::<DurableReceivedDdlArrival>(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            &key,
        )?;
        if let Some(record) = &record
            && (record.value.local_lsn, record.value.persisted_ordinal) != occurrence
        {
            return Err(crate::funded_error::corruption_text(
                Ok(read_memory.clone()),
                &key,
                "received DDL arrival key does not bind one exact local occurrence".into(),
            ));
        }
        visit(record.as_ref().map(|record| &record.value))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_superseded_ddl_occurrence<T>(
        &self,
        occurrence: (Lsn, u32),
        visit: impl FnOnce(Option<&DurableSupersededDdlOccurrence>) -> Result<T>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            return self.with_superseded_ddl_occurrence_at_root(
                &root,
                occurrence,
                read_memory,
                visit,
            );
        }
        visit(self.superseded_ddl_occurrences.read().get(&occurrence))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_superseded_ddl_occurrence_at_root<T>(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        occurrence: (Lsn, u32),
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        visit: impl FnOnce(Option<&DurableSupersededDdlOccurrence>) -> Result<T>,
    ) -> Result<T> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        RedbPersistence::validate_ddl_occurrence_ordinal(occurrence.1)?;
        let key = Self::durable_superseded_ddl_occurrence_key(occurrence.0, occurrence.1);
        let record = root.config_point::<DurableSupersededDdlOccurrence>(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            &key,
        )?;
        if let Some(record) = &record
            && (record.value.local_lsn, record.value.persisted_ordinal) != occurrence
        {
            return Err(crate::funded_error::corruption_text(
                Ok(read_memory.clone()),
                &key,
                "superseded DDL key does not bind one exact local occurrence".into(),
            ));
        }
        visit(record.as_ref().map(|record| &record.value))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_ddl_occurrence_config(
        diagnostic_owner: &contextdb_core::read_memory::ReadMemoryOwner,
        key: &str,
        occurrence: (Lsn, u32),
        bytes: &[u8],
    ) -> Result<()> {
        if RedbPersistence::ddl_config_occurrence_coordinate(key)? != occurrence {
            return Err(crate::funded_error::corruption_text(
                Ok(diagnostic_owner.clone()),
                key,
                "declaration configuration key does not bind one exact local occurrence".into(),
            ));
        }
        let recorded = if key.starts_with(Self::DURABLE_RECEIVED_DDL_ARRIVAL_CONFIG_PREFIX) {
            let record: DurableReceivedDdlArrival =
                RedbPersistence::decode_config_value_exact(bytes)?;
            Some((record.local_lsn, record.persisted_ordinal))
        } else if key.starts_with(Self::DURABLE_SUPERSEDED_DDL_CONFIG_PREFIX) {
            let record: DurableSupersededDdlOccurrence =
                RedbPersistence::decode_config_value_exact(bytes)?;
            Some((record.local_lsn, record.persisted_ordinal))
        } else {
            let _: DurableDdlGenerationSidecar = RedbPersistence::decode_config_value_exact(bytes)?;
            None
        };
        if recorded.is_some_and(|recorded| recorded != occurrence) {
            return Err(crate::funded_error::corruption_text(
                Ok(diagnostic_owner.clone()),
                key,
                "declaration marker does not bind one exact local occurrence".into(),
            ));
        }
        Ok(())
    }

    pub(super) const DURABLE_SUPERSEDED_DDL_CONFIG_PREFIX: &str = "sync_ddl_superseded.v1.";

    pub(super) fn durable_superseded_ddl_occurrence_key(
        local_lsn: Lsn,
        persisted_ordinal: u32,
    ) -> String {
        format!(
            "{}{:020}.{persisted_ordinal:06}",
            Self::DURABLE_SUPERSEDED_DDL_CONFIG_PREFIX,
            local_lsn.0
        )
    }

    /// Decide which locally authored DDL occurrences an incoming authenticated
    /// vector restates, given the local DDL log as it stood before that vector
    /// was appended. Occurrences already carrying arrival or superseded
    /// evidence are left alone, and each incoming declaration consumes at most
    /// the first still-unmatched local occurrence with the same persisted
    /// spelling, so duplicate DDL cannot be counted twice.
    ///
    /// A local schema commit is superseded only in full: marking part of one
    /// commit would let a later push carry a fragment of a transaction whose
    /// remaining statements the peer has never seen.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn superseded_local_ddl_occurrences(
        &self,
        local_ddl_log: &[(Lsn, DdlChange)],
        received_ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<((Lsn, u32), DurableSupersededDdlOccurrence)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut candidates = Vec::<((Lsn, u32), DurableSupersededDdlOccurrence)>::new();
        for change in received_ddl {
            for (position, (lsn, local)) in local_ddl_log.iter().enumerate() {
                if local != change {
                    continue;
                }
                let ordinal = local_ddl_log[..position]
                    .iter()
                    .filter(|(prior, _)| prior == lsn)
                    .count() as u32;
                let key = (*lsn, ordinal);
                if self.with_received_ddl_arrival(
                    key,
                    |value| Ok(value.is_some()),
                    read_memory.clone(),
                )? || self.with_superseded_ddl_occurrence(
                    key,
                    |value| Ok(value.is_some()),
                    read_memory.clone(),
                )? || candidates.iter().any(|(candidate, _)| *candidate == key)
                {
                    continue;
                }
                let Some(generation) = self.recorded_ddl_generation_sidecar(key.0, key.1)? else {
                    continue;
                };
                candidates.push((
                    key,
                    DurableSupersededDdlOccurrence {
                        local_lsn: key.0,
                        persisted_ordinal: key.1,
                        digest: Self::canonical_local_ddl_arrival_digest(
                            local,
                            key.0,
                            key.1,
                            &generation,
                        )?,
                    },
                ));
                break;
            }
        }
        let mut complete_commits = HashSet::new();
        for (key, _) in &candidates {
            if complete_commits.contains(&key.0) {
                continue;
            }
            let mut complete = true;
            for (ordinal, (_, _change)) in local_ddl_log
                .iter()
                .filter(|(lsn, _)| *lsn == key.0)
                .enumerate()
            {
                let occurrence = (key.0, ordinal as u32);
                if self.with_received_ddl_arrival(
                    occurrence,
                    |value| Ok(value.is_some()),
                    read_memory.clone(),
                )? || self.with_superseded_ddl_occurrence(
                    occurrence,
                    |value| Ok(value.is_some()),
                    read_memory.clone(),
                )? {
                    continue;
                }
                if !candidates
                    .iter()
                    .any(|(candidate, _)| *candidate == occurrence)
                {
                    complete = false;
                    break;
                }
            }
            if complete {
                complete_commits.insert(key.0);
            }
        }
        candidates.retain(|(key, _)| complete_commits.contains(&key.0));
        candidates.sort_by_key(|(key, _)| *key);
        Ok(candidates)
    }
}
