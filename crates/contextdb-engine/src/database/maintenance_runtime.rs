use super::*;

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_partition_routes(
    vector: &VectorStore,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<Vec<VectorPartitionRef>> {
    let mut routes = Vec::new();
    for index in vector.index_layout_infos(vector_copy_owner)? {
        routes.extend(
            vector
                .partition_keys(&index.index, vector_copy_owner)?
                .into_iter()
                .map(|key| VectorPartitionRef::new(index.index.clone(), key)),
        );
    }
    Ok(routes)
}
impl MetadataAuthorizedCandidates {
    pub(super) fn as_ref(&self) -> Option<&RoaringTreemap> {
        self.ids.as_ref()
    }
}
impl DatabaseAssemblyBudget {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn into_accountant(
        self,
    ) -> Result<(
        crate::memory_accounting::AccountantHandle,
        Option<crate::memory_accounting::WriterUse>,
        contextdb_vector::MemoryBudgetOwner,
    )> {
        match self {
            Self::Writer(budget) => {
                let (accountant, writer_use, vector_accountant) = budget.into_parts();
                Ok((accountant, Some(writer_use), vector_accountant))
            }
            Self::ReadImage(accountant) => {
                let vector = accountant.vector_budget_for_original_reader()?;
                Ok((accountant, None, vector))
            }
        }
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn resident_insertion_runtime_bytes(width: usize) -> Result<usize> {
    HnswIndex::INSERTION_WORKER_STACK_BYTES
        .checked_add(RESIDENT_INSERTION_RUNTIME_RETAINED_PER_WORKER)
        .and_then(|n| n.checked_mul(width))
        .ok_or_else(|| Error::Other("resident insertion runtime size overflow".into()))
}

impl VectorGenerationWorkspacePlan {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(
        raw_count: usize,
        graph_count: usize,
        layout: &VectorIndexLayout,
        partition: &VectorPartitionRef,
        policy: contextdb_vector::store::ResolvedVectorPolicy,
        prior_base_bytes: usize,
    ) -> Result<Self> {
        let graph_build_bytes = HnswIndex::estimated_build_reservation(
            graph_count,
            layout.dimension,
            layout.quantization,
            policy,
        );
        let entry_bytes = std::mem::size_of::<VectorEntry>()
            .saturating_add(layout.dimension.saturating_mul(4))
            .saturating_add(partition.index.table.len())
            .saturating_add(partition.index.column.len())
            .saturating_add(128);
        let raw_entry_bytes = raw_count.saturating_mul(entry_bytes);
        let selected_entry_bytes = graph_count.saturating_mul(entry_bytes);
        let encoded_graph_bound = HnswIndex::durable_generation_capacity_bound(
            graph_count,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        )?;
        // Encoding sorts borrowed exact-map entries and copied row-id pairs;
        // the vendor codec copies one point's adjacency at a time. Each
        // nonempty Vec has a minimum capacity of four neighbour slots.
        let encoding_scratch_bytes = graph_count
            .saturating_mul(32)
            .saturating_add(policy.hnsw_m.max(4).saturating_mul(17).saturating_mul(32))
            .saturating_add(16 * std::mem::size_of::<Vec<usize>>());
        let durable_metadata_bytes = partition
            .partition_key
            .estimated_bytes()
            .saturating_mul(4)
            .saturating_add(
                partition
                    .index
                    .table
                    .len()
                    .saturating_add(partition.index.column.len())
                    .saturating_mul(4),
            )
            .saturating_add(4096);
        Ok(Self {
            graph_build_bytes,
            insertion_workers: 1,
            insertion_worker_bytes: 0,
            raw_entry_bytes,
            // Direct raw loading decodes one persisted value then clones or
            // dequantizes it. Cover declared-body copies and serde's capped
            // initial Vec allocation on a short malformed length prefix, even
            // when construction has no live-point allowance.
            raw_decode_scratch_bytes: layout
                .dimension
                .saturating_mul(4)
                .saturating_mul(3)
                .saturating_add(std::mem::size_of::<VectorEntry>())
                .saturating_add(1024 * 1024),
            selected_entry_bytes,
            encoded_graph_bound,
            encoding_scratch_bytes,
            prior_base_bytes,
            durable_metadata_bytes,
            digest_reference_bytes: graph_count
                .saturating_mul(std::mem::size_of::<&VectorEntry>())
                .saturating_mul(2),
            carrier_metadata_bytes: 0,
        })
    }

    pub(super) fn assign_raw_decode_owner(
        &mut self,
        partition_state_present: bool,
        delete_carriers: bool,
    ) {
        // Partition snapshots copy resident entries into the funded raw owner.
        // If its bodies are dormant/evicted, the partition loader independently
        // admits its decoder scratch before hydration. Direct durable reads and
        // delete-only carriers instead decode under this generation workspace.
        if partition_state_present && !delete_carriers {
            self.raw_decode_scratch_bytes = 0;
        }
    }

    pub(super) fn estimated_generation_bytes(self) -> usize {
        self.encoded_graph_bound
            .saturating_add(self.prior_base_bytes)
            .saturating_add(self.durable_metadata_bytes)
    }

    pub(super) fn complete_peak_bytes(self) -> usize {
        self.build_phase_bytes()
            .max(self.encoding_phase_bytes(self.graph_build_bytes))
            .max(self.persistence_phase_bytes(self.encoded_graph_bound))
            .max(self.prepared_phase_bytes(self.encoded_graph_bound))
    }

    pub(super) fn resident_export_phase_bytes(self, batch_bytes: usize) -> usize {
        // The whole graph and authoritative entries coexist with one borrowed
        // row directory and a native point/identity/dirty-page transaction.
        // Build-phase pricing conservatively retains the now-dropped inputs.
        self.build_phase_bytes()
            .saturating_add(batch_bytes)
            .saturating_add(self.digest_reference_bytes)
            .saturating_add(self.durable_metadata_bytes.saturating_mul(64))
            .saturating_add(64 * 1024)
    }

    pub(super) fn build_phase_bytes(self) -> usize {
        // Raw snapshot, selected input clones and construction workspace coexist.
        // Neither the envelope nor a prior base is loaded during construction.
        self.graph_build_bytes
            .saturating_add(self.insertion_worker_bytes)
            .saturating_add(self.raw_entry_bytes)
            .saturating_add(self.raw_decode_scratch_bytes)
            .saturating_add(self.selected_entry_bytes)
            .saturating_add(self.durable_metadata_bytes)
            .saturating_add(self.carrier_metadata_bytes)
    }

    pub(super) fn encoding_phase_bytes(self, graph_resident_bytes: usize) -> usize {
        // Input clones are gone. The builder, authoritative raw snapshot and
        // one checked codec envelope (including sorting scratch) remain.
        graph_resident_bytes
            .saturating_add(self.raw_entry_bytes)
            .saturating_add(self.encoded_graph_bound)
            .saturating_add(self.encoding_scratch_bytes)
            .saturating_add(self.durable_metadata_bytes)
    }

    pub(super) fn persistence_phase_bytes(self, encoded_graph_capacity: usize) -> usize {
        // The builder is gone. The raw snapshot and one candidate graph
        // coexist; change publication decodes one prior base directly from
        // its borrowed durable record. Digesting sorts borrowed raw entries
        // and streams their encoding into the hasher.
        self.raw_entry_bytes
            .saturating_add(encoded_graph_capacity)
            .saturating_add(self.prior_base_bytes)
            .saturating_add(self.durable_metadata_bytes)
            .saturating_add(self.digest_reference_bytes)
    }

    pub(super) fn prepared_phase_bytes(self, encoded_graph_capacity: usize) -> usize {
        // Builder and input clones are gone. The change path alone loads its
        // prior base. Six payload bounds cover candidate, bincode payload,
        // record envelope, validation decode, prepared body, and insertion.
        // Page keys are streamed one at a time: even a near-64KiB root never
        // repeats in retained preparation memory. Reserve its framed page
        // (key + header + body <= 64KiB) separately before insertion.
        encoded_graph_capacity
            .saturating_add(self.prior_base_bytes)
            .saturating_add(self.durable_metadata_bytes)
            .saturating_mul(VECTOR_GENERATION_PERSISTENCE_COPIES)
            .saturating_add(64 * 1024)
    }

    pub(super) fn prepared_candidate_phase_bytes(
        self,
        encoded_graph_capacity: usize,
        frame_bytes: usize,
    ) -> usize {
        let candidate = encoded_graph_capacity
            .saturating_add(self.prior_base_bytes)
            .saturating_add(self.durable_metadata_bytes)
            .saturating_mul(VECTOR_GENERATION_PERSISTENCE_COPIES)
            .saturating_add(frame_bytes);
        debug_assert!(candidate <= self.prepared_phase_bytes(encoded_graph_capacity));
        candidate
    }
}

impl VectorPartitionPublicationOutcome {
    pub(super) fn published() -> Self {
        Self {
            published: true,
            cleanup_failure: None,
        }
    }

    pub(super) fn published_with_cleanup_failure(error: &Error) -> Self {
        Self {
            published: true,
            cleanup_failure: Some(
                VectorMaintenanceFailureDetails::from_published_cleanup_error(error),
            ),
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn publish_direct_metadata<R>(
    tx_mgr: &DatabaseTransactionHost,
    write: impl FnOnce() -> Result<R>,
) -> Result<R> {
    // Dual-use helpers inherit their actual caller's commit guard. Their
    // direct commit still needs a refresh if that enclosing commit errors.
    if tx_mgr.commit_boundary_held_by_current_thread() {
        return tx_mgr.with_deferred_metadata_write(write);
    }
    tx_mgr.with_maintenance_publication(|publication| {
        publication.reserve()?;
        let result = write()?;
        publication.complete();
        Ok(result)
    })
}

// The narrow publisher knows exactly when its durable write and infallible
// memory swap completed. Preserve that publication even if its later observer
// reports an error; precommit failures still abandon their unused reservation.
#[expect(
    clippy::result_large_err,
    reason = "Preserve the existing public diagnostic through postcommit publication."
)]
pub(super) fn publish_direct_metadata_after_commit<R>(
    tx_mgr: &DatabaseTransactionHost,
    committed: &std::cell::Cell<bool>,
    write: impl FnOnce() -> Result<R>,
) -> Result<R> {
    if tx_mgr.commit_boundary_held_by_current_thread() {
        return tx_mgr.with_deferred_metadata_write_after_commit(committed, write);
    }
    tx_mgr.with_maintenance_publication(|publication| {
        publication.reserve()?;
        let result = write();
        if committed.get() {
            publication.complete();
        }
        result
    })
}
impl MaintenanceCleanup {
    pub(super) fn finish(&mut self) {
        for step in 0..27 {
            if !std::mem::replace(&mut self.initialized[step], false) {
                continue;
            }
            unsafe {
                match step {
                    0 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).reader_memory_limit
                        ));
                    }
                    1 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).read_request_provider
                        ));
                    }
                    2 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).writer_receipt_observer
                            ));
                        }
                    }
                    3 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).disk_limit));
                    }
                    4 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).vector_serving_cache_bytes
                        ));
                    }
                    5 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).resource_closed
                        ));
                    }
                    6 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).publication_interrupted
                        ));
                    }
                    7 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).worker_stop));
                    }
                    8 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).relational));
                    }
                    9 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).graph));
                    }
                    10 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).vector));
                    }
                    11 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).persistence));
                    }
                    12 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).change_log));
                    }
                    13 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).change_log_table_index
                        ));
                    }
                    14 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).change_log_lsn_refcounts
                        ));
                    }
                    15 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).trigger));
                    }
                    16 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).tx_mgr));
                    }
                    17 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_watermark
                        ));
                    }
                    18 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).trigger_audit_retention_secs
                        ));
                    }
                    19 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).snapshot_registry
                        ));
                    }
                    20 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).retention_deferred_edge_nodes
                        ));
                    }
                    21 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).last_maintenance_cycle_at
                        ));
                    }
                    22 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).last_auto_compact_at
                        ));
                    }
                    23 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).storage_compaction_policy
                        ));
                    }
                    24 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).snapshot_free_retirement_pass_count
                            ));
                        }
                    }
                    25 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).finalization
                        ));
                    }
                    26 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pending_local_schema_stages
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
impl Drop for MaintenanceCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}
impl Drop for MaintenanceContext {
    fn drop(&mut self) {
        // These optional worker owners retire before remaining payload fields.
        #[cfg(any(test, feature = "test-seams"))]
        drop(self.writer_receipt_observer.take());
        drop(self.worker_stop.take());
        drop(self.persistence.take());
    }
}

impl MaintenanceContext {
    pub(super) fn remove_original_terminal_stages(&self, original_tx: TxId) {
        self.pending_local_schema_stages.lock().remove(&original_tx);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_without_callback_tx_control(&self, tx: TxId) -> Result<()> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => {
                let terminal = handle.with_host(|host| host.rollback_terminal(tx))?;
                self.remove_original_terminal_stages(terminal.identity().original_tx);
                drop(terminal);
                Ok(())
            }
            DatabaseTransactionHost::Legacy(_) => Err(DatabaseTransactionHost::producer_gap(
                "maintenance_owned_retention_rollback",
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_direct_metadata<R>(
        &self,
        write: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        publish_direct_metadata(&self.tx_mgr, write)
    }

    /// T2 for maintenance: the covered journal prefix is removed one bounded
    /// committed step at a time, each under its own commit-lock hold, so a
    /// write admitted while a backlog remains commits between steps instead
    /// of waiting for the whole cleanup. `decode` installs the decode owner a
    /// step runs under, if the caller has one.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn truncate_vector_journal_in_published_steps(
        &self,
        persistence: &RedbPersistence,
        catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut contextdb_vector::VectorWorkspaceReservation,
        mut decode: impl FnMut() -> Result<Option<crate::read_image_memory::ImageMemoryScope>>,
    ) -> Result<bool> {
        loop {
            let (complete, removed) = self.publish_direct_metadata(|| {
                let _decode = decode()?;
                persistence.truncate_vector_partition_journal_step_with_checkpoint(
                    catalog,
                    workspace,
                    || self.build_checkpoint(),
                )
            })?;
            if complete || removed == 0 {
                return Ok(complete);
            }
            #[cfg(any(test, feature = "test-seams"))]
            maybe_pause_between_vector_journal_cleanup_steps_for_test();
            self.build_checkpoint()?;
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_published_vector_cleanup_failure(
        &self,
        partition: &VectorPartitionRef,
        error: &Error,
    ) -> Result<VectorPartitionPublicationOutcome> {
        let outcome = VectorPartitionPublicationOutcome::published_with_cleanup_failure(error);
        let details = outcome
            .cleanup_failure
            .expect("a cleanup-failure outcome carries its retained detail");
        self.vector
            .record_partition_maintenance_failure_details(partition, details)?;
        Ok(outcome)
    }

    /// Reclaim the deferred-free backlog in at most `RECLAIM_STEPS_PER_CYCLE` bounded durable
    /// steps, stopping at the cycle's sampled boundary, at work a reader or savepoint still
    /// protects, at a stop request (through `build_checkpoint` before every step), or as soon as a
    /// foreground writer waits for the write slot. Committed steps stay durable; whatever remains is
    /// reclaimed by later cycles.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_deferred_storage(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        // A storage compaction sweep reclaims deferred frees in its own batches while it relocates
        // pages toward the front of the file; extra commits here would allocate behind it. The
        // stage resumes on the first cycle after the sweep completes.
        if persistence.active_storage_compaction()?.is_some() {
            return Ok(());
        }
        let mut boundary = None;
        #[cfg(any(test, feature = "test-seams"))]
        RECLAIM_STEPS_LAST_CYCLE_FOR_TEST.with(|steps| steps.set(0));
        for _ in 0..reclaim_steps_per_cycle() {
            self.build_checkpoint()?;
            match persistence.reclaim_deferred_step(boundary)? {
                redb::ReclaimStep::Progressed(next) => {
                    boundary = Some(next);
                    #[cfg(any(test, feature = "test-seams"))]
                    RECLAIM_STEPS_LAST_CYCLE_FOR_TEST.with(|steps| steps.set(steps.get() + 1));
                }
                redb::ReclaimStep::Drained
                | redb::ReclaimStep::ReaderHeld
                | redb::ReclaimStep::Blocked => break,
            }
            if persistence.has_waiting_writer() {
                break;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn build_checkpoint(&self) -> Result<()> {
        #[cfg(test)]
        resident_history_test_seams::note_checkpoint();
        #[cfg(any(test, feature = "test-seams"))]
        resident_history_test_seams::note(|work| {
            work.workspace_peak = work.workspace_peak.max(
                self.vector
                    .memory_ownership_snapshot_for_test()
                    .temporary_workspace,
            );
        });
        // Maintenance derives its work from memory, which no longer matches
        // the durable image once a publication was interrupted.
        if self.publication_interrupted.load(Ordering::SeqCst) {
            return Err(Error::PublicationInterrupted);
        }
        if self.resource_closed.load(Ordering::SeqCst)
            || self
                .worker_stop
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::SeqCst))
        {
            return Err(Error::ReadCancelled);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_generation_disk(&self, bytes: usize) -> Result<()> {
        self.build_checkpoint()?;
        let limit = self.disk_limit.load(Ordering::SeqCst);
        let current_bytes = self.file_bytes().unwrap_or(0);
        if limit == 0 || current_bytes.saturating_add(bytes as u64) <= limit {
            return Ok(());
        }
        Err(Error::DiskBudgetExceeded {
            operation: "prepare_vector_generation".into(),
            current_bytes,
            budget_limit_bytes: limit,
            hint: "Reduce retained file-backed data or raise DISK_LIMIT before writing more data."
                .into(),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn retry_one_vector_journal_truncation(
        &self,
        persistence: &RedbPersistence,
        catalog: &VectorPartitionGenerationCatalogRecord,
        partition: &VectorPartitionRef,
        partition_state_present: bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        vector_source_page_admission::with_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || {
                self.retry_one_vector_journal_truncation_with_source_admission(
                    persistence,
                    catalog,
                    partition,
                    partition_state_present,
                    read_memory.clone(),
                )
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn retry_one_vector_journal_truncation_with_source_admission(
        &self,
        persistence: &RedbPersistence,
        catalog: &VectorPartitionGenerationCatalogRecord,
        partition: &VectorPartitionRef,
        partition_state_present: bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut cleanup_workspace = self.vector.reserve_workspace_static(
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            0,
            "truncate_vector_partition_journal",
            "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before cleaning this maintained route.",
        )?;
        self.retry_one_vector_journal_truncation_in_workspace(
            persistence,
            catalog,
            partition,
            partition_state_present,
            &mut cleanup_workspace,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retry_one_vector_journal_truncation_in_workspace(
        &self,
        persistence: &RedbPersistence,
        catalog: &VectorPartitionGenerationCatalogRecord,
        partition: &VectorPartitionRef,
        partition_state_present: bool,
        cleanup_workspace: &mut contextdb_vector::VectorWorkspaceReservation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        match self.truncate_vector_journal_in_published_steps(
            persistence,
            catalog,
            cleanup_workspace,
            || Ok(None),
        ) {
            Ok(_) => {}
            Err(Error::StoreCorrupted { .. }) => {
                self.vector.quarantine_partition_route(
                    partition,
                    VectorRouteQuarantineReason::CorruptChanges,
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        if !partition_state_present {
            // The route was safely retired after its final live vector
            // disappeared. Physical journal cleanup still runs, but must not
            // recreate an empty partition merely because its catalog remains.
            return Ok(());
        }
        let selected = catalog.change.unwrap_or(catalog.base).generation_id;
        let installed = self
            .vector
            .partition_graph_generation_status(partition)
            .is_some_and(|status| {
                [
                    status.base,
                    status.change,
                    status.dormant_base,
                    status.dormant_change,
                ]
                .into_iter()
                .flatten()
                .any(|generation| generation.generation_id == selected)
            });
        // A quarantined descriptor or body must be replaced from the
        // authoritative raw vectors. Reinstalling the same catalog here
        // would decode the damaged generation before repair can replace it.
        if self.vector.partition_route_quarantine(partition).is_some() {
            return Ok(());
        }
        if !installed {
            self.install_published_vector_catalog(catalog, read_memory.clone())?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retry_vector_journal_truncations(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<VectorJournalCleanupRetryOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.build_checkpoint()?;
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(VectorJournalCleanupRetryOutcome::default());
        };
        let stream = self.maintenance_catalog_stream()?;
        #[cfg(test)]
        vector_journal_cleanup_cancellation_tests::after_inventory_capture_for_test();
        self.build_checkpoint()?;
        let mut outcome = VectorJournalCleanupRetryOutcome {
            first_failure_details: None,
            failed_partitions: FailedMaintenancePartitions::default(),
        };
        let mut terminal = None;
        let inventory = self.visit_maintenance_catalog_stream(stream, |entry| {
          let retain = (|| {
            if terminal.is_some() {
                return false;
            }
            if let Err(error) = self.build_checkpoint() {
                terminal = Some(error);
                return false;
            }
            let catalog = &entry.catalog;
            let mut route_memory =
                crate::memory_accounting::OwnedMemoryReservation::new(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant));
            let heap = RedbPersistence::maintenance_vector_catalog_heap_bytes(catalog);
            let route_admission = route_memory.try_grow_for(
                heap,
                "vector_maintenance",
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before inspecting this maintained route.",
            );
            let retry = route_admission.and_then(|()| {
                let partition =
                    VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone());
                let partition_state_present = self.tx_mgr.with_commit_lock(|| {
                    self.vector
                        .try_partition_state(&partition.index, &partition.partition_key)
                        .is_some_and(|state| !state.source_maintenance_metadata().historical_only)
                });
                let selected = catalog.change.unwrap_or(catalog.base).generation_id;
                let installed = self
                    .vector
                    .partition_graph_generation_status(&partition)
                    .is_some_and(|status| {
                        [
                            status.base,
                            status.change,
                            status.dormant_base,
                            status.dormant_change,
                        ]
                        .into_iter()
                        .flatten()
                        .any(|generation| generation.generation_id == selected)
                    });
                // The same catalog workspace funds route keys before the
                // identity probe; pending cleanup keeps its early operation.
                entry.workspace.resize(
                    heap.saturating_add(RedbPersistence::vector_journal_cleanup_key_reservation(
                        &catalog.index,
                        &catalog.partition_key,
                    )),
                    if entry.pending_cleanup {
                        "truncate_vector_partition_journal"
                    } else {
                        "prepare_vector_generation"
                    },
                    "Raise MEMORY_LIMIT before inspecting this maintained route.",
                )?;
                let cleanup_only = !partition_state_present
                    || installed
                    || self.vector.partition_route_quarantine(&partition).is_some();
                let covered = if cleanup_only {
                    vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                        || persistence.vector_partition_has_covered_journal_records(catalog),
                    )
                } else {
                    Ok(true)
                };
                let completed = match covered {
                    Ok(false) => Ok(()),
                    Err(
                        error @ (Error::MemoryBudgetExceeded { .. }
                        | Error::ReadCancelled
                        | Error::PublicationInterrupted),
                    ) => Err(error),
                    _ => {
                        let retry = || {
                            self.retry_one_vector_journal_truncation_in_workspace(
                                persistence,
                                catalog,
                                &partition,
                                partition_state_present,
                                &mut entry.workspace,
                             read_memory.clone())
                        };
                        if cleanup_only {
                            vector_source_page_admission::with_journal_cleanup_source_page_admission(
                                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                                retry,
                            )
                        } else {
                            // Restoring an uninstalled catalog reads and
                            // installs its generation after cleaning the journal.
                            vector_source_page_admission::with_source_page_admission(
                                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                                retry,
                            )
                        }
                    }
                };
                if completed.is_ok() && partition_state_present {
                    self.vector.clear_partition_maintenance_failure(&partition)?;
                }
                completed
            });
            #[cfg(test)]
            vector_journal_cleanup_cancellation_tests::after_retry_for_test(catalog);
            // A completed T2 stays committed, but a stop must reach the
            // caller before a later route or the cycle's closing work starts.
            let retry = self.build_checkpoint().and(retry);
            match retry {
                Ok(()) => false,
                Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                    terminal = Some(error);
                    false
                }
                Err(error) => {
                    let details =
                        VectorMaintenanceFailureDetails::from_published_cleanup_error(&error);
                    self.record_catalog_metadata_failure(catalog, details);
                    outcome.first_failure_details.get_or_insert(details);
                    // Cleanup locals are gone. Retain only this authenticated
                    // catalog's actual heap; its route blocks a build this cycle.
                    if let Err(shrink_error) = entry.workspace.resize(
                        heap,
                        "truncate_vector_partition_journal",
                        "Raise MEMORY_LIMIT before cleaning this maintained route.",
                    ) {
                        terminal = Some(shrink_error);
                    }
                    true
                }
            }
          })();
          Ok(retain)
        })?;
        outcome.first_failure_details = outcome.first_failure_details.or(inventory.failures.first);
        if let Some(error) = terminal {
            return Err(error);
        }
        outcome.failed_partitions.inventory = Some(inventory);
        outcome.failed_partitions.sources = Some(Arc::clone(&self.vector));
        Ok(outcome)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn install_published_vector_catalog(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.tx_mgr.with_maintenance_publication(|publication| {
            // The catalog and pending summary are already durable. New reads
            // must capture that committed storage version before using the
            // replacement generation; existing readers retain their root.
            publication.reserve()?;
            publication.complete();
            Ok(())
        })?;
        // Registration, graph loading and eviction run outside the commit gate.
        self.install_published_vector_catalog_at_boundary(catalog, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn install_published_vector_catalog_at_boundary(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        // Installing a published catalog is writer work: its layout and
        // route copies are admitted by the writer, never the caller's
        // reader.
        let mut maintenance_vector_copy_owner = contextdb_vector::VectorCopyAdmission::new(
            self.finalization.accountant.vector_budget()?,
        );
        let _publication_read = writer_read::suspend_root_for_manager(&self.tx_mgr);
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        // Final-delete publication still advances and cleans the durable
        // catalog, but retired raw history must not regain an indexed route.
        if self
            .vector
            .partition_is_historical_only(&catalog.index, &catalog.partition_key)
        {
            return Ok(());
        }
        let partition =
            VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone());
        let layout = contextdb_vector::scoped_index_layout(
            &self.vector,
            &catalog.index,
            &mut maintenance_vector_copy_owner,
        )?;
        let live_rows = self
            .vector
            .try_partition_state(&catalog.index, &catalog.partition_key)
            .map_or(0, |state| state.vector_count());
        let base_resident_bytes = DurableVectorGenerationLoader::descriptor_usize(
            catalog.base.resident_bytes,
            "published base generation resident byte declaration",
        )?;
        let base_load_bytes = DurableVectorGenerationLoader::checked_add(
            base_resident_bytes,
            DurableVectorGenerationLoader::descriptor_usize(
                catalog.base.durable_bytes,
                "published base generation durable byte declaration",
            )?,
            "published base graph load admission",
        )?;
        let base_dormant = DormantVectorGraphGeneration {
            identity: durable_vector_generation_identity(catalog.base, &layout, live_rows)?,
            resident_bytes: base_resident_bytes,
            load_bytes: base_load_bytes,
            reclaim_bytes: base_load_bytes,
        };
        let base_loader: Arc<dyn DormantVectorGraphLoader> =
            Arc::new(DurableVectorGenerationLoader {
                source: DurableVectorGenerationSource::Writable(persistence.clone()),
                accountant: crate::memory_accounting::AccountantHandle::clone(
                    &self.finalization.accountant,
                ),
                partition: partition.clone(),
                descriptor: catalog.base,
                base_generation_id: catalog.base.generation_id,
                is_base: true,
                base_covered: (catalog.base.covered_tx, catalog.base.covered_lsn),
                dimension: layout.dimension,
                quantization: layout.quantization,
                layout: layout.clone(),
                live_rows,
                declared_serving_cache: DeclaredServingCache::Live(
                    self.vector_serving_cache_bytes.clone(),
                ),
                // Captured serving owns journal replay at its selected root. The
                // shared retained loader installs only this sealed generation.
                tail_covered_lsn: None,
            });
        self.vector.register_dormant_partition_generation(
            &partition,
            true,
            base_dormant,
            base_loader,
        )?;
        if let Some(change) = catalog.change {
            let change_resident_bytes = DurableVectorGenerationLoader::descriptor_usize(
                change.resident_bytes,
                "published change generation resident byte declaration",
            )?;
            let change_load_bytes = DurableVectorGenerationLoader::checked_add(
                change_resident_bytes,
                DurableVectorGenerationLoader::descriptor_usize(
                    change.durable_bytes,
                    "published change generation durable byte declaration",
                )?,
                "published change graph load admission",
            )?;
            let change_dormant = DormantVectorGraphGeneration {
                identity: durable_vector_generation_identity(change, &layout, live_rows)?,
                resident_bytes: change_resident_bytes,
                load_bytes: change_load_bytes,
                reclaim_bytes: change_load_bytes,
            };
            let change_loader: Arc<dyn DormantVectorGraphLoader> =
                Arc::new(DurableVectorGenerationLoader {
                    source: DurableVectorGenerationSource::Writable(persistence.clone()),
                    accountant: crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    partition: partition.clone(),
                    descriptor: change,
                    base_generation_id: catalog.base.generation_id,
                    is_base: false,
                    base_covered: (catalog.base.covered_tx, catalog.base.covered_lsn),
                    dimension: layout.dimension,
                    quantization: layout.quantization,
                    layout: layout.clone(),
                    live_rows,
                    declared_serving_cache: DeclaredServingCache::Live(
                        self.vector_serving_cache_bytes.clone(),
                    ),
                    tail_covered_lsn: None,
                });
            self.vector.register_dormant_partition_generation(
                &partition,
                false,
                change_dormant,
                change_loader,
            )?;
        }
        if catalog.base.resident_bytes != 0 {
            self.vector
                .preload_dormant_partition_generation(&partition, true)?;
        }
        if catalog
            .change
            .is_some_and(|change| change.resident_bytes != 0)
        {
            self.vector
                .preload_dormant_partition_generation(&partition, false)?;
        }
        self.vector.clear_partition_route_quarantine(&partition);
        Ok(())
    }

    pub(super) fn file_bytes(&self) -> Option<u64> {
        self.persistence
            .as_ref()
            .and_then(|persistence| std::fs::metadata(persistence.path()).ok())
            .map(|meta| meta.len())
    }

    /// The optional, MUCH RARER automatic compaction path — see
    /// [`AUTO_COMPACT_MIN_INTERVAL`]'s own doc comment for why this exists
    /// separately from any scoped cleanup pass's own compact decision.
    /// Starts a sweep only after the configured interval and fragmentation
    /// threshold are satisfied. An active sweep advances one bounded durable
    /// batch per cycle; completion starts the next interval, and so does a
    /// check that found the file below the threshold. The threshold is
    /// measured on the allocator's reclaimable free-page ratio -- interior
    /// free pages, which only a whole-page relocation gives back, not in-page
    /// slack and not the trailing free run, which the file reuses before it
    /// grows and which compaction's final trim returns (an ordinary commit
    /// trims it only once it reaches half of the last region) -- which
    /// costs the same at any store size; untrusted totals take no decision. It runs outside
    /// the commit lock, since
    /// this is called from `run_cycle` after both `run_pruning` and
    /// `run_currency` have already returned.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn maybe_auto_compact(&self) -> Result<CompactionReport> {
        // The policy that governs this cycle's decision, named in its receipt.
        let (policy, min_interval) = {
            let state = self.storage_compaction_policy.lock();
            (state.declared, state.min_interval())
        };
        let not_run = |fragmentation_before| CompactionReport {
            fragmentation_before,
            policy,
            ..CompactionReport::default()
        };
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(not_run(None));
        };
        let due = self
            .last_auto_compact_at
            .lock()
            .is_none_or(|at| compaction_clock_now().saturating_duration_since(at) >= min_interval);
        let active = vector_source_page_admission::with_preparation_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || persistence.active_storage_compaction(),
        )?;
        let fragmentation_before = match active {
            // An active sweep resumes on the measurement it started with,
            // which stays unknown if it was never measured.
            Some(sample) => sample,
            None if !due => return Ok(not_run(None)),
            None => {
                #[cfg(any(test, feature = "test-seams"))]
                AUTOMATIC_COMPACTION_CHECKS.with(|checks| checks.set(checks.get() + 1));
                match persistence.allocator_free_page_ratio()? {
                    // A completed negative check starts the interval just as
                    // a completed sweep does, and reports what it measured.
                    Some(ratio) if ratio < policy.start_ratio() => {
                        *self.last_auto_compact_at.lock() = Some(compaction_clock_now());
                        return Ok(not_run(Some(ratio)));
                    }
                    Some(ratio) => Some(ratio),
                    // Untrusted totals: nothing measured, no decision taken,
                    // and the receipt says unknown.
                    None => return Ok(not_run(None)),
                }
            }
        };
        let bytes_before = self.file_bytes();
        let start = Instant::now();
        let Some((progress, handle_recycle_micros)) =
            publish_storage_compaction_batch_with_deferral(
                &self.tx_mgr,
                persistence,
                &self.finalization.accountant,
                &self.publication_interrupted,
                fragmentation_before,
                true,
            )?
        else {
            return Ok(not_run(fragmentation_before));
        };
        let duration_micros = start.elapsed().as_micros() as u64;
        if progress.complete {
            *self.last_auto_compact_at.lock() = Some(compaction_clock_now());
        }
        let bytes_after = self.file_bytes();
        Ok(CompactionReport {
            ran: true,
            duration_micros,
            bytes_before,
            bytes_after,
            file_shrank: matches!((bytes_before, bytes_after), (Some(b), Some(a)) if a < b),
            fragmentation_before,
            page_slack_ratio: None,
            policy,
            handle_recycled: handle_recycle_micros.is_some(),
            handle_recycle_micros: handle_recycle_micros.unwrap_or(0),
        })
    }

    /// Generic retention, then — once the rows are gone — the compaction
    /// decision on the SAME conservative threshold the currency path uses.
    /// True when any table declares RETAIN. Retention borrows eligible rows
    /// at the commit frontier and admits only selected identities and mutations;
    /// without a retention declaration even that selection has no possible effect.
    pub(super) fn has_retention_work(&self) -> bool {
        self.relational
            .table_meta
            .read()
            .values()
            .any(|meta| meta.default_ttl_seconds.is_some())
    }

    #[cfg(test)]
    pub(super) fn stamp_expired_rows(&self, now: Wallclock) -> Result<()> {
        self.stamp_expired_rows_for(now, RetentionPreparation::Expiry)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stamp_expired_rows_for(
        &self,
        now: Wallclock,
        preparation: RetentionPreparation,
    ) -> Result<()> {
        let _memory = crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        )?;
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| self.stamp_expired_rows_owned(host, now, preparation));
        }
        let tx = self.tx_mgr.begin();
        let workspace =
            std::cell::RefCell::new(crate::memory_accounting::OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            ));
        // Armed just before the durable apply; see `PublicationInterruptionWatch`.
        // This commit is durable the moment the manager applies it, and the
        // in-memory commit bookkeeping that makes it visible happens after --
        // so an unwind in between leaves this handle behind its own durable
        // image, exactly as it does on the local and received doors.
        let publication_watch = std::cell::RefCell::new(None::<PublicationInterruptionWatch>);
        let maintenance_read_memory: contextdb_core::read_memory::ReadMemoryOwner =
            self.finalization.accountant.read_budget()?;
        let result = self.tx_mgr.commit_with_reserved_lsn_callback_mut(
            tx,
            maintenance_read_memory.clone(),
            |expiry_lsn| {
                // The reserved-LSN callback holds the commit frontier. Borrow
                // authoritative rows there; copy only admitted mutation identities.
                let metas = self.relational.table_meta.read();
                let frontier = MaintenanceFrontier::capture(
                    &self.relational,
                    &self.tx_mgr,
                    crate::persistence::writer_source::SourceWindowOwner::writer(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                    ),
                )?
                .retention_batch(&metas, "expiry", &mut workspace.borrow_mut())?;
                let watermark = self.sync_watermark.load(Ordering::SeqCst);
                let candidates = retention_prune_candidates(
                    &metas,
                    &frontier,
                    now,
                    watermark,
                    &mut workspace.borrow_mut(),
                    RetentionPreparation::Expiry,
                )?;
                let protected = fk_protected_prune_candidates(&metas, &frontier, &candidates)?;
                let cohort = frontier
                    .cohort_progress(|table, key| protected.contains_physical(table, key))?;
                self.tx_mgr.with_write_set(tx, |ws| -> Result<()> {
                    ws.config_writes
                        .extend(frontier.retention_progress().iter().cloned());
                    ws.config_writes.extend(cohort);
                    let mut expired_nodes = HashSet::new();
                    for table in metas.keys() {
                        if metas
                            .get(table)
                            .is_none_or(|meta| meta.default_ttl_seconds.is_none())
                        {
                            continue;
                        }
                        frontier.visit(table, |row| {
                            #[cfg(any(test, feature = "test-seams"))]
                            resident_history_test_seams::note(|work| {
                                work.retention_keys_examined += 1
                            });
                            if row.deleted_tx.is_some() || !candidates.contains(table, row) {
                                return Ok(true);
                            }
                            if prune_blocker_for_referenced_parent(
                                table,
                                row,
                                &metas,
                                &frontier,
                                &candidates,
                                &protected,
                            )?
                            .is_some()
                            {
                                return Ok(true);
                            }
                            // Own the write-set strings, config encoding, node
                            // set and vector deletion identities before allocating.
                            let vector_bytes = metas.get(table).map_or(0, |meta| {
                                meta.columns
                                    .iter()
                                    .filter(|column| {
                                        matches!(column.column_type, ColumnType::Vector(_))
                                    })
                                    .fold(0usize, |bytes, column| {
                                        bytes.saturating_add(256 + table.len() + column.name.len())
                                    })
                            });
                            workspace.borrow_mut().try_grow_for(
                                1024usize
                                    .saturating_add(table.len().saturating_mul(8))
                                    .saturating_add(vector_bytes),
                                "retention",
                                "prepare_expiry",
                                "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
                            )?;
                            ws.relational_deletes.push((table.clone(), row.row_id, tx));
                            if let Some(Value::Uuid(id)) = row.values.get("id") {
                                expired_nodes.insert(*id);
                            }
                            if let Some(meta) = metas.get(table) {
                                for column in &meta.columns {
                                    if matches!(column.column_type, ColumnType::Vector(_)) {
                                        ws.vector_deletes.push((
                                            VectorIndexRef::new(table, &column.name),
                                            row.row_id,
                                            tx,
                                        ));
                                    }
                                }
                            }
                            let identity = (table.clone(), row.row_id, row.created_tx, expiry_lsn);
                            ws.config_writes.push((
                                crate::composite_store::retention_expiry_key(
                                    table,
                                    row.row_id,
                                    row.created_tx,
                                ),
                                RedbPersistence::encode_config_value(&identity)?,
                            ));
                            Ok(true)
                        })?;
                    }
                    writer_read::visit_graph_versions_for_nodes(
                        &self.graph,
                        writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?,
                        &expired_nodes,
                        |entry| {
                            if entry.deleted_tx.is_none()
                                && (expired_nodes.contains(&entry.source)
                                    || expired_nodes.contains(&entry.target))
                            {
                                workspace.borrow_mut().try_grow_for(
                                    128usize.saturating_add(entry.edge_type.len()),
                                    "retention",
                                    "prepare_expiry",
                                    "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
                                )?;
                                ws.adj_deletes.push((
                                    entry.source,
                                    entry.edge_type.clone(),
                                    entry.target,
                                    tx,
                                ));
                            }
                            Ok(())
                        },
                        maintenance_read_memory.clone(),
                    )?;
                    Ok(())
                })?
            },
            |_| {
                *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(
                    &self.publication_interrupted,
                ));
                Ok(())
            },
        );
        #[cfg(test)]
        take_local_commit_stage_fault_for_test(
            LocalCommitStageFaultPoint::AfterReservedLsnDurableCommit,
        );
        let watch_completed = publication_watch.borrow_mut().take();
        match result {
            Ok(committed) => {
                if let Some(watch) = watch_completed {
                    watch.completed();
                }
                // Callback selections are gone; free the returned write-set
                // payload before its original selection workspace settles.
                drop(committed);
                drop(workspace);
                Ok(())
            }
            Err(failure) => {
                // A failure before the manager takes the write set leaves the
                // transaction active; both failure paths publish no expiry.
                if let Some(watch) = watch_completed {
                    watch.completed();
                }
                let _ = self.tx_mgr.rollback(tx);
                Err(retention_source_admission_error(failure.error, preparation))
            }
        }
    }

    /// Retention's reserved callback owns every produced body before the
    /// paired terminal extracts the original source and journal nodes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stamp_expired_rows_owned(
        &self,
        host: &EngineDatabaseTransactions,
        now: Wallclock,
        preparation: RetentionPreparation,
    ) -> Result<()> {
        let tx = self.tx_mgr.try_begin()?;
        let workspace =
            std::cell::RefCell::new(crate::memory_accounting::OwnedMemoryReservation::new(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            ));
        // Armed just before the durable apply; see `PublicationInterruptionWatch`.
        // This commit is durable the moment the manager applies it, and the
        // in-memory commit bookkeeping that makes it visible happens after --
        // so an unwind in between leaves this handle behind its own durable
        // image, exactly as it does on the local and received doors.
        let publication_watch = std::cell::RefCell::new(None::<PublicationInterruptionWatch>);
        let maintenance_read_memory: contextdb_core::read_memory::ReadMemoryOwner =
            self.finalization.accountant.read_budget()?;
        let result = host.commit_reserved_terminal(
            tx,
            maintenance_read_memory.clone(),
            |expiry_lsn| {
                // The reserved-LSN callback holds the commit frontier. Borrow
                // authoritative rows there; copy only admitted mutation identities.
                let metas = self.relational.table_meta.read();
                let frontier = MaintenanceFrontier::capture(
                    &self.relational,
                    &self.tx_mgr,
                    crate::persistence::writer_source::SourceWindowOwner::writer(
                        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                    ),
                )?
                .retention_batch(&metas, "expiry", &mut workspace.borrow_mut())?;
                let watermark = self.sync_watermark.load(Ordering::SeqCst);
                let candidates = retention_prune_candidates(
                    &metas,
                    &frontier,
                    now,
                    watermark,
                    &mut workspace.borrow_mut(),
                    RetentionPreparation::Expiry,
                )?;
                let protected = fk_protected_prune_candidates(&metas, &frontier, &candidates)?;
                let cohort = frontier
                    .cohort_progress(|table, key| protected.contains_physical(table, key))?;
                host.transactions.with_transactions(|transactions| transactions.with_engine(tx, |guard| {
                    if !frontier.retention_progress().is_empty() {
                        let progress = guard.copy_aux(
                            contextdb_tx::staged_source::AuxBorrow::ConfigWrites(frontier.retention_progress()),
                        )?;
                        let at = guard.read().config_writes.len();
                        let prepared = guard.prepare_aux_splice(at, &[], progress)?;
                        guard.publish_tx(prepared);
                    }
                    if let Some(cohort) = &cohort {
                        let cohort = guard.copy_aux(
                            contextdb_tx::staged_source::AuxBorrow::ConfigWrites(std::slice::from_ref(cohort)),
                        )?;
                        let at = guard.read().config_writes.len();
                        let prepared = guard.prepare_aux_splice(at, &[], cohort)?;
                        guard.publish_tx(prepared);
                    }
                    let mut expired_nodes = HashSet::new();
                    for table in metas.keys() {
                        if metas
                            .get(table)
                            .is_none_or(|meta| meta.default_ttl_seconds.is_none())
                        {
                            continue;
                        }
                        frontier.visit(table, |row| {
                            #[cfg(any(test, feature = "test-seams"))]
                            resident_history_test_seams::note(|work| {
                                work.retention_keys_examined += 1
                            });
                            if row.deleted_tx.is_some() || !candidates.contains(table, row) {
                                return Ok(true);
                            }
                            if prune_blocker_for_referenced_parent(
                                table,
                                row,
                                &metas,
                                &frontier,
                                &candidates,
                                &protected,
                            )?
                            .is_some()
                            {
                                return Ok(true);
                            }
                            // Own the write-set strings, config encoding, node
                            // set and vector deletion identities before allocating.
                            let vector_bytes = metas.get(table).map_or(0, |meta| {
                                meta.columns
                                    .iter()
                                    .filter(|column| {
                                        matches!(column.column_type, ColumnType::Vector(_))
                                    })
                                    .fold(0usize, |bytes, column| {
                                        bytes.saturating_add(256 + table.len() + column.name.len())
                                    })
                            });
                            workspace.borrow_mut().try_grow_for(
                                1024usize
                                    .saturating_add(table.len().saturating_mul(8))
                                    .saturating_add(vector_bytes),
                                "retention",
                                "prepare_expiry",
                                "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
                            )?;
                            self.stage_retention_aux(host, guard, table.len(), || {
                                Ok(contextdb_tx::staged_source::AuxBody::RelationalDelete(
                                    (table.clone(), row.row_id, tx),
                                ))
                            })?;
                            if let Some(Value::Uuid(id)) = row.values.get("id") {
                                expired_nodes.insert(*id);
                            }
                            if let Some(meta) = metas.get(table) {
                                for column in &meta.columns {
                                    if matches!(column.column_type, ColumnType::Vector(_)) {
                                        self.stage_retention_aux(
                                            host, guard, table.len().saturating_add(column.name.len()),
                                            || Ok(contextdb_tx::staged_source::AuxBody::VectorDelete((
                                                VectorIndexRef::new(table, &column.name), row.row_id, tx,
                                            ))),
                                        )?;
                                    }
                                }
                            }
                            self.stage_retention_expiry_config(
                                host, guard, table, row.row_id, row.created_tx, expiry_lsn,
                            )?;
                            Ok(true)
                        })?;
                    }
                    writer_read::visit_graph_versions_for_nodes(
                        &self.graph,
                        writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?,
                        &expired_nodes,
                        |entry| {
                            if entry.deleted_tx.is_none()
                                && (expired_nodes.contains(&entry.source)
                                    || expired_nodes.contains(&entry.target))
                            {
                                workspace.borrow_mut().try_grow_for(
                                    128usize.saturating_add(entry.edge_type.len()),
                                    "retention",
                                    "prepare_expiry",
                                    "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
                                )?;
                                self.stage_retention_aux(host, guard, entry.edge_type.len(), || {
                                    Ok(contextdb_tx::staged_source::AuxBody::AdjDelete((
                                        entry.source, entry.edge_type.clone(), entry.target, tx,
                                    )))
                                })?;
                            }
                            Ok(())
                        },
                        maintenance_read_memory.clone(),
                    )?;
                    Ok(())
                }))
            },
            |_, _| {
                *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(
                    &self.publication_interrupted,
                ));
                Ok(())
            },
        );
        #[cfg(test)]
        take_local_commit_stage_fault_for_test(
            LocalCommitStageFaultPoint::AfterReservedLsnDurableCommit,
        );
        let watch_completed = publication_watch.borrow_mut().take();
        match result {
            Ok((_, terminal)) => {
                self.remove_original_terminal_stages(terminal.identity().original_tx);
                drop(terminal);
                if let Some(watch) = watch_completed {
                    watch.completed();
                }
                // The callback selections, staged bodies and terminal have
                // physically dropped before their selection workspace ends.
                drop(workspace);
                Ok(())
            }
            Err(failure) => {
                // A failure before the manager takes the write set leaves the
                // transaction active; both failure paths publish no expiry.
                if let Some(watch) = watch_completed {
                    watch.completed();
                }
                let (error, terminal) = failure.into_parts();
                if let Some(terminal) = terminal {
                    self.remove_original_terminal_stages(terminal.identity().original_tx);
                    drop(terminal);
                } else {
                    // Before-commit refusal leaves the original source and
                    // journal installed; only paired rollback may retire them.
                    let _ = self.rollback_without_callback_tx_control(tx);
                }
                Err(retention_source_admission_error(error, preparation))
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_retention_aux(
        &self,
        host: &EngineDatabaseTransactions,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        bytes: usize,
        produce: impl FnOnce() -> Result<contextdb_tx::staged_source::AuxBody>,
    ) -> Result<()> {
        let mut admission = OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            bytes,
            "retention",
            "prepare_expiry",
            "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
        )?;
        // Admission precedes the original native allocation. Keep its body
        // local until the actual funder accepts this original reservation.
        let body = produce()?;
        let read = guard.read();
        let at = match &body {
            contextdb_tx::staged_source::AuxBody::RelationalDelete(_) => {
                read.relational_deletes.len()
            }
            contextdb_tx::staged_source::AuxBody::VectorDelete(_) => read.vector_deletes.len(),
            contextdb_tx::staged_source::AuxBody::AdjDelete(_) => read.adj_deletes.len(),
            contextdb_tx::staged_source::AuxBody::ConfigWrite(_) => read.config_writes.len(),
            _ => return Err(Error::Other("unsupported retention mutation".into())),
        };
        let invoice = admission.into_source_aux_invoice_for(&host.funder)?;
        let batch = guard.prepare_owned_aux(body, invoice)?;
        let prepared = guard.prepare_aux_splice(at, &[], batch)?;
        guard.publish_tx(prepared);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_retention_expiry_config(
        &self,
        host: &EngineDatabaseTransactions,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        row: RowId,
        created: TxId,
        lsn: Lsn,
    ) -> Result<()> {
        struct ByteCount(usize);
        impl std::fmt::Write for ByteCount {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        impl std::io::Write for ByteCount {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("retention config size overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let prefix = crate::composite_store::RETENTION_EXPIRY_PREFIX;
        let mut key_count = ByteCount(0);
        std::fmt::Write::write_fmt(
            &mut key_count,
            format_args!("{prefix}{}:{table}:{}:{}", table.len(), row.0, created.0,),
        )
        .map_err(|_| Error::Other("retention config size overflow".into()))?;
        // Serde encodes borrowed str and the original String identically;
        // count without allocating the temporary String from the Legacy body.
        let identity = (table, row, created, lsn);
        let mut encoded_count = ByteCount(0);
        bincode::serde::encode_into_std_write(
            identity,
            &mut encoded_count,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        let bytes = key_count
            .0
            .checked_add(encoded_count.0)
            .ok_or_else(|| Error::Other("retention config size overflow".into()))?;
        self.stage_retention_aux(host, guard, bytes, || {
            let mut key = String::with_capacity(key_count.0);
            std::fmt::Write::write_fmt(
                &mut key,
                format_args!("{prefix}{}:{table}:{}:{}", table.len(), row.0, created.0,),
            )
            .map_err(|_| Error::Other("retention config formatting failed".into()))?;
            let mut encoded = Vec::with_capacity(encoded_count.0);
            bincode::serde::encode_into_std_write(
                identity,
                &mut encoded,
                bincode::config::standard(),
            )
            .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
            Ok(contextdb_tx::staged_source::AuxBody::ConfigWrite((
                key, encoded,
            )))
        })
    }

    /// A pruning-only pass is a boundary of its own: it observes the process
    /// ceiling once before its first admission.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_pruning(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<PruningReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        crate::memory_accounting::observe_inherited_process_ceiling();
        self.run_pruning_within_cycle(read_memory)
    }

    /// Pruning under a boundary that has already observed the process ceiling.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_pruning_within_cycle(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<PruningReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Nothing declares RETAIN, so there is nothing this pass could prune,
        // no fragmentation decision it could take, and no report field it could
        // fill. Bail BEFORE the commit lock and candidate selection.
        let policy = self.storage_compaction_policy.lock().declared;
        if !self.has_retention_work() {
            return Ok(PruningReport {
                policy,
                ..PruningReport::default()
            });
        }
        // Admit the carry destination before expiry opens any row cursor. Its
        // published owner remains charged; this receipt covers the replacement
        // workspace and is transferred into reclamation rather than duplicated.
        let mut carry_workspace = crate::memory_accounting::OwnedMemoryReservation::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
        let prepaid_carry_bytes = {
            let carried = self.retention_deferred_edge_nodes.lock();
            let bytes = carried.nodes.len().saturating_mul(128);
            carry_workspace.try_grow_for(
                bytes,
                "retention",
                "prepare_prune",
                "Raise MEMORY_LIMIT to admit reader-deferred retention identities.",
            )?;
            bytes
        };
        let prepaid_durable_carry_bytes = match &self.persistence {
            Some(persistence) => persistence.retention_deferred_edge_workspace_bytes()?,
            None => 0,
        };
        carry_workspace.try_grow_for(
            prepaid_durable_carry_bytes,
            "retention",
            "prepare_prune",
            "Raise MEMORY_LIMIT to load reader-deferred retention identities.",
        )?;
        let file_bytes_before = self.file_bytes();
        // Sampled BEFORE the prune, deliberately. redb counts just-freed pages
        // as fragmented, so a reading taken AFTER a prune is dominated by the
        // pages that prune released — it measures the prune, not the file, and
        // reads high whatever fraction was removed. The pre-prune sample
        // measures the thing the shared threshold is written about: waste that
        // SURVIVED page reuse across earlier cycles. One consequence is
        // deliberate: a large prune of a previously tight file may not compact
        // until the next cycle, which is the rare-not-per-cycle posture the
        // constant's own doc describes.
        //
        // The sample is the allocator's maintained free-page ratio, read
        // without visiting a page, so a cycle's decision costs the same at
        // any store size. `None` (untrusted totals) takes no decision.
        let fragmentation_before = match self.persistence.as_ref() {
            Some(persistence) => persistence.allocator_free_page_ratio()?,
            None => None,
        };
        let now = Wallclock::now();
        self.stamp_expired_rows_for(now, RetentionPreparation::Prune)?;
        let mut report = self
            .tx_mgr
            .with_maintenance_publication(|publication| {
                prune_expired_rows(
                    self,
                    self.sync_watermark.load(Ordering::SeqCst),
                    now,
                    publication,
                    carry_workspace,
                    prepaid_carry_bytes,
                    prepaid_durable_carry_bytes,
                    read_memory.clone(),
                )
            })
            .map_err(|error| {
                retention_source_admission_error(error, RetentionPreparation::Prune)
            })?;
        report.file_bytes_before = file_bytes_before;
        report.fragmentation_before = fragmentation_before;
        report.policy = policy;
        // Compaction itself still runs AFTER the prune (there is no point
        // rewriting the file before the rows leave it) and OUTSIDE the commit
        // lock: redb compaction takes the redb mutex with `&mut` and must not
        // run while a txn is outstanding on this thread. Only the DECISION
        // rides the pre-prune observation above, and the reported
        // `fragmentation_before` is that same observed value — the decision
        // input and the receipt are one number, never a derived stand-in.
        if report.pruned_rows > 0
            && let Some(persistence) = self.persistence.as_ref()
            && let Some(fragmentation_before) = fragmentation_before
            && fragmentation_before >= policy.start_ratio()
        {
            let progress = publish_storage_compaction_batch_with_deferral(
                &self.tx_mgr,
                persistence,
                &self.finalization.accountant,
                &self.publication_interrupted,
                Some(fragmentation_before),
                true,
            )?;
            if let Some((progress, _)) = progress {
                if progress.complete {
                    *self.last_auto_compact_at.lock() = Some(compaction_clock_now());
                }
                report.compacted = true;
            } else {
                *self.last_auto_compact_at.lock() = None;
            }
        }
        report.file_bytes_after = self.file_bytes();
        report.file_shrank = matches!(
            (report.file_bytes_before, report.file_bytes_after),
            (Some(before), Some(after)) if after < before
        );
        Ok(report)
    }

    /// Every table that DECLARES `HISTORY CURRENT ONLY` right now, whether it
    /// was created here, arrived on reopen, or arrived over synced DDL. The
    /// declaration-driven replacement for the hardcoded `VERSION_COMPACTION_
    /// TABLES` list: eligibility is a property of what a table declared, not
    /// of its name.
    pub(super) fn version_cleanup_eligible_tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = self
            .relational
            .table_meta
            .read()
            .iter()
            .filter(|(_, meta)| meta.history_policy == Some(HistoryPolicy::CurrentOnly))
            .map(|(name, _)| name.clone())
            .collect();
        tables.sort();
        tables
    }

    /// The scoped cleanup pass only; this never compacts the storage file.
    /// Selection visits eligible row histories and their vector directories;
    /// durable removal visits selected keys and affected change-log LSN groups.
    /// Coupling this work to whole-file compaction made steady-state churn on
    /// a small file (where routine superseded-version debris is a large
    /// fraction of a small file) retrigger a full-file rewrite on every
    /// cycle, which is exactly the per-cycle-compaction cost the
    /// version-cleanup scaling bench measures and bounds separately from the
    /// scoped pass's own cost. Compaction is now EITHER an explicit operator
    /// action (`Database::compact_now`, `.maintenance compact`) or the much
    /// rarer, interval-gated automatic path `run_cycle` drives separately
    /// (`MaintenanceContext::maybe_auto_compact`) — see
    /// [`AUTO_COMPACT_MIN_INTERVAL`].
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_currency(
        &self,
        gate: CurrencyGate,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CurrencyCompactionReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let eligible = self.version_cleanup_eligible_tables();
        let tables = eligible.iter().map(String::as_str).collect::<Vec<_>>();
        if gate == CurrencyGate::Scheduled
            && !currency_compaction_pending(
                &self.relational,
                &self.tx_mgr,
                &self.finalization.accountant,
                &tables,
                CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD,
            )?
        {
            return Ok(CurrencyCompactionReport::default());
        }
        // Timed INSIDE the closure, from the first instruction that runs once
        // `with_commit_lock` has actually granted the lock -- never before it.
        // Timing from before the call measures however long this thread
        // waited to ACQUIRE the lock plus the hold itself, which is a
        // different, larger number that can make a quiet database look like
        // an expensive hold when the pass itself was instant.
        self.tx_mgr.with_maintenance_publication(
            |publication| -> Result<CurrencyCompactionReport> {
                let hold_start = Instant::now();
                let mut report = compact_currency_versions_inner(
                    &self.relational,
                    &self.graph,
                    &self.vector,
                    &self.finalization.accountant,
                    self.persistence.as_ref(),
                    &self.change_log,
                    &self.change_log_table_index,
                    &self.change_log_lsn_refcounts,
                    &self.tx_mgr,
                    &self.snapshot_registry,
                    publication,
                    &self.publication_interrupted,
                    &tables,
                    read_memory.clone(),
                )?;
                report.commit_lock_hold_micros = hold_start.elapsed().as_micros() as u64;
                Ok(report)
            },
        )
    }

    /// Test-only twin of `run_currency`: the identical real mechanism
    /// (`compact_currency_versions_inner`), unconditionally (no scheduled
    /// gate), over a CALLER-SUPPLIED table list instead of the declared
    /// eligibility set `run_currency` computes. `run_currency` above is
    /// untouched -- this exists so a test can exercise the real pass against
    /// a table shape it has not declared `HISTORY CURRENT ONLY` on (yet), with
    /// zero change to production behavior. Also never compacts -- see
    /// `run_currency`'s own doc comment.
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn run_currency_for_tables(
        &self,
        tables: &[&str],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CurrencyCompactionReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Hold-only timing -- see the comment in `run_currency` above.
        self.tx_mgr.with_maintenance_publication(
            |publication| -> Result<CurrencyCompactionReport> {
                let hold_start = Instant::now();
                let mut report = compact_currency_versions_inner(
                    &self.relational,
                    &self.graph,
                    &self.vector,
                    &self.finalization.accountant,
                    self.persistence.as_ref(),
                    &self.change_log,
                    &self.change_log_table_index,
                    &self.change_log_lsn_refcounts,
                    &self.tx_mgr,
                    &self.snapshot_registry,
                    publication,
                    &self.publication_interrupted,
                    tables,
                    read_memory.clone(),
                )?;
                report.commit_lock_hold_micros = hold_start.elapsed().as_micros() as u64;
                Ok(report)
            },
        )
    }

    /// The engine's own durable trigger-audit history is the first internal
    /// consumer of retention: entries older than the shipped default window
    /// age out, including entries a DROP TABLE orphaned. The in-memory ring
    /// keeps its own bounded semantics and is never touched here.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_trigger_audit_retention(&self) -> Result<u64> {
        let now = Wallclock::now();
        let window = Duration::from_secs(self.trigger_audit_retention_secs.load(Ordering::SeqCst));
        let cutoff = now.0.saturating_sub(window.as_millis() as u64);
        match self.persistence.as_ref() {
            Some(persistence) => {
                self.publish_direct_metadata(|| persistence.prune_trigger_audit_history(cutoff))
            }
            None => {
                let mut history = self.trigger.volatile_audit_history.lock();
                let before = history.len();
                history.retain(|(stamped_at, _)| stamped_at.0 >= cutoff);
                let removed = (before - history.len()) as u64;
                drop(history);
                Ok(removed.saturating_add(self.trigger.prune_original_audit_history(cutoff)))
            }
        }
    }

    /// Cheap pre-check for `retire_snapshot_free_vector_partitions`: whether
    /// this maintenance cycle could retire or reclaim anything at all.
    /// Conservative -- `true` whenever real work cannot be ruled out
    /// cheaply -- so `false` is the only answer that may safely skip
    /// beginning the guarded removal pass, and with it, parking every new
    /// read for the pass's duration. Never takes the commit lock and never
    /// begins a pass itself; the pass's own sampling (registered snapshots,
    /// persisted catalog contents) remains exactly as before whenever it
    /// does run.
    pub(super) fn vector_partition_retirement_is_possible(&self) -> bool {
        self.vector.has_snapshot_free_retirement_candidate()
            || self.vector.has_retired_graph_generations()
            || (self.persistence.is_some()
                && self
                    .snapshot_registry
                    .no_snapshots_besides_construction_floors())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_snapshot_free_vector_partitions(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<MaintenanceMetadataFailures> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut maintenance_vector_copy_owner =
            crate::vector_read_memory::admission(read_memory.clone());
        if !self.vector_partition_retirement_is_possible() {
            return Ok(MaintenanceMetadataFailures::default());
        }
        self.tx_mgr.with_maintenance_publication(|publication| {
            #[cfg(any(test, feature = "test-seams"))]
            self.snapshot_free_retirement_pass_count
                .fetch_add(1, Ordering::SeqCst);
            let active_removal_pass = self
                .snapshot_registry
                .begin_vector_removal_pass(self.tx_mgr.current_tx_max());
            let registered = active_removal_pass.registered_snapshots();
            // This is a fresh authenticated sample inside the removal boundary,
            // before any catalog-driven removal or resident retirement.
            let (mut inventory, mut retired) =
                self.reclaim_closing_source_graphs_locked(registered)?;
            if !inventory.complete {
                return Ok(inventory.failures);
            }
            retired = retired.saturating_add(self.vector.retire_snapshot_free_partition_states(
                registered,
                self.finalization.accountant.as_ref(),
                &mut maintenance_vector_copy_owner,
            )?);
            if registered.is_empty() || retired != 0 {
                self.reclaim_catalogs_locked(
                    &mut inventory,
                    publication,
                    active_removal_pass.registered_snapshots(),
                )?;
            }
            Ok(inventory.failures)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_snapshot_free_vector_graphs(
        &self,
    ) -> Result<MaintenanceMetadataFailures> {
        if !self.vector.has_retired_graph_generations() {
            return Ok(MaintenanceMetadataFailures::default());
        }
        self.tx_mgr.with_maintenance_publication(|publication| {
            let active_removal_pass = self
                .snapshot_registry
                .begin_vector_removal_pass(self.tx_mgr.current_tx_max());
            let (mut inventory, reclaimed) = self
                .reclaim_closing_source_graphs_locked(active_removal_pass.registered_snapshots())?;
            if !inventory.complete {
                return Ok(inventory.failures);
            }
            if reclaimed != 0 {
                self.reclaim_catalogs_locked(
                    &mut inventory,
                    publication,
                    active_removal_pass.registered_snapshots(),
                )?;
            }
            Ok(inventory.failures)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_partitions_requiring_build(&self) -> Result<MaintenanceBuildCandidates> {
        self.maintenance_build_candidates()
    }

    /// Metadata workspace a maintained route admits before it inspects its
    /// catalog or frontier: route keys, decoded count metadata and bounded
    /// catalog clones. It owns no vector body.
    pub(super) fn vector_route_inventory_bytes(partition: &VectorPartitionRef) -> usize {
        partition
            .partition_key
            .estimated_bytes()
            .saturating_add(partition.index.table.len())
            .saturating_add(partition.index.column.len())
            .saturating_add(1024)
            .saturating_mul(64)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persist_built_vector_partition(
        &self,
        partition: &VectorPartitionRef,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<VectorPartitionPublicationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Metadata and membership decoders share the caller's original
        // operation capability before any partition construction can enter them.
        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader_per_call(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
        )?
        .enter();

        let mut maintenance_vector_copy_owner =
            crate::vector_read_memory::admission(read_memory.clone());
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(VectorPartitionPublicationOutcome::default());
        };
        // Every retry re-enters format selection. A captured frontier that
        // outgrew an earlier estimate forces the existing paged base route.
        // A quarantined catalog is not a serving-format source. Repair must
        // reach the admitted retirement/rebuild path without decoding that
        // damaged pointer; canonical membership and body authentication still
        // decide whether a replacement can be built.
        //
        // Reading the catalog is metadata inspection: it is admitted the way
        // the loop below admits it, the route's metadata workspace first and
        // every cold catalog page through preparation source-page admission,
        // so an unfunded route reads nothing and an oversized catalog is
        // refused before it is decoded.
        let mut force_paged = if self.vector.partition_route_quarantine(partition).is_some() {
            false
        } else {
            let _workspace = self.vector.reserve_workspace_static(
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                Self::vector_route_inventory_bytes(partition),
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before inspecting this maintained route.",
            )?;
            vector_source_page_admission::with_preparation_source_page_admission(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                || {
                    persistence
                        .load_vector_partition_generation_catalog_for(
                            &partition.index,
                            &partition.partition_key,
                        )?
                        .map(|catalog| persistence.catalog_has_native_import_backing(&catalog))
                        .transpose()
                        .map(|backed| backed.unwrap_or(false))
                },
            )?
        };
        let mut force_base_export = false;
        loop {
            if let Some(outcome) =
                vector_source_page_admission::with_preparation_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || {
                        self.advance_paged_vector_build(
                            partition,
                            &mut maintenance_vector_copy_owner,
                            read_memory.clone(),
                            force_paged,
                        )
                    },
                )?
            {
                return Ok(outcome);
            }
            let mut maintenance_vector_copy_owner = maintenance_vector_copy_owner.scratch();
            let layout = contextdb_vector::scoped_index_layout(
                &self.vector,
                &partition.index,
                &mut maintenance_vector_copy_owner,
            )?;
            let info = vector_source_page_admission::with_preparation_source_page_admission(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                || {
                    self.vector.try_partition_info(
                        &partition.index,
                        &partition.partition_key,
                        &mut maintenance_vector_copy_owner,
                    )
                },
            )?;
            let live_vectors = info.as_ref().map_or(0, |info| info.live_rows);
            let policy = layout.resolve_policy(live_vectors, 1);
            let topology_replacement = info.as_ref().is_some_and(|info| {
                info.graph_available
                    && info.serving_policy.is_none_or(|serving| {
                        serving.hnsw_m != policy.hnsw_m
                            || serving.hnsw_ef_construction != policy.hnsw_ef_construction
                    })
            });
            let partition_state_present = self
                .vector
                .partition_graph_generation_status(partition)
                .is_some();
            // Durable-only routes have no state to own policy/progress marks.
            // Keep their source in persistence until the admitted cold build.
            let _policy_build = partition_state_present
                .then(|| {
                    self.vector
                        .begin_partition_policy_build(partition, policy.policy_revision)
                })
                .transpose()?;
            let quarantined = self.vector.partition_route_quarantine(partition).is_some();
            // Admit route keys, decoded count metadata and bounded catalog
            // clones before inspecting the frontier. This owns no vector body.
            let inventory_bytes = Self::vector_route_inventory_bytes(partition);
            let mut workspace = self.vector.reserve_workspace_static(
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                inventory_bytes,
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT before inspecting this maintained route.",
            )?;
            // The column's schema binding is a storage read; it follows the
            // metadata admission so an unfunded route reads nothing.
            let schema_binding = persistence.vector_schema_binding(&partition.index)?;
            // Install cleanup only after metadata admission, so an early
            // refusal cannot read storage while dropping a construction floor.
            let _whole_build_floor = vector_paged_build::WholeBuildFloor::new(self, partition);
            // Borrow the fixed catalog suffix for pricing under the admitted
            // metadata workspace; graph owners wait for complete build sizing.
            let current_descriptors = if quarantined {
                None
            } else {
                vector_source_page_admission::with_preparation_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || {
                        persistence.load_vector_partition_generation_descriptors_for(
                            &partition.index,
                            &partition.partition_key,
                        )
                    },
                )?
            };
            let reader_whole_fits = if quarantined {
                false
            } else {
                match info.as_ref() {
                    Some(info) => self.whole_reader_serving_fits(partition, info, &layout)?,
                    None => false,
                }
            };
            let publish_change = current_descriptors
                .as_ref()
                .is_some_and(|(_, change)| change.is_none())
                && !self.vector.partition_has_paged_storage(partition)
                && partition_state_present
                && !topology_replacement
                && !force_base_export
                && reader_whole_fits;
            let after_lsn = publish_change.then(|| {
                current_descriptors
                    .as_ref()
                    .expect("change publication has a base")
                    .0
                    .covered_lsn
            });
            // Count the raw snapshot and actual graph selection at the same
            // committed frontier, before cloning any unadmitted vector body.
            // The pin protects this history throughout preparation; later
            // commits remain in the journal suffix and mutable tail.
            let (covered_tx, covered_lsn, snapshot_pin, raw_count, graph_count, delete_carriers) =
                vector_source_page_admission::with_preparation_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || {
                        self.tx_mgr.with_commit_lock(|| -> Result<_> {
                    let covered_tx = self.tx_mgr.current_tx_max();
                    let covered_lsn = self.tx_mgr.current_lsn();
                    let snapshot = SnapshotId(covered_tx.0);
                    let pin = self.snapshot_registry.register(snapshot);
                    // A crash during this whole build must leave its frontier
                    // protected at reopen, before any cleanup runs.
                    persistence.record_construction_floor(partition, covered_tx)?;
                    self.snapshot_registry
                        .hold_construction_floor(partition, snapshot);
                    let (raw_count, mut graph_count) = if partition_state_present {
                        self.vector.partition_generation_entry_counts(
                            &partition.index,
                            &partition.partition_key,
                            snapshot,
                            after_lsn, &mut maintenance_vector_copy_owner)?
                    } else {
                        persistence.vector_partition_maintenance_entry_counts(
                            partition, covered_tx, after_lsn,
                        )?
                    };
                    let delete_carriers = after_lsn.is_some() && graph_count == 0;
                    if let Some(after_lsn) = after_lsn
                        && delete_carriers
                    {
                        let carriers = persistence
                            .vector_partition_uncovered_tombstone_entry_count(
                                partition,
                                after_lsn,
                                covered_lsn,
                                |identities| {
                                    workspace.resize(
                                        inventory_bytes.saturating_add(identities.saturating_mul(128)),
                                        "prepare_vector_generation",
                                        "Raise MEMORY_LIMIT before counting delete-only graph carriers.",
                                    )
                                },
                            )?;
                        // Preserve the existing delete-only codec carrier rule.
                        graph_count = if carriers == 1 { 2 } else { carriers };
                    }
                    Ok((
                        covered_tx,
                        covered_lsn,
                        pin,
                        raw_count,
                        graph_count,
                        delete_carriers,
                    ))
                    })
                    },
                )?;
            // This is the shared whole-publication choke point: price the
            // frontier actually pinned above, before any body or graph build.
            // Quarantine repair prices all whole phases below. A route that
            // cannot fit retires its unusable catalog under the durable fence
            // and enters the same bounded initial paged construction.
            if !quarantined
                && publish_change
                && graph_count != 0
                && (!delete_carriers || live_vectors != 0)
                && !self.captured_whole_reader_serving_fits(
                    partition,
                    graph_count,
                    &layout,
                    policy,
                    current_descriptors
                        .as_ref()
                        .filter(|_| publish_change)
                        .map(|(base, _)| base),
                )?
            {
                force_base_export = true;
                // All floor, pin and temporary owners end before full re-selection.
                continue;
            }
            let base_bytes = if publish_change {
                current_descriptors
                    .as_ref()
                    .map_or(0, |(base, _)| base.durable_bytes as usize)
            } else {
                0
            };
            // Ordinary resident builds consume only the pinned live selection.
            // Cold snapshots and delete-only carriers retain their full raw
            // owner and the existing independently funded decoder paths.
            let selected_resident_snapshot = partition_state_present && !delete_carriers;
            let snapshot_count = if selected_resident_snapshot {
                graph_count
            } else {
                raw_count
            };
            let mut workspace_plan = VectorGenerationWorkspacePlan::new(
                snapshot_count,
                graph_count,
                &layout,
                partition,
                policy,
                base_bytes,
            )?;
            workspace_plan.assign_raw_decode_owner(partition_state_present, delete_carriers);
            // A fitting captured base keeps the whole codec and sealed-change
            // chain. Only a reader-unfit base needs the native export bridge.
            let resident_paged_export = !quarantined
                && !publish_change
                && graph_count != 0
                && !self.captured_whole_reader_serving_fits(
                    partition,
                    graph_count,
                    &layout,
                    policy,
                    None,
                )?;
            // The lease bounds simultaneous native pools; the domain's short
            // admission gate is released before any body, worker or callback.
            let _insertion_build_lease =
                crate::memory_accounting::RESIDENT_INSERTION_BUILD_LEASE.lock();
            if !quarantined
                && layout.quantization == VectorQuantization::F32
                && graph_count > 1
                && cfg!(all(
                    target_os = "linux",
                    target_env = "gnu",
                    target_arch = "x86_64",
                    target_pointer_width = "64"
                ))
                && std::env::var_os("GLIBC_TUNABLES").is_none()
            {
                let export_batch = if resident_paged_export {
                    let declaration = vector_paged_build::declaration_for(&layout, &policy)?;
                    Some(vector_paged_build::paged_batch_plan(&declaration)?.workspace_bytes()?)
                } else {
                    None
                };
                let held = crate::memory_accounting::resident_insertion_runtime_snapshot();
                let host_width = std::thread::available_parallelism()
                    .map_or(1, |n| n.get())
                    .min(16)
                    .min(graph_count);
                let width = held.map_or(host_width, |(width, _, _)| host_width.min(width));
                #[cfg(any(test, feature = "test-seams"))]
                let width = if self
                    .vector
                    .has_maintenance_progress_pause_for_test(partition)
                {
                    // An exact insertion boundary must not be crossed by a parallel wave.
                    1
                } else {
                    width
                };
                for workers in (2..=width).rev() {
                    let extra =
                        HnswIndex::parallel_insertion_extra_bytes(graph_count, policy, workers)?;
                    let mut candidate_plan = workspace_plan;
                    candidate_plan.insertion_worker_bytes = extra;
                    let peak = candidate_plan.complete_peak_bytes().max(
                        export_batch
                            .map_or(0, |batch| candidate_plan.resident_export_phase_bytes(batch)),
                    );
                    if held.is_none() {
                        let native_bytes = resident_insertion_runtime_bytes(workers)?;
                        if !self
                            .finalization
                            .accountant
                            .activate_resident_insertion_runtime(
                                workers,
                                native_bytes,
                                workspace.retained_bytes(),
                                peak,
                                || {
                                    workspace.resize(
                                    peak,
                                    "prepare_vector_generation",
                                    "Raise MEMORY_LIMIT before compacting this maintained route.",
                                )
                                },
                            )?
                        {
                            continue;
                        }
                    } else {
                        // Every writer headroom already includes the one held
                        // process reservation. Never debit it in this helper.
                        if self
                            .finalization
                            .accountant
                            .available_after_releasing(workspace.retained_bytes())
                            .is_some_and(|available| available < peak)
                        {
                            continue;
                        }
                        match workspace.resize(
                            peak,
                            "prepare_vector_generation",
                            "Raise MEMORY_LIMIT before compacting this maintained route.",
                        ) {
                            Ok(()) => {}
                            Err(Error::MemoryBudgetExceeded { .. }) => continue,
                            Err(error) => return Err(error),
                        }
                    }
                    workspace_plan.insertion_workers = workers;
                    workspace_plan.insertion_worker_bytes = extra;
                    break;
                }
            }
            // Empty routes retain their existing whole publication/refusal.
            // A forced paged attempt can return no build if its live frontier
            // became empty; do not re-enter the sampled-fit retry in that case.
            if !quarantined && live_vectors != 0 && graph_count != 0 && !force_paged {
                let export_peak = if resident_paged_export {
                    let declaration = vector_paged_build::declaration_for(&layout, &policy)?;
                    workspace_plan.resident_export_phase_bytes(
                        vector_paged_build::paged_batch_plan(&declaration)?.workspace_bytes()?,
                    )
                } else {
                    0
                };
                let peak = workspace_plan.complete_peak_bytes().max(export_peak);
                if self
                    .finalization
                    .accountant
                    .available_after_releasing(workspace.retained_bytes())
                    .is_some_and(|available| available < peak)
                {
                    force_paged = true;
                    continue;
                }
            }
            let retire_for_paged =
                |workspace: &mut contextdb_vector::VectorWorkspaceReservation| -> Result<()> {
                    self.tx_mgr.with_maintenance_publication(|publication| -> Result<()> {
                        self.build_checkpoint()?;
                        let (_removal, _) = self.snapshot_registry
                            .begin_removal_pass_with_sample_admission(self.tx_mgr.current_tx_max(), |bytes| {
                                workspace.resize(inventory_bytes.saturating_add(bytes),
                                    "retire_broken_vector_generation", "Free writer memory before retiring this damaged route.")
                            })?;
                        publication.reserve()?;
                        vector_source_page_admission::with_preparation_source_page_admission(crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant), || {
                            persistence.retire_broken_vector_catalog(partition, || self.build_checkpoint())
                        })?;
                        self.vector.reset_partition_after_broken_catalog_retirement(
                            partition, self.finalization.accountant.as_ref(),
                        );
                        self.vector.forget_partition_paged_build(partition);
                        self.snapshot_registry.release_construction_floor(partition);
                        publication.complete();
                        Ok(())
                    })?;
                    Ok(())
                };
            if quarantined {
                let snapshot_count = self.snapshot_registry.state.lock().active.len();
                let cleanup_peak = inventory_bytes
                    .saturating_mul(8)
                    .saturating_add(snapshot_count.saturating_mul(256))
                    .saturating_add(64 * 1024);
                let peak = workspace_plan.complete_peak_bytes().max(
                    workspace_plan
                        .prepared_phase_bytes(workspace_plan.encoded_graph_bound)
                        .saturating_add(cleanup_peak),
                );
                let complete_fits = self
                    .finalization
                    .accountant
                    .usage()
                    .available
                    .is_none_or(|available| available >= peak);
                let reader_fits = self.captured_whole_reader_serving_fits(
                    partition,
                    graph_count,
                    &layout,
                    policy,
                    None,
                )?;
                if !reader_fits || !complete_fits {
                    // Retirement publishes no index claim. The next selector
                    // starts the existing bounded initial paged construction.
                    retire_for_paged(&mut workspace)?;
                    force_paged = true;
                    continue;
                }
            }
            let attempt = (|| -> Result<Option<VectorPartitionPublicationOutcome>> {
                let encoded_bound = workspace_plan.estimated_generation_bytes();
                self.admit_generation_disk(
                    encoded_bound.saturating_mul(2).saturating_add(64 * 1024),
                )?;
                // Carrier collection retains selected membership metadata while
                // loading its bodies; ordinary graph selections never own it.
                if delete_carriers {
                    workspace_plan.carrier_metadata_bytes = graph_count.saturating_mul(
                        partition
                            .partition_key
                            .estimated_bytes()
                            .saturating_add(partition.index.table.len())
                            .saturating_add(partition.index.column.len())
                            .saturating_add(512),
                    );
                }
                workspace.resize(
                    workspace_plan.build_phase_bytes(),
                    "prepare_vector_generation",
                    "Raise MEMORY_LIMIT before compacting this maintained route.",
                )?;
                // Catalog clones and quarantine inventory scratch are bounded
                // metadata owners. They cannot preempt the build's early refusal.
                workspace.resize(
                    workspace_plan.build_phase_bytes().max(inventory_bytes),
                    "prepare_vector_generation",
                    "Raise MEMORY_LIMIT before inspecting this maintained route.",
                )?;
                let current = if quarantined {
                    None
                } else {
                    vector_source_page_admission::with_preparation_source_page_admission(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        || {
                            persistence.load_vector_partition_generation_catalog_for(
                                &partition.index,
                                &partition.partition_key,
                            )
                        },
                    )?
                };
                if current
                    .as_ref()
                    .map(|catalog| (catalog.base, catalog.change))
                    != current_descriptors
                {
                    // A concurrent durable publication changed the priced chain.
                    // Drop this pin and reservation before sampling its successor.
                    return Ok(None);
                }
                // The pinned directory counts and complete construction admission
                // precede generation/vector-body reads. Metadata extents were
                // admitted separately during preparation. Keep the source guard
                // around quarantine inventory, payload, publication and cleanup.
                let outcome = vector_source_page_admission::with_source_page_admission(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    || -> Result<Option<VectorPartitionPublicationOutcome>> {
                        let previous_generation = if quarantined {
                            match persistence.load_vector_partition_generation_catalog_for(
                                &partition.index,
                                &partition.partition_key,
                            ) {
                                Ok(Some(catalog)) => catalog
                                    .change
                                    .map(|change| change.generation_id)
                                    .unwrap_or(catalog.base.generation_id)
                                    .max(catalog.base.generation_id),
                                Ok(None) | Err(Error::StoreCorrupted { .. }) => {
                                    let highest = persistence
                                        .highest_retired_vector_generation_key(partition)?;
                                    // Inspection scratch is dead before construction
                                    // begins allocating its payload owners.
                                    workspace.resize(
                                    inventory_bytes,
                                    "inspect_vector_generation_inventory",
                                    "Raise MEMORY_LIMIT before inspecting this maintained route.",
                                )?;
                                    highest
                                }
                                Err(error) => return Err(error),
                            }
                        } else {
                            current
                                .as_ref()
                                .map(|catalog| {
                                    catalog
                                        .change
                                        .map(|change| change.generation_id)
                                        .unwrap_or(catalog.base.generation_id)
                                        .max(catalog.base.generation_id)
                                })
                                .unwrap_or(0)
                        };
                        let generation_id =
                            previous_generation.checked_add(1).ok_or_else(|| {
                                Error::Other(
                                    (format!(
                                        "vector generation identity exhausted for {}.{}",
                                        partition.index.table, partition.index.column
                                    ))
                                    .into(),
                                )
                            })?;
                        workspace.resize(
                            workspace_plan.build_phase_bytes(),
                            "prepare_vector_generation",
                            "Raise MEMORY_LIMIT before compacting this maintained route.",
                        )?;
                        let quarantine = self.vector.partition_route_quarantine(partition);
                        let phase = if quarantine.is_some() {
                            "repairing"
                        } else if current.is_none() || topology_replacement {
                            "building"
                        } else if publish_change {
                            "replaying"
                        } else {
                            "compacting"
                        };
                        let reason = if let Some(reason) = quarantine {
                            reason.as_str()
                        } else if current.is_none() {
                            "initial_build"
                        } else if vector_source_page_admission::with_preparation_source_page_admission(
                            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                            || self.vector.try_partition_info(
                                &partition.index,
                                &partition.partition_key,
                                &mut maintenance_vector_copy_owner,
                            ),
                        )?.is_some_and(|info| info.tombstones != 0)
                        {
                            "tombstones"
                        } else {
                            "new_changes"
                        };
                        let progress = partition_state_present
                            .then(|| {
                                self.vector.begin_partition_maintenance_progress(
                                    partition,
                                    reason,
                                    live_vectors,
                                    covered_tx,
                                    phase,
                                )
                            })
                            .flatten();
                        self.build_checkpoint()?;
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
                        );

                        if selected_resident_snapshot {
                            let (_, selected_count) = self.tx_mgr.with_commit_lock(|| {
                                self.vector.partition_generation_entry_counts(
                                    &partition.index,
                                    &partition.partition_key,
                                    SnapshotId(covered_tx.0),
                                    after_lsn,
                                    &mut maintenance_vector_copy_owner,
                                )
                            })?;
                            if selected_count != graph_count {
                                // Drop the old admission and pin, then price a new
                                // frontier before copying any generation body.
                                return Ok(None);
                            }
                        }
                        let mut entries = if selected_resident_snapshot {
                            self.vector.entries_for_partition_generation_at_snapshot(
                                &partition.index,
                                &partition.partition_key,
                                SnapshotId(covered_tx.0),
                                after_lsn,
                                &workspace,
                            )?
                        } else if partition_state_present {
                            self.vector.entries_for_partition_at_snapshot(
                                &partition.index,
                                &partition.partition_key,
                                SnapshotId(covered_tx.0),
                                &workspace,
                            )?
                        } else {
                            persistence.load_vector_partition_entries_at_frontier(
                                partition,
                                covered_tx,
                                layout.dimension,
                                raw_count,
                            )?
                        };
                        #[cfg(any(test, feature = "test-seams"))]
                        resident_history_test_seams::note(|work| {
                            for entry in &entries {
                                if work.maintenance_source_keys
                                    < work.maintenance_source_identities.len()
                                {
                                    work.maintenance_source_identities
                                        [work.maintenance_source_keys] =
                                        Some((entry.row_id.0, entry.lsn.0));
                                }
                                work.maintenance_source_keys += 1;
                            }
                        });
                        entries.retain(|entry| entry.created_tx <= covered_tx);
                        for entry in &mut entries {
                            if entry.deleted_tx.is_some_and(|tx| tx > covered_tx) {
                                entry.deleted_tx = None;
                            }
                        }

                        // Selection was counted at this pinned frontier. Allocate its
                        // slots once so the plan's exact selected-entry owner does not
                        // coexist with geometric old/new Vec allocations.
                        let mut graph_entries = Vec::with_capacity(graph_count);
                        if publish_change {
                            graph_entries.extend(
                                entries
                                    .iter()
                                    .filter(|entry| {
                                        entry.deleted_tx.is_none()
                                            && current.as_ref().is_none_or(|catalog| {
                                                catalog.change.is_some()
                                                    || entry.lsn > catalog.base.covered_lsn
                                            })
                                    })
                                    .cloned(),
                            );
                            if graph_entries.is_empty() {
                                // HNSW's durable codec cannot decode a graph with fewer
                                // than two points. Tombstoned rows carry the delete-only
                                // frontier while authoritative raw membership still
                                // excludes every carrier from results.
                                let base_covered_lsn = current
                                    .as_ref()
                                    .expect("change publication has one current base catalog")
                                    .base
                                    .covered_lsn;
                                let carriers = persistence
                                    .load_vector_partition_uncovered_tombstone_entries(
                                        &partition.index,
                                        &partition.partition_key,
                                        base_covered_lsn,
                                        covered_lsn,
                                        layout.dimension,
                                    )?;
                                graph_entries.extend(carriers.into_iter().map(|mut entry| {
                                    entry.deleted_tx = None;
                                    entry
                                }));
                                if graph_entries.len() == 1 {
                                    let mut second = graph_entries[0].clone();
                                    if let Some(component) = second.vector.first_mut() {
                                        *component = if *component == 0.0 {
                                            f32::EPSILON
                                        } else {
                                            -*component
                                        };
                                    }
                                    graph_entries.push(second);
                                }
                            }
                        } else {
                            graph_entries.extend(
                                entries
                                    .iter()
                                    .filter(|entry| entry.deleted_tx.is_none())
                                    .cloned(),
                            );
                        }
                        let progress =
                            if partition_state_present && graph_entries.len() != live_vectors {
                                drop(progress);
                                self.vector.begin_partition_maintenance_progress(
                                    partition,
                                    reason,
                                    graph_entries.len(),
                                    covered_tx,
                                    phase,
                                )
                            } else {
                                progress
                            };
                        let publication_stop = &self.publication_interrupted;
                        let worker_stop = &self.worker_stop;
                        let graph = HnswIndex::from_vector_entries_with_parallel_progress(
                            &graph_entries,
                            layout.dimension,
                            layout.quantization,
                            policy,
                            workspace_plan.insertion_workers,
                            || {
                                publication_stop.load(Ordering::SeqCst)
                                    || worker_stop
                                        .as_ref()
                                        .is_some_and(|stop| stop.load(Ordering::SeqCst))
                            },
                            |done| {
                                self.build_checkpoint()?;
                                if let Some(progress) = &progress {
                                    progress.advance(done);
                                }
                                self.build_checkpoint()
                            },
                        )?;
                        self.build_checkpoint()?;
                        // The builder owns its point values now; its input is not needed
                        // while the durable envelope is allocated.
                        drop(graph_entries);
                        if resident_paged_export {
                            let export_reason = if current.is_none() {
                                crate::persistence::vector_paged::PagedBuildReason::InitialBuild
                            } else if topology_replacement {
                                crate::persistence::vector_paged::PagedBuildReason::PolicyChange
                            } else if reason == "tombstones" {
                                crate::persistence::vector_paged::PagedBuildReason::Tombstones
                            } else {
                                crate::persistence::vector_paged::PagedBuildReason::NewChanges
                            };
                            let outcome = self.publish_resident_paged_generation(
                                partition,
                                persistence,
                                &layout,
                                policy,
                                export_reason,
                                generation_id,
                                previous_generation,
                                covered_tx,
                                covered_lsn,
                                schema_binding,
                                graph,
                                entries,
                                &mut workspace,
                                workspace_plan,
                                &mut maintenance_vector_copy_owner,
                                read_memory.clone(),
                            )?;
                            drop(snapshot_pin);
                            return Ok(Some(outcome));
                        }
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::AdmitEncoding,
                        );
                        workspace.resize(
                workspace_plan.encoding_phase_bytes(graph.estimated_resident_bytes()),
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before compacting this maintained route.",
            )?;
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::EncodeGraph,
                        );
                        let encoded_graph = graph.encode_durable_generation()?;
                        let resident_bytes = u64::try_from(graph.estimated_resident_bytes())
                            .map_err(|_| {
                                Error::Other(
                                    ("vector generation residency exceeds the durable range"
                                        .to_string())
                                    .into(),
                                )
                            })?;
                        let durable_bytes = u64::try_from(encoded_graph.len()).map_err(|_| {
                            Error::Other(
                                ("vector generation bytes exceed the durable range".to_string())
                                    .into(),
                            )
                        })?;
                        let encoded_graph_capacity = encoded_graph.capacity();
                        let descriptor =
                            VectorPartitionGenerationDescriptor {
                                generation_id,
                                covered_tx,
                                covered_lsn,
                                resident_bytes,
                                durable_bytes,
                                policy_revision: policy.policy_revision,
                                hnsw_m: u32::try_from(policy.hnsw_m).map_err(|_| {
                                    Error::Other(
                                        ("resolved HNSW M exceeds the durable range".to_string())
                                            .into(),
                                    )
                                })?,
                                hnsw_ef_construction: u32::try_from(policy.hnsw_ef_construction)
                                    .map_err(|_| {
                                        Error::Other((
                                        "resolved HNSW EF_CONSTRUCTION exceeds the durable range"
                                            .to_string()).into())
                                    })?,
                            };
                        drop(graph);
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::AdmitCandidate,
                        );
                        workspace.resize(
                workspace_plan.persistence_phase_bytes(encoded_graph_capacity),
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before compacting this maintained route.",
            )?;
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::CandidateAdmitted,
                        );
                        self.build_checkpoint()?;
                        let candidate =
                            if publish_change {
                                let catalog = current
                                    .as_ref()
                                    .expect("change publication has one current base catalog");
                                let base = persistence
                                    .load_vector_partition_base_manifest_with_expected_graph_bytes(
                                        &partition.index,
                                        &partition.partition_key,
                                        catalog.base.generation_id,
                                        catalog.base.durable_bytes,
                                    )?;
                                let change = VectorPartitionChangeGenerationRecord {
                                    index: partition.index.clone(),
                                    partition_key: partition.partition_key.clone(),
                                    base_generation_id: catalog.base.generation_id,
                                    change_generation_id: generation_id,
                                    covered_through_tx: covered_tx,
                                    covered_through_lsn: covered_lsn,
                                    fresh_tail_from_tx: covered_tx,
                                    fresh_tail_from_lsn: covered_lsn,
                                    dimension: u32::try_from(layout.dimension).map_err(|_| {
                                        Error::Other((
                                        "vector dimension exceeds the durable generation range"
                                            .to_string()).into())
                                    })?,
                                    quantization: layout.quantization,
                                    algorithm_version: VECTOR_GRAPH_ALGORITHM_VERSION,
                                    graph_format_version: VECTOR_GRAPH_FORMAT_VERSION,
                                    graph: VectorPartitionGraphBytes::verified(encoded_graph),
                                };
                                VectorPartitionGenerationCandidate {
                                    base,
                                    change: Some(change),
                                    catalog: VectorPartitionGenerationCatalogRecord {
                                        index: partition.index.clone(),
                                        partition_key: partition.partition_key.clone(),
                                        base: catalog.base,
                                        change: Some(descriptor),
                                        published_tx: covered_tx,
                                        published_lsn: covered_lsn,
                                    },
                                }
                            } else {
                                let mut live_entries = entries
                                    .iter()
                                    .filter(|entry| entry.deleted_tx.is_none())
                                    .collect::<Vec<_>>();
                                live_entries.sort_by_key(|entry| {
                                    (entry.row_id, entry.created_tx, entry.lsn)
                                });
                                #[cfg(any(test, feature = "test-seams"))]
                                self.vector.maybe_pause_maintenance_preparation_for_test(
                                    partition,
                                    VectorMaintenancePreparationPhaseForTest::HashStateDigest,
                                );
                                // Stream the identical bincode sequence directly into the
                                // digest; no vector-sized serialization buffer coexists.
                                let mut state_hasher = blake3::Hasher::new();
                                bincode::serde::encode_into_std_write(
                                    &live_entries,
                                    &mut state_hasher,
                                    bincode::config::standard(),
                                )
                                .map_err(|error| {
                                    Error::Other((format!(
                                    "could not encode vector generation state digest: {error}"
                                )).into())
                                })?;
                                let base = VectorPartitionBaseGenerationRecord {
                                    index: partition.index.clone(),
                                    partition_key: partition.partition_key.clone(),
                                    generation_id,
                                    base_tx: covered_tx,
                                    base_lsn: covered_lsn,
                                    highest_included_tx: covered_tx,
                                    highest_included_lsn: covered_lsn,
                                    dimension: u32::try_from(layout.dimension).map_err(|_| {
                                        Error::Other((
                                        "vector dimension exceeds the durable generation range"
                                            .to_string()).into())
                                    })?,
                                    quantization: layout.quantization,
                                    algorithm_version: VECTOR_GRAPH_ALGORITHM_VERSION,
                                    graph_format_version: VECTOR_GRAPH_FORMAT_VERSION,
                                    graph: VectorPartitionGraphBytes::verified(encoded_graph),
                                    state_digest: *state_hasher.finalize().as_bytes(),
                                };
                                VectorPartitionGenerationCandidate {
                                    base,
                                    change: None,
                                    catalog: VectorPartitionGenerationCatalogRecord {
                                        index: partition.index.clone(),
                                        partition_key: partition.partition_key.clone(),
                                        base: descriptor,
                                        change: None,
                                        published_tx: covered_tx,
                                        published_lsn: covered_lsn,
                                    },
                                }
                            };
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::PrepareDurableGeneration,
                        );
                        drop(entries);
                        drop(snapshot_pin);
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::AdmitPersistence,
                        );
                        let persistence_owned = workspace_plan.prepared_candidate_phase_bytes(
                            encoded_graph_capacity,
                            RedbPersistence::vector_candidate_streamed_frame_bytes(&candidate)?,
                        );
                        workspace.resize(
                persistence_owned,
                "prepare_vector_generation",
                "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before compacting this maintained route.",
            )?;
                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_maintenance_preparation_for_test(
                            partition,
                            VectorMaintenancePreparationPhaseForTest::PersistenceAdmitted,
                        );
                        self.build_checkpoint()?;
                        let paged_candidate = persistence
                            .prepare_saved_topology_candidate_with_schema_binding(
                                &candidate,
                                schema_binding,
                                self.vector.partition_route_quarantine(partition).is_some(),
                                &mut |scan_bytes| {
                                    self.build_checkpoint()?;
                                    workspace.resize(
                                        persistence_owned.saturating_add(scan_bytes),
                                        "prepare_vector_generation",
                                        "Free writer memory before exporting saved topology.",
                                    )
                                },
                            )?;
                        // The saved export owns only native manifests now. Destroy
                        // the original encoded envelope before any later phase can
                        // reduce its graph reservation, while retaining the durable
                        // abandonment guard independently of the selected buffers.
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            use crate::vector_observations::{
                                VectorGraphBufferKindForTest, observe_graph_buffer_for_test,
                            };
                            // All backing allocations are still live here. Native
                            // manifests and original envelopes have distinct Vecs.
                            observe_graph_buffer_for_test(
                                VectorGraphBufferKindForTest::OriginalEnvelope,
                                &candidate.base.graph.bytes,
                            );
                            if let Some(change) = &candidate.change {
                                observe_graph_buffer_for_test(
                                    VectorGraphBufferKindForTest::OriginalEnvelope,
                                    &change.graph.bytes,
                                );
                            }
                            let selected = paged_candidate
                                .as_ref()
                                .map(|saved| &saved.candidate)
                                .unwrap_or(&candidate);
                            observe_graph_buffer_for_test(
                                VectorGraphBufferKindForTest::SelectedBase,
                                &selected.base.graph.bytes,
                            );
                            if let Some(change) = &selected.change {
                                observe_graph_buffer_for_test(
                                    VectorGraphBufferKindForTest::SelectedChange,
                                    &change.graph.bytes,
                                );
                            }
                        }
                        let (candidate, _saved_export_ownership) = match paged_candidate {
                            Some(saved) => {
                                drop(candidate);
                                (saved.candidate, saved.ownership)
                            }
                            None => (candidate, None),
                        };
                        let prepared = persistence
                            .prepare_vector_partition_generation_candidate_with_admission(
                                &candidate,
                                |scan_bytes| {
                                    workspace.resize(
                            persistence_owned.saturating_add(scan_bytes),
                            "prepare_vector_generation",
                            "Raise MEMORY_LIMIT before preparing generation catalog metadata.",
                        )
                                },
                            )?;
                        let prepared_workspace_bytes =
                            persistence_owned.saturating_add(prepared.stale_metadata_bytes());
                        workspace.resize(
                            prepared_workspace_bytes,
                            "prepare_vector_generation",
                            "Raise MEMORY_LIMIT before publishing this maintained route.",
                        )?;
                        self.admit_generation_disk(prepared.write_reservation_bytes())?;

                        #[cfg(any(test, feature = "test-seams"))]
                        self.vector.maybe_pause_memory_workspace_for_test();
                        #[cfg(any(test, feature = "test-seams"))]
                        if VECTOR_DURABLE_COMPACTION_FAILURE_ARMED_FOR
                            .compare_exchange(
                                Arc::as_ptr(&self.vector) as usize,
                                0,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            VECTOR_DURABLE_COMPACTION_FAILURE_CONSUMED_BY
                                .store(Arc::as_ptr(&self.vector) as usize, Ordering::SeqCst);
                            return Err(Error::Other(
                                ("injected vector compaction prepublication failure".to_string())
                                    .into(),
                            ));
                        }

                        let published = self.tx_mgr.with_maintenance_publication(|publication| {
                self.vector.with_partition_generation_publication(
                    partition,
                    || -> contextdb_core::Result<bool> {
                        let current_layout = contextdb_vector::scoped_index_layout(&self.vector, &partition.index, &mut maintenance_vector_copy_owner)?;
                        if current_layout.policy_revision != layout.policy_revision
                            || current_layout.hnsw_m != layout.hnsw_m
                            || current_layout.hnsw_ef_construction != layout.hnsw_ef_construction
                        {
                            return Ok(false);
                        }
                        let durable_generation = if quarantined {
                            match persistence.load_vector_partition_generation_catalog_for(
                                &partition.index,
                                &partition.partition_key,
                            ) {
                                Ok(Some(catalog)) => catalog
                                    .change
                                    .map(|change| change.generation_id)
                                    .unwrap_or(catalog.base.generation_id)
                                    .max(catalog.base.generation_id),
                                Ok(None) | Err(Error::StoreCorrupted { .. }) => {
                                    persistence.highest_retired_vector_generation_key(partition)?
                                },
                                Err(error) => return Err(error),
                            }
                        } else {
                            persistence
                                .load_vector_partition_generation_catalog_for(
                                    &partition.index,
                                    &partition.partition_key,
                                )?
                                .as_ref()
                                .map(|catalog| {
                                    catalog
                                        .change
                                        .map(|change| change.generation_id)
                                        .unwrap_or(catalog.base.generation_id)
                                        .max(catalog.base.generation_id)
                                })
                                .unwrap_or(0)
                        };
                        // A quarantined route with no readable catalog falls back to
                        // the highest generation key on disk. The candidate's own
                        // saved objects are already installed under its ids before
                        // this check, so they are its staging, not a competing
                        // publication; counting them made every attempt abandon
                        // itself and retry under a fresh id without end.
                        let candidate_owns_durable = quarantined
                            && (durable_generation == candidate.catalog.base.generation_id
                                || candidate
                                    .catalog
                                    .change
                                    .is_some_and(|change| change.generation_id == durable_generation));
                        if durable_generation != previous_generation && !candidate_owns_durable {
                            return Ok(false);
                        }
                        self.admit_generation_disk(prepared.write_reservation_bytes())?;
                        publication.reserve()?;
                        let published_pending = persistence
                            .publish_prepared_vector_partition_generation_candidate(&prepared)?;
                        publication.complete();
                        #[cfg(not(any(test, feature = "test-seams")))]
                        let _ = published_pending;
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::GenerationAfterRedb);
                        #[cfg(any(test, feature = "test-seams"))]
                        if partition_state_present && let Some(published_pending) = published_pending {
                            self.vector.observe_durable_base_publication(
                                partition,
                                candidate.catalog.base.generation_id,
                                candidate.catalog.base.covered_tx,
                                published_pending,
                            );
                        }
                        Ok(true)
                    },
                )
            })?;
                        if !published {
                            let current_layout = contextdb_vector::scoped_index_layout(
                                &self.vector,
                                &partition.index,
                                &mut maintenance_vector_copy_owner,
                            )?;
                            if current_layout.policy_revision != layout.policy_revision
                                || current_layout.hnsw_m != layout.hnsw_m
                                || current_layout.hnsw_ef_construction
                                    != layout.hnsw_ef_construction
                            {
                                // A topology declaration made this candidate stale. One
                                // maintenance cycle owns one sampled revision; the next
                                // cycle builds the replacement the newer declaration asks
                                // for while the old complete graph keeps serving.
                                return Ok(Some(VectorPartitionPublicationOutcome::default()));
                            }
                            return Ok(None);
                        }
                        #[cfg(any(test, feature = "test-seams"))]
            if self.vector.take_journal_truncation_fault_for_test(
                partition,
                VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
            ) {
                let error = Error::Other((
                    "vector journal truncation crash injected after durable catalog T1".to_string()).into());
                return self.retain_published_vector_cleanup_failure(partition, &error).map(Some);
            }

                        let repaired_generation_id = candidate.catalog.base.generation_id;
                        let (published_catalog, cleanup_complete) = if quarantined {
                            let cleanup = self.tx_mgr.with_maintenance_publication(|publication| -> Result<_> {
                    self.build_checkpoint()?;
                    let persistence_phase_owned = prepared_workspace_bytes;
                    // Count, admit, and capture the registered snapshot sample
                    // under the registry's one atomic registration/removal
                    // lock. During Vec-to-Arc conversion both allocations are
                    // charged before either is constructed.
                    let (active_removal_pass, snapshot_guard_owned) = self
                        .snapshot_registry
                        .begin_removal_pass_with_sample_admission(
                            self.tx_mgr.current_tx_max(),
                            |sample_transition_bound| {
                                workspace.resize(
                                    persistence_phase_owned
                                        .saturating_add(sample_transition_bound),
                                    "finalize_repaired_vector_partition",
                                    "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before repairing this maintained route.",
                                )
                            },
                        )?;
                    let snapshot_count = active_removal_pass.registered_snapshots().len();
                    // `snapshot_retained_partition_chain_ids` temporarily
                    // builds a HashSet and returns a Vec with at most one
                    // chain per sampled snapshot. Grow before that allocation;
                    // the finalizer replaces this conservative capture bound
                    // with reported surviving capacities.
                    let repair_capture_bound = snapshot_count
                        .saturating_mul(
                            std::mem::size_of::<(u64, Option<u64>)>()
                                .saturating_mul(4)
                                .saturating_add(std::mem::size_of::<SnapshotId>()),
                        )
                        .saturating_add(snapshot_guard_owned)
                        .saturating_add(4096);
                    workspace.resize(
                        persistence_phase_owned.saturating_add(repair_capture_bound),
                        "finalize_repaired_vector_partition",
                        "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before repairing this maintained route.",
                    )?;
                    let retained_chains = self.vector.snapshot_retained_partition_chain_ids(
                        partition,
                        active_removal_pass.registered_snapshots(),
                    );
                    let repair_context_owned = retained_chains
                        .capacity()
                        .saturating_mul(std::mem::size_of::<(u64, Option<u64>)>())
                        .saturating_add(snapshot_guard_owned);

                    // Publication owns durable copies now. Repair needs only
                    // the catalog, not two more encoded graph buffers. The
                    // finalizer immediately replaces this persistence-phase
                    // reservation with its admitted cleanup ownership.
                    let VectorPartitionGenerationCandidate {
                        base,
                        change,
                        catalog: published_catalog,
                    } = candidate;
                    drop(base);
                    drop(change);
                    drop(prepared);
                    publication.reserve()?;
                    let (finalized, catalog_owned) =
                        persistence.finalize_repaired_vector_partition(
                            &published_catalog,
                            &retained_chains,
                            repair_context_owned,
                            &mut workspace,
                            || self.build_checkpoint(),
                        )?;
                    publication.complete();
                    drop(active_removal_pass);
                    drop(retained_chains);
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::vector_observations::observe_workspace_resize_for_test(
                        workspace.retained_bytes(), catalog_owned, "repair_cleanup",
                    );
                    workspace.resize(
                        catalog_owned,
                        "finalize_repaired_vector_partition",
                        "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before repairing this maintained route.",
                    )?;
                    Ok((published_catalog, finalized))
                });
                            match cleanup {
                                Ok(cleanup) => cleanup,
                                Err(
                                    error @ (Error::ReadCancelled | Error::PublicationInterrupted),
                                ) => {
                                    // A stop after the repair's durable publication
                                    // does not undo it: the replacement serves, and
                                    // only its covered-evidence cleanup is left for
                                    // the next cycle.
                                    if partition_state_present
                                        && let Some(durable) = persistence
                                            .load_vector_partition_generation_catalog_for(
                                                &partition.index,
                                                &partition.partition_key,
                                            )?
                                            .filter(|durable| {
                                                durable.base.generation_id == repaired_generation_id
                                            })
                                    {
                                        match self.install_published_vector_catalog(
                                            &durable,
                                            read_memory.clone(),
                                        ) {
                                            Ok(()) => {}
                                            Err(
                                                install @ (Error::MemoryBudgetExceeded { .. }
                                                | Error::DiskBudgetExceeded { .. }),
                                            ) => {
                                                self.retain_published_vector_cleanup_failure(
                                                    partition, &install,
                                                )?;
                                            }
                                            Err(install) => return Err(install),
                                        }
                                    }
                                    return Err(error);
                                }
                                Err(error) => {
                                    return self
                                        .retain_published_vector_cleanup_failure(partition, &error)
                                        .map(Some);
                                }
                            }
                        } else {
                            // Normal T2 likewise needs only the surviving catalog. Its
                            // first action replaces the graph-buffer reservation before
                            // allocating cleanup keys or decoded validation records.
                            let VectorPartitionGenerationCandidate {
                                base,
                                change,
                                catalog: published_catalog,
                            } = candidate;
                            drop(base);
                            drop(change);
                            drop(prepared);
                            #[cfg(test)]
                        vector_journal_cleanup_cancellation_tests::before_published_cleanup_for_test(
                            &published_catalog,
                        );
                            let truncated = match self.truncate_vector_journal_in_published_steps(
                                persistence,
                                &published_catalog,
                                &mut workspace,
                                || Ok(None),
                            ) {
                                Ok(truncated) => truncated,
                                Err(
                                    error @ (Error::ReadCancelled | Error::PublicationInterrupted),
                                ) => {
                                    return Err(error);
                                }
                                Err(error) => {
                                    return self
                                        .retain_published_vector_cleanup_failure(partition, &error)
                                        .map(Some);
                                }
                            };
                            (published_catalog, truncated)
                        };
                        if !cleanup_complete {
                            if quarantined {
                                drop(published_catalog);
                                #[cfg(any(test, feature = "test-seams"))]
                                crate::vector_observations::observe_workspace_resize_for_test(
                                    workspace.retained_bytes(),
                                    0,
                                    "repair_cleanup",
                                );
                                workspace.resize(
                                    0,
                                    "prepare_vector_generation",
                                    "Free writer memory before completing vector publication.",
                                )?;
                                return Ok(Some(VectorPartitionPublicationOutcome::published()));
                            }
                            drop(published_catalog);
                            #[cfg(any(test, feature = "test-seams"))]
                            crate::vector_observations::observe_workspace_resize_for_test(
                                workspace.retained_bytes(),
                                0,
                                "retry",
                            );
                            workspace.resize(
                                0,
                                "prepare_vector_generation",
                                "Free writer memory before completing vector publication.",
                            )?;
                            let cleanup_retry =
                                self.retry_vector_journal_truncations(read_memory.clone())?;
                            return Ok(Some(VectorPartitionPublicationOutcome {
                                published: true,
                                cleanup_failure: cleanup_retry.first_failure_details,
                            }));
                        }
                        if partition_state_present {
                            #[cfg(test)]
                            published_vector_install_tests::maybe_pause_before_install(
                                &self.finalization.accountant,
                            );
                            match self.install_published_vector_catalog(
                                &published_catalog,
                                read_memory.clone(),
                            ) {
                                Ok(()) => {}
                                Err(
                                    error @ (Error::MemoryBudgetExceeded { .. }
                                    | Error::DiskBudgetExceeded { .. }),
                                ) => {
                                    // T1 is authoritative even when T2 has already removed
                                    // the covered journal. Keep the old serving chain and
                                    // report this publication; catalog retry installs it
                                    // once the refused resource becomes available.
                                    return self
                                        .retain_published_vector_cleanup_failure(partition, &error)
                                        .map(Some);
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        drop(published_catalog);
                        workspace.resize(
                            0,
                            "prepare_vector_generation",
                            "Free writer memory before completing vector publication.",
                        )?;
                        Ok(Some(VectorPartitionPublicationOutcome::published()))
                    },
                )?;
                Ok(outcome)
            })();
            let outcome = match attempt {
                Ok(outcome) => outcome,
                // A paged build fits a smaller workspace; it cannot read a
                // source page the budget already refused, so that refusal is
                // the answer rather than a reason to retry.
                Err(Error::MemoryBudgetExceeded { ref operation, .. })
                    if quarantined && operation != "read_vector_generation_source_page" =>
                {
                    retire_for_paged(&mut workspace)?;
                    force_paged = true;
                    continue;
                }
                Err(Error::MemoryBudgetExceeded { .. })
                    if !quarantined && live_vectors != 0 && graph_count != 0 && !force_paged =>
                {
                    // The fit sample predicts admission; phase reservations
                    // decide it. The failed closure has dropped graph/body
                    // owners, and continue drops this attempt's workspace,
                    // snapshot pin and floor before finite paged selection.
                    force_paged = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(outcome) = outcome {
                return Ok(outcome);
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn run_cycle(
        &self,
        gate: CurrencyGate,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<MaintenanceReport> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        // Background cycles carry this context's writer; thread-local state
        // from the spawning caller does not transfer to the worker.
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        )?;

        // Every maintenance cycle -- the background worker's and an explicit
        // one -- is a boundary: it observes the process ceiling once, before
        // its first admission.
        crate::memory_accounting::observe_inherited_process_ceiling();
        let mut maintenance_vector_copy_owner =
            crate::vector_read_memory::admission(read_memory.clone());
        // Release obsolete graphs before another partition allocates its
        // replacement. Pending route/journal work still forbids retiring the
        // route itself; the snapshot guard independently protects old graphs.
        let mut cycle_failures = self.reclaim_snapshot_free_vector_graphs()?;
        match self.reclaim_paged_vector_storage() {
            Ok(()) => {}
            Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                return Err(error);
            }
            Err(error) => {
                cycle_failures
                    .first
                    .get_or_insert_with(|| VectorMaintenanceFailureDetails::from_error(&error));
            }
        }
        // Idempotent T2 retry never clears or rebuilds a healthy route. Newer
        // journal suffix records remain outside each catalog's covered LSN. A
        // local retry refusal is retained in the report while the rest of the
        // fixed cycle continues.
        let VectorJournalCleanupRetryOutcome {
            first_failure_details: cleanup_retry_failure,
            failed_partitions: cleanup_retry_failed_partitions,
        } = self.retry_vector_journal_truncations(read_memory.clone())?;
        cycle_failures.first = cycle_failures.first.or(cleanup_retry_failure);
        let pruning = self.run_pruning_within_cycle(read_memory.clone())?;
        let pruned_trigger_audit_rows = self.run_trigger_audit_retention()?;
        let currency = match self.run_currency(gate, read_memory.clone()) {
            Ok(report) => report,
            Err(error @ Error::MemoryBudgetExceeded { .. }) if cycle_failures.first.is_some() => {
                let details = VectorMaintenanceFailureDetails::from_error(&error);
                CurrencyCompactionReport {
                    first_failure: Some(details.failure),
                    first_failure_details: Some(details),
                    ..CurrencyCompactionReport::default()
                }
            }
            Err(error) => return Err(error),
        };
        let candidate_sample = self
            .persistence
            .as_ref()
            .map(|_| self.vector_partitions_requiring_build())
            .transpose()?;
        if let Some(sample) = candidate_sample.as_ref() {
            cycle_failures.first = cycle_failures.first.or(sample.failures.first);
        }
        let pending_vector_partitions = candidate_sample
            .as_ref()
            .map(|sample| sample.candidates.as_slice())
            .unwrap_or(&[]);
        // An unpublished paged build keeps advancing even when this cycle's
        // sampled inventory does not select its route.
        let unselected_paged_builds = self.unselected_paged_builds(pending_vector_partitions)?;
        let mut vector = if !pending_vector_partitions.is_empty()
            || !unselected_paged_builds.is_empty()
        {
            // File-backed construction publishes through the durable catalog
            // first. Building a separate process-only graph here let the
            // writer call the route ready while a reader of the same
            // committed store still saw `initial_build`.
            let mut built_partitions = 0usize;
            let mut built_indexes = HashSet::new();
            let mut first_failure_details = cycle_failures.first;
            for partition in pending_vector_partitions
                .iter()
                .map(|candidate| &candidate.partition)
                .chain(unselected_paged_builds.iter())
            {
                if cleanup_retry_failed_partitions.contains(partition) {
                    continue;
                }
                let outcome =
                    match self.persist_built_vector_partition(partition, read_memory.clone()) {
                        Ok(outcome) => outcome,
                        Err(error @ (Error::ReadCancelled | Error::PublicationInterrupted)) => {
                            return Err(error);
                        }
                        Err(error) => {
                            self.vector
                                .record_partition_maintenance_error(partition, &error)?;
                            first_failure_details.get_or_insert_with(|| {
                                VectorMaintenanceFailureDetails::from_error(&error)
                            });
                            continue;
                        }
                    };
                if let Some(failure) = outcome.cleanup_failure {
                    first_failure_details.get_or_insert(failure);
                }
                // A repair can make its replacement catalog durable while the
                // worker is being stopped. Keep that publication authoritative,
                // but observe the stop before counting success or beginning any
                // later reclamation, retirement, compaction, or cycle stamping.
                self.build_checkpoint()?;
                if !outcome.published {
                    continue;
                }
                built_partitions = built_partitions.saturating_add(1);
                built_indexes.insert(partition.index.clone());
                first_failure_details =
                    first_failure_details.or(self.reclaim_snapshot_free_vector_graphs()?.first);
            }
            let mut report = self.vector.hnsw_maintenance_status();
            report.built_partitions = built_partitions;
            report.built_indexes = built_indexes.len();
            report.first_failure = first_failure_details.map(|details| details.failure);
            report.first_failure_details = first_failure_details;
            report
        } else if self.persistence.is_some() {
            let mut report = self.vector.hnsw_maintenance_status();
            let first_failure_details = cycle_failures.first.or(report.first_failure_details);
            report.first_failure = first_failure_details.map(|details| details.failure);
            report.first_failure_details = first_failure_details;
            report
        } else {
            self.vector.run_hnsw_maintenance_cycle_with(
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                &mut || self.build_checkpoint(),
                &mut maintenance_vector_copy_owner,
            )?
        };
        // A replacement first publishes the prior graph as a retired snapshot
        // chain. Reclaim only after publication, under the registry's removal
        // guard, so active readers keep that route and unselected generations
        // do not linger until another maintenance wake.
        vector.first_failure_details = cycle_failures.first.or(vector.first_failure_details);
        vector.first_failure_details = vector
            .first_failure_details
            .or(self.reclaim_snapshot_free_vector_graphs()?.first);
        // Retirement is independent of whether another partition had work,
        // but follows this wake's durable generation publications. That lets
        // a delete-only route first represent and truncate its final journal
        // prefix, then releases it even while a sibling keeps producing work.
        vector.first_failure_details = vector.first_failure_details.or(self
            .retire_snapshot_free_vector_partitions(read_memory.clone())?
            .first);
        vector.first_failure = vector.first_failure_details.map(|details| details.failure);
        // Reclaim the deferred-free backlog before compaction, so compaction sees the free space.
        self.reclaim_deferred_storage()?;
        // The rare, interval-gated automatic compaction path -- see
        // `maybe_auto_compact`'s own doc comment. Runs LAST, after every
        // scoped pass this cycle already did its own work, and (like every
        // compact call) outside any commit lock.
        let compaction = match self.maybe_auto_compact() {
            Ok(report) => report,
            Err(Error::MemoryBudgetExceeded { .. } | Error::DiskBudgetExceeded { .. })
                if vector.first_failure.is_some() =>
            {
                // The cycle already exposes the first partition-local
                // resource refusal. An opportunistic closing compaction uses
                // the same exhausted limit; attempting it must not erase the
                // successful vector counts or prevent the closing stamp.
                CompactionReport::default()
            }
            Err(error) => return Err(error),
        };
        // Recorded whether this cycle reclaimed anything or not -- a caller
        // that dutifully drives an empty cycle is still driving, and must
        // never trip the caller-driven backlog warning.
        *self.last_maintenance_cycle_at.lock() = Some(std::time::Instant::now());
        Ok(MaintenanceReport {
            pruning,
            currency,
            vector,
            pruned_trigger_audit_rows,
            compaction,
        })
    }
}

/// Observability: an operator must be able to see maintenance happen. Silent
/// when a tick reclaimed nothing.
pub(super) fn log_maintenance_cycle(report: &MaintenanceReport) {
    if report.pruning.pruned_rows == 0
        && report.currency.pruned_versions == 0
        && report.pruned_trigger_audit_rows == 0
        && report.pruning.future_dated_rows == 0
        && report.vector.built_indexes == 0
        && report.vector.built_partitions == 0
        && report.vector.first_failure.is_none()
        && !report.compaction.ran
    {
        return;
    }
    eprintln!(
        "maintenance_cycle pruned_rows={} reclaimed_bytes={} compacted={} file_shrank={} \
         future_dated_rows={} future_dated_tables={} trigger_audit_rows={} \
         currency_versions={} currency_redb_compacted={} auto_compact_ran={} \
         auto_compact_micros={} vector_nonempty_indexes={} vector_ready_indexes={} \
         vector_built_indexes={} vector_remaining_indexes={} \
         vector_nonempty_partitions={} vector_ready_partitions={} \
         vector_built_partitions={} vector_remaining_partitions={} vector_first_failure={}",
        report.pruning.pruned_rows,
        report.pruning.reclaimed_bytes,
        report.pruning.compacted,
        report.pruning.file_shrank,
        report.pruning.future_dated_rows,
        report.pruning.future_dated_tables.join(","),
        report.pruned_trigger_audit_rows,
        report.currency.pruned_versions,
        report.currency.redb_compacted,
        report.compaction.ran,
        report.compaction.duration_micros,
        report.vector.nonempty_indexes,
        report.vector.ready_indexes,
        report.vector.built_indexes,
        report.vector.remaining_indexes,
        report.vector.nonempty_partitions,
        report.vector.ready_partitions,
        report.vector.built_partitions,
        report.vector.remaining_partitions,
        report
            .vector
            .first_failure
            .map_or("none", VectorMaintenanceFailure::reason),
    );
}

/// Consume the armed currency-compaction injection flag, if any. Reads a
/// thread-local that is never set outside a test, so the production path
/// takes the `false` arm.
pub(super) fn take_currency_compaction_persist_fault_for_test() -> bool {
    CURRENCY_COMPACTION_PERSIST_FAULT.with(|f| f.replace(false))
}

/// Consume the armed snapshot-registration race-window seam, if any. Reads
/// a thread-local that is never set outside a test, so the production path
/// takes the `false` arm.
pub(super) fn take_close_in_snapshot_registration_race_window_for_test() -> bool {
    CLOSE_IN_SNAPSHOT_REGISTRATION_RACE_WINDOW.with(|f| f.replace(false))
}

/// Consume the armed thread-local sync-apply pre-commit pause selector, if
/// any. Reads a thread-local that is never set outside a test, so the
/// production path takes the `false` arm on every thread.
pub(super) fn take_sync_apply_pre_commit_pause_armed_here_for_test() -> bool {
    SYNC_APPLY_PRE_COMMIT_PAUSE_ARMED_HERE.with(|f| f.replace(false))
}

#[cfg(test)]
pub(super) fn maybe_pause_before_query_trace_rows_examined_capture_for_test() {
    if QUERY_TRACE_ROWS_EXAMINED_CAPTURE_PAUSE_ARMED_HERE.with(|flag| flag.replace(false)) {
        query_trace_rows_examined_capture_pause_for_test().maybe_pause();
    }
}

impl StorageCompactionPolicy {
    pub fn start_ratio(&self) -> f64 {
        self.start_ratio
            .unwrap_or(REDB_COMPACT_FRAGMENTATION_THRESHOLD)
    }

    pub fn min_interval(&self) -> Duration {
        self.min_interval_ms
            .map_or(AUTO_COMPACT_MIN_INTERVAL, Duration::from_millis)
    }

    pub fn stop_ratio(&self) -> f64 {
        self.stop_ratio.unwrap_or(EXPLICIT_COMPACTION_TIGHT_RATIO)
    }

    pub fn max_sweeps(&self) -> u32 {
        self.max_sweeps.unwrap_or(EXPLICIT_COMPACTION_MAX_SWEEPS)
    }

    /// Refuse a declared value outside its range. The SQL statement and the
    /// Rust setter both pass through here, so they refuse the same values with
    /// the same issue; a persisted declaration is checked again at open.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn validate(&self) -> Result<()> {
        use contextdb_core::StorageCompactionDeclarationIssue as Issue;
        let refuse = |issue| Err(Error::InvalidStorageCompactionDeclaration { issue });
        let ratio_in_range = |ratio: f64| ratio > 0.0 && ratio <= 1.0;
        if self.start_ratio.is_some_and(|ratio| !ratio_in_range(ratio)) {
            return refuse(Issue::StartRatioOutOfRange);
        }
        if self.stop_ratio.is_some_and(|ratio| !ratio_in_range(ratio)) {
            return refuse(Issue::StopRatioOutOfRange);
        }
        if self.max_sweeps == Some(0) {
            return refuse(Issue::MaxSweepsNotPositive);
        }
        match self.min_interval_ms {
            Some(0) => refuse(Issue::MinIntervalNotPositive),
            Some(milliseconds) if i64::try_from(milliseconds).is_err() => {
                refuse(Issue::MinIntervalOutOfRange)
            }
            _ => Ok(()),
        }
    }

    /// Every setting as `(name, value, declared)`, in the order
    /// `SHOW STORAGE_COMPACTION` and the maintenance receipts list them;
    /// `declared` is false when the value is the default.
    pub fn settings(&self) -> [(&'static str, StorageCompactionValue, bool); 4] {
        [
            (
                "start_ratio",
                StorageCompactionValue::Ratio(self.start_ratio()),
                self.start_ratio.is_some(),
            ),
            (
                "min_interval_ms",
                StorageCompactionValue::Count(
                    u64::try_from(self.min_interval().as_millis()).unwrap_or(u64::MAX),
                ),
                self.min_interval_ms.is_some(),
            ),
            (
                "stop_ratio",
                StorageCompactionValue::Ratio(self.stop_ratio()),
                self.stop_ratio.is_some(),
            ),
            (
                "max_sweeps",
                StorageCompactionValue::Count(u64::from(self.max_sweeps())),
                self.max_sweeps.is_some(),
            ),
        ]
    }

    /// Declare the clauses one statement named; omitted clauses keep their
    /// current value.
    pub(super) fn declare(&mut self, clauses: contextdb_parser::StorageCompactionClauses) {
        if clauses.start_ratio.is_some() {
            self.start_ratio = clauses.start_ratio;
        }
        if clauses.min_interval_ms.is_some() {
            self.min_interval_ms = clauses.min_interval_ms;
        }
        if clauses.stop_ratio.is_some() {
            self.stop_ratio = clauses.stop_ratio;
        }
        if clauses.max_sweeps.is_some() {
            self.max_sweeps = clauses.max_sweeps;
        }
    }
}

impl StorageCompactionState {
    pub(crate) fn min_interval(&self) -> Duration {
        self.min_interval_override
            .unwrap_or_else(|| self.declared.min_interval())
    }
}

/// The monotonic clock the automatic compaction interval is measured on.
pub(super) fn compaction_clock_now() -> std::time::Instant {
    let now = std::time::Instant::now();
    #[cfg(any(test, feature = "test-seams"))]
    let now = now + COMPACTION_CLOCK_ADVANCE.with(std::cell::Cell::get);
    now
}

/// Production-dead: whether the explicit compaction on this thread must
/// report its exact measurement as failed.
pub(super) fn exact_fragmentation_measurement_fails_for_test() -> bool {
    #[cfg(any(test, feature = "test-seams"))]
    {
        FAIL_EXACT_FRAGMENTATION_MEASUREMENT.with(|fail| fail.replace(false))
    }
    #[cfg(not(any(test, feature = "test-seams")))]
    {
        false
    }
}

/// Test-only: cap the reclaim steps a maintenance cycle run on this thread may take (`None`
/// restores the production limit), so a test can prove the per-cycle bound with a small backlog.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn set_reclaim_steps_per_cycle_for_test(steps: Option<usize>) {
    RECLAIM_STEPS_PER_CYCLE_FOR_TEST.with(|limit| limit.set(steps));
}

/// Test-only: durable reclaim steps committed by the last maintenance cycle run on this thread.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn reclaim_steps_last_cycle_for_test() -> usize {
    RECLAIM_STEPS_LAST_CYCLE_FOR_TEST.with(std::cell::Cell::get)
}

pub(super) fn reclaim_steps_per_cycle() -> usize {
    #[cfg(any(test, feature = "test-seams"))]
    if let Some(steps) = RECLAIM_STEPS_PER_CYCLE_FOR_TEST.with(std::cell::Cell::get) {
        return steps;
    }
    RECLAIM_STEPS_PER_CYCLE
}

/// The declared serving-cache size a stored value stands for.
///
/// The setting is absent far more often than it is set, and both the durable
/// config value and the in-memory cell spell "absent" as zero, because there is
/// no size a consumer could mean by it — nothing serves from an empty cache.
/// Every reader of either goes through here, so that convention is written once
/// instead of at each site that happens to need it.
pub(super) fn declared_serving_cache_bytes(stored: u64) -> Option<usize> {
    match stored {
        0 => None,
        bytes => Some(usize::try_from(bytes).unwrap_or(usize::MAX)),
    }
}

impl StagedTupleKey {
    pub(super) fn new(table: &str, columns: &[String], values: &[Value]) -> Self {
        Self {
            table: table.to_string(),
            columns: columns.to_vec(),
            values: values
                .iter()
                .cloned()
                .map(|value| DirectedValue::Asc(TotalOrdAsc(value)))
                .collect(),
        }
    }
}

impl StagedTupleLookup {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rebuild(db: &Database, ws: &WriteSet) -> Result<Self> {
        let columns_by_table = db.staged_tuple_key_columns_by_table();
        Ok(Self::from_write_set(&columns_by_table, ws))
    }

    pub(super) fn from_write_set(
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        ws: &WriteSet,
    ) -> Self {
        let mut lookup = Self::default();
        for (table, row) in &ws.relational_inserts {
            lookup.add_row_with_columns(columns_by_table, table, row);
        }
        lookup
    }

    pub(super) fn add_row_with_columns(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        table: &str,
        row: &VersionedRow,
    ) {
        let Some(table_columns) = columns_by_table.get(table) else {
            return;
        };
        for columns in table_columns {
            let Some(values) = tuple_values_for_row(row, columns) else {
                continue;
            };
            if !Database::values_can_be_tuple_key(&values) {
                continue;
            }
            self.entries
                .entry(StagedTupleKey::new(table, columns, &values))
                .or_default()
                .insert(row.row_id);
        }
    }

    pub(super) fn remove_row_with_columns(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        table: &str,
        row: &VersionedRow,
    ) {
        let Some(table_columns) = columns_by_table.get(table) else {
            return;
        };
        for columns in table_columns {
            let Some(values) = tuple_values_for_row(row, columns) else {
                continue;
            };
            if !Database::values_can_be_tuple_key(&values) {
                continue;
            }
            let key = StagedTupleKey::new(table, columns, &values);
            let remove_entry = self.entries.get_mut(&key).is_some_and(|row_ids| {
                row_ids.remove(&row.row_id);
                row_ids.is_empty()
            });
            if remove_entry {
                self.entries.remove(&key);
            }
        }
    }

    pub(super) fn apply_delta(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        delta: &StagedTupleLookupDelta,
    ) {
        for event in &delta.events {
            match event {
                StagedTupleLookupEvent::Add { table, row } => {
                    self.add_row_with_columns(columns_by_table, table, row);
                }
                StagedTupleLookupEvent::Remove { table, row } => {
                    self.remove_row_with_columns(columns_by_table, table, row);
                }
            }
        }
    }

    pub(super) fn staged_unique_conflict(
        &self,
        table: &str,
        row_id: RowId,
        columns: &[String],
        values: &[Value],
        stats: &mut CommitStageStats,
    ) -> Option<RowId> {
        stats.staged_vs_staged_comparisons = stats.staged_vs_staged_comparisons.saturating_add(1);
        self.entries
            .get(&StagedTupleKey::new(table, columns, values))
            .and_then(|row_ids| row_ids.iter().copied().find(|other| *other != row_id))
    }

    pub(super) fn contains_tuple(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        stats: Option<&mut CommitStageStats>,
    ) -> bool {
        if let Some(stats) = stats {
            stats.staged_vs_staged_comparisons =
                stats.staged_vs_staged_comparisons.saturating_add(1);
        }
        self.entries
            .get(&StagedTupleKey::new(table, columns, values))
            .is_some_and(|row_ids| !row_ids.is_empty())
    }
}

impl StagedTupleLookupDelta {
    pub(super) fn add(&mut self, table: &str, row: &VersionedRow) {
        self.events.push(StagedTupleLookupEvent::Add {
            table: table.to_string(),
            row: row.clone(),
        });
    }

    pub(super) fn remove(&mut self, table: &str, row: &VersionedRow) {
        self.events.push(StagedTupleLookupEvent::Remove {
            table: table.to_string(),
            row: row.clone(),
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl CommitStageTimer {
    pub(super) fn new() -> Self {
        Self {
            last: Instant::now(),
        }
    }

    pub(super) fn record(&mut self, stats: &mut CommitStageStats, stage: usize) {
        let now = Instant::now();
        let delta = now
            .duration_since(self.last)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        self.last = now;
        let stages = stats.stage_wall_nanos.get_or_insert([0; 7]);
        if let Some(slot) = stages.get_mut(stage) {
            *slot = slot.saturating_add(delta);
        }
    }
}

impl<'a> CommitStageStatsGuard<'a> {
    pub(super) fn new(db: &'a Database) -> Self {
        Self {
            db,
            stats: CommitStageStats::default(),
            flushed: false,
        }
    }

    pub(super) fn stats_mut(&mut self) -> &mut CommitStageStats {
        &mut self.stats
    }

    pub(super) fn flush(&mut self) {
        if !self.flushed {
            self.db.add_commit_stage_stats(&self.stats);
            self.flushed = true;
        }
    }
}

impl Drop for CommitStageStatsGuard<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl PruningRuntime {
    pub(super) fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            interval_ms: Arc::new(AtomicU64::new(MAINTENANCE_TICK.as_millis() as u64)),
            #[cfg(test)]
            wakes: Arc::new(AtomicU64::new(0)),
            worker_clock: Arc::new(MaintenanceWorkerClock::default()),
        }
    }

    /// Request shutdown before handing the worker to its joiner. The clock is
    /// the worker's production wait in every build, so notification cannot be
    /// limited to test configurations.
    pub(super) fn request_shutdown_and_take_handle(&mut self) -> Option<JoinHandle<()>> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.worker_clock.wake();
        self.handle.take()
    }
}

/// Open an in-memory database with a startup memory ceiling without exposing
/// the mutable accountant that enforces it. This is a safe bootstrap surface:
/// callers provide only the declared byte limit and every later limit change
/// still goes through [`Database::set_memory_limit`].
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn open_memory_with_startup_limit(
    plugin: Arc<dyn DatabasePlugin>,
    memory_limit: Option<usize>,
) -> Result<Database> {
    open_with_startup_limits(MEMORY_DATABASE_PATH, plugin, memory_limit, None)
}

/// Open a file-backed database with numeric startup ceilings. The mutable
/// memory-accounting capability never crosses the engine boundary.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn open_with_startup_limits(
    path: impl AsRef<Path>,
    plugin: Arc<dyn DatabasePlugin>,
    memory_limit: Option<usize>,
    disk_limit: Option<u64>,
) -> Result<Database> {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            plugin,
            memory_limit,
            disk_limit,
            ..DatabaseOpenOptions::default()
        },
    )
}
