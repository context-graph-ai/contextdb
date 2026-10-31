use super::*;

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_partition_routes(
    vector: &VectorStore,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<Vec<VectorPartitionRef>> {
    let mut routes: Vec<VectorPartitionRef> = Vec::new();
    for index in vector.index_layout_infos(vector_copy_owner)? {
        let keys = vector.partition_keys(&index.index, vector_copy_owner)?;
        #[cfg(all(test, feature = "test-seams"))]
        let output = (routes.len(), routes.as_ptr() as usize, routes.capacity());
        let append = || -> Result<()> {
            let output_capacity = routes
                .len()
                .checked_add(keys.len())
                .ok_or_else(|| Error::Other("vector route output size overflow".into()))?;
            let output_bytes = output_capacity
                .saturating_sub(routes.capacity())
                .checked_mul(std::mem::size_of::<VectorPartitionRef>())
                .ok_or_else(|| Error::Other("vector route output size overflow".into()))?;
            let index_bytes = index
                .index
                .table
                .len()
                .checked_add(index.index.column.len())
                .and_then(|bytes| bytes.checked_mul(keys.len()))
                .ok_or_else(|| Error::Other("vector route index size overflow".into()))?;
            let retained_bytes = output_bytes
                .checked_add(index_bytes)
                .ok_or_else(|| Error::Other("vector route copy size overflow".into()))?;
            // The keys and layout inputs remain live. Prepay both the complete
            // destination growth and every fresh index-string clone before
            // either allocation, using this operation's original copy owner.
            vector_copy_owner.reserve_for_static(
                retained_bytes,
                "vector_copy",
                "maintenance_route_output",
                "Reduce selected routes or raise maintenance reader memory.",
            )?;
            routes.reserve_exact(keys.len());
            for key in keys {
                routes.push(VectorPartitionRef::new(index.index.clone(), key));
            }
            Ok(())
        };
        #[cfg(all(test, feature = "test-seams"))]
        super::vector_public_cycle_admission_tests::with_route_output_for_test(
            output.0, output.1, output.2, append,
        )?;
        #[cfg(not(all(test, feature = "test-seams")))]
        append()?;
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
            Self::ReadImage(accountant, vector) => Ok((accountant, None, vector)),
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
        let persistence = &self.persistence;
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
        #[cfg(feature = "test-seams")]
        crate::vector_ivf_observations::maintenance_cancel_checkpoint()?;
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
    ) -> Result<()> {
        vector_source_page_admission::with_source_page_admission(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            || {
                self.retry_one_vector_journal_truncation_with_source_admission(persistence, catalog)
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
    ) -> Result<()> {
        let mut cleanup_workspace = self.vector.reserve_workspace_static(
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            0,
            "truncate_vector_partition_journal",
            "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before cleaning this maintained route.",
        )?;
        self.retry_one_vector_journal_truncation_in_workspace(
            persistence,
            catalog,
            &mut cleanup_workspace,
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
        cleanup_workspace: &mut contextdb_vector::VectorWorkspaceReservation,
    ) -> Result<()> {
        self.truncate_vector_journal_in_published_steps(
            persistence,
            catalog,
            cleanup_workspace,
            || Ok(None),
        )
        .map(|_| ())
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
        let persistence = &self.persistence;
        let stream = self.maintenance_catalog_stream()?;
        #[cfg(test)]
        vector_journal_cleanup_cancellation_tests::after_inventory_capture_for_test(
            stream.lazy_cursor_memory_ownership_for_test(),
        );
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
                                &mut entry.workspace,
                            )
                        };
                        if cleanup_only {
                            vector_source_page_admission::with_journal_cleanup_source_page_admission(
                                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                                retry,
                            )
                        } else {
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

    pub(super) fn file_bytes(&self) -> Option<u64> {
        std::fs::metadata(self.persistence.path())
            .ok()
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
        let persistence = &self.persistence;
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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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
        let mut staged = super::legacy_expiry_staging::LegacyExpiryStaging::new(
            &self.tx_mgr,
            tx,
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
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
                let workspace = std::cell::RefCell::new(OwnedMemoryReservation::new(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                ));
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
                    for (key, value) in frontier.retention_progress() {
                        staged.copy_config(&mut ws.config_writes, key, value)?;
                    }
                    if let Some((key, value)) = &cohort {
                        staged.copy_config(&mut ws.config_writes, key, value)?;
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
                            staged.push(&mut ws.relational_deletes, table.len(), || {
                                Ok((table.clone(), row.row_id, tx))
                            })?;
                            if let Some(Value::Uuid(id)) = row.values.get("id") {
                                expired_nodes.insert(*id);
                            }
                            if let Some(meta) = metas.get(table) {
                                for column in &meta.columns {
                                    if matches!(column.column_type, ColumnType::Vector(_)) {
                                        staged.push(
                                            &mut ws.vector_deletes,
                                            table.len().checked_add(column.name.len()).ok_or_else(
                                                || {
                                                    Error::Other(
                                                        "expiry vector identity size overflow"
                                                            .into(),
                                                    )
                                                },
                                            )?,
                                            || {
                                                Ok((
                                                    VectorIndexRef::new(table, &column.name),
                                                    row.row_id,
                                                    tx,
                                                ))
                                            },
                                        )?;
                                    }
                                }
                            }
                            staged.expiry_config(
                                &mut ws.config_writes,
                                table,
                                row.row_id,
                                row.created_tx,
                                expiry_lsn,
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
                                staged.push(&mut ws.adj_deletes, entry.edge_type.len(), || {
                                    Ok((entry.source, entry.edge_type.clone(), entry.target, tx))
                                })?;
                            }
                            Ok(())
                        },
                        maintenance_read_memory.clone(),
                    )?;
                    // Endpoint expiry retires the exact SQL-owned binding at
                    // this commit, independently of later physical reclamation.
                    writer_read::visit_graph_versions_for_nodes_with_owner(
                        &self.graph,
                        writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?,
                        &expired_nodes,
                        |entry, owner| {
                            let Some(owner) = owner else {
                                return Ok(());
                            };
                            if entry.deleted_tx.is_some()
                                || !(expired_nodes.contains(&entry.source)
                                    || expired_nodes.contains(&entry.target))
                            {
                                return Ok(());
                            }
                            let bytes = owner
                                .estimated_bytes()
                                .saturating_add(entry.estimated_bytes());
                            workspace.borrow_mut().try_grow_for(
                                bytes.saturating_mul(4).saturating_add(512),
                                "retention",
                                "prepare_logical_row_expiry",
                                "Raise MEMORY_LIMIT to publish the exact row retirement.",
                            )?;
                            let owned = contextdb_tx::OwnedAdjEntry {
                                owner: owner.clone(),
                                entry: entry.clone(),
                            };
                            let image = crate::read_image_memory::ImageMemory::charging_reader(
                                crate::memory_accounting::AccountantHandle::clone(
                                    &self.finalization.accountant,
                                ),
                                maintenance_read_memory.clone(),
                            );
                            let (key, value) = image.scope(|| -> Result<_> {
                                let key = crate::persistence::row_graph::retirement_key(owner)?;
                                let mut retired = owned.clone();
                                retired.entry.deleted_tx = Some(tx);
                                Ok((
                                    key,
                                    RedbPersistence::encode_config_value(
                                        &crate::persistence::row_graph::Retirement {
                                            version: 1,
                                            owned: retired,
                                        },
                                    )?,
                                ))
                            })?;
                            ws.row_adj_deletes.push((owned, tx));
                            ws.config_writes.push((key, value));
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
        // Disarm rollback before a post-commit fault. The returned result is
        // declared after staged, so its bodies still free before staged credit.
        if result.is_ok() {
            staged.committed();
        }
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
                // Selection scratch ended at callback return. This separate
                // invoice still covers every staged body through physical free.
                drop(committed);
                drop(staged);
                Ok(())
            }
            Err(failure) => {
                // A failure before the manager takes the write set leaves the
                // transaction active; both failure paths publish no expiry.
                if let Some(watch) = watch_completed {
                    watch.completed();
                }
                staged.rollback();
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
                    // Endpoint expiry retires the exact SQL-owned binding at
                    // this commit, independently of later physical reclamation.
                    writer_read::visit_graph_versions_for_nodes_with_owner(
                        &self.graph,
                        writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?,
                        &expired_nodes,
                        |entry, owner| {
                            let Some(owner) = owner else { return Ok(()); };
                            if entry.deleted_tx.is_some() || !(expired_nodes.contains(&entry.source)
                                || expired_nodes.contains(&entry.target)) { return Ok(()); }
                            let bytes = owner.estimated_bytes().saturating_add(entry.estimated_bytes());
                            workspace.borrow_mut().try_grow_for(bytes.saturating_mul(4).saturating_add(512),
                                "retention", "prepare_logical_row_expiry",
                                "Raise MEMORY_LIMIT to publish the exact row retirement.")?;
                            let owned = contextdb_tx::OwnedAdjEntry { owner: owner.clone(), entry: entry.clone() };
                            let image = crate::read_image_memory::ImageMemory::charging_reader(
                                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                                maintenance_read_memory.clone());
                            let (key, value) = image.scope(|| -> Result<_> {
                                let key = crate::persistence::row_graph::retirement_key(owner)?;
                                let mut retired = owned.clone();
                                retired.entry.deleted_tx = Some(tx);
                                Ok((key, RedbPersistence::encode_config_value(
                                    &crate::persistence::row_graph::Retirement { version: 1, owned: retired })?))
                            })?;
                            self.stage_retention_aux(host, guard, bytes, ||
                                Ok(contextdb_tx::staged_source::AuxBody::RowAdjDelete((owned, tx))))?;
                            self.stage_retention_aux(host, guard, key.len().saturating_add(value.len()), ||
                                Ok(contextdb_tx::staged_source::AuxBody::ConfigWrite((key, value))))?;
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
            contextdb_tx::staged_source::AuxBody::RowAdjDelete(_) => read.row_adj_deletes.len(),
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
        let prepaid_durable_carry_bytes =
            self.persistence.retention_deferred_edge_workspace_bytes()?;
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
        let fragmentation_before = self.persistence.allocator_free_page_ratio()?;
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
            && let Some(fragmentation_before) = fragmentation_before
            && fragmentation_before >= policy.start_ratio()
        {
            let progress = publish_storage_compaction_batch_with_deferral(
                &self.tx_mgr,
                &self.persistence,
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
                    &self.persistence,
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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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
                    &self.persistence,
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
        self.publish_direct_metadata(|| self.persistence.prune_trigger_audit_history(cutoff))
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
            || self
                .snapshot_registry
                .no_snapshots_besides_construction_floors()
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
        let candidate_sample = Some(self.vector_partitions_requiring_build()?);
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
        // Step 41 deletes this selection with the graph bodies below it.
        let graph_route_store = Some(&self.persistence);
        let mut vector = if graph_route_store.is_some() {
            self.run_native_vector_cycle(read_memory.clone())?
        } else if !pending_vector_partitions.is_empty() || !unselected_paged_builds.is_empty() {
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
        } else if graph_route_store.is_some() {
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
        record_maintenance_attempt_finished(&self.last_maintenance_cycle_at);
        Ok(MaintenanceReport {
            pruning,
            currency,
            vector,
            pruned_trigger_audit_rows,
            compaction,
        })
    }
}

/// Record a finished wake after its attempt-owned work has ended.
pub(super) fn record_maintenance_attempt_finished(
    stamp: &parking_lot::Mutex<Option<std::time::Instant>>,
) {
    *stamp.lock() = Some(std::time::Instant::now());
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

/// Open a throwaway database with a startup memory ceiling without exposing
/// the mutable accountant that enforces it. This is a safe bootstrap surface:
/// callers provide only the declared byte limit and every later limit change
/// still goes through [`Database::set_memory_limit`].
#[cfg(any(test, feature = "test-seams"))]
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
