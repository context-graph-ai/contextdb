//! Production-dead ownership and work observations for resident-history tests.
use super::*;
use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub pull_source_entries: usize,
    pub cleanup_versions_examined: usize,
    pub retention_keys_examined: usize,
    pub graph_keys_examined: usize,
    pub membership_envelopes_verified: usize,
    /// Calls to the stored vector-entry decoder, including import replay.
    pub stored_vector_decodes: usize,
    pub membership_keys_examined: usize,
    pub pending_summary_reads: usize,
    pub log_keys_examined: usize,
    pub direction_summary_reads: usize,
    pub capability_summary_reads: usize,
    pub workspace_peak: usize,
    pub maintenance_source_keys: usize,
    pub maintenance_source_visits: usize,
    pub maintenance_source_identities: [Option<(u64, u64)>; 64],
    pub compaction_pages_examined: usize,
    pub compaction_pages_relocated: usize,
}
impl Default for Work {
    fn default() -> Self {
        Self {
            pull_source_entries: 0,
            cleanup_versions_examined: 0,
            retention_keys_examined: 0,
            graph_keys_examined: 0,
            membership_envelopes_verified: 0,
            stored_vector_decodes: 0,
            membership_keys_examined: 0,
            pending_summary_reads: 0,
            log_keys_examined: 0,
            direction_summary_reads: 0,
            capability_summary_reads: 0,
            workspace_peak: 0,
            maintenance_source_keys: 0,
            maintenance_source_visits: 0,
            maintenance_source_identities: [None; 64],
            compaction_pages_examined: 0,
            compaction_pages_relocated: 0,
        }
    }
}
thread_local! { static WORK: Cell<Option<Work>> = const { Cell::new(None) }; }
pub fn observe<T>(operation: impl FnOnce() -> T) -> (T, Work) {
    struct Restore(Option<Work>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WORK.with(|work| work.set(self.0));
        }
    }
    let restore = Restore(WORK.with(|work| work.replace(Some(Work::default()))));
    let (result, source) = VectorStore::observe_generation_source_visits_for_test(operation);
    note(|work| {
        work.maintenance_source_visits += source
            .directory_identities_examined
            .saturating_add(source.raw_identities_examined)
            .saturating_add(source.entry_bodies_copied)
            as usize;
        // Durable raw-loader prefix passes are real source visits too.
        work.maintenance_source_visits += work.membership_keys_examined
    });
    let observed = WORK.with(|work| work.get().unwrap());
    drop(restore);
    (result, observed)
}
pub(crate) fn note(operation: impl FnOnce(&mut Work)) {
    WORK.with(|work| {
        if let Some(mut observed) = work.get() {
            operation(&mut observed);
            work.set(Some(observed));
        }
    });
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct SidecarOwners {
    pub tombstone_arrivals: usize,
    pub graph_arrivals: usize,
    pub received_ddl: usize,
    pub superseded_ddl: usize,
    pub terminal_refusals: usize,
    pub log_table_lookup: usize,
    pub log_refcounts: usize,
}
impl SidecarOwners {
    pub fn total(&self) -> usize {
        self.tombstone_arrivals
            + self.graph_arrivals
            + self.received_ddl
            + self.superseded_ddl
            + self.terminal_refusals
            + self.log_table_lookup
            + self.log_refcounts
    }
}
fn map_capacity<K, V>(map: &HashMap<K, V>) -> usize {
    map.capacity() * (std::mem::size_of::<(K, V)>() + 1)
}
impl Database {
    /// Actual declared allocator regions and allocated durable pages.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __durable_page_population_for_test(&self) -> Result<(usize, u64)> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_page_population_for_test()
    }

    /// Resident owners omitted from the older resident mirror set.
    ///
    /// Slots report collection backing capacity: reverse adjacency,
    /// accepted sync authors, refusal scans and DDL slots.
    #[doc(hidden)]
    pub fn __additional_resident_owners_for_test(&self) -> [usize; 4] {
        let reverse = self.graph_store.reverse_adj.read();
        let authors = self.accepted_sync_row_authors.read();
        let refusals = self.terminal_refusal_scans.read();
        let ddl = self.ddl_log.read();
        [
            map_capacity(&reverse)
                + reverse
                    .values()
                    .map(|entries| entries.capacity() * std::mem::size_of::<AdjEntry>())
                    .sum::<usize>(),
            map_capacity(&authors)
                + authors
                    .iter()
                    .map(|((table, key), author)| {
                        table.capacity() + key.capacity() + author.capacity()
                    })
                    .sum::<usize>(),
            map_capacity(&refusals),
            ddl.capacity() * std::mem::size_of::<(Lsn, DdlChange)>(),
        ]
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __create_region_fixture_for_test(path: &Path, region_bytes: u64) -> Result<()> {
        RedbPersistence::create_region_fixture_for_test(path, region_bytes)
    }
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __durable_ddl_for_test(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_ddl_for_test()
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __native_read_transaction_count_for_test(&self, transaction_id: u64) -> Result<u64> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .native_read_transaction_count_for_test(transaction_id)
    }
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __durable_sidecars_for_test(&self) -> Result<(usize, Vec<(String, Vec<u8>)>)> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_sidecars_for_test()
    }

    /// Actual resident commit entries, separately from the complete logical inventory.
    #[doc(hidden)]
    pub fn __resident_commit_directory_bytes_for_test(&self) -> usize {
        self.tx_mgr.retained_commit_directory_bytes_for_test()
    }

    /// Physical row-slot capacity excludes fixed table hash slots and names.
    #[doc(hidden)]
    pub fn __resident_row_directory_capacity_for_test(&self) -> usize {
        self.relational_store
            .tables
            .read()
            .values()
            .map(|rows| rows.capacity() * std::mem::size_of::<VersionedRow>())
            .sum()
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __physical_snapshot_for_test(&self) -> Result<redb::StorageSnapshotIdentity> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .physical_snapshot_for_test()
    }

    /// Actual retained fork allocator capacities; no charge is synthesized.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __allocator_metadata_bytes_for_test(&self) -> Result<usize> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .allocator_metadata_bytes_for_test()
    }

    /// Observe allocated retention directory capacity without reading any body.
    #[doc(hidden)]
    pub fn __retention_directory_bytes_for_test(&self) -> usize {
        self.relational_store
            .resident_retention_directory_bytes_for_test()
    }

    /// Durable survivor oracle for version-pruning tests; no resident row read.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __durable_version_count_for_test(&self, table: &str) -> Result<usize> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_version_count_for_test(table)
    }

    /// Actual durable relation height for multi-leaf fixture setup.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __durable_relation_tree_height_for_test(&self, table: &str) -> Result<u32> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_relation_tree_height_for_test(table)
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __maintenance_with_stop_for_test(
        &self,
        stop: Arc<AtomicBool>,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<MaintenanceReport> {
        let mut context = self.maintenance_context()?;
        context.worker_stop = Some(stop);
        context.run_cycle(CurrencyGate::Always, read_memory.clone())
    }
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __storage_batch_for_test(&self) -> Result<(bool, usize, usize)> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        let (progress, _) = publish_storage_compaction_batch(
            &self.tx_mgr,
            persistence,
            &self.accountant,
            &self.publication_interrupted,
            persistence.fragmentation_ratio().ok(),
        )?;
        Ok((
            progress.complete,
            progress.pages_examined,
            progress.pages_relocated,
        ))
    }
    #[doc(hidden)]
    pub fn __storage_cursor_for_test(&self) -> String {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .resident_storage_cursor_for_test()
    }
    /// Observes actual resident hash-table capacities; never loads sidecars.
    #[doc(hidden)]
    pub fn __history_sidecar_owners_for_test(&self) -> SidecarOwners {
        SidecarOwners {
            tombstone_arrivals: map_capacity(&self.sync_tombstone_arrivals.read()),
            graph_arrivals: map_capacity(&self.sync_graph_arrivals.read()),
            received_ddl: map_capacity(&self.received_ddl_arrivals.read()),
            superseded_ddl: map_capacity(&self.superseded_ddl_occurrences.read()),
            terminal_refusals: map_capacity(&self.terminal_refusal_markers.read()),
            log_table_lookup: {
                let index = self.change_log_table_index.read();
                map_capacity(&index)
                    + index
                        .iter()
                        .map(|(name, values)| {
                            name.capacity()
                                + values.capacity() * std::mem::size_of::<(Lsn, RowId)>()
                        })
                        .sum::<usize>()
            },
            // BTreeMap exposes no capacity; this nonzero allocation lower bound
            // catches every retained entry, rather than asserting a fake charge.
            log_refcounts: self.change_log_lsn_refcounts.read().len()
                * (std::mem::size_of::<(Lsn, u64)>() + 16),
        }
    }
    /// Reset the existing writer peak and return the live charged baseline.
    #[doc(hidden)]
    pub fn __reset_writer_peak_for_test(&self) -> usize {
        self.finalization.accountant.reset_high_water_for_test();
        self.finalization.accountant.usage().used
    }
    /// Sample the existing writer counter without constructing a receipt.
    #[doc(hidden)]
    pub fn __writer_charged_bytes_for_test(&self) -> usize {
        self.finalization.accountant.usage().used
    }
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __writer_cache_stats_for_test(&self) -> Result<(u64, u64, u64, usize)> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .resident_cache_stats_for_test()
    }
    /// Actual allocated native cache-index bytes, excluding page payload reserve.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __writer_cache_index_bytes_for_test(&self) -> Result<usize> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .cache_index_bytes_for_test()
    }
    /// An actual redb body-page owner survives cache eviction until drop.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __hold_row_page_for_test(
        &self,
        table: &str,
        id: usize,
    ) -> Result<redb::AccessGuard<'static, &'static [u8]>> {
        vector_source_page_admission::with_source_page_admission(self.accountant.clone(), || {
            self.persistence
                .as_ref()
                .expect("file-backed fixture")
                .hold_row_page_for_test(table, id)
        })
    }
    /// Explicit displacement setup only; ordinary hold calls preserve pinned residents.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __detach_cached_row_page_for_test(
        &self,
        page: &redb::AccessGuard<'_, &'static [u8]>,
    ) -> Result<(u64, usize, usize, bool)> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .detach_cached_row_page_for_test(page)
    }
    /// Deliberate checksummed-envelope corruption, only on a task fixture.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __damage_membership_envelope_for_test(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<()> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .damage_membership_envelope_for_test(partition)
    }
}

/// Passive cuts after each source-unit scope has destroyed its scratch owners.
#[derive(Debug, Clone, Copy)]
pub struct SourceUnitCreditCut {
    pub committed_lsn: Lsn,
    pub used: usize,
    pub peak: usize,
}

#[derive(Debug)]
pub struct SourceUnitCreditReport {
    pub baseline: usize,
    pub cuts: Vec<SourceUnitCreditCut>,
    pub before_caller_drop: usize,
    pub after_caller_drop: usize,
    pub after_standing_drop: usize,
    pub parent_before: u64,
    pub parent_after: u64,
}

#[derive(Debug)]
struct SourceUnitCreditObserver {
    source: contextdb_core::read_memory::ReadMemoryOwner,
    used: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    cuts: Mutex<Vec<SourceUnitCreditCut>>,
}
impl contextdb_core::read_memory::ReadMemoryBudget for SourceUnitCreditObserver {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.source.try_reserve(bytes, subsystem, operation, hint)?;
        let used = self.used.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak.fetch_max(used, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        use std::sync::atomic::Ordering;
        let before = self.used.fetch_sub(bytes, Ordering::SeqCst);
        assert!(before >= bytes, "source-unit credit releases at most once");
        self.source.release(bytes);
    }
}
thread_local! {
    static SOURCE_UNIT_CREDIT_OBSERVER: std::cell::RefCell<Option<Arc<SourceUnitCreditObserver>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn note_source_unit_credit_boundary_for_test(committed_lsn: Lsn) {
    use std::sync::atomic::Ordering;
    SOURCE_UNIT_CREDIT_OBSERVER.with(|active| {
        if let Some(observer) = active.borrow().as_ref() {
            let used = observer.used.load(Ordering::SeqCst);
            let peak = observer.peak.swap(used, Ordering::SeqCst);
            observer.cuts.lock().push(SourceUnitCreditCut {
                committed_lsn,
                used,
                peak,
            });
        }
    });
}

pub(crate) fn source_unit_boundaries_for_test<'a>(
    units: Vec<ChangeSet>,
    database: &'a Database,
) -> impl Iterator<Item = ChangeSet> + 'a {
    let mut units = units.into_iter();
    std::iter::from_fn(move || {
        // Includes the final next() returning None, after the final iteration
        // has destroyed all of its unit-local owners.
        note_source_unit_credit_boundary_for_test(database.current_lsn());
        units.next()
    })
}

// These controls reach the manager already installed in this Database. They
// never build a detached directory, context, or memory owner for a test.
impl Database {
    #[doc(hidden)]
    pub fn __has_captured_commit_directory_for_test(&self) -> bool {
        self.tx_mgr.has_captured_commit_directory_for_test()
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_directory_get_with_context_for_test(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<TxId>> {
        self.note_commit_directory_funding(context);
        let result = self
            .tx_mgr
            .try_commit_index_get_with_context(lsn, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_directory_contains_with_context_for_test(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<bool> {
        self.note_commit_directory_funding(context);
        let result = self
            .tx_mgr
            .try_commit_index_contains_with_context(lsn, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_directory_floor_with_context_for_test(
        &self,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.note_commit_directory_funding(context);
        let result = self
            .tx_mgr
            .try_commit_index_floor_with_context(upper, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_directory_census_with_context_for_test(
        &self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<(usize, Lsn)> {
        self.note_commit_directory_funding(context);
        let census = self.tx_mgr.try_commit_index_census_with_context(context)?;
        note_commit_directory_sample(&self.tx_mgr, census.0);
        Ok(census)
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __commit_directory_cursor_with_context_for_test(
        &self,
        lower: std::ops::Bound<Lsn>,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Box<dyn contextdb_tx::ReadCommitCursor>> {
        self.note_commit_directory_funding(context);
        let cursor = self
            .tx_mgr
            .read_commit_directory_cursor(lower, upper, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(Box::new(ObservedReadCommitCursor {
            cursor,
            manager: self.tx_mgr.clone(),
            image: self
                .read_image_memory
                .as_ref()
                .map(|image| crate::read_image_memory::ImageMemoryObservation::new(image)),
        }))
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __sample_commit_directory_residency_with_context_for_test(
        &self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<()> {
        self.__commit_directory_census_with_context_for_test(context)?;
        Ok(())
    }

    fn note_commit_directory_funding(&self, context: &contextdb_tx::ReadCommitContext<'_>) {
        self.tx_mgr.visit_captured_read_memory_for_test(
            crate::read_work_probe::note_commit_directory_open_funding,
        );
        if let Some(image) = self.read_image_memory.as_ref() {
            crate::read_work_probe::note_commit_directory_image_memory(image);
        }
        crate::read_work_probe::note_commit_directory_request_funding(&context.read_memory);
    }

    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __try_begin_for_test(&self) -> Result<TxId> {
        self.tx_mgr.try_begin()
    }
}

fn note_commit_directory_sample(manager: &DatabaseTransactionHost, durable_entries: usize) {
    // retained_entry_bytes counts the actual decoded pair slots; source cache
    // pages and caller-owned cursor output are not resident directory entries.
    let resident_entries =
        manager.retained_commit_directory_bytes_for_test() / std::mem::size_of::<(Lsn, TxId)>();
    crate::read_work_probe::note_commit_directory_residency(
        resident_entries as u64,
        durable_entries as u64,
    );
}

struct ObservedReadCommitCursor {
    cursor: Box<dyn contextdb_tx::ReadCommitCursor>,
    manager: DatabaseTransactionHost,
    image: Option<crate::read_image_memory::ImageMemoryObservation>,
}

impl contextdb_tx::ReadCommitCursor for ObservedReadCommitCursor {
    fn next_entry(
        &mut self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.manager.visit_captured_read_memory_for_test(
            crate::read_work_probe::note_commit_directory_open_funding,
        );
        if let Some(image) = self.image {
            crate::read_work_probe::note_commit_directory_image_observation(image);
        }
        crate::read_work_probe::note_commit_directory_request_funding(&context.read_memory);
        let entry = self.cursor.next_entry(context)?;
        let (durable_entries, _) = self.manager.try_commit_index_census_with_context(context)?;
        note_commit_directory_sample(&self.manager, durable_entries);
        Ok(entry)
    }
}
/// Drive the real received apply kernel with an observed declared operation child.
#[doc(hidden)]
pub fn observe_source_unit_credit_for_test(
    database: &Database,
    changes: ChangeSet,
    policies: &ConflictPolicies,
) -> (Result<ApplyResult>, SourceUnitCreditReport) {
    use contextdb_core::read_memory::ReadCredit;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Restore(Option<Arc<SourceUnitCreditObserver>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SOURCE_UNIT_CREDIT_OBSERVER.with(|active| *active.borrow_mut() = self.0.take());
        }
    }
    let parent_before = database
        .declared_read_memory
        .retained_bytes_for_source_unit_test();
    let observer = Arc::new(SourceUnitCreditObserver {
        source: database
            .declared_reader_memory()
            .expect("declare the received apply child"),
        used: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        cuts: Mutex::new(Vec::new()),
    });
    let standing = ReadCredit::try_new(
        observer.clone(),
        257,
        "test",
        "standing_source_unit_credit",
        "test",
    )
    .expect("retain a nonzero source-unit operation baseline");
    let baseline = observer.used.load(Ordering::SeqCst);
    let restore = Restore(
        SOURCE_UNIT_CREDIT_OBSERVER.with(|active| active.borrow_mut().replace(observer.clone())),
    );
    let mut caller = crate::vector_read_memory::admission(observer.clone());
    let result =
        database.apply_changes_with_read_memory(changes, policies, &mut caller, observer.clone());
    let before_caller_drop = observer.used.load(Ordering::SeqCst);
    drop(caller);
    let after_caller_drop = observer.used.load(Ordering::SeqCst);
    drop(standing);
    let after_standing_drop = observer.used.load(Ordering::SeqCst);
    let cuts = observer.cuts.lock().clone();
    drop(restore);
    drop(observer);
    let parent_after = database
        .declared_read_memory
        .retained_bytes_for_source_unit_test();
    (
        result,
        SourceUnitCreditReport {
            baseline,
            cuts,
            before_caller_drop,
            after_caller_drop,
            after_standing_drop,
            parent_before,
            parent_after,
        },
    )
}

/// A real root and bounded session retained together throughout public calls.
/// The callback must consume its semantic and SQL answers before returning.
#[doc(hidden)]
pub struct SavedVectorRootForTest {
    manager: usize,
    writer: Option<crate::persistence::WriterSnapshotHandle>,
    retained: Option<crate::persistence::RetainedReadSource>,
    session: crate::read_session::ReadSession,
    pub snapshot: SnapshotId,
    pub storage: redb::StorageSnapshotIdentity,
}
impl SavedVectorRootForTest {
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn with_public_calls<T>(
        &self,
        database: &Database,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
        action: impl FnOnce(&Database, &crate::read_session::ReadSession) -> Result<T>,
    ) -> Result<T> {
        if self.manager != database.tx_mgr.identity() {
            return Err(Error::Other(
                "saved vector proof root belongs to another database".into(),
            ));
        }
        if let Some(retained) = &self.retained {
            let original = database.retained_relational_source.as_ref().or_else(|| {
                database
                    .committed_image_startup
                    .as_ref()
                    .map(|startup| &startup.config_source)
            });
            if !original.is_some_and(|source| {
                crate::persistence::RetainedReadSource::ptr_eq(source, retained)
            }) {
                return Err(Error::Other(
                    "saved vector proof retained source changed".into(),
                ));
            }
        }
        let _root = database.enter_writer_read(self.writer.clone(), read_memory);
        action(database, &self.session)
    }
}
impl Database {
    /// Captures the existing public read root. This opens no alternate route.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __capture_saved_vector_root_for_test(
        &self,
        limits: ReadLimits,
    ) -> Result<SavedVectorRootForTest> {
        let _operation = self.open_operation()?;
        let writer = self.writer_read_root()?;
        let retained = self.retained_relational_source.clone().or_else(|| {
            self.committed_image_startup
                .as_ref()
                .map(|startup| startup.config_source.clone())
        });
        let (snapshot, storage) = if let Some(root) = &writer {
            (root.snapshot, root.storage_identity())
        } else if let Some(source) = &retained {
            (self.snapshot(), source.storage_identity())
        } else {
            return Err(Error::Other(
                "saved vector proof requires a durable root".into(),
            ));
        };
        Ok(SavedVectorRootForTest {
            manager: self.tx_mgr.identity(),
            writer,
            retained,
            session: self.read_session(limits)?,
            snapshot,
            storage,
        })
    }
}

#[cfg(test)]
std::thread_local! {
    static CHECKPOINT_OBSERVER: std::cell::RefCell<Option<Arc<dyn Fn() + Send + Sync>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
pub(super) fn with_checkpoint_observer<T>(
    observer: Arc<dyn Fn() + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    // Extend the existing observation surface at the production cancellation checkpoint.
    struct Restore(Option<Arc<dyn Fn() + Send + Sync>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CHECKPOINT_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(CHECKPOINT_OBSERVER.with(|slot| slot.replace(Some(observer))));
    operation()
}

#[cfg(test)]
pub(super) fn note_checkpoint() {
    let observer = CHECKPOINT_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer();
    }
}

impl Database {
    /// Remove one index's physical storage while its declaration stays in the table metadata, so
    /// the next statement that picks it takes the executor's missing-physical-index fallback.
    /// Returns whether storage was present. Test seams only; production never drops storage alone.
    pub fn __remove_physical_index_storage_for_test(&self, table: &str, index: &str) -> bool {
        self.relational_store()
            .indexes
            .write()
            .get_mut(table)
            .is_some_and(|indexes| indexes.remove(index).is_some())
    }
}
