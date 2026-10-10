//! Production-dead ownership and work observations for resident-history tests.
use super::*;
use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub cleanup_versions_examined: usize,
    pub retention_keys_examined: usize,
    pub graph_keys_examined: usize,
    pub membership_envelopes_verified: usize,
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
            cleanup_versions_examined: 0,
            retention_keys_examined: 0,
            graph_keys_examined: 0,
            membership_envelopes_verified: 0,
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
    pub fn __durable_page_population_for_test(&self) -> Result<(usize, u64)> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_page_population_for_test()
    }

    /// Resident owners omitted from the older resident mirror set.
    ///
    /// Slots 1–5 report collection backing capacity: reverse adjacency,
    /// accepted sync authors, refusal scans, DDL slots, and paged publications.
    /// Slot 6 is vector graph ownership receipt, not allocator backing capacity.
    #[doc(hidden)]
    pub fn __additional_resident_owners_for_test(&self) -> [usize; 6] {
        let reverse = self.graph_store.reverse_adj.read();
        let authors = self.accepted_sync_row_authors.read();
        let refusals = self.terminal_refusal_scans.read();
        let ddl = self.ddl_log.read();
        let vectors = self.vector_store.memory_ownership_snapshot_for_test();
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
            self.persistence
                .as_ref()
                .map_or(0, |p| p.paged_publication_owners_for_test()),
            vectors.base_graph + vectors.change_graph + vectors.retired_pinned_graph_bytes,
        ]
    }

    #[doc(hidden)]
    pub fn __create_region_fixture_for_test(path: &Path, region_bytes: u64) -> Result<()> {
        RedbPersistence::create_region_fixture_for_test(path, region_bytes)
    }
    #[doc(hidden)]
    pub fn __durable_ddl_for_test(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_ddl_for_test()
    }

    #[doc(hidden)]
    pub fn __native_read_transaction_count_for_test(&self, transaction_id: u64) -> Result<u64> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .native_read_transaction_count_for_test(transaction_id)
    }
    #[doc(hidden)]
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
    pub fn __physical_snapshot_for_test(&self) -> Result<redb::StorageSnapshotIdentity> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .physical_snapshot_for_test()
    }

    /// Actual retained fork allocator capacities; no charge is synthesized.
    #[doc(hidden)]
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
    pub fn __durable_version_count_for_test(&self, table: &str) -> Result<usize> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_version_count_for_test(table)
    }

    /// Actual durable relation height for multi-leaf fixture setup.
    #[doc(hidden)]
    pub fn __durable_relation_tree_height_for_test(&self, table: &str) -> Result<u32> {
        self.persistence
            .as_ref()
            .expect("file fixture")
            .durable_relation_tree_height_for_test(table)
    }

    #[doc(hidden)]
    pub fn __maintenance_with_stop_for_test(
        &self,
        stop: Arc<AtomicBool>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<MaintenanceReport> {
        let mut context = self.maintenance_context();
        context.worker_stop = Some(stop);
        context.run_cycle(CurrencyGate::Always, read_memory.clone())
    }
    #[doc(hidden)]
    pub fn __storage_batch_for_test(&self) -> Result<(bool, usize, usize)> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        let (progress, _) = publish_storage_compaction_batch(
            &self.tx_mgr,
            persistence,
            &self.accountant,
            &self.publication_interrupted,
            persistence.fragmentation_ratio().unwrap_or(0.0),
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
    #[doc(hidden)]
    pub fn __writer_cache_stats_for_test(&self) -> Result<(u64, u64, u64, usize)> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .resident_cache_stats_for_test()
    }
    /// Actual allocated native cache-index bytes, excluding page payload reserve.
    #[doc(hidden)]
    pub fn __writer_cache_index_bytes_for_test(&self) -> Result<usize> {
        self.persistence
            .as_ref()
            .expect("file-backed fixture")
            .cache_index_bytes_for_test()
    }
    /// An actual redb body-page owner survives cache eviction until drop.
    #[doc(hidden)]
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
    /// Deliberate checksummed-envelope corruption, only on a task fixture.
    #[doc(hidden)]
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
    source: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
    pub fn __commit_directory_get_with_context_for_test(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<TxId>> {
        self.note_commit_directory_funding(context);
        let result = self.tx_mgr.try_commit_index_get_with_context(lsn, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
    pub fn __commit_directory_contains_with_context_for_test(
        &self,
        lsn: Lsn,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<bool> {
        self.note_commit_directory_funding(context);
        let result = self.tx_mgr.try_commit_index_contains_with_context(lsn, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
    pub fn __commit_directory_floor_with_context_for_test(
        &self,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.note_commit_directory_funding(context);
        let result = self.tx_mgr.try_commit_index_floor_with_context(upper, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(result)
    }

    #[doc(hidden)]
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
    pub fn __commit_directory_cursor_with_context_for_test(
        &self,
        lower: std::ops::Bound<Lsn>,
        upper: std::ops::Bound<Lsn>,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Box<dyn contextdb_tx::ReadCommitCursor>> {
        self.note_commit_directory_funding(context);
        let cursor = self.tx_mgr.read_commit_directory_cursor(lower, upper, context)?;
        self.__sample_commit_directory_residency_with_context_for_test(context)?;
        Ok(Box::new(ObservedReadCommitCursor {
            cursor,
            manager: self.tx_mgr.clone(),
            image: self.read_image_memory.as_ref().map(Arc::downgrade),
        }))
    }

    #[doc(hidden)]
    pub fn __sample_commit_directory_residency_with_context_for_test(
        &self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<()> {
        self.__commit_directory_census_with_context_for_test(context)?;
        Ok(())
    }

    fn note_commit_directory_funding(&self, context: &contextdb_tx::ReadCommitContext<'_>) {
        self.tx_mgr.visit_captured_read_memory_for_test(crate::read_work_probe::note_commit_directory_open_funding);
        if let Some(image) = self.read_image_memory.as_ref() {
            crate::read_work_probe::note_commit_directory_image_memory(image);
        }
        crate::read_work_probe::note_commit_directory_request_funding(&context.read_memory);
    }

    #[doc(hidden)]
    pub fn __try_begin_for_test(&self) -> Result<TxId> {
        self.tx_mgr.try_begin()
    }
}

fn note_commit_directory_sample(manager: &TransactionManager<DynStore>, durable_entries: usize) {
    // retained_entry_bytes counts the actual decoded pair slots; source cache
    // pages and caller-owned cursor output are not resident directory entries.
    let resident_entries = manager.retained_commit_directory_bytes_for_test()
        / std::mem::size_of::<(Lsn, TxId)>();
    crate::read_work_probe::note_commit_directory_residency(
        resident_entries as u64,
        durable_entries as u64,
    );
}

struct ObservedReadCommitCursor {
    cursor: Box<dyn contextdb_tx::ReadCommitCursor>,
    manager: Arc<TransactionManager<DynStore>>,
    image: Option<std::sync::Weak<crate::read_image_memory::ImageMemory>>,
}

impl contextdb_tx::ReadCommitCursor for ObservedReadCommitCursor {
    fn next_entry(
        &mut self,
        context: &contextdb_tx::ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>> {
        self.manager.visit_captured_read_memory_for_test(crate::read_work_probe::note_commit_directory_open_funding);
        if let Some(image) = self.image.as_ref().and_then(std::sync::Weak::upgrade) {
            crate::read_work_probe::note_commit_directory_image_memory(&image);
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
    writer: Option<Arc<crate::persistence::WriterSnapshot>>,
    retained: Option<Arc<crate::persistence::RetainedReadSource>>,
    session: crate::read_session::ReadSession,
    pub snapshot: SnapshotId,
    pub storage: redb::StorageSnapshotIdentity,
}
impl SavedVectorRootForTest {
    #[doc(hidden)]
    pub fn with_public_calls<T>(
        &self,
        database: &Database,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        action: impl FnOnce(&Database, &crate::read_session::ReadSession) -> Result<T>,
    ) -> Result<T> {
        if self.manager != Arc::as_ptr(&database.tx_mgr) as usize {
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
            if !original.is_some_and(|source| Arc::ptr_eq(source, retained)) {
                return Err(Error::Other(
                    "saved vector proof retained source changed".into(),
                ));
            }
        }
        let _root = database.enter_writer_read(self.writer.clone(), read_memory);
        action(database, &self.session)
    }
}
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct SavedVectorGenerationForTest {
    pub graph_format_version: u16,
    pub is_base: bool,
    pub base_generation_id: u64,
    pub base_tx: TxId,
    pub base_lsn: Lsn,
    pub phase: contextdb_vector::hnsw::paged_builder::PagedVectorPhase,
    pub extend_candidates: bool,
    pub keep_pruned: bool,
    pub max_layer: u8,
    pub scale_bits: u64,
    pub hnsw_ef_search: u32,
    pub generation_id: u64,
    pub storage_id: u64,
    pub covered_tx: TxId,
    pub covered_lsn: Lsn,
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub manifest: Vec<u8>,
    pub source_digest: [u8; 32],
    pub record_digest: [u8; 32],
    pub distinct_rows: u64,
    pub points: usize,
    pub record_bytes: u64,
    pub node_limit: u64,
    pub declaration_digest: u64,
    pub policy_revision: u64,
    pub hnsw_m: u32,
    pub hnsw_ef_construction: u32,
}
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct SavedTopologyFixtureForTest {
    pub partition: VectorPartitionRef,
    pub base: SavedVectorGenerationForTest,
    pub change: Option<SavedVectorGenerationForTest>,
    /// Actual original producer inputs, identified by role and generation.
    pub original_sources: Vec<(bool, u64, Arc<[u8]>)>,
    pub cycles: usize,
}
fn saved_generation_receipt(
    version: u16,
    generation_id: u64,
    bytes: Vec<u8>,
    descriptor: VectorPartitionGenerationDescriptor,
) -> Result<SavedVectorGenerationForTest> {
    let meta = crate::persistence::vector_paged::PagedGenerationMeta::decode(&bytes)?;
    let progress = meta.progress()?;
    let saved = meta
        .saved
        .as_ref()
        .ok_or_else(|| Error::Other("proof fixture did not publish saved topology".into()))?;
    if version != 1 {
        return Err(Error::Other(
            "proof fixture did not publish graph format 1".into(),
        ));
    }
    Ok(SavedVectorGenerationForTest {
        graph_format_version: version,
        generation_id,
        storage_id: meta.storage_id,
        is_base: saved.is_base,
        base_generation_id: saved.base_generation_id,
        base_tx: saved.base_tx,
        base_lsn: saved.base_lsn,
        phase: progress.phase,
        extend_candidates: saved.extend_candidates,
        keep_pruned: saved.keep_pruned,
        max_layer: saved.max_layer,
        scale_bits: saved.scale_bits,
        hnsw_ef_search: meta.declaration.hnsw_ef_search,
        covered_tx: progress.frontier_tx,
        covered_lsn: progress.frontier_lsn,
        dimension: meta.declaration.dimension as usize,
        quantization: meta.declaration.quantization,
        source_digest: saved.source_digest,
        record_digest: saved.record_digest,
        distinct_rows: saved.distinct_rows,
        points: progress.graph.points,
        record_bytes: saved.record_bytes,
        node_limit: saved.node_limit,
        declaration_digest: progress.declaration_digest,
        policy_revision: descriptor.policy_revision,
        hnsw_m: descriptor.hnsw_m,
        hnsw_ef_construction: descriptor.hnsw_ef_construction,
        manifest: bytes,
    })
}
impl Database {
    /// Captures the existing public read root. This opens no alternate route.
    #[doc(hidden)]
    pub fn __capture_saved_vector_root_for_test(
        &self,
        limits: ReadLimits,
    ) -> Result<SavedVectorRootForTest> {
        let _operation = self.open_operation()?;
        let writer = self.writer_read_root()?;
        let retained = self.retained_relational_source.clone().or_else(|| {
            self.committed_image_startup
                .as_ref()
                .map(|startup| Arc::clone(&startup.config_source))
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
            manager: Arc::as_ptr(&self.tx_mgr) as usize,
            writer,
            retained,
            session: self.read_session(limits)?,
            snapshot,
            storage,
        })
    }
    /// Drive ordinary finite maintenance, without selecting a native format or
    /// changing declared policy. A route that publishes another format refuses.
    #[doc(hidden)]
    pub fn __saved_topology_fixture_for_test(
        &self,
        partition: &VectorPartitionRef,
        require_change: bool,
        max_cycles: usize,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<SavedTopologyFixtureForTest> {
        let sources = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&sources);
        let selected_partition = partition.clone();
        let prior =
            crate::vector_observations::observe_additional_journal_replay(Arc::new(move |event| {
                if event.partition != selected_partition {
                    return;
                }
                if let crate::vector_observations::VectorJournalReplayWork::SavedTopologyExport {
                    is_base,
                    generation_id,
                    bytes,
                } = &event.work
                {
                    captured
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push((*is_base, *generation_id, Arc::clone(bytes)));
                }
            }));
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other("saved topology fixture requires writable persistence".into())
        })?;
        for cycles in 1..=max_cycles {
            self.run_maintenance_cycle_with_read_memory(read_memory.clone())?;
            let Some(catalog) = persistence.load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            else {
                continue;
            };
            if require_change && catalog.change.is_none() {
                continue;
            }
            let base = persistence.load_vector_partition_base_generation(
                &partition.index,
                &partition.partition_key,
                catalog.base.generation_id,
            )?;
            let base = saved_generation_receipt(
                base.graph_format_version,
                base.generation_id,
                base.graph.bytes,
                catalog.base,
            )?;
            let change = catalog
                .change
                .map(|descriptor| {
                    let change = persistence.load_vector_partition_change_generation(
                        &partition.index,
                        &partition.partition_key,
                        base.generation_id,
                        descriptor.generation_id,
                    )?;
                    saved_generation_receipt(
                        change.graph_format_version,
                        change.change_generation_id,
                        change.graph.bytes,
                        descriptor,
                    )
                })
                .transpose()?;
            let original_sources = sources
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            let selected = if require_change {
                change.as_ref().expect("requested change")
            } else {
                &base
            };
            if !original_sources.iter().any(|(role, _generation, bytes)| {
                *role == selected.is_base
                    && blake3::hash(bytes).as_bytes() == &selected.source_digest
            }) {
                continue;
            }
            drop(prior);
            return Ok(SavedTopologyFixtureForTest {
                partition: partition.clone(),
                base,
                change,
                original_sources,
                cycles,
            });
        }
        Err(Error::Other("ordinary maintenance did not publish the requested saved topology within the supplied cycle bound".into()))
    }
}

impl SavedTopologyFixtureForTest {
    /// Borrow the exact immutable producer input captured at publication.
    /// A previous publication's source must be retained from its factory call.
    #[doc(hidden)]
    pub fn original_source(&self, is_base: bool, generation_id: u64) -> Option<&[u8]> {
        let selected = if is_base {
            Some(&self.base)
        } else {
            self.change.as_ref()
        }?;
        if selected.generation_id != generation_id {
            return None;
        }
        self.original_sources
            .iter()
            .find(|(role, _generation, bytes)| {
                *role == is_base && blake3::hash(bytes).as_bytes() == &selected.source_digest
            })
            .map(|(_, _, bytes)| bytes.as_ref())
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
