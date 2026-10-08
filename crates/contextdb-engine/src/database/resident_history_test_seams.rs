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
