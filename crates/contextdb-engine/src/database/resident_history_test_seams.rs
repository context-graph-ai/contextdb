//! Production-dead ownership and work observations for resident-history tests.
use super::*;
use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub cleanup_versions_examined: usize,
    pub retention_keys_examined: usize,
    pub membership_envelopes_verified: usize,
    pub membership_keys_examined: usize,
    pub log_keys_examined: usize,
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
            membership_envelopes_verified: 0,
            membership_keys_examined: 0,
            log_keys_examined: 0,
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
    ) -> Result<MaintenanceReport> {
        let mut context = self.maintenance_context();
        context.worker_stop = Some(stop);
        context.run_cycle(CurrencyGate::Always)
    }
    #[doc(hidden)]
    pub fn __storage_batch_for_test(&self) -> Result<(bool, usize, usize)> {
        let persistence = self.persistence.as_ref().expect("file fixture");
        let (progress, _) = persistence.compact_incremental(
            self.accountant.clone(),
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
