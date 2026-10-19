//! Private-owner evidence for unpartitioned_vector_membership_pages_reload_at_constant_residency.

use super::{RawVectorDirectoryEntry, VectorPartitionKey, VectorStore};
use contextdb_core::{RowId, TxId};
use std::cell::RefCell;
use std::mem::size_of;

/// Actual source-loop events for paged_initial_replacement_and_reclamation_progress_under_writes.
///
/// Counts identities before the snapshot/LSN predicate can reject them, including
/// invisible retained versions. These are work events, not returned-entry counts.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorGenerationSourceVisitsForTest {
    pub directory_identities_examined: u64,
    pub raw_identities_examined: u64,
    /// Stored-entry conversions at the snapshot generation/history copy doors.
    pub entry_bodies_copied: u64,
}

thread_local! {
    static GENERATION_SOURCE_VISITS: RefCell<Option<VectorGenerationSourceVisitsForTest>> =
        const { RefCell::new(None) };
}

pub(super) fn observe_generation_directory_identity() {
    GENERATION_SOURCE_VISITS.with(|slot| {
        if let Some(receipt) = slot.borrow_mut().as_mut() {
            receipt.directory_identities_examined =
                receipt.directory_identities_examined.saturating_add(1);
        }
    });
}

pub(super) fn observe_generation_raw_identity() {
    GENERATION_SOURCE_VISITS.with(|slot| {
        if let Some(receipt) = slot.borrow_mut().as_mut() {
            receipt.raw_identities_examined = receipt.raw_identities_examined.saturating_add(1);
        }
    });
}

pub(super) fn observe_generation_entry_copy() {
    GENERATION_SOURCE_VISITS.with(|slot| {
        if let Some(receipt) = slot.borrow_mut().as_mut() {
            receipt.entry_bodies_copied = receipt.entry_bodies_copied.saturating_add(1);
        }
    });
}

impl VectorStore {
    /// Observe generation source scans performed by `operation` on this thread.
    ///
    /// Covers the directory count in `partition_generation_entry_counts`, the
    /// directory/raw counts and raw selection pass in
    /// `entries_for_partition_generation_at_snapshot`, and the raw count and
    /// selection pass in `entries_for_partition_at_snapshot`. Copy events occur
    /// at their actual stored-entry conversions. Repeated visits count repeatedly.
    /// Loaders, graph building, other entry APIs and spawned threads are excluded.
    /// Nested scopes replace the outer observer until they return; panic unwinding
    /// also restores the previous scope. Each event counter saturates at u64::MAX.
    /// Required by paged_initial_replacement_and_reclamation_progress_under_writes.
    #[doc(hidden)]
    pub fn observe_generation_source_visits_for_test<T>(
        operation: impl FnOnce() -> T,
    ) -> (T, VectorGenerationSourceVisitsForTest) {
        struct Restore(Option<VectorGenerationSourceVisitsForTest>);
        impl Drop for Restore {
            fn drop(&mut self) {
                GENERATION_SOURCE_VISITS.with(|slot| slot.replace(self.0.take()));
            }
        }
        let previous = GENERATION_SOURCE_VISITS
            .with(|slot| slot.replace(Some(VectorGenerationSourceVisitsForTest::default())));
        let _restore = Restore(previous);
        let result = operation();
        let receipt = GENERATION_SOURCE_VISITS.with(|slot| {
            slot.borrow()
                .as_ref()
                .copied()
                .expect("generation source observer scope remains installed")
        });
        (result, receipt)
    }

    /// Allocated element-capacity bytes of resident membership directories.
    ///
    /// Reads existing owners only: raw identities and reverse positions,
    /// visibility arrays, current row placement, and retained row placement.
    /// Excludes schema/partition-state overhead, raw bodies and their shared
    /// headers, and body/graph caches. HashMap's private control bytes, spare
    /// buckets beyond its public capacity, and allocator rounding are excluded;
    /// this is a capacity-based lower bound, not the logical charge receipt.
    /// Call at a settled publication boundary for a coherent cross-owner sum.
    /// Required by unpartitioned_vector_membership_pages_reload_at_constant_residency
    /// and writer_reopen_and_doubling_keep_a_fixed_resident_envelope.
    #[doc(hidden)]
    pub fn resident_membership_directory_bytes_for_test(&self) -> usize {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            let mut bytes = 0usize;
            for column in registry.values() {
                let partitions = column.partitions.read();
                for state in partitions.values() {
                    bytes = bytes.saturating_add(
                        state
                            .raw_directory
                            .read()
                            .capacity()
                            .saturating_mul(size_of::<RawVectorDirectoryEntry>()),
                    );
                    let positions = state.raw_directory_by_row.read();
                    bytes = bytes.saturating_add(positions.capacity().saturating_mul(size_of::<(
                        RowId,
                        Vec<usize>,
                    )>(
                    )));
                    for positions in positions.values() {
                        bytes = bytes.saturating_add(
                            positions.capacity().saturating_mul(size_of::<usize>()),
                        );
                    }
                    drop(positions);
                    let visibility = state.visibility_counts.read();
                    bytes = bytes
                        .saturating_add(visibility.intervals.capacity().saturating_mul(size_of::<(
                            u64,
                            u64,
                        )>(
                        )))
                        .saturating_add(
                            visibility
                                .created
                                .capacity()
                                .saturating_mul(size_of::<TxId>()),
                        )
                        .saturating_add(
                            visibility
                                .deleted
                                .capacity()
                                .saturating_mul(size_of::<TxId>()),
                        );
                }
                drop(partitions);
                let current = column.row_to_partition.read();
                bytes = bytes.saturating_add(current.capacity().saturating_mul(size_of::<(
                    RowId,
                    VectorPartitionKey,
                )>(
                )));
                for key in current.values() {
                    bytes = bytes.saturating_add(
                        key.estimated_bytes()
                            .saturating_sub(size_of::<VectorPartitionKey>()),
                    );
                }
                drop(current);
                let history = column.row_partition_history.read();
                bytes = bytes.saturating_add(history.capacity().saturating_mul(size_of::<(
                    RowId,
                    Vec<VectorPartitionKey>,
                )>(
                )));
                for keys in history.values() {
                    bytes = bytes.saturating_add(
                        keys.capacity()
                            .saturating_mul(size_of::<VectorPartitionKey>()),
                    );
                    for key in keys {
                        bytes = bytes.saturating_add(
                            key.estimated_bytes()
                                .saturating_sub(size_of::<VectorPartitionKey>()),
                        );
                    }
                }
            }
            bytes
        })
    }
}
