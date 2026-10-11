//! Passive writer work observations for integration tests.

use std::cell::{Cell, RefCell};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriterQueryWork {
    pub staged_sort_calls: u64,
    pub staged_rows_sorted: u64,
    pub graph_delete_entries_decoded: u64,
    pub history: WriterHistoryWork,
    pub saved_graph_loads: u64,
    pub saved_graph_bytes: u64,
    pub constraint_staged_rows_examined: u64,
    pub constraint_postings_examined: u64,
    pub captured_read_snapshot: Option<u64>,
    pub physical_sources: [Option<(PhysicalFamily, redb::StorageSnapshotIdentity)>; 32],
    pub physical_source_calls: usize,
    pub target_source_calls: [u64; 4],
    pub target_backend_bytes: [u64; 4],
    pub target_entries_examined: u64,
    /// Actual writer key-projection admission retained by this observed apply.
    pub sync_parent_key_projection_bytes: usize,
    /// Maximum simultaneous key-projection admission within this observation.
    pub sync_parent_key_projection_peak_bytes: usize,
    #[cfg(any(test, feature = "test-seams"))]
    pub content_size_table_totals_visits: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriterHistoryWork {
    pub row_versions: u64,
    pub index_rebuild_inputs: u64,
    pub graph_versions: u64,
    pub change_log_entries: u64,
    pub commit_index_entries: u64,
    pub vector_memberships: u64,
    pub commit_bound_keys: u64,
    pub ddl_occurrences: u64,
    #[cfg(any(test, feature = "test-seams"))]
    pub direction_summary_reads: u64,
    #[cfg(any(test, feature = "test-seams"))]
    pub capability_summary_reads: u64,
    pub sidecar_entries: [u64; 8],
    pub sidecar_decodes: [u64; 8],
    pub selection_entries: u64,
    #[cfg(any(test, feature = "test-seams"))]
    pub allocator_regions_loaded: u64,
}

pub(crate) enum HistoryFamily {
    Row,
    #[allow(
        dead_code,
        reason = "Retained zero-work oracle for writable startup index rebuilding"
    )]
    IndexRebuildInput,
    Graph,
    ChangeLog,
    CommitIndex,
    VectorMembership,
    CommitBound,
}

// writable_open_never_touches_history_families observes these storage doors during open.
pub(crate) fn note_history(family: HistoryFamily) {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            let count = match family {
                HistoryFamily::Row => &mut counters.history.row_versions,
                HistoryFamily::IndexRebuildInput => &mut counters.history.index_rebuild_inputs,
                HistoryFamily::Graph => &mut counters.history.graph_versions,
                HistoryFamily::ChangeLog => &mut counters.history.change_log_entries,
                HistoryFamily::CommitIndex => &mut counters.history.commit_index_entries,
                HistoryFamily::VectorMembership => &mut counters.history.vector_memberships,
                HistoryFamily::CommitBound => &mut counters.history.commit_bound_keys,
            };
            *count += 1;
            work.set(Some(counters));
        }
    });
}

thread_local! {
    static WORK: Cell<Option<WriterQueryWork>> = const { Cell::new(None) };
}

pub fn observe<T>(operation: impl FnOnce() -> T) -> (T, WriterQueryWork) {
    struct Restore(Option<WriterQueryWork>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WORK.with(|work| work.set(self.0));
        }
    }
    let restore = Restore(WORK.with(|work| work.replace(Some(WriterQueryWork::default()))));
    #[cfg(any(test, feature = "test-seams"))]
    let (value, regions) = redb::observe_allocator_region_loads_for_test(operation);
    #[cfg(not(any(test, feature = "test-seams")))]
    let value = operation();
    let counters = WORK.with(|work| work.get().expect("observation remains installed"));
    #[cfg(any(test, feature = "test-seams"))]
    let counters = WriterQueryWork {
        history: WriterHistoryWork {
            allocator_regions_loaded: regions,
            ..counters.history
        },
        ..counters
    };
    drop(restore);
    (value, counters)
}

// newest_event_merges_staged_order_without_sort observes the actual overlay sort,
// which the SQL trace's sort_elided flag does not distinguish from index order.
pub(crate) fn note_staged_sort(rows: usize) {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.staged_sort_calls += 1;
            counters.staged_rows_sorted += rows as u64;
            work.set(Some(counters));
        }
    });
}

// graph_delete_seeks_only_its_edge_identity counts the actual durable delete probe.
pub(crate) fn note_graph_delete_decode() {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.graph_delete_entries_decoded += 1;
            work.set(Some(counters));
        }
    });
}

// first_search_after_writer_reopen_uses_saved_pages_without_build observes the
// persisted generation returned by the real writable/direct generation loader.
pub(crate) fn note_saved_graph_load(bytes: usize) {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.saved_graph_loads += 1;
            counters.saved_graph_bytes += bytes as u64;
            work.set(Some(counters));
        }
    });
}

// paged_constraints_preserve_staging_and_competing_commit_conflicts observes
// comparisons at the real single-column uniqueness probe, including nonmatches.
pub(crate) fn note_constraint_staged_row() {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.constraint_staged_rows_examined += 1;
            work.set(Some(counters));
        }
    });
}

pub(crate) fn note_constraint_posting() {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.constraint_postings_examined += 1;
            work.set(Some(counters));
        }
    });
}

// writer_reader_root_survives_commit_eviction_and_cleanup ties the answer to
// the snapshot actually admitted by the production reader, not an unused pin.
pub(crate) fn note_read_snapshot(snapshot: contextdb_core::SnapshotId) {
    WORK.with(|work| {
        if let Some(mut counters) = work.get() {
            counters.captured_read_snapshot = Some(snapshot.0);
            work.set(Some(counters));
        }
    });
}

/// Actual storage transactions used by page-source doors, never logical snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalFamily {
    Row,
    Posting,
    Vector,
    Graph,
}

pub(crate) fn note_physical_source(family: PhysicalFamily, read: &redb::ReadTransaction) {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            let index = work.physical_source_calls;
            if index < work.physical_sources.len() {
                work.physical_sources[index] = Some((family, read.storage_snapshot_identity()));
            }
            work.physical_source_calls += 1;
            slot.set(Some(work));
        }
    });
}

pub(crate) fn note_ddl_occurrence() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.history.ddl_occurrences += 1;
            slot.set(Some(work));
        }
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_direction_summary_read() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.history.direction_summary_reads += 1;
            slot.set(Some(work));
        }
    });
}

#[cfg(all(feature = "test-seams", feature = "sync-orchestration"))]
pub(crate) fn note_capability_summary_read() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.history.capability_summary_reads += 1;
            slot.set(Some(work));
        }
    });
}

// Called after successful enumeration, immediately before the existing decoder.
pub(crate) fn note_config_enumeration(key: &str) {
    const PREFIXES: [&str; 8] = [
        "sync_ddl_received_arrival.v1.",
        "sync_ddl_superseded.v1.",
        "sync_row_lineage.",
        "sync_graph_arrival.v1.",
        "sync_lineage.v1.record.",
        "sync_terminal_refusal.v1.",
        "retention_expiry.v1.",
        "sync_row_author.",
    ];
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            for (index, prefix) in PREFIXES.iter().enumerate() {
                if key.starts_with(prefix) {
                    work.history.sidecar_entries[index] += 1;
                }
            }
            slot.set(Some(work));
        }
    });
}

thread_local! { static TARGET: Cell<Option<u64>> = const { Cell::new(None) }; }
fn target_id(table: &str) -> u64 {
    table.bytes().fold(14695981039346656037, |hash, byte| {
        (hash ^ byte as u64).wrapping_mul(1099511628211)
    })
}
/// Attribute observations only to this physical relation, including nested constraint probes.
pub fn observe_target<T>(table: &str, operation: impl FnOnce() -> T) -> (T, WriterQueryWork) {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TARGET.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(TARGET.with(|slot| slot.replace(Some(target_id(table)))));
    observe(operation)
}
pub(crate) fn note_target_entry(table: &str) {
    if TARGET.with(|slot| slot.get()) == Some(target_id(table)) {
        WORK.with(|slot| {
            if let Some(mut work) = slot.get() {
                work.target_entries_examined += 1;
                slot.set(Some(work));
            }
        });
    }
}
pub(crate) fn physical_operation(
    family: PhysicalFamily,
    table: &str,
) -> redb::BackendReadObservation {
    physical_operation_selected(family, table, true)
}
fn physical_operation_selected(
    family: PhysicalFamily,
    table: &str,
    matches: bool,
) -> redb::BackendReadObservation {
    let selected = matches && TARGET.with(|slot| slot.get()) == Some(target_id(table));
    let index = family as usize;
    if selected {
        WORK.with(|slot| {
            if let Some(mut work) = slot.get() {
                work.target_source_calls[index] += 1;
                slot.set(Some(work));
            }
        });
    }
    redb::observe_backend_reads(std::sync::Arc::new(move |bytes| {
        if selected {
            WORK.with(|slot| {
                if let Some(mut work) = slot.get() {
                    work.target_backend_bytes[index] += bytes as u64;
                    slot.set(Some(work));
                }
            });
        }
    }))
}
pub(crate) fn note_selection_entry() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.history.selection_entries += 1;
            slot.set(Some(work));
        }
    });
}
pub(crate) fn note_config_decode(key: &str) {
    let family = sidecar_family(key);
    WORK.with(|slot| {
        if let (Some(mut work), Some(family)) = (slot.get(), family) {
            work.history.sidecar_decodes[family] += 1;
            slot.set(Some(work));
        }
    });
}
pub(crate) fn sidecar_family(key: &str) -> Option<usize> {
    [
        "sync_ddl_received_arrival.v1.",
        "sync_ddl_superseded.v1.",
        "sync_row_lineage.",
        "sync_graph_arrival.v1.",
        "sync_lineage.v1.record.",
        "sync_terminal_refusal.v1.",
        "retention_expiry.v1.",
        "sync_row_author.",
    ]
    .iter()
    .position(|prefix| key.starts_with(prefix))
}

thread_local! {
    static TARGET_KEY: RefCell<Option<contextdb_core::IndexKey>> = const { RefCell::new(None) };
}
/// Select the exact constraint tuple as well as its relation.
pub fn observe_target_key<T>(
    table: &str,
    values: &[contextdb_core::Value],
    operation: impl FnOnce() -> T,
) -> (T, WriterQueryWork) {
    struct Restore(Option<contextdb_core::IndexKey>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TARGET_KEY.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let columns = values
        .iter()
        .map(|_| (String::new(), contextdb_core::SortDirection::Asc))
        .collect::<Vec<_>>();
    let key = contextdb_relational::index_key_from_values(&columns, values);
    let _restore = Restore(TARGET_KEY.with(|slot| slot.replace(Some(key))));
    observe_target(table, operation)
}
pub(crate) fn posting_operation(
    table: &str,
    cursor: &contextdb_relational::mem::BoundedOrderedRowCursor,
) -> redb::BackendReadObservation {
    // Match the actual sought tuple before any physical index open or fetch.
    let matches = TARGET_KEY.with(|slot| {
        slot.borrow().as_ref().is_none_or(|target| {
        matches!(cursor.source_seek(), std::ops::Bound::Included(key) if key == target.as_slice())
    })
    });
    physical_operation_selected(PhysicalFamily::Posting, table, matches)
}

/// Match an exact durable tuple at the source cursor before its physical read.
pub(crate) fn source_posting_operation(
    table: &str,
    matches: impl FnOnce(&contextdb_core::IndexKey) -> bool,
) -> redb::BackendReadObservation {
    let selected = TARGET_KEY.with(|slot| slot.borrow().as_ref().is_none_or(matches));
    physical_operation_selected(PhysicalFamily::Posting, table, selected)
}

/// Observe each actual declared-table content-total lookup, including refusals.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_content_size_table_total() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.content_size_table_totals_visits += 1;
            slot.set(Some(work));
        }
    });
}

/// Observe an existing apply reservation without changing its funding or lifetime.
/// Use `observe` around the complete apply, including commit or abort.
pub(crate) fn note_sync_parent_key_projection(bytes: usize, retained: bool) {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            if retained {
                work.sync_parent_key_projection_bytes =
                    work.sync_parent_key_projection_bytes.saturating_add(bytes);
                work.sync_parent_key_projection_peak_bytes = work
                    .sync_parent_key_projection_peak_bytes
                    .max(work.sync_parent_key_projection_bytes);
            } else {
                work.sync_parent_key_projection_bytes =
                    work.sync_parent_key_projection_bytes.saturating_sub(bytes);
            }
            slot.set(Some(work));
        }
    });
}
