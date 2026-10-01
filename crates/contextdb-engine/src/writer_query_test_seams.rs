//! Passive writer work observations for integration tests.

use std::cell::Cell;

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
}

pub(crate) enum HistoryFamily {
    Row,
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
    let value = operation();
    let counters = WORK.with(|work| work.get().expect("observation remains installed"));
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
