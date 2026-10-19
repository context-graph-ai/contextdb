//! What one durable commit had to list, compiled only for tests.
//!
//! A write transaction hands its commit two page lists: the allocation extents this
//! transaction's own copy-on-write made unreachable (`store_data_freed_pages`)
//! and, when allocation tracking is on, the allocation extents it allocated
//! (`store_allocated_pages`). Both are written into system tables before the
//! commit lands, so their lengths are the work the commit itself must perform.
//! Allocation tracking is switched off the moment a transaction becomes dirty
//! while no savepoint exists, so an ordinary write reports zero allocated
//! list items — a store holding a savepoint open across writes does not.
//!
//! Counts are per thread and are only meaningful for a commit performed on the
//! observing thread.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Work boundaries for exact root-capture counters and admission-failure proofs.
/// This entire module is absent without `test-seams`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootWorkKindForTest {
    CatalogBegin,
    CatalogEntry,
    DirectoryLookup,
    CaptureBegin,
    CaptureEnd,
    FinalizeBegin,
    FinalizeEnd,
    ChecksumPage,
    ChecksumBytes,
    PublishBegin,
    PublishEnd,
    HeaderBegin,
    HeaderEnd,
}

/// A borrowed observation; the callback owns any storage used to retain it.
/// `page` is the native serialized page number (including allocation order),
/// and `bytes` is the complete extent size for a checksum visit.
#[derive(Debug, Clone, Copy)]
pub struct RootWorkForTest<'a> {
    pub kind: RootWorkKindForTest,
    pub table: &'a str,
    pub page: Option<[u8; 8]>,
    pub bytes: usize,
    pub tag: u8,
}

pub type RootWorkObserverForTest = Arc<dyn Fn(&RootWorkForTest<'_>) -> crate::Result + Send + Sync>;

thread_local! {
    static ROOT_WORK_OBSERVER: RefCell<Option<RootWorkObserverForTest>> = const { RefCell::new(None) };
    static ROOT_WORK_TABLE: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };
}

/// Observe work on this thread, restoring the previous callback on return or
/// unwind. Returning an error injects a failure at that exact work boundary;
/// it does not set the backend's storage-failed bit. Callbacks must not reenter
/// the observed database, whose directory/page locks may still be held.
pub fn observe_root_work_for_test<T>(
    observer: RootWorkObserverForTest,
    operation: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<RootWorkObserverForTest>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ROOT_WORK_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(ROOT_WORK_OBSERVER.with(|slot| slot.replace(Some(observer))));
    operation()
}

pub(crate) struct RootWorkTableScope(Option<Arc<str>>);

impl Drop for RootWorkTableScope {
    fn drop(&mut self) {
        ROOT_WORK_TABLE.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

pub(crate) fn root_work_table_scope(name: &str) -> RootWorkTableScope {
    let active = ROOT_WORK_OBSERVER.with(|slot| slot.borrow().is_some());
    RootWorkTableScope(if active {
        ROOT_WORK_TABLE.with(|slot| slot.replace(Some(Arc::from(name))))
    } else {
        None
    })
}

pub(crate) fn observe_root_work(
    kind: RootWorkKindForTest,
    table: Option<&str>,
    page: Option<[u8; 8]>,
    bytes: usize,
) -> crate::Result {
    let observer = ROOT_WORK_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        let scoped_table = ROOT_WORK_TABLE.with(|slot| slot.borrow().clone());
        observer(&RootWorkForTest {
            kind,
            table: table.or(scoped_table.as_deref()).unwrap_or(""),
            page,
            bytes,
            tag: CURRENT_TAG.with(Cell::get),
        })?;
    }
    Ok(())
}

/// Commits observed and allocated extents in their freed and allocated lists.
/// The legacy `*_pages` field names count list items, not physical pages. One
/// allocated extent can span multiple physical pages. `reclaimed_entries`
/// counts visited free-list entries, including empty entries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitPagesForTest {
    pub commits: u64,
    pub freed_pages: u64,
    pub allocated_pages: u64,
    /// Allocated extents reclaimed; one extent can span multiple physical pages.
    pub reclaimed_pages: u64,
    /// Free-list entries visited, including entries with no extents.
    pub reclaimed_entries: u64,
}

thread_local! {
    static COMMIT_PAGES: Cell<CommitPagesForTest> =
        const { Cell::new(CommitPagesForTest { commits: 0, freed_pages: 0, allocated_pages: 0, reclaimed_pages: 0, reclaimed_entries: 0 }) };
}

pub(crate) fn observe_commit(freed_pages: usize, allocated_pages: usize) {
    COMMIT_PAGES.with(|counts| {
        let mut current = counts.get();
        current.commits = current.commits.saturating_add(1);
        current.freed_pages = current.freed_pages.saturating_add(freed_pages as u64);
        current.allocated_pages = current
            .allocated_pages
            .saturating_add(allocated_pages as u64);
        counts.set(current);
    });
}

pub(crate) fn observe_reclaimed_page() {
    COMMIT_PAGES.with(|counts| {
        let mut current = counts.get();
        current.reclaimed_pages += 1;
        counts.set(current);
    });
}

pub(crate) fn observe_reclaimed_entry() {
    COMMIT_PAGES.with(|counts| {
        let mut current = counts.get();
        current.reclaimed_entries += 1;
        counts.set(current);
    });
}

/// Forget every commit observed on this thread so far.
#[doc(hidden)]
pub fn reset_commit_pages_for_test() {
    COMMIT_PAGES.with(|counts| counts.set(CommitPagesForTest::default()));
}

/// The commits observed on this thread since the last reset, and the allocated
/// extents their two lists held.
#[doc(hidden)]
pub fn take_commit_pages_for_test() -> CommitPagesForTest {
    COMMIT_PAGES.with(|counts| counts.replace(CommitPagesForTest::default()))
}

/// The tag no caller has claimed: every freed page copied outside a tagged
/// region is counted here.
pub const TAG_UNTAGGED: u8 = 0xff;

/// The commit's own bookkeeping: the page lists it writes into the system
/// tables, the reclamation pass and the system tree's own flush. These copies
/// are made after the commit has taken the data tree's freed list, so they
/// belong to the NEXT list rather than to the one the commit is carrying, and
/// counting them separately is what lets the tagged copies of a window add up
/// to exactly the list its commit handed over.
pub const TAG_COMMIT_BOOKKEEPING: u8 = 0xfe;

thread_local! {
    static CURRENT_TAG: Cell<u8> = const { Cell::new(TAG_UNTAGGED) };
    static FREED_BY_TAG: RefCell<BTreeMap<u8, u64>> = const { RefCell::new(BTreeMap::new()) };
    static COMMIT_SPANS: Cell<CommitSpansForTest> =
        const { Cell::new(CommitSpansForTest::ZERO) };
}

/// Claim every copy-on-write this thread performs until the tag is put back,
/// and hand back the tag that was in force. A caller that knows which of its
/// own trees it is editing uses this to say so; `TAG_UNTAGGED` is the default.
#[doc(hidden)]
pub fn set_tag(tag: u8) -> u8 {
    CURRENT_TAG.with(|current| current.replace(tag))
}

/// Pages copied on this thread since the last reset, across every tag. A caller
/// that reads it before and after one operation learns what that operation
/// copied.
#[doc(hidden)]
pub fn freed_pages_so_far() -> u64 {
    FREED_BY_TAG.with(|counts| counts.borrow().values().sum())
}

pub(crate) fn note_freed_page() {
    let tag = CURRENT_TAG.with(Cell::get);
    FREED_BY_TAG.with(|counts| {
        *counts.borrow_mut().entry(tag).or_default() += 1;
    });
}

/// The pages this thread's copy-on-write made unreachable since the last
/// reset, by the tag in force when each was copied. The total equals the freed
/// lists the commits of the same window handed over, so a page counted here
/// under no tag is a page copied outside every tagged region rather than a page
/// that went unobserved.
#[doc(hidden)]
pub fn take_freed_pages_by_tag() -> Vec<(u8, u64)> {
    FREED_BY_TAG.with(|counts| {
        std::mem::take(&mut *counts.borrow_mut())
            .into_iter()
            .collect()
    })
}

/// Where the wall time of one durable commit went, in nanoseconds. The five
/// spans partition `commit_inner`: rewriting the dirty B-tree pages and
/// computing their checksums, writing the two page lists into the system
/// tables, reclaiming pages no live read still needs, flushing the system tree,
/// and the storage commit that makes the transaction durable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitSpansForTest {
    pub commits: u64,
    pub flush_and_close_nanos: u64,
    pub store_page_lists_nanos: u64,
    pub process_freed_pages_nanos: u64,
    pub system_tree_nanos: u64,
    pub storage_commit_nanos: u64,
}

impl CommitSpansForTest {
    const ZERO: Self = Self {
        commits: 0,
        flush_and_close_nanos: 0,
        store_page_lists_nanos: 0,
        process_freed_pages_nanos: 0,
        system_tree_nanos: 0,
        storage_commit_nanos: 0,
    };
}

/// Which span a measured region belongs to. Nothing outside `transactions.rs`
/// reports one.
pub(crate) enum CommitSpan {
    FlushAndClose,
    StorePageLists,
    ProcessFreedPages,
    SystemTree,
    StorageCommit,
}

pub(crate) fn observe_commit_span(span: CommitSpan, nanos: u64) {
    COMMIT_SPANS.with(|spans| {
        let mut current = spans.get();
        let field = match span {
            CommitSpan::FlushAndClose => &mut current.flush_and_close_nanos,
            CommitSpan::StorePageLists => &mut current.store_page_lists_nanos,
            CommitSpan::ProcessFreedPages => &mut current.process_freed_pages_nanos,
            CommitSpan::SystemTree => &mut current.system_tree_nanos,
            CommitSpan::StorageCommit => &mut current.storage_commit_nanos,
        };
        *field = field.saturating_add(nanos);
        spans.set(current);
    });
}

pub(crate) fn observe_commit_span_count() {
    COMMIT_SPANS.with(|spans| {
        let mut current = spans.get();
        current.commits = current.commits.saturating_add(1);
        spans.set(current);
    });
}

/// Forget every page tag and every commit span observed on this thread.
#[doc(hidden)]
pub fn reset_page_tags_and_spans_for_test() {
    CURRENT_TAG.with(|current| current.set(TAG_UNTAGGED));
    FREED_BY_TAG.with(|counts| counts.borrow_mut().clear());
    COMMIT_SPANS.with(|spans| spans.set(CommitSpansForTest::ZERO));
}

/// The commit spans observed on this thread since the last reset.
#[doc(hidden)]
pub fn take_commit_spans_for_test() -> CommitSpansForTest {
    COMMIT_SPANS.with(|spans| spans.replace(CommitSpansForTest::ZERO))
}
