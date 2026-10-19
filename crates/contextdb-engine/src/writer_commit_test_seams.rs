//! Deterministic observations of the ordinary writer's durable publication.

use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitBoundary {
    BeforeRedb,
    /// A scoped erasure has announced its tables and is waiting for readers.
    ErasureWaitingForReaders,
    /// Before a standalone metadata operation allocates its local LSN.
    BeforeDdlLsnAllocation,
    /// The snapshot artifact is complete, before its final publication fence.
    ExportBeforePublication,
    DuringRedbSync,
    AfterRedb,
    BeforeMemoryPublication,
    GenerationBeforeRedb,
    GenerationAfterRedb,
    /// Every paged build transaction (insertion, encoding, completion and
    /// publication batches), around its redb commit.
    BuildBatchBeforeRedb,
    BuildBatchAfterRedb,
    /// Every replacement-candidate catch-up transaction, around its commit.
    CandidateCatchUpBeforeRedb,
    CandidateCatchUpAfterRedb,
}

/// The durable successor the build or catch-up transaction on this thread
/// is about to commit, readable from an observer at its boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildSuccessor {
    pub phase: contextdb_vector::hnsw::paged_builder::PagedVectorPhase,
    pub points: usize,
    pub frontier_lsn: contextdb_core::Lsn,
    pub frontier_tx: contextdb_core::TxId,
    pub last_source_key: Option<Vec<u8>>,
    pub encoded_points: usize,
}

thread_local! {
    static SUCCESSOR: RefCell<Option<BuildSuccessor>> = const { RefCell::new(None) };
}

/// The successor of the build or catch-up transaction last observed on this
/// thread, if any.
pub fn last_build_successor() -> Option<BuildSuccessor> {
    SUCCESSOR.with(|slot| slot.borrow().clone())
}

thread_local! {
    static CATCH_UP_COHORTS_PER_WAKE: Cell<Option<usize>> = const { Cell::new(None) };
}

/// While held, one maintenance wake on this thread absorbs at most `cap`
/// committed cohorts into a complete replacement before yielding, so a test
/// can read a candidate's counters between individual catch-ups.
pub struct CatchUpCohortCap {
    previous: Option<usize>,
}

pub fn cap_catch_up_cohorts_per_wake(cap: usize) -> CatchUpCohortCap {
    CatchUpCohortCap {
        previous: CATCH_UP_COHORTS_PER_WAKE.with(|slot| slot.replace(Some(cap))),
    }
}

impl Drop for CatchUpCohortCap {
    fn drop(&mut self) {
        CATCH_UP_COHORTS_PER_WAKE.with(|slot| slot.set(self.previous));
    }
}

pub(crate) fn catch_up_cohorts_per_wake() -> Option<usize> {
    CATCH_UP_COHORTS_PER_WAKE.with(Cell::get)
}

pub(crate) fn note_build_successor(
    next: &contextdb_vector::hnsw::paged_builder::PagedVectorProgress,
) {
    SUCCESSOR.with(|slot| {
        *slot.borrow_mut() = Some(BuildSuccessor {
            phase: next.phase,
            points: next.graph.points,
            frontier_lsn: next.frontier_lsn,
            frontier_tx: next.frontier_tx,
            last_source_key: next.last_source_key.clone(),
            encoded_points: next.encoded_points,
        })
    });
}

type Observer = Arc<dyn Fn(CommitBoundary) + Send + Sync>;
thread_local! {
    static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) };
    static COMMITTING: Cell<bool> = const { Cell::new(false) };
}

pub fn observe<T>(observer: Observer, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<Observer>, bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
            COMMITTING.with(|slot| slot.set(self.1));
        }
    }
    let _restore = Restore(
        OBSERVER.with(|slot| slot.replace(Some(observer))),
        COMMITTING.with(|slot| slot.replace(false)),
    );
    operation()
}

// crash_cutpoints_recover_one_complete_row_index_vector_log_commit and
// writer_reader_root_survives_commit_eviction_and_cleanup pause at the actual
// durability/publication boundaries without changing their order or outcomes.
pub(crate) fn reached(boundary: CommitBoundary) {
    match boundary {
        CommitBoundary::BeforeRedb => COMMITTING.with(|slot| slot.set(true)),
        CommitBoundary::AfterRedb => COMMITTING.with(|slot| slot.set(false)),
        CommitBoundary::DuringRedbSync if !COMMITTING.with(Cell::get) => return,
        _ => {}
    }
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(boundary);
    }
}
