//! Evidence from actual journal replay, scoped to one calling operation.
use contextdb_core::{Lsn, RowId, TxId};
use contextdb_vector::VectorPartitionRef;
#[cfg(any(test, feature = "test-seams"))]
pub use contextdb_vector::observations::{
    VectorBacklog, VectorBasePublicationEvent, VectorPublicationObservation,
    observe_base_publications,
};
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

#[derive(Clone, Debug)]
pub enum VectorJournalReplayWork {
    /// Original immutable producer input, copied only while a proof observer is installed.
    #[cfg(any(test, feature = "test-seams"))]
    SavedTopologyExport {
        is_base: bool,
        generation_id: u64,
        bytes: Arc<[u8]>,
    },
    /// Bytes returned by the storage backend while selecting or loading the
    /// replay suffix. Counts cache misses at the backend, independently of
    /// decoded journal records and unique immutable vector identities.
    Read {
        bytes: usize,
    },
    Record {
        lsn: Lsn,
    },
    Vector {
        row_id: RowId,
        created_tx: TxId,
        lsn: Lsn,
    },
}

#[derive(Clone, Debug)]
pub struct VectorJournalReplayEvent {
    pub partition: VectorPartitionRef,
    pub covered_lsn: Lsn,
    pub work: VectorJournalReplayWork,
}
type Observer = Arc<dyn Fn(&VectorJournalReplayEvent) + Send + Sync>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) }; }
pub fn observe_journal_replay(observer: Observer) -> VectorJournalObservation {
    let previous = OBSERVER.with(|slot| slot.replace(Some(observer)));
    VectorJournalObservation {
        previous,
        _thread: PhantomData,
    }
}
pub struct VectorJournalObservation {
    previous: Option<Observer>,
    _thread: PhantomData<Rc<()>>,
}
impl Drop for VectorJournalObservation {
    fn drop(&mut self) {
        OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}
pub(crate) fn emit(
    index: &contextdb_core::VectorIndexRef,
    key: &contextdb_core::VectorPartitionKey,
    covered_lsn: Lsn,
    work: VectorJournalReplayWork,
) {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(&VectorJournalReplayEvent {
            partition: VectorPartitionRef::new(index.clone(), key.clone()),
            covered_lsn,
            work,
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn observe_additional_journal_replay(observer: Observer) -> VectorJournalObservation {
    let prior = OBSERVER.with(|slot| slot.borrow().clone());
    observe_journal_replay(Arc::new(move |event| {
        observer(event);
        if let Some(prior) = &prior {
            prior(event);
        }
    }))
}

/// Inline ownership observations for the allocator proof; events never copy
/// graph bytes or route identities.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorGraphBufferKindForTest {
    OriginalEnvelope,
    SelectedBase,
    SelectedChange,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorGraphWorkspaceEventForTest {
    GraphBuffers {
        kind: VectorGraphBufferKindForTest,
        pointer: usize,
        capacity: usize,
    },
    ExportIdentity {
        original_generation_id: u64,
        artifact_generation_id: u64,
        storage_id: u64,
        is_base: bool,
        source_digest: [u8; 32],
        generation_key_pointer: usize,
        generation_key_len: usize,
        root_key_pointer: usize,
        root_key_len: usize,
    },
    WorkspaceResize {
        previous: usize,
        next: usize,
        operation: &'static str,
    },
}

#[cfg(any(test, feature = "test-seams"))]
type GraphWorkspaceObserver = Arc<dyn Fn(&VectorGraphWorkspaceEventForTest) + Send + Sync>;
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static GRAPH_WORKSPACE_OBSERVER: RefCell<Option<GraphWorkspaceObserver>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn observe_graph_workspace_for_test(
    observer: GraphWorkspaceObserver,
) -> VectorGraphWorkspaceObservationForTest {
    let previous = GRAPH_WORKSPACE_OBSERVER.with(|slot| slot.replace(Some(observer)));
    VectorGraphWorkspaceObservationForTest {
        previous,
        _thread: PhantomData,
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct VectorGraphWorkspaceObservationForTest {
    previous: Option<GraphWorkspaceObserver>,
    _thread: PhantomData<Rc<()>>,
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for VectorGraphWorkspaceObservationForTest {
    fn drop(&mut self) {
        GRAPH_WORKSPACE_OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}

/// One boundary of an import-tail suffix load, as the writer accountant and
/// the active decode owner see it.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportTailLoadForTest {
    /// `persist_import_vector_tail` or `catch_up_native_import_candidate`.
    pub site: &'static str,
    pub boundary: ImportTailLoadBoundaryForTest,
    /// The writer accountant's live charge at this boundary.
    pub writer_used: usize,
    pub writer_limit: Option<usize>,
    /// Upserts admitted into this cohort's workspace.
    pub cohort_vectors: usize,
    /// The last committed LSN the cohort covers when it is a committed
    /// prefix of the suffix; `None` when it is the whole suffix.
    pub cohort_through: Option<contextdb_core::Lsn>,
    /// The cohort's decoded-body workspace, admitted before the load.
    pub workspace_bytes: usize,
    /// `subsystem/operation` of the decode owner the load runs under.
    pub decode_owner: (&'static str, &'static str),
    pub decode_owner_funded_by_writer: bool,
    pub decode_owner_per_call: bool,
    /// Bytes that owner still holds, its current per-call owner included.
    pub decode_held: usize,
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportTailLoadBoundaryForTest {
    /// The cohort workspace is admitted and its decode owner entered.
    Enter,
    /// The load returned this many vectors; its decode owner is gone.
    /// `adopted_output` is the prepaid kept-vector credit its decodes adopted.
    Loaded {
        vectors: usize,
        adopted_output: usize,
    },
    /// The load refused with this display text; its decode owner is gone.
    Refused { error: String },
}

/// The observer may return a writer memory limit to install at this boundary.
#[cfg(any(test, feature = "test-seams"))]
type ImportTailLoadObserver =
    Arc<dyn Fn(&ImportTailLoadForTest) -> Option<Option<usize>> + Send + Sync>;
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static IMPORT_TAIL_LOAD_OBSERVER: RefCell<Option<ImportTailLoadObserver>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn observe_import_tail_loads_for_test(
    observer: ImportTailLoadObserver,
) -> ImportTailLoadObservationForTest {
    let previous = IMPORT_TAIL_LOAD_OBSERVER.with(|slot| slot.replace(Some(observer)));
    ImportTailLoadObservationForTest {
        previous,
        _thread: PhantomData,
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct ImportTailLoadObservationForTest {
    previous: Option<ImportTailLoadObserver>,
    _thread: PhantomData<Rc<()>>,
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for ImportTailLoadObservationForTest {
    fn drop(&mut self) {
        IMPORT_TAIL_LOAD_OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}

/// Called on the import's thread between replacement-build steps, after
/// every commit and partition gate is released: a test may commit there.
/// It receives the bytes the import's own decode owner still holds at that
/// boundary, after the step's decode owner has returned everything.
#[cfg(any(test, feature = "test-seams"))]
type ImportRebuildStepObserver = Arc<dyn Fn(usize) + Send + Sync>;
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static IMPORT_REBUILD_STEP_OBSERVER: RefCell<Option<ImportRebuildStepObserver>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn observe_import_rebuild_steps_for_test(
    observer: ImportRebuildStepObserver,
) -> ImportRebuildStepObservationForTest {
    let previous = IMPORT_REBUILD_STEP_OBSERVER.with(|slot| slot.replace(Some(observer)));
    ImportRebuildStepObservationForTest {
        previous,
        _thread: PhantomData,
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct ImportRebuildStepObservationForTest {
    previous: Option<ImportRebuildStepObserver>,
    _thread: PhantomData<Rc<()>>,
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for ImportRebuildStepObservationForTest {
    fn drop(&mut self) {
        IMPORT_REBUILD_STEP_OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}

// Membership records decoded to count a build's live inputs at its
// frontier, in total and while a build start held the commit lock.
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static BUILD_START_LOCK_HELD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static MEMBERSHIP_COUNT_DECODES: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// `(total, under the build start's commit lock)` membership decodes made
/// to count build inputs on this thread since the last call; resets both.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn take_membership_count_decodes_for_test() -> (usize, usize) {
    MEMBERSHIP_COUNT_DECODES.with(|count| count.replace((0, 0)))
}
