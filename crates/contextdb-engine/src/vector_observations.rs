//! Evidence from actual journal replay, scoped to one calling operation.
use contextdb_core::{Lsn, RowId, TxId};
use contextdb_vector::VectorPartitionRef;
#[cfg(feature = "test-seams")]
pub use contextdb_vector::observations::{
    VectorBacklog, VectorBasePublicationEvent, VectorPublicationObservation,
    observe_base_publications,
};
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

#[derive(Clone, Debug)]
pub enum VectorJournalReplayWork {
    /// Original immutable producer input, copied only while a proof observer is installed.
    #[cfg(feature = "test-seams")]
    SavedTopologyExport { is_base: bool, generation_id: u64, bytes: Arc<[u8]> },
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

pub(crate) fn observe_reads(
    index: &contextdb_core::VectorIndexRef,
    key: &contextdb_core::VectorPartitionKey,
    covered_lsn: Lsn,
) -> redb::BackendReadObservation {
    let index = index.clone();
    let key = key.clone();
    redb::observe_backend_reads(Arc::new(move |bytes| {
        emit(
            &index,
            &key,
            covered_lsn,
            VectorJournalReplayWork::Read { bytes },
        );
    }))
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

#[cfg(feature = "test-seams")]
pub(crate) fn observe_saved_topology_export(
    partition: &VectorPartitionRef, covered_lsn: Lsn, is_base: bool,
    generation_id: u64, bytes: &[u8],
) {
    if OBSERVER.with(|slot| slot.borrow().is_some()) {
        emit(&partition.index, &partition.partition_key, covered_lsn,
            VectorJournalReplayWork::SavedTopologyExport {
                is_base, generation_id, bytes: Arc::from(bytes),
            });
    }
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn observe_additional_journal_replay(observer: Observer) -> VectorJournalObservation {
    let prior = OBSERVER.with(|slot| slot.borrow().clone());
    observe_journal_replay(Arc::new(move |event| {
        observer(event);
        if let Some(prior) = &prior { prior(event); }
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
    VectorGraphWorkspaceObservationForTest { previous, _thread: PhantomData }
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

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn observe_graph_buffer_for_test(kind: VectorGraphBufferKindForTest, bytes: &Vec<u8>) {
    if bytes.capacity() != 0 {
        emit_graph_workspace_for_test(VectorGraphWorkspaceEventForTest::GraphBuffers {
            kind, pointer: bytes.as_ptr() as usize, capacity: bytes.capacity(),
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn observe_export_identity_for_test(
    original_generation_id: u64, artifact_generation_id: u64, storage_id: u64,
    is_base: bool, source_digest: [u8; 32], generation_key: &[u8], root_key: &[u8],
) {
    // Key addresses are borrowed only during the synchronous callback. The
    // fixed inline event adds no allocation or production observation.
    emit_graph_workspace_for_test(VectorGraphWorkspaceEventForTest::ExportIdentity {
        original_generation_id, artifact_generation_id, storage_id, is_base, source_digest,
        generation_key_pointer: generation_key.as_ptr() as usize, generation_key_len: generation_key.len(),
        root_key_pointer: root_key.as_ptr() as usize, root_key_len: root_key.len(),
    });
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn observe_workspace_resize_for_test(previous: usize, next: usize, operation: &'static str) {
    if next < previous {
        emit_graph_workspace_for_test(VectorGraphWorkspaceEventForTest::WorkspaceResize {
            previous, next, operation,
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
fn emit_graph_workspace_for_test(event: VectorGraphWorkspaceEventForTest) {
    let observer = GRAPH_WORKSPACE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(&event);
    }
}
