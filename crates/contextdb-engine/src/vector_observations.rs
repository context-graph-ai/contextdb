//! Evidence from actual journal replay, scoped to one calling operation.
use contextdb_core::{Lsn, RowId, TxId};
use contextdb_vector::VectorPartitionRef;
pub use contextdb_vector::observations::{
    VectorBacklog, VectorBasePublicationEvent, VectorPublicationObservation,
    observe_base_publications,
};
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

#[derive(Clone, Debug)]
pub enum VectorJournalReplayWork {
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
