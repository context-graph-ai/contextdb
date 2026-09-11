//! Operation-scoped evidence emitted at real engine work boundaries.
use crate::VectorPartitionRef;
use contextdb_core::TxId;
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VectorBacklog {
    pub inserts: usize,
    pub tombstones: usize,
}
impl VectorBacklog {
    pub(crate) fn include(&mut self, other: Self) {
        self.inserts = self.inserts.max(other.inserts);
        self.tombstones = self.tombstones.max(other.tombstones);
    }
}

#[derive(Clone, Debug)]
pub struct VectorBasePublicationEvent {
    pub partition: VectorPartitionRef,
    pub previous_generation: u64,
    pub generation: u64,
    pub covered_tx: TxId,
    pub pending_before: VectorBacklog,
    pub pending_after: VectorBacklog,
    /// High-water observed during this process's ownership of the previous generation.
    pub previous_generation_high_water: VectorBacklog,
    pub previous_generation_total_high_water: usize,
    pub generation_high_water: VectorBacklog,
}
type Observer = Arc<dyn Fn(&VectorBasePublicationEvent) + Send + Sync>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) }; }

/// Installs an observer for work performed on the calling thread. A caller
/// driving maintenance on another thread installs its observer on that thread.
pub fn observe_base_publications(observer: Observer) -> VectorPublicationObservation {
    let previous = OBSERVER.with(|slot| slot.replace(Some(observer)));
    VectorPublicationObservation {
        previous,
        _thread: PhantomData,
    }
}
pub struct VectorPublicationObservation {
    previous: Option<Observer>,
    _thread: PhantomData<Rc<()>>,
}
impl Drop for VectorPublicationObservation {
    fn drop(&mut self) {
        OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}
pub(crate) fn emit(event: &VectorBasePublicationEvent) {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(event);
    }
}

#[derive(Default)]
pub(crate) struct BaseBacklog {
    pub generation: u64,
    pub covered_tx: u64,
    pub pending: VectorBacklog,
    pub high_water: VectorBacklog,
    pub total_high_water: usize,
}
impl BaseBacklog {
    pub fn record_high_water(&mut self) {
        self.high_water.include(self.pending);
        self.total_high_water = self
            .total_high_water
            .max(self.pending.inserts.saturating_add(self.pending.tombstones));
    }
}
