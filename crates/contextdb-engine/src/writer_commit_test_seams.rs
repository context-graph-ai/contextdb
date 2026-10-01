//! Deterministic observations of the ordinary writer's durable publication.

use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitBoundary {
    BeforeRedb,
    DuringRedbSync,
    AfterRedb,
    BeforeMemoryPublication,
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
