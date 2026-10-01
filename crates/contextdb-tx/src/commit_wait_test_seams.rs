//! Passive observation of a contested commit boundary.
use std::{cell::RefCell, sync::Arc};
type Observer = Arc<dyn Fn() + Send + Sync>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) }; }
pub fn observe<T>(observer: Observer, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<Observer>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(OBSERVER.with(|slot| slot.replace(Some(observer))));
    operation()
}
// writer_reader_root_survives_commit_eviction_and_cleanup advances its injected
// read clock only after a real failed lock attempt, so a waiting reader fails
// at an assertion instead of hanging or depending on elapsed wall time.
pub(crate) fn note_wait() {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer();
    }
}
