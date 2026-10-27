use super::{CodebookAllocationLayouts, WriterWorkspaceLayout};
use std::{cell::RefCell, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub enum IvfConstructorAllocationForTest {
    Codebook(CodebookAllocationLayouts),
    WriterWorkspace(WriterWorkspaceLayout),
}
pub trait IvfConstructorObserverForTest: Send + Sync {
    fn admitted(&self, allocation: IvfConstructorAllocationForTest);
}
thread_local! {
    static OBSERVER: RefCell<Option<Arc<dyn IvfConstructorObserverForTest>>> = const { RefCell::new(None) };
}
pub fn with_ivf_constructor_observer_for_test<R>(
    observer: Arc<dyn IvfConstructorObserverForTest>,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn IvfConstructorObserverForTest>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    let _restore = Restore(OBSERVER.with(|slot| slot.replace(Some(observer))));
    run()
}
pub(super) fn admitted(allocation: IvfConstructorAllocationForTest) {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.admitted(allocation);
    }
}
