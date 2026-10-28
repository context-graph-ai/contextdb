//! Test-owned observations of actual native vector construction.
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

pub trait VectorBuildObserver: Send + Sync {
    fn before_vector_build(&self);
}

thread_local! {
    static OBSERVER: RefCell<Option<Arc<dyn VectorBuildObserver>>> = const { RefCell::new(None) };
    static ACTIVITY: RefCell<Option<Arc<dyn NativeIndexActivity>>> = const { RefCell::new(None) };
}

pub(crate) trait NativeIndexActivity: Send + Sync {
    fn before_build(&self);
    fn before_rewrite(&self);
}

pub struct NativeIndexActivityScopeForTest {
    previous: Option<Arc<dyn NativeIndexActivity>>,
    _thread: PhantomData<Rc<()>>,
}
impl Drop for NativeIndexActivityScopeForTest {
    fn drop(&mut self) {
        let displaced = ACTIVITY.with(|slot| slot.replace(self.previous.take()));
        drop(displaced);
    }
}
pub(crate) fn activity_scope(
    activity: Arc<dyn NativeIndexActivity>,
) -> NativeIndexActivityScopeForTest {
    NativeIndexActivityScopeForTest {
        previous: ACTIVITY.with(|slot| slot.replace(Some(activity))),
        _thread: PhantomData,
    }
}

pub fn with_vector_build_observer<R>(
    observer: Arc<dyn VectorBuildObserver>,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn VectorBuildObserver>>);
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

pub(crate) fn before_vector_build() {
    let activity = ACTIVITY.with(|slot| slot.borrow().clone());
    if let Some(activity) = activity {
        activity.before_build();
    }
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.before_vector_build();
    }
}

pub(crate) fn before_vector_rewrite() {
    let activity = ACTIVITY.with(|slot| slot.borrow().clone());
    if let Some(activity) = activity {
        activity.before_rewrite();
    }
}
