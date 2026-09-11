//! Operation-scoped admission before allocating a source page. The caller
//! keeps admitted bytes owned until its read handles have been released.
use std::{cell::RefCell, io, sync::Arc};
use std::{marker::PhantomData, rc::Rc};
type Admission = Arc<dyn Fn(usize) -> io::Result<()> + Send + Sync>;
thread_local! { static ACTIVE: RefCell<Vec<Admission>> = const { RefCell::new(Vec::new()) }; }

/// Execute synchronous source reads with admission before each cache-miss
/// allocation. Nested operations restore the enclosing admission on unwind.
#[doc(hidden)]
pub fn with_read_memory_admission<R>(admit: Admission, operation: impl FnOnce() -> R) -> R {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| {
                active.borrow_mut().pop();
            });
        }
    }
    ACTIVE.with(|active| active.borrow_mut().push(admit));
    let _reset = Reset;
    operation()
}

pub(crate) fn admit_read_memory(bytes: usize) -> io::Result<()> {
    ACTIVE.with(|active| match active.borrow().last() {
        Some(admit) => admit(bytes),
        None => Ok(()),
    })
}

type ReadObserver = Arc<dyn Fn(usize) + Send + Sync>;
thread_local! { static READ_OBSERVER: RefCell<Option<ReadObserver>> = const { RefCell::new(None) }; }

/// Observe bytes returned by successful backend reads on this thread. Cache
/// hits contribute zero. This measures storage API I/O, not physical device
/// traffic: the operating system may satisfy a file read from its page cache.
#[doc(hidden)]
pub fn observe_backend_reads(observer: ReadObserver) -> BackendReadObservation {
    BackendReadObservation {
        previous: READ_OBSERVER.with(|slot| slot.replace(Some(observer))),
        _thread: PhantomData,
    }
}

#[doc(hidden)]
pub struct BackendReadObservation {
    previous: Option<ReadObserver>,
    _thread: PhantomData<Rc<()>>,
}

impl Drop for BackendReadObservation {
    fn drop(&mut self) {
        READ_OBSERVER.with(|slot| slot.replace(self.previous.take()));
    }
}

pub(crate) fn record_backend_read(bytes: usize) {
    let observer = READ_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(bytes);
    }
}
