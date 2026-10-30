//! Thread-scoped observation of the real allocator, present only in test builds.
//!
//! Callers supply attribution by limiting the scope to the operation they observe.
//! No engine class, ceiling, or allocation policy is retained in this module.
use std::cell::RefCell;
use std::sync::Arc;

/// The allocator's actual minimum retry layout before an allocation executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocationPlanForTest {
    pub requested_bytes: usize,
    pub current_store_bytes: u64,
    pub minimum_growth_bytes: u64,
    pub reusable_space_withheld: bool,
}

/// Callbacks run without the allocator state lock. They may inspect the allocator
/// and change an external backend ceiling, but must not start another write
/// transaction while the observed write transaction is active.
pub trait AllocationObserverForTest: Send + Sync {
    fn before_allocate(&self, plan: AllocationPlanForTest) -> crate::Result;
    fn allocated(&self, plan: AllocationPlanForTest, page: [u8; 8]);
    fn before_existing_write(&self, _plan: AllocationPlanForTest) -> crate::Result { Ok(()) }
    fn before_write(&self, _plan: AllocationPlanForTest, _page: [u8;8], _bytes:usize) -> crate::Result { Ok(()) }
    fn written(&self, _plan: AllocationPlanForTest, _page: [u8;8], _bytes:usize) {}
    fn refused(&self, _plan: AllocationPlanForTest, _error:&crate::StorageError) {}
}

#[derive(Clone)]
enum Observer {
    Owned(Arc<dyn AllocationObserverForTest>),
    Borrowed(&'static dyn AllocationObserverForTest),
}
#[derive(Clone)]
pub(crate) struct Scope {
    observer: Observer,
    pub withhold_reusable_space: bool,
}
impl Scope {
    pub(crate) fn observer(&self)->&dyn AllocationObserverForTest {
        match &self.observer {Observer::Owned(observer)=>observer.as_ref(),Observer::Borrowed(observer)=>*observer}
    }
}
struct Restore(Option<Scope>);
impl Drop for Restore {
    fn drop(&mut self) { OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take()); }
}

thread_local! {
    static OBSERVER: RefCell<Option<Scope>> = const { RefCell::new(None) };
}

/// Observe allocations made by `operation`, restoring the previous scope on
/// return or unwind. Withholding excludes every preexisting free block for each
/// allocation in this scope; other operations retain ordinary reuse.
pub fn observe_allocations_for_test<T>(
    observer: Arc<dyn AllocationObserverForTest>,
    withhold_reusable_space: bool,
    operation: impl FnOnce() -> T,
) -> T {
    let _restore = Restore(OBSERVER.with(|slot| {
        slot.replace(Some(Scope {
            observer:Observer::Owned(observer),
            withhold_reusable_space,
        }))
    }));
    operation()
}

pub(crate) fn current() -> Option<Scope> {
    OBSERVER.with(|slot| slot.borrow().clone())
}


/// Borrow a calling-thread observer without allocating a control for each
/// operation. Callbacks are synchronous; no page or returned value retains it.
/// The previous observer is restored on return and unwind.
pub fn with_borrowed_allocation_observer_for_test<T>(
    observer:&dyn AllocationObserverForTest,
    withhold_reusable_space:bool,
    operation:impl FnOnce()->T,
)->T {
    // SAFETY: only synchronous page-manager callbacks use the private slot.
    // They never store its Scope in pages or returned objects or send it to a
    // different thread. Restore removes this reference before the borrow ends,
    // including on unwind; nested scopes restore their still-live parent.
    let observer=unsafe {std::mem::transmute::<&dyn AllocationObserverForTest,&'static dyn AllocationObserverForTest>(observer)};
    let _restore=Restore(OBSERVER.with(|slot|slot.replace(Some(Scope{observer:Observer::Borrowed(observer),withhold_reusable_space}))));
    operation()
}
