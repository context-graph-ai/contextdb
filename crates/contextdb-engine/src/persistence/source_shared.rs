//! Private movable source owners. No Weak or independently escaping control.
use crate::memory_accounting::ControlCredit;
use contextdb_core::{Error, Result};
use std::alloc::{Layout, alloc, dealloc};
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

struct Block<T> {
    owners: AtomicUsize,
    value: ManuallyDrop<T>,
    control: Option<ControlCredit>,
}
pub(crate) struct SourceShared<T> {
    block: NonNull<Block<T>>,
}
// SAFETY: only the checked atomic count mutates while shared. Last release
// exclusively destroys the unpinned payload; no Weak, raw, or mutable alias escapes.
unsafe impl<T: Send + Sync> Send for SourceShared<T> {}
unsafe impl<T: Send + Sync> Sync for SourceShared<T> {}
impl<T> SourceShared<T> {
    pub(super) fn layout() -> Layout {
        Layout::new::<Block<T>>()
    }
    /// The original control invoice covers this exact Layout before entry.
    /// Payload data funding follows its data; control custody is independent.
    pub(super) fn try_new_prepaid(value: T, control: ControlCredit) -> Result<Self> {
        let layout = Self::layout();
        #[cfg(test)]
        let shared_observation = super::direct_source_contract_rows::begin_shared(&control);
        let Some(block) = NonNull::new(unsafe { alloc(layout) }.cast::<Block<T>>()) else {
            // No backing exists. Payload cleanup precedes its control refund.
            drop(value);
            drop(control);
            return Err(Error::NativeEngineOther(
                contextdb_core::read_memory::NativeRefusal::AllocatorFailed {
                    site: "direct source shared control",
                    layout,
                },
            ));
        };
        #[cfg(test)]
        drop(shared_observation);
        unsafe {
            block.as_ptr().write(Block {
                owners: AtomicUsize::new(1),
                value: ManuallyDrop::new(value),
                control: Some(control),
            })
        };
        Ok(Self { block })
    }
    pub(crate) fn identity(&self) -> usize {
        self.block.as_ptr() as usize
    }
    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        left.block == right.block
    }
}
impl<T: Send + Sync + 'static> SourceShared<T> {
    /// Move a strong owner through the existing native receipt protocol.
    /// The same already-paid control is retained; this allocates nothing.
    pub(super) fn into_native_credit(self) -> redb::NativeStorageCredit {
        unsafe fn retain<T>(raw: NonNull<()>) {
            unsafe { raw.cast::<Block<T>>().as_ref() }
                .owners
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    n.checked_add(1).filter(|n| *n <= isize::MAX as usize)
                })
                .expect("direct source strong count overflow");
        }
        unsafe fn finish<T>(raw: NonNull<()>) {
            drop(SourceShared {
                block: raw.cast::<Block<T>>(),
            });
        }
        let raw = self.block.cast();
        std::mem::forget(self);
        // SAFETY: owned concrete Send + Sync source cell with checked strong
        // count and free-before-refund; no borrowed callback or self-cycle.
        unsafe { redb::NativeStorageCredit::from_owned_receipt(raw, retain::<T>, finish::<T>) }
    }
}
impl<T> Deref for SourceShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &unsafe { self.block.as_ref() }.value
    }
}
impl<T> Clone for SourceShared<T> {
    fn clone(&self) -> Self {
        unsafe { self.block.as_ref() }
            .owners
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(1).filter(|n| *n <= isize::MAX as usize)
            })
            .expect("direct source strong count overflow");
        Self { block: self.block }
    }
}
struct Free<T> {
    block: NonNull<Block<T>>,
    _control: ControlCredit,
}
impl<T> Drop for Free<T> {
    fn drop(&mut self) {
        unsafe { dealloc(self.block.as_ptr().cast(), SourceShared::<T>::layout()) };
        // The original invoice is a field of this stack guard, dropped only
        // after its destructor has physically freed the complete shared block.
    }
}
impl<T> Drop for SourceShared<T> {
    fn drop(&mut self) {
        if unsafe { self.block.as_ref() }
            .owners
            .fetch_sub(1, Ordering::Release)
            != 1
        {
            return;
        }
        fence(Ordering::Acquire);
        let block = unsafe { self.block.as_mut() };
        let _free = Free {
            block: self.block,
            _control: block.control.take().expect("live source control invoice"),
        };
        // The guard also runs if payload cleanup unwinds. Data destructors
        // finish before shared backing free, which precedes control refund.
        unsafe { ManuallyDrop::drop(&mut block.value) };
    }
}
