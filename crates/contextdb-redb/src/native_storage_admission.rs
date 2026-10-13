//! Internal owned admission for exact byte reads; no host or engine dependency.
use std::alloc::{Layout, alloc, dealloc};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct NativeStorageRefused;

/// One owned, already-paid receipt. No borrowed pointer or raw accessor exists.
#[doc(hidden)]
pub struct NativeStorageCredit {
    receipt: NonNull<()>,
    retain: unsafe fn(NonNull<()>),
    finish: unsafe fn(NonNull<()>),
}
unsafe impl Send for NativeStorageCredit {}
unsafe impl Sync for NativeStorageCredit {}
impl NativeStorageCredit {
    /// Transfer one real funded strong receipt to storage.
    ///
    /// # Safety
    /// `receipt` must denote an initialized, paid, Send + Sync owned cell.
    /// The static callbacks must operate on that exact concrete type, check
    /// strong-count overflow and free its cell before refunding its invoice.
    /// No stack/borrowed receiver, synthetic owner or uncharged fallback is legal.
    pub unsafe fn from_owned_receipt(
        receipt: NonNull<()>,
        retain: unsafe fn(NonNull<()>),
        finish: unsafe fn(NonNull<()>),
    ) -> Self {
        Self { receipt, retain, finish }
    }
}
impl Clone for NativeStorageCredit {
    fn clone(&self) -> Self {
        unsafe { (self.retain)(self.receipt) };
        Self { receipt: self.receipt, retain: self.retain, finish: self.finish }
    }
}
impl Drop for NativeStorageCredit {
    fn drop(&mut self) { unsafe { (self.finish)(self.receipt) } }
}

#[doc(hidden)]
pub struct NativeStorageAdmission {
    owner: NativeStorageCredit,
    admit: unsafe fn(NonNull<()>, Layout) -> Result<NativeStorageCredit, NativeStorageRefused>,
}
impl NativeStorageAdmission {
    /// # Safety
    /// The credit contract above also applies to this paid scope cell. `admit`
    /// must be a static callback for the SAME concrete cell, preserving the
    /// original payer and placing its original typed failure in its paid slot.
    pub unsafe fn from_owned_receipt(
        receipt: NonNull<()>,
        retain: unsafe fn(NonNull<()>),
        finish: unsafe fn(NonNull<()>),
        admit: unsafe fn(NonNull<()>, Layout) -> Result<NativeStorageCredit, NativeStorageRefused>,
    ) -> Self {
        Self { owner: unsafe { NativeStorageCredit::from_owned_receipt(receipt, retain, finish) }, admit }
    }
}
impl Clone for NativeStorageAdmission {
    fn clone(&self) -> Self { Self { owner: self.owner.clone(), admit: self.admit } }
}
thread_local! {
    static NATIVE: RefCell<Option<NativeStorageAdmission>> = const { RefCell::new(None) };
}
struct Restore {
    previous: Option<NativeStorageAdmission>,
    _thread: PhantomData<*mut ()>,
}
impl Drop for Restore {
    fn drop(&mut self) {
        let displaced = NATIVE.with(|slot| slot.replace(self.previous.take()));
        drop(displaced);
    }
}
#[doc(hidden)]
pub fn with_native_storage_admission<R>(
    admission: NativeStorageAdmission,
    operation: impl FnOnce() -> R,
) -> R {
    let previous = NATIVE.with(|slot| slot.replace(Some(admission)));
    let _restore = Restore { previous, _thread: PhantomData };
    operation()
}
pub(crate) fn admit_native_storage(layout: Layout) -> Result<NativeStorageCredit, NativeStorageRefused> {
    let owner = NATIVE.with(|slot| slot.borrow().clone()).ok_or(NativeStorageRefused)?;
    unsafe { (owner.admit)(owner.owner.receipt, layout) }
}
pub(crate) fn native_storage_active() -> bool {
    NATIVE.with(|slot| slot.borrow().is_some())
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct NativeColdFault {
    pub reason: &'static str,
    pub page: [u8; 8],
    pub offset: usize,
    pub expected: u128,
    pub actual: u128,
}
impl NativeColdFault {
    pub(crate) fn new(reason: &'static str) -> Self {
        Self { reason, page: [0; 8], offset: 0, expected: 0, actual: 0 }
    }
}
#[doc(hidden)]
#[derive(Debug)]
pub enum NativeColdReadError {
    AdmissionRefused,
    TableAbsent,
    TypeMismatch,
    Corrupt(NativeColdFault),
    Io(std::io::Error),
}
impl From<NativeStorageRefused> for NativeColdReadError {
    fn from(_: NativeStorageRefused) -> Self { Self::AdmissionRefused }
}

/// Exact fresh backing; its invoice is outside Vec's physical destruction.
pub(crate) struct NativeStorageBuffer<T> {
    data: ManuallyDrop<Vec<T>>,
    invoice: Mutex<Option<NativeStorageCredit>>,
}
impl<T> NativeStorageBuffer<T> {
    pub(crate) fn new() -> Self {
        Self { data: ManuallyDrop::new(Vec::new()), invoice: Mutex::new(None) }
    }
    pub(crate) fn with_capacity(capacity: usize) -> Result<Self, NativeColdReadError> {
        let layout = Layout::array::<T>(capacity).map_err(|_| {
            NativeColdReadError::Corrupt(NativeColdFault::new("native buffer layout overflow"))
        })?;
        if layout.size() == 0 { return Ok(Self::new()); }
        let invoice = admit_native_storage(layout)?;
        let pointer = unsafe { alloc(layout) }.cast::<T>();
        let Some(pointer) = NonNull::new(pointer) else {
            // Inline ErrorKind keeps the lower allocator failure unqualified,
            // with no diagnostic allocation or fabricated budget refusal.
            return Err(NativeColdReadError::Io(std::io::ErrorKind::OutOfMemory.into()));
        };
        Ok(Self {
            data: ManuallyDrop::new(unsafe { Vec::from_raw_parts(pointer.as_ptr(), 0, capacity) }),
            invoice: Mutex::new(Some(invoice)),
        })
    }
    pub(crate) fn push(&mut self, item: T) -> Result<(), NativeColdReadError> {
        if self.data.len() == self.data.capacity() {
            let capacity = self.data.capacity().checked_mul(2).filter(|n| *n != 0)
                .or_else(|| self.data.is_empty().then_some(1))
                .ok_or_else(|| NativeColdReadError::Corrupt(NativeColdFault::new("native buffer capacity overflow")))?;
            let mut next = Self::with_capacity(capacity)?;
            // No item callback or allocator is invoked while moving initialized items.
            unsafe {
                std::ptr::copy_nonoverlapping(self.data.as_ptr(), next.data.as_mut_ptr(), self.data.len());
                next.data.set_len(self.data.len());
                self.data.set_len(0);
            }
            let old = std::mem::replace(self, next);
            drop(old); // old backing free precedes its independent receipt refund
        }
        self.data.push(item);
        Ok(())
    }
    pub(crate) fn pop(&mut self) -> Option<T> { self.data.pop() }
    pub(crate) fn as_slice(&self) -> &[T] { &self.data }
}
impl NativeStorageBuffer<u8> {
    pub(crate) fn zeroed(len: usize) -> Result<Self, NativeColdReadError> {
        let mut buffer = Self::with_capacity(len)?;
        if len != 0 {
            unsafe { std::ptr::write_bytes(buffer.data.as_mut_ptr(), 0, len); buffer.data.set_len(len); }
        }
        Ok(buffer)
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] { &mut self.data }
}
impl<T> Drop for NativeStorageBuffer<T> {
    fn drop(&mut self) {
        let invoice = self.invoice.get_mut().unwrap_or_else(|e| e.into_inner()).take();
        // Vec's own drop glue releases its allocation even if an element panics.
        // The external local is unwound only after that glue has finished.
        unsafe { ManuallyDrop::drop(&mut self.data) };
        drop(invoice);
    }
}

pub(crate) struct NativeStorageSharedBlock<T> {
    owners: AtomicUsize,
    payload: ManuallyDrop<T>,
    ownership: Mutex<Option<NativeStorageCredit>>,
}
pub(crate) struct NativeStorageShared<T> { block: NonNull<NativeStorageSharedBlock<T>> }
unsafe impl<T: Send + Sync> Send for NativeStorageShared<T> {}
unsafe impl<T: Send + Sync> Sync for NativeStorageShared<T> {}
impl<T> NativeStorageShared<T> {
    pub(crate) fn layout() -> Layout { Layout::new::<NativeStorageSharedBlock<T>>() }
    pub(crate) fn new(payload: T) -> Result<Self, NativeColdReadError> {
        let layout = Self::layout();
        let invoice = admit_native_storage(layout)?;
        let pointer = unsafe { alloc(layout) }.cast::<NativeStorageSharedBlock<T>>();
        let Some(block) = NonNull::new(pointer) else {
            // Payload can hold independently paid page bytes. Destroy it
            // BEFORE returning this control invoice, including panic unwind.
            drop(payload);
            drop(invoice);
            return Err(NativeColdReadError::Io(std::io::ErrorKind::OutOfMemory.into()));
        };
        unsafe { block.as_ptr().write(NativeStorageSharedBlock {
            owners: AtomicUsize::new(1), payload: ManuallyDrop::new(payload),
            ownership: Mutex::new(Some(invoice)),
        }); }
        Ok(Self { block })
    }
    pub(crate) fn get(&self) -> &T { unsafe { &self.block.as_ref().payload } }
    pub(crate) fn get_mut(&mut self) -> Option<&mut T> {
        if unsafe { self.block.as_ref() }.owners.load(Ordering::Acquire) != 1 { return None; }
        Some(unsafe { &mut self.block.as_mut().payload })
    }
}
impl<T> Clone for NativeStorageShared<T> {
    fn clone(&self) -> Self {
        unsafe { self.block.as_ref() }.owners.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
            |n| n.checked_add(1).filter(|n| *n <= isize::MAX as usize)).expect("native strong count overflow");
        Self { block: self.block }
    }
}
struct SharedFree<T> { block: NonNull<NativeStorageSharedBlock<T>>, invoice: Option<NativeStorageCredit> }
impl<T> Drop for SharedFree<T> {
    fn drop(&mut self) {
        unsafe { dealloc(self.block.as_ptr().cast(), NativeStorageShared::<T>::layout()) };
        drop(self.invoice.take());
    }
}
impl<T> Drop for NativeStorageShared<T> {
    fn drop(&mut self) {
        if unsafe { self.block.as_ref() }.owners.fetch_sub(1, Ordering::Release) != 1 { return; }
        fence(Ordering::Acquire);
        let block = unsafe { self.block.as_mut() };
        let invoice = block.ownership.get_mut().unwrap_or_else(|e| e.into_inner()).take()
            .expect("native header keeps its genuine original invoice");
        let _free = SharedFree { block: self.block, invoice: Some(invoice) };
        unsafe { ManuallyDrop::drop(&mut block.payload) };
    }
}
pub(crate) struct NativePagePayload {
    pub(crate) bytes: NativeStorageBuffer<u8>,
    pub(crate) checksum: Mutex<Option<u128>>,
}
