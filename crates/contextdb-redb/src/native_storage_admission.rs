//! Internal owned admission for exact byte reads; no host or engine dependency.
#![cfg_attr(not(feature = "contextdb-internal"), allow(dead_code))]
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
        Self {
            receipt,
            retain,
            finish,
        }
    }
}
impl Clone for NativeStorageCredit {
    fn clone(&self) -> Self {
        unsafe { (self.retain)(self.receipt) };
        Self {
            receipt: self.receipt,
            retain: self.retain,
            finish: self.finish,
        }
    }
}
impl Drop for NativeStorageCredit {
    fn drop(&mut self) {
        unsafe { (self.finish)(self.receipt) }
    }
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
        Self {
            owner: unsafe { NativeStorageCredit::from_owned_receipt(receipt, retain, finish) },
            admit,
        }
    }
}
impl Clone for NativeStorageAdmission {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            admit: self.admit,
        }
    }
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
    let _restore = Restore {
        previous,
        _thread: PhantomData,
    };
    operation()
}
pub(crate) fn admit_native_storage(
    layout: Layout,
) -> Result<NativeStorageCredit, NativeStorageRefused> {
    let owner = NATIVE
        .with(|slot| slot.borrow().clone())
        .ok_or(NativeStorageRefused)?;
    unsafe { (owner.admit)(owner.owner.receipt, layout) }
}
#[doc(hidden)]
pub fn native_storage_active() -> bool {
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
        Self {
            reason,
            page: [0; 8],
            offset: 0,
            expected: 0,
            actual: 0,
        }
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
    fn from(_: NativeStorageRefused) -> Self {
        Self::AdmissionRefused
    }
}

/// Exact fresh backing; its invoice is outside Vec's physical destruction.
pub(crate) struct NativeStorageBuffer<T> {
    data: ManuallyDrop<Vec<T>>,
    invoice: Mutex<Option<NativeStorageCredit>>,
}
impl<T> NativeStorageBuffer<T> {
    pub(crate) fn new() -> Self {
        Self {
            data: ManuallyDrop::new(Vec::new()),
            invoice: Mutex::new(None),
        }
    }
    pub(crate) fn with_capacity(capacity: usize) -> Result<Self, NativeColdReadError> {
        let layout = Layout::array::<T>(capacity).map_err(|_| {
            NativeColdReadError::Corrupt(NativeColdFault::new("native buffer layout overflow"))
        })?;
        if layout.size() == 0 {
            return Ok(Self::new());
        }
        let invoice = admit_native_storage(layout)?;
        let pointer = unsafe { alloc(layout) }.cast::<T>();
        let Some(pointer) = NonNull::new(pointer) else {
            // Inline ErrorKind keeps the lower allocator failure unqualified,
            // with no diagnostic allocation or fabricated budget refusal.
            return Err(NativeColdReadError::Io(
                std::io::ErrorKind::OutOfMemory.into(),
            ));
        };
        Ok(Self {
            data: ManuallyDrop::new(unsafe { Vec::from_raw_parts(pointer.as_ptr(), 0, capacity) }),
            invoice: Mutex::new(Some(invoice)),
        })
    }
    pub(crate) fn push(&mut self, item: T) -> Result<(), NativeColdReadError> {
        if self.data.len() == self.data.capacity() {
            let capacity = self
                .data
                .capacity()
                .checked_mul(2)
                .filter(|n| *n != 0)
                .or_else(|| self.data.is_empty().then_some(1))
                .ok_or_else(|| {
                    NativeColdReadError::Corrupt(NativeColdFault::new(
                        "native buffer capacity overflow",
                    ))
                })?;
            let mut next = Self::with_capacity(capacity)?;
            // No item callback or allocator is invoked while moving initialized items.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.data.as_ptr(),
                    next.data.as_mut_ptr(),
                    self.data.len(),
                );
                next.data.set_len(self.data.len());
                self.data.set_len(0);
            }
            let old = std::mem::replace(self, next);
            drop(old); // old backing free precedes its independent receipt refund
        }
        self.data.push(item);
        Ok(())
    }
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.data.pop()
    }
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.data
    }
}
impl NativeStorageBuffer<u8> {
    pub(crate) fn zeroed(len: usize) -> Result<Self, NativeColdReadError> {
        let mut buffer = Self::with_capacity(len)?;
        if len != 0 {
            unsafe {
                std::ptr::write_bytes(buffer.data.as_mut_ptr(), 0, len);
                buffer.data.set_len(len);
            }
        }
        Ok(buffer)
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
impl<T> Drop for NativeStorageBuffer<T> {
    fn drop(&mut self) {
        let invoice = self
            .invoice
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
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
pub(crate) struct NativeStorageShared<T> {
    block: NonNull<NativeStorageSharedBlock<T>>,
}
unsafe impl<T: Send + Sync> Send for NativeStorageShared<T> {}
unsafe impl<T: Send + Sync> Sync for NativeStorageShared<T> {}
impl<T> NativeStorageShared<T> {
    pub(crate) fn layout() -> Layout {
        Layout::new::<NativeStorageSharedBlock<T>>()
    }
    pub(crate) fn new(payload: T) -> Result<Self, NativeColdReadError> {
        let layout = Self::layout();
        let invoice = admit_native_storage(layout)?;
        let pointer = unsafe { alloc(layout) }.cast::<NativeStorageSharedBlock<T>>();
        let Some(block) = NonNull::new(pointer) else {
            // Payload can hold independently paid page bytes. Destroy it
            // BEFORE returning this control invoice, including panic unwind.
            drop(payload);
            drop(invoice);
            return Err(NativeColdReadError::Io(
                std::io::ErrorKind::OutOfMemory.into(),
            ));
        };
        unsafe {
            block.as_ptr().write(NativeStorageSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                ownership: Mutex::new(Some(invoice)),
            });
        }
        Ok(Self { block })
    }
    pub(crate) fn get(&self) -> &T {
        unsafe { &self.block.as_ref().payload }
    }
    pub(crate) fn get_mut(&mut self) -> Option<&mut T> {
        if unsafe { self.block.as_ref() }
            .owners
            .load(Ordering::Acquire)
            != 1
        {
            return None;
        }
        Some(unsafe { &mut self.block.as_mut().payload })
    }
}
impl<T> Clone for NativeStorageShared<T> {
    fn clone(&self) -> Self {
        unsafe { self.block.as_ref() }
            .owners
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(1).filter(|n| isize::try_from(*n).is_ok())
            })
            .expect("native strong count overflow");
        Self { block: self.block }
    }
}
struct SharedFree<T> {
    block: NonNull<NativeStorageSharedBlock<T>>,
    invoice: Option<NativeStorageCredit>,
}
impl<T> Drop for SharedFree<T> {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.block.as_ptr().cast(),
                NativeStorageShared::<T>::layout(),
            );
        };
        drop(self.invoice.take());
    }
}
impl<T> Drop for NativeStorageShared<T> {
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
        let invoice = block
            .ownership
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("native header keeps its genuine original invoice");
        let _free = SharedFree {
            block: self.block,
            invoice: Some(invoice),
        };
        unsafe { ManuallyDrop::drop(&mut block.payload) };
    }
}
pub(crate) struct NativePagePayload {
    pub(crate) bytes: NativeStorageBuffer<u8>,
    pub(crate) checksum: Mutex<Option<u128>>,
}

// One physical hash-table allocation, including its original invoice header.
// This allocator never consults current admission at destruction.
struct ReaderRegistryHeader {
    invoice: Option<NativeStorageCredit>,
}
#[derive(Clone)]
pub(crate) struct ReaderRegistryAllocator {
    pub(crate) qualified: bool,
    refused: std::cell::Cell<bool>,
}
impl ReaderRegistryAllocator {
    pub(crate) fn new(qualified: bool) -> Self {
        Self {
            qualified,
            refused: std::cell::Cell::new(false),
        }
    }
    pub(crate) fn error(&self) -> crate::StorageError {
        if self.refused.replace(false) {
            native_storage_error(NativeColdReadError::AdmissionRefused)
        } else {
            crate::StorageError::Io(std::io::ErrorKind::OutOfMemory.into())
        }
    }
    fn combined(layout: Layout) -> (Layout, usize) {
        let (whole, offset) = Layout::new::<ReaderRegistryHeader>()
            .extend(layout)
            .expect("hash table and receipt header layout");
        (whole.pad_to_align(), offset)
    }
}
unsafe impl allocator_api2::alloc::Allocator for ReaderRegistryAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, allocator_api2::alloc::AllocError> {
        use allocator_api2::alloc::AllocError;
        let (whole, offset) = Self::combined(layout);
        let invoice = if self.qualified {
            Some(admit_native_storage(whole).map_err(|_| {
                self.refused.set(true);
                AllocError
            })?)
        } else {
            None
        };
        let pointer = NonNull::new(unsafe { alloc(whole) }).ok_or(AllocError)?;
        unsafe {
            pointer
                .cast::<ReaderRegistryHeader>()
                .as_ptr()
                .write(ReaderRegistryHeader { invoice });
        }
        Ok(NonNull::slice_from_raw_parts(
            unsafe { NonNull::new_unchecked(pointer.as_ptr().add(offset)) },
            layout.size(),
        ))
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        let (whole, offset) = Self::combined(layout);
        let start = unsafe { pointer.as_ptr().sub(offset) };
        let header = unsafe { start.cast::<ReaderRegistryHeader>().read() };
        unsafe {
            dealloc(start, whole);
        }
        drop(header.invoice);
    }
}

pub(crate) fn native_storage_error(error: NativeColdReadError) -> crate::StorageError {
    match error {
        NativeColdReadError::Io(error) => crate::StorageError::Io(error),
        NativeColdReadError::AdmissionRefused => {
            crate::StorageError::Io(std::io::ErrorKind::OutOfMemory.into())
        }
        NativeColdReadError::TableAbsent => {
            crate::StorageError::Corrupted("required native table is absent".into())
        }
        NativeColdReadError::TypeMismatch => {
            crate::StorageError::Corrupted("native table type mismatch".into())
        }
        NativeColdReadError::Corrupt(fault) => crate::StorageError::Corrupted(fault.reason.into()),
    }
}
impl NativeStorageAdmission {
    // Private capability retention for native views, not a snapshot/test accessor.
    pub(crate) fn retain_current() -> Result<Self, crate::StorageError> {
        NATIVE
            .with(|slot| slot.borrow().clone())
            .ok_or_else(|| native_storage_error(NativeColdReadError::AdmissionRefused))
    }
}

// Pending directory backing uses the existing owned native receipt protocol.
// The receipt is stored in the same allocation: no unfunded side registry.
thread_local! { static PENDING_REFUSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[derive(Clone, Default)]
pub(crate) struct PendingAlloc(Option<NativeStorageAdmission>);
impl PendingAlloc {
    pub(crate) fn current() -> Self {
        Self(NATIVE.with(|slot| slot.borrow().clone()))
    }
    fn layout(layout: Layout) -> (Layout, usize) {
        let (full, offset) = Layout::new::<Option<NativeStorageCredit>>()
            .extend(layout)
            .expect("pending allocation layout");
        (full.pad_to_align(), offset)
    }
}
unsafe impl allocator_api2::alloc::Allocator for PendingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, allocator_api2::alloc::AllocError> {
        PENDING_REFUSED.with(|flag| flag.set(false));
        let (full, offset) = Self::layout(layout);
        let invoice = match &self.0 {
            Some(owner) => Some(unsafe { (owner.admit)(owner.owner.receipt, full) }.map_err(
                |_| {
                    PENDING_REFUSED.with(|flag| flag.set(true));
                    allocator_api2::alloc::AllocError
                },
            )?),
            None => None, // Ordinary standalone redb has no embedding memory budget.
        };
        let raw = NonNull::new(unsafe { alloc(full) }).ok_or(allocator_api2::alloc::AllocError)?;
        unsafe {
            raw.cast::<Option<NativeStorageCredit>>()
                .as_ptr()
                .write(invoice);
        }
        Ok(NonNull::slice_from_raw_parts(
            unsafe { NonNull::new_unchecked(raw.as_ptr().add(offset)) },
            layout.size(),
        ))
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        let (full, offset) = Self::layout(layout);
        let raw = unsafe { pointer.as_ptr().sub(offset) };
        let invoice = unsafe { raw.cast::<Option<NativeStorageCredit>>().read() };
        unsafe {
            dealloc(raw, full);
        }
        drop(invoice);
    }
}

pub(crate) struct PendingName(allocator_api2::boxed::Box<[u8], PendingAlloc>);
impl PendingName {
    pub(crate) fn try_new(name: &str, alloc: PendingAlloc) -> Result<Self, crate::StorageError> {
        let mut bytes = allocator_api2::vec::Vec::new_in(alloc);
        bytes
            .try_reserve_exact(name.len())
            .map_err(|_| pending_allocation_error())?;
        bytes.extend_from_slice(name.as_bytes());
        Ok(Self(bytes.into_boxed_slice()))
    }
    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("table name UTF-8")
    }
}
impl std::ops::Deref for PendingName {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl std::borrow::Borrow<str> for PendingName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl std::hash::Hash for PendingName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
impl PartialEq for PendingName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for PendingName {}
pub(crate) fn pending_allocation_error() -> crate::StorageError {
    if PENDING_REFUSED.with(|flag| flag.replace(false)) {
        #[derive(Debug)]
        struct Refused;
        impl std::fmt::Display for Refused {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("pending directory admission refused")
            }
        }
        impl std::error::Error for Refused {}
        crate::StorageError::AdmissionRefused(Box::new(Refused))
    } else {
        crate::StorageError::Io(std::io::ErrorKind::OutOfMemory.into())
    }
}

/// Preparation retains the actual lower producer error. An admission marker
/// must be recovered from its producing scope, never formatted as replacement
/// I/O or corruption. This type is private to the internal feature protocol.
#[doc(hidden)]
#[derive(Debug)]
pub enum PreparedReadError {
    /// Same existing static corruption diagnosis, before String materialization.
    StaticCorrupted(&'static str),
    Storage(crate::StorageError),
    Native(NativeColdReadError),
    TableDefinition(PreparedTableDefinitionError),
}

/// Keep malformed/mismatched descriptor bytes in their original paid page.
/// A later formatter can inspect the actual stored type names without having
/// lost them to a scalar TypeMismatch marker or an allocating owned decoder.
#[doc(hidden)]
pub struct PreparedTableDefinitionError {
    pub cause: NativeColdReadError,
    descriptor: crate::AccessGuard<'static, &'static [u8]>,
}
impl PreparedTableDefinitionError {
    pub(crate) fn new(cause: NativeColdReadError, descriptor: crate::AccessGuard<'static, &'static [u8]>) -> Self {
        Self { cause, descriptor }
    }
    pub fn descriptor(&self) -> &[u8] { self.descriptor.value() }
}
impl std::fmt::Debug for PreparedTableDefinitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedTableDefinitionError")
            .field("cause", &self.cause)
            .field("descriptor", &self.descriptor.value()).finish()
    }
}
impl From<crate::StorageError> for PreparedReadError {
    fn from(error: crate::StorageError) -> Self { Self::Storage(error) }
}
impl From<NativeColdReadError> for PreparedReadError {
    fn from(error: NativeColdReadError) -> Self { Self::Native(error) }
}
