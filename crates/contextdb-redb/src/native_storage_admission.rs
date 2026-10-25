//! Internal owned admission for exact byte reads; no host or engine dependency.
#![cfg_attr(not(feature = "contextdb-internal"), allow(dead_code))]
use crate::atomic_compat::AtomicUpdate as _;
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
// A page has a single original invoice owner; its own strong page handles
// provide sharing. Retain is therefore unnecessary in this moved receipt.
#[cfg(feature = "contextdb-internal")]
pub(crate) struct NativePageCharge {
    receipt: NonNull<()>,
    finish: unsafe fn(NonNull<()>),
}
#[cfg(feature = "contextdb-internal")]
unsafe impl Send for NativePageCharge {}
#[cfg(feature = "contextdb-internal")]
unsafe impl Sync for NativePageCharge {}
#[cfg(feature = "contextdb-internal")]
impl Drop for NativePageCharge {
    fn drop(&mut self) {
        unsafe { (self.finish)(self.receipt) };
    }
}
impl NativeStorageCredit {
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn into_page_charge(self) -> NativePageCharge {
        let credit = ManuallyDrop::new(self);
        NativePageCharge {
            receipt: credit.receipt,
            finish: credit.finish,
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn identity_for_test(&self) -> usize {
        self.receipt.as_ptr() as usize
    }

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
    inspect_failure: Option<NativeFailureInspector>,
    #[cfg(feature = "test-seams")]
    allocation_funding: Option<NativeAllocationFundingInspector>,
}
#[cfg(feature = "test-seams")]
type NativeAllocationFundingInspector = unsafe fn(NonNull<()>) -> Option<(usize, usize)>;
type NativeFailureInspector = unsafe fn(
    NonNull<()>,
    &mut dyn FnMut(Option<&(dyn std::error::Error + Send + Sync + 'static)>),
);
impl std::fmt::Debug for NativeStorageAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeStorageAdmission")
            .finish_non_exhaustive()
    }
}
impl NativeStorageAdmission {
    /// Observe the actual payer and receipt allocation overhead, without admission.
    /// # Safety
    /// The callback must borrow this same concrete scope and report only the
    /// payer and receipt layout used by its successful allocation callback.
    #[cfg(feature = "test-seams")]
    #[must_use]
    pub unsafe fn with_allocation_funding_inspector(
        mut self,
        inspect: NativeAllocationFundingInspector,
    ) -> Self {
        self.allocation_funding = Some(inspect);
        self
    }

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
            inspect_failure: None,
            #[cfg(feature = "test-seams")]
            allocation_funding: None,
        }
    }

    /// # Safety
    /// The callback borrows only this SAME concrete owned scope's failure and
    /// invokes its visitor exactly once, while the diagnosis is protected. It
    /// may neither retain the visitor/reference nor allocate or move a primary
    /// into a scope retained by that primary. The receipt contract still holds.
    #[must_use]
    pub unsafe fn with_failure_inspector(mut self, inspect: NativeFailureInspector) -> Self {
        self.inspect_failure = Some(inspect);
        self
    }
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn reserve_cache_preparation(
        &self,
        layout: Layout,
    ) -> Result<(), crate::StorageError> {
        unsafe { (self.admit)(self.owner.receipt, layout) }
            .map(drop)
            .map_err(|_| {
                crate::StorageError::Native(NativeColdReadError::AdmissionRefusedAt(self.clone()))
            })
    }

    pub(crate) fn cache_preparation_credit(&self) -> NativeStorageCredit {
        self.owner.clone()
    }

    /// Read-only identity comparison; no raw owner address escapes.
    pub fn same_source(&self, other: &Self) -> bool {
        self.owner.receipt == other.owner.receipt
    }
    /// Lend the producing scope's actual typed cause without copying its text,
    /// transferring its ownership, or recovering an unrelated current scope.
    pub fn inspect_failure<R>(
        &self,
        inspect: impl FnOnce(Option<&(dyn std::error::Error + Send + Sync + 'static)>) -> R,
    ) -> R {
        let Some(callback) = self.inspect_failure else {
            return inspect(None);
        };
        let mut inspect = Some(inspect);
        let mut result = None;
        unsafe {
            callback(self.owner.receipt, &mut |cause| {
                result = Some(inspect.take().expect("one native failure inspection")(
                    cause,
                ));
            });
        };
        result.expect("native failure inspector invokes its visitor")
    }
}
impl Clone for NativeStorageAdmission {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            admit: self.admit,
            inspect_failure: self.inspect_failure,
            #[cfg(feature = "test-seams")]
            allocation_funding: self.allocation_funding,
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
) -> Result<NativeStorageCredit, NativeColdReadError> {
    let owner = NATIVE
        .with(|slot| slot.borrow().clone())
        .ok_or(NativeColdReadError::AdmissionRefused)?;
    unsafe { (owner.admit)(owner.owner.receipt, layout) }
        .map_err(|_| NativeColdReadError::AdmissionRefusedAt(owner))
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
/// Finite causes emitted by the original writer backend before I/O error allocation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum NativeIoCause {
    WriterOwnerClosed,
    ProjectedGrowthPoisoned,
    DiskGrowthRefused {
        current_bytes: u64,
        projected_bytes: u64,
        limit_bytes: u64,
    },
}
impl NativeIoCause {
    pub fn kind(self) -> std::io::ErrorKind {
        match self {
            Self::DiskGrowthRefused { .. } => std::io::ErrorKind::QuotaExceeded,
            Self::WriterOwnerClosed | Self::ProjectedGrowthPoisoned => std::io::ErrorKind::Other,
        }
    }
}
impl std::fmt::Display for NativeIoCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WriterOwnerClosed => f.write_str("writer file ownership is closed"),
            Self::ProjectedGrowthPoisoned => {
                f.write_str("projected file-growth reservation was poisoned")
            }
            Self::DiskGrowthRefused {
                current_bytes,
                projected_bytes,
                limit_bytes,
            } => write!(
                f,
                "projected storage growth from {current_bytes} to {projected_bytes} bytes exceeds the {limit_bytes} byte limit"
            ),
        }
    }
}
impl std::error::Error for NativeIoCause {}

#[doc(hidden)]
#[derive(Debug)]
pub enum NativeBackendError {
    Closed(NativeIoCause),
    Io(std::io::Error),
}
impl From<std::io::Error> for NativeBackendError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl NativeBackendError {
    pub(crate) fn into_native(self) -> NativeColdReadError {
        match self {
            Self::Io(error) => NativeColdReadError::Io(error),
            Self::Closed(cause) => NativeColdReadError::BackendIo(cause),
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum NativeColdReadError {
    AdmissionRefused,
    /// This retained producer can differ from the scope executing the read.
    AdmissionRefusedAt(NativeStorageAdmission),
    /// The allocator actually returned null after this exact layout was admitted.
    AllocatorFailed {
        site: &'static str,
        layout: Layout,
    },
    TableAbsent,
    TypeMismatch,
    Corrupt(NativeColdFault),
    Io(std::io::Error),
    BackendIo(NativeIoCause),
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
            return Err(NativeColdReadError::AllocatorFailed {
                site: "native storage buffer",
                layout,
            });
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
            return Err(NativeColdReadError::AllocatorFailed {
                site: "native shared control",
                layout,
            });
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
}
impl<T> Clone for NativeStorageShared<T> {
    fn clone(&self) -> Self {
        unsafe { self.block.as_ref() }
            .owners
            .fetch_update_compat(Ordering::Relaxed, Ordering::Relaxed, |n| {
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

// One physical hash-table allocation, including its original invoice header.
// This allocator never consults current admission at destruction.
struct ReaderRegistryHeader {
    invoice: Option<NativeStorageCredit>,
}
pub(crate) struct ReaderRegistryAllocator {
    pub(crate) qualified: bool,
    failure: RefCell<Option<NativeColdReadError>>,
    #[cfg(feature = "test-seams")]
    allocation: std::cell::Cell<Option<(usize, usize, usize)>>,
}
impl Clone for ReaderRegistryAllocator {
    fn clone(&self) -> Self {
        Self::new(self.qualified)
    }
}
impl ReaderRegistryAllocator {
    pub(crate) fn new(qualified: bool) -> Self {
        Self {
            qualified,
            failure: RefCell::new(None),
            #[cfg(feature = "test-seams")]
            allocation: std::cell::Cell::new(None),
        }
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn charge_for(&self, accountant: usize) -> usize {
        self.allocation
            .get()
            .filter(|(_, owner, _)| *owner == accountant)
            .map_or(0, |(_, _, bytes)| bytes)
    }
    pub(crate) fn error(&self) -> crate::StorageError {
        self.failure.borrow_mut().take().map_or_else(
            // No observed allocator call: capacity/layout failure is unqualified.
            || crate::StorageError::Io(std::io::ErrorKind::OutOfMemory.into()),
            native_storage_error,
        )
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
        self.failure.borrow_mut().take();
        let (whole, offset) = Self::combined(layout);
        let invoice = if self.qualified {
            Some(admit_native_storage(whole).map_err(|error| {
                *self.failure.borrow_mut() = Some(error);
                AllocError
            })?)
        } else {
            None
        };
        let pointer = NonNull::new(unsafe { alloc(whole) }).ok_or_else(|| {
            *self.failure.borrow_mut() = Some(NativeColdReadError::AllocatorFailed {
                site: "reader registry",
                layout: whole,
            });
            AllocError
        })?;
        #[cfg(feature = "test-seams")]
        {
            // The source that admitted this allocation also names its receipt.
            // Unknown/legacy/process sources do not become writer capacity.
            let funding = if invoice.is_some() {
                NATIVE.with(|slot| {
                    slot.borrow().as_ref().and_then(|scope| {
                        scope
                            .allocation_funding
                            .and_then(|inspect| unsafe { inspect(scope.owner.receipt) })
                    })
                })
            } else {
                None
            };
            self.allocation.set(funding.map(|(owner, overhead)| {
                (pointer.as_ptr() as usize, owner, whole.size() + overhead)
            }));
        }
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
        let start = unsafe { NonNull::new_unchecked(pointer.as_ptr().sub(offset)) };
        let header = unsafe { start.cast::<ReaderRegistryHeader>().as_ptr().read() };
        #[cfg(feature = "test-seams")]
        if self
            .allocation
            .get()
            .is_some_and(|(pointer, _, _)| pointer == start.as_ptr() as usize)
        {
            self.allocation.set(None);
        }
        unsafe {
            dealloc(start.as_ptr(), whole);
        }
        drop(header.invoice);
    }
}

pub(crate) fn native_storage_error(error: NativeColdReadError) -> crate::StorageError {
    // Internal native provenance survives intermediate legacy Storage wrappers.
    // Genuine backend I/O remains the same io::Error, including custom payloads.
    #[cfg(feature = "contextdb-internal")]
    return match error {
        NativeColdReadError::Io(error) => crate::StorageError::Io(error),
        error => crate::StorageError::Native(error),
    };
    #[cfg(not(feature = "contextdb-internal"))]
    match error {
        NativeColdReadError::Io(error) => crate::StorageError::Io(error),
        NativeColdReadError::BackendIo(cause) => {
            crate::StorageError::Io(std::io::Error::new(cause.kind(), cause))
        }
        NativeColdReadError::AdmissionRefused
        | NativeColdReadError::AdmissionRefusedAt(_)
        | NativeColdReadError::AllocatorFailed { .. } => {
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
thread_local! { static PENDING_FAILURE: RefCell<Option<NativeColdReadError>> = const { RefCell::new(None) }; }
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
        PENDING_FAILURE.with(|flag| flag.borrow_mut().take());
        let (full, offset) = Self::layout(layout);
        let invoice = match &self.0 {
            Some(owner) => Some(unsafe { (owner.admit)(owner.owner.receipt, full) }.map_err(
                |_| {
                    PENDING_FAILURE.with(|flag| {
                        *flag.borrow_mut() =
                            Some(NativeColdReadError::AdmissionRefusedAt(owner.clone()));
                    });
                    allocator_api2::alloc::AllocError
                },
            )?),
            None => None, // Ordinary standalone redb has no embedding memory budget.
        };
        let raw = NonNull::new(unsafe { alloc(full) }).ok_or_else(|| {
            PENDING_FAILURE.with(|flag| {
                *flag.borrow_mut() = Some(NativeColdReadError::AllocatorFailed {
                    site: "pending directory",
                    layout: full,
                });
            });
            allocator_api2::alloc::AllocError
        })?;
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
        let raw = unsafe { NonNull::new_unchecked(pointer.as_ptr().sub(offset)) };
        let invoice = unsafe { raw.cast::<Option<NativeStorageCredit>>().as_ptr().read() };
        unsafe {
            dealloc(raw.as_ptr(), full);
        }
        drop(invoice);
    }
}

#[cfg(feature = "test-seams")]
type PendingNameObserver = fn(&str, usize, Layout, Option<&NativeStorageCredit>);
#[cfg(feature = "test-seams")]
thread_local! {
    static PENDING_NAME_OBSERVER: std::cell::Cell<Option<PendingNameObserver>> = const { std::cell::Cell::new(None) };
}

/// Observe a real pending name and its borrowed native invoice after construction.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn observe_pending_names_for_test<T>(
    observer: PendingNameObserver,
    action: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<PendingNameObserver>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PENDING_NAME_OBSERVER.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(PENDING_NAME_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

pub(crate) struct PendingName(allocator_api2::boxed::Box<[u8], PendingAlloc>);
impl PendingName {
    pub(crate) fn try_new(name: &str, alloc: PendingAlloc) -> Result<Self, crate::StorageError> {
        let mut bytes = allocator_api2::vec::Vec::new_in(alloc);
        bytes
            .try_reserve_exact(name.len())
            .map_err(|_| pending_allocation_error())?;
        bytes.extend_from_slice(name.as_bytes());
        let name = Self(bytes.into_boxed_slice());
        #[cfg(feature = "test-seams")]
        PENDING_NAME_OBSERVER.with(|slot| {
            if let Some(observe) = slot.get() {
                let (full, offset) =
                    PendingAlloc::layout(Layout::array::<u8>(name.0.len()).unwrap());
                // The name owns this initialized combined allocation throughout the callback.
                let raw = unsafe { NonNull::new_unchecked(name.0.as_ptr().sub(offset).cast_mut()) };
                let invoice = unsafe { raw.cast::<Option<NativeStorageCredit>>().as_ref() };
                observe(name.as_str(), raw.as_ptr() as usize, full, invoice.as_ref());
            }
        });
        Ok(name)
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
    let failure = PENDING_FAILURE.with(|flag| flag.borrow_mut().take());
    failure.map_or_else(
        // No producer tag: a capacity/layout error is not an observed null allocation.
        || crate::StorageError::Io(std::io::ErrorKind::OutOfMemory.into()),
        native_storage_error,
    )
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
    requested: NativeStorageBuffer<u8>,
}
impl PreparedTableDefinitionError {
    pub(crate) fn new(
        cause: NativeColdReadError,
        descriptor: crate::AccessGuard<'static, &'static [u8]>,
        requested: NativeStorageBuffer<u8>,
    ) -> Self {
        Self {
            cause,
            descriptor,
            requested,
        }
    }
    pub fn requested_table(&self) -> &str {
        std::str::from_utf8(self.requested.as_slice()).expect("copied table name UTF-8")
    }
    pub fn descriptor(&self) -> &[u8] {
        self.descriptor.value()
    }
}
impl std::fmt::Debug for PreparedTableDefinitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedTableDefinitionError")
            .field("cause", &self.cause)
            .field("descriptor", &self.descriptor.value())
            .finish_non_exhaustive()
    }
}
impl From<crate::StorageError> for PreparedReadError {
    fn from(error: crate::StorageError) -> Self {
        match error {
            #[cfg(feature = "contextdb-internal")]
            crate::StorageError::Native(error) => Self::Native(error),
            error => Self::Storage(error),
        }
    }
}
impl From<NativeColdReadError> for PreparedReadError {
    fn from(error: NativeColdReadError) -> Self {
        Self::Native(error)
    }
}

impl std::fmt::Display for NativeColdReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionRefused => f.write_str("native storage admission refused"),
            Self::AdmissionRefusedAt(owner) => owner.inspect_failure(|cause| match cause {
                Some(cause) => std::fmt::Display::fmt(cause, f),
                None => {
                    f.write_str("native storage admission refused (producer cause unavailable)")
                }
            }),
            Self::AllocatorFailed { site, layout } => write!(
                f,
                "native allocator failed at {site} (size={}, alignment={})",
                layout.size(),
                layout.align()
            ),
            Self::TableAbsent => f.write_str("required native table is absent"),
            Self::TypeMismatch => f.write_str("native table type mismatch"),
            Self::Corrupt(fault) => write!(
                f,
                "DB corrupted: {} (page={:?}, offset={}, expected={}, actual={})",
                fault.reason, fault.page, fault.offset, fault.expected, fault.actual
            ),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::BackendIo(cause) => write!(f, "I/O error: {cause}"),
        }
    }
}
impl std::error::Error for NativeColdReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
impl std::fmt::Display for PreparedReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaticCorrupted(reason) => write!(f, "DB corrupted: {reason}"),
            Self::Storage(error) => std::fmt::Display::fmt(error, f),
            Self::Native(error) => std::fmt::Display::fmt(error, f),
            Self::TableDefinition(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for PreparedReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Native(error) => Some(error),
            Self::TableDefinition(error) => Some(error),
            Self::StaticCorrupted(_) => None,
        }
    }
}
impl std::fmt::Display for PreparedTableDefinitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn name(bytes: &[u8]) -> Option<&str> {
            bytes.get(1..).and_then(|s| std::str::from_utf8(s).ok())
        }
        let data = self.descriptor();
        // Same serialized descriptor offsets as the native validator. Invalid
        // descriptors keep their concrete corruption facts; never decode ownership.
        if !matches!(self.cause, NativeColdReadError::TypeMismatch) {
            return std::fmt::Display::fmt(&self.cause, f);
        }
        let table = self.requested_table();
        if data.first() == Some(&4) {
            return write!(f, "{table} is a multimap table");
        }
        let width = 10 + crate::tree_store::BtreeHeader::serialized_size();
        let align = width + 10;
        let names = align + 12;
        let Some(fixed) = data.get(..names) else {
            return std::fmt::Display::fmt(&self.cause, f);
        };
        let key_len = u32::from_le_bytes(fixed[align + 8..names].try_into().unwrap()) as usize;
        let Some(end) = names.checked_add(key_len) else {
            return std::fmt::Display::fmt(&self.cause, f);
        };
        let Some(key) = data.get(names..end).and_then(name) else {
            return std::fmt::Display::fmt(&self.cause, f);
        };
        let Some(value) = data.get(end..).and_then(name) else {
            return std::fmt::Display::fmt(&self.cause, f);
        };
        let alignment =
            |start| u32::from_le_bytes(fixed[start..start + 4].try_into().unwrap()) as usize;
        let stored_width = |start| {
            if fixed[start] == 1 {
                Some(u32::from_le_bytes(fixed[start + 1..start + 5].try_into().unwrap()) as usize)
            } else {
                None
            }
        };
        // Preserve the ordinary table validator's diagnosis precedence.
        for (name, start, offset) in [(key, align, width), (value, align + 4, width + 5)] {
            if alignment(start) != crate::tree_store::ALIGNMENT {
                return write!(
                    f,
                    "Current definition of {name} does not match stored definition (width={:?}, alignment={})",
                    stored_width(offset),
                    alignment(start)
                );
            }
        }
        if data.get(names..end) != Some(b"\x01&[u8]".as_slice())
            || data.get(end..) != Some(b"\x01&[u8]".as_slice())
        {
            return write!(f, "{table} is of type Table<{key}, {value}>");
        }
        for (start, offset) in [(align, width), (align + 4, width + 5)] {
            if stored_width(offset).is_some() {
                return write!(
                    f,
                    "Current definition of &[u8] does not match stored definition (width={:?}, alignment={})",
                    stored_width(offset),
                    alignment(start)
                );
            }
        }
        std::fmt::Display::fmt(&self.cause, f)
    }
}
impl std::error::Error for PreparedTableDefinitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

// Passive identities for the existing engine allocator oracle. No owner escapes.
#[cfg(all(feature = "contextdb-internal", feature = "test-seams"))]
impl<T> NativeStorageShared<T> {
    pub(crate) fn allocation_observation_for_test(&self) -> (usize, Layout, usize) {
        let block = unsafe { self.block.as_ref() };
        let invoice = block.ownership.lock().unwrap_or_else(|e| e.into_inner());
        (
            self.block.as_ptr() as usize,
            Self::layout(),
            invoice
                .as_ref()
                .expect("live shared invoice")
                .receipt
                .as_ptr() as usize,
        )
    }
}
#[cfg(all(feature = "contextdb-internal", feature = "test-seams"))]
impl NativeStorageCredit {
    pub(crate) fn receipt_identity_for_test(&self) -> usize {
        self.receipt.as_ptr() as usize
    }
}
