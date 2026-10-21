//! Owned memory charges shared by storage and suspendable readers.
//!
//! The engine supplies the budget capability; storage cannot change its limit.
use crate::Result;
use std::alloc::{Layout, alloc, dealloc};
use std::borrow::Cow;
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// Native admission and allocator failures contain no owned diagnostic allocation.
/// This companion is opt-in and does not change the public Error contract.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub enum NativeRefusal {
    /// The allocator returned null for this exact admitted layout.
    /// This is an Other diagnostic, never a memory-budget refusal.
    AllocatorFailed { site: &'static str, layout: Layout },
    MemoryBudgetExceeded {
        subsystem: &'static str,
        operation: &'static str,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: &'static str,
    },
    ProcessMemoryBudgetExceeded {
        operation: &'static str,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: &'static str,
    },
    NativeUnsupported {
        subsystem: &'static str,
        operation: &'static str,
    },
    Other(&'static str),
}
/// A native admission that runs a fallible producer: either the admission or
/// allocation refused (no owned diagnostic allocation), or the producer failed
/// with its own error, returned unchanged.
#[derive(Debug)]
#[doc(hidden)]
pub enum NativeProduce<E> {
    Refused(NativeRefusal),
    Producer(E),
}

/// Precomposed static inspection strings for native refusal routes.
pub const CUSTODY_DURABLE_ROOT_INSPECTION: &str = "custody/durable_root";
pub const CUSTODY_DURABLE_GROUP_INDEX_INSPECTION: &str = "custody/durable_group_index";
pub const READER_REQUEST_OWNER_INSPECTION: &str = "reader/request_owner";
pub const REDB_EXACT_BYTE_READ_INSPECTION: &str = "redb/exact_byte_read";
pub const SYNC_TRANSPORT_DELIVERY_RESULT_INSPECTION: &str = "sync_transport/delivery_result";

/// Precomposed static full display strings for native refusal routes.
pub const CUSTODY_DURABLE_ROOT_FULL_DISPLAY: &str =
    "native reader admission unsupported: custody/durable_root";
pub const CUSTODY_DURABLE_GROUP_INDEX_FULL_DISPLAY: &str =
    "native reader admission unsupported: custody/durable_group_index";
pub const READER_REQUEST_OWNER_FULL_DISPLAY: &str =
    "native reader admission unsupported: reader/request_owner";
pub const REDB_EXACT_BYTE_READ_FULL_DISPLAY: &str =
    "native reader admission unsupported: redb/exact_byte_read";
pub const SYNC_TRANSPORT_DELIVERY_RESULT_FULL_DISPLAY: &str =
    "native reader admission unsupported: sync_transport/delivery_result";

/// Map callsite-origin subsystem and operation to precomposed static inspection text,
/// preserving arbitrary dynamic errors as paid owned paths.
#[doc(hidden)]
pub fn native_unsupported_inspection(subsystem: &str, operation: &str) -> Cow<'static, str> {
    match (subsystem, operation) {
        ("custody", "durable_root") => Cow::Borrowed(CUSTODY_DURABLE_ROOT_INSPECTION),
        ("custody", "durable_group_index") => Cow::Borrowed(CUSTODY_DURABLE_GROUP_INDEX_INSPECTION),
        ("reader", "request_owner") => Cow::Borrowed(READER_REQUEST_OWNER_INSPECTION),
        ("redb", "exact_byte_read") => Cow::Borrowed(REDB_EXACT_BYTE_READ_INSPECTION),
        ("sync_transport", "delivery_result") => {
            Cow::Borrowed(SYNC_TRANSPORT_DELIVERY_RESULT_INSPECTION)
        }
        _ => Cow::Owned(format!("{subsystem}/{operation}")),
    }
}

/// Map callsite-origin subsystem and operation to precomposed static full display text,
/// preserving arbitrary dynamic errors as paid owned paths.
#[doc(hidden)]
pub fn native_unsupported_full_display(subsystem: &str, operation: &str) -> Cow<'static, str> {
    match (subsystem, operation) {
        ("custody", "durable_root") => Cow::Borrowed(CUSTODY_DURABLE_ROOT_FULL_DISPLAY),
        ("custody", "durable_group_index") => {
            Cow::Borrowed(CUSTODY_DURABLE_GROUP_INDEX_FULL_DISPLAY)
        }
        ("reader", "request_owner") => Cow::Borrowed(READER_REQUEST_OWNER_FULL_DISPLAY),
        ("redb", "exact_byte_read") => Cow::Borrowed(REDB_EXACT_BYTE_READ_FULL_DISPLAY),
        ("sync_transport", "delivery_result") => {
            Cow::Borrowed(SYNC_TRANSPORT_DELIVERY_RESULT_FULL_DISPLAY)
        }
        _ => Cow::Owned(format!(
            "native reader admission unsupported: {subsystem}/{operation}"
        )),
    }
}

impl NativeRefusal {
    /// Return the precomposed inspection for an unsupported route, or formatted dynamically
    /// for unknown non-static origins.
    #[doc(hidden)]
    pub fn route_inspection(&self) -> Option<Cow<'static, str>> {
        match self {
            Self::NativeUnsupported {
                subsystem,
                operation,
            } => Some(native_unsupported_inspection(subsystem, operation)),
            _ => None,
        }
    }

    /// Return precomposed full display text when available without heap allocation,
    /// or formatted dynamically for arbitrary dynamic origins.
    #[doc(hidden)]
    pub fn full_display(&self) -> Cow<'static, str> {
        match self {
            Self::Other(reason) => Cow::Borrowed(reason),
            Self::NativeUnsupported {
                subsystem,
                operation,
            } => native_unsupported_full_display(subsystem, operation),
            other => Cow::Owned(other.to_string()),
        }
    }
}

impl std::fmt::Display for NativeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => write!(
                f,
                "memory budget exceeded: {subsystem}/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: {hint}"
            ),
            Self::ProcessMemoryBudgetExceeded {
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => write!(
                f,
                "memory budget exceeded: process_native_ceiling/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: The process memory ceiling also holds the resident insertion runtime. {hint}"
            ),
            Self::NativeUnsupported {
                subsystem,
                operation,
            } => write!(
                f,
                "native reader admission unsupported: {subsystem}/{operation}"
            ),
            Self::AllocatorFailed { site, layout } => write!(f,
                "native allocator failed at {site} (size={}, alignment={})",
                layout.size(), layout.align()),
            Self::Other(message) => f.write_str(message),
        }
    }
}

#[doc(hidden)]
pub trait ReadMemoryBudget: Debug + Send + Sync {
    /// Original fixed-control invoice for a private forwarding allocation.
    /// Consumers pair this alias outside the Arc before retaining it.
    #[doc(hidden)]
    fn control_invoice(&self) -> Option<ReadShared<ReadCredit>> { None }

    /// Private operation binding for a genuine child whose listener invoices
    /// bypass an outer trait adapter. Host providers keep their existing path.
    #[doc(hidden)]
    fn constrain_operation_memory(&self, _limit: usize) {}

    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()>;
    /// Opt-in native admission. Never call the allocating Legacy method here.
    #[doc(hidden)]
    fn try_reserve_native(
        &self,
        _bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        _hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        Err(NativeRefusal::NativeUnsupported {
            subsystem,
            operation,
        })
    }
    /// Whether a native owner may use this payload for scalar admission.
    #[doc(hidden)]
    fn native_reserve_supported(&self) -> bool {
        false
    }
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    fn native_allocation_for_test(&self, _layout: Layout) {}
    fn release(&self, bytes: usize);

    /// Test observations name a forwarding capability's actual funder.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        None
    }
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    fn funding_owner_handle_for_test(&self) -> Option<&ReadMemoryOwner> { None }
}

/// An opaque strong owner. Legacy owners retain their original Arc identity;
/// native owners retain one explicitly laid-out allocation without a Weak tail.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct ReadMemoryOwner(OwnerKind);

enum OwnerKind {
    Legacy(Arc<dyn ReadMemoryBudget>, Option<ReadShared<ReadCredit>>),
    Native(NativeOwner),
}

impl Clone for OwnerKind {
    fn clone(&self) -> Self {
        match self {
            Self::Legacy(reader, invoice) => Self::Legacy(reader.clone(), invoice.clone()),
            Self::Native(owner) => Self::Native(owner.clone()),
        }
    }
}

impl Debug for OwnerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(owner, _) => f.debug_tuple("Legacy").field(owner).finish(),
            Self::Native(owner) => f.debug_tuple("Native").field(owner).finish(),
        }
    }
}

/// A typed projection of one native owner. Only this module constructs the
/// projection function and pairs it with its monomorphized allocation. The
/// shared borrow cannot outlive this strong handle; cloning allocates nothing.
#[doc(hidden)]
pub struct NativeReadMemory<T> {
    owner: ReadMemoryOwner,
    project: fn(&ReadMemoryOwner) -> &T,
    _payload: std::marker::PhantomData<T>,
}
impl<T> Clone for NativeReadMemory<T> {
    fn clone(&self) -> Self {
        Self { owner: self.owner.clone(), project: self.project,
            _payload: std::marker::PhantomData }
    }
}
impl<T> Debug for NativeReadMemory<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeReadMemory").field("owner", &self.owner).finish_non_exhaustive()
    }
}
impl<T> std::ops::Deref for NativeReadMemory<T> {
    type Target = T;
    fn deref(&self) -> &T { (self.project)(&self.owner) }
}
impl<T> NativeReadMemory<T> {
    /// The same strong capability, with no forwarding allocation or new child.
    pub fn owner(&self) -> ReadMemoryOwner { self.owner.clone() }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn strong_count_for_test(&self) -> usize { self.owner.strong_count_for_test() }
}

struct NativeReadBlock<T, I> {
    owners: AtomicUsize,
    payload: ManuallyDrop<T>,
    invoice: ManuallyDrop<I>,
}

struct NativeOwner {
    block: NonNull<()>,
    vtable: &'static NativeVtable,
}

type NativeReserve = unsafe fn(
    NonNull<()>,
    usize,
    &'static str,
    &'static str,
    &'static str,
) -> std::result::Result<(), NativeRefusal>;

struct NativeVtable {
    native_reserve_supported: unsafe fn(NonNull<()>) -> bool,
    constrain_operation_memory: unsafe fn(NonNull<()>, usize),
    reserve_native: NativeReserve,
    reserve: unsafe fn(NonNull<()>, usize, &'static str, &'static str, &'static str) -> Result<()>,
    release: unsafe fn(NonNull<()>, usize),
    #[cfg(any(test, feature = "test-seams"))]
    strong_count: unsafe fn(NonNull<()>) -> usize,
    clone_owner: unsafe fn(NonNull<()>),
    drop_owner: unsafe fn(NonNull<()>),
}

// Only try_native constructs this erased pointer, with T: ReadMemoryBudget
// (Debug + Send + Sync) and I: Send + Sync + 'static. Its monomorphized vtable
// preserves those types and layout; the atomic count keeps both alive for each
// shared access. No mutable payload access or raw-pointer export is available.
unsafe impl Send for NativeOwner {}
unsafe impl Sync for NativeOwner {}

impl Debug for NativeOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeOwner")
            .field("block", &self.block)
            .finish_non_exhaustive()
    }
}
impl Clone for NativeOwner {
    fn clone(&self) -> Self {
        // SAFETY: this strong owner keeps the correctly typed block alive.
        unsafe { (self.vtable.clone_owner)(self.block) };
        Self {
            block: self.block,
            vtable: self.vtable,
        }
    }
}
impl Drop for NativeOwner {
    fn drop(&mut self) {
        // SAFETY: exactly one strong reference is consumed by this Drop.
        unsafe { (self.vtable.drop_owner)(self.block) };
    }
}

/// Owns the moved invoice while the payload is destroyed, including unwind.
/// Field destruction settles the invoice only after this Drop frees the block.
struct NativeFreeGuard<I> {
    block: NonNull<()>,
    layout: Layout,
    _invoice: I,
}
impl<I> Drop for NativeFreeGuard<I> {
    fn drop(&mut self) {
        // SAFETY: the final owner uniquely transfers the allocation here,
        // with its construction layout; the moved invoice is outside it.
        unsafe { dealloc(self.block.as_ptr().cast(), self.layout) };
    }
}

impl<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static> NativeReadBlock<T, I> {
    const VTABLE: NativeVtable = NativeVtable {
        native_reserve_supported: Self::native_reserve_supported,
        constrain_operation_memory: Self::constrain_operation_memory,
        reserve_native: Self::reserve_native,
        reserve: Self::reserve,
        release: Self::release,
        #[cfg(any(test, feature = "test-seams"))]
        strong_count: Self::strong_count,
        clone_owner: Self::clone_owner,
        drop_owner: Self::drop_owner,
    };
    unsafe fn native_reserve_supported(block: NonNull<()>) -> bool {
        // SAFETY: the strong owner retains this exact monomorphized payload.
        unsafe { block.cast::<Self>().as_ref().payload.native_reserve_supported() }
    }
    unsafe fn constrain_operation_memory(block: NonNull<()>, limit: usize) {
        // SAFETY: the strong owner retains this monomorphized provider block.
        unsafe { block.cast::<Self>().as_ref().payload.constrain_operation_memory(limit) };
    }
    unsafe fn reserve_native(
        block: NonNull<()>,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        // SAFETY: the same typed block and strong owner as Legacy reserve.
        unsafe {
            block
                .cast::<Self>()
                .as_ref()
                .payload
                .try_reserve_native(bytes, subsystem, operation, hint)
        }
    }
    unsafe fn reserve(
        block: NonNull<()>,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        // SAFETY: the vtable belongs to this exact block type; a strong owner
        // holds the payload alive throughout this shared call.
        unsafe {
            block
                .cast::<Self>()
                .as_ref()
                .payload
                .try_reserve(bytes, subsystem, operation, hint)
        }
    }
    unsafe fn release(block: NonNull<()>, bytes: usize) {
        // SAFETY: same strong-owner/type invariant as reserve.
        unsafe { block.cast::<Self>().as_ref().payload.release(bytes) };
    }
    #[cfg(any(test, feature = "test-seams"))]
    unsafe fn strong_count(block: NonNull<()>) -> usize {
        // SAFETY: the observing strong owner pins this exact typed block.
        unsafe { block.cast::<Self>().as_ref().owners.load(Ordering::SeqCst) }
    }
    unsafe fn clone_owner(block: NonNull<()>) {
        // SAFETY: the calling strong owner excludes destruction.
        let owners = unsafe { &block.cast::<Self>().as_ref().owners };
        if owners
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_add(1)
            })
            .is_err()
        {
            std::process::abort();
        }
    }
    unsafe fn drop_owner(block: NonNull<()>) {
        let ptr = block.cast::<Self>().as_ptr();
        // SAFETY: each owner releases once; only the final winner destroys.
        if unsafe { (*ptr).owners.fetch_sub(1, Ordering::Release) } != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // SAFETY: no owners remain, and no Weak/raw capabilities exist. Move
        // the invoice outside the allocation before dropping the payload.
        let invoice = unsafe { ManuallyDrop::take(&mut (*ptr).invoice) };
        let guard = NativeFreeGuard {
            block,
            layout: Layout::new::<Self>(),
            _invoice: invoice,
        };
        // SAFETY: uniquely owned payload is destroyed exactly once. The guard
        // frees the block before settling its invoice even if payload Drop panics.
        unsafe { ManuallyDrop::drop(&mut (*ptr).payload) };
        drop(guard);
    }
}

impl From<Arc<dyn ReadMemoryBudget>> for ReadMemoryOwner {
    fn from(reader: Arc<dyn ReadMemoryBudget>) -> Self { Self::from_legacy(reader) }
}
impl<T: ReadMemoryBudget + 'static> From<Arc<T>> for ReadMemoryOwner {
    fn from(reader: Arc<T>) -> Self { Self::from_legacy(reader) }
}
impl From<&Arc<dyn ReadMemoryBudget>> for ReadMemoryOwner {
    fn from(reader: &Arc<dyn ReadMemoryBudget>) -> Self { Self::from_legacy(reader.clone()) }
}
impl<T: ReadMemoryBudget + 'static> From<&Arc<T>> for ReadMemoryOwner {
    fn from(reader: &Arc<T>) -> Self { Self::from_legacy(reader.clone()) }
}
impl From<&ReadMemoryOwner> for ReadMemoryOwner {
    fn from(reader: &ReadMemoryOwner) -> Self { reader.clone() }
}

impl ReadMemoryOwner {
    /// Borrow through the existing test-only forwarding receipt. This never
    /// manufactures a native owner from its address or acquires a strong count.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn original_native_owner_for_test(&self) -> Option<&ReadMemoryOwner> {
        match &self.0 {
            OwnerKind::Native(_) => Some(self),
            OwnerKind::Legacy(reader, _) => reader.funding_owner_handle_for_test()
                .and_then(ReadMemoryOwner::original_native_owner_for_test),
        }
    }

    pub fn from_legacy(reader: Arc<dyn ReadMemoryBudget>) -> Self {
        let invoice = reader.control_invoice();
        Self(OwnerKind::Legacy(reader, invoice))
    }

    /// The concrete allocation layout, including payload and invoice alignment.
    #[doc(hidden)]
    pub fn native_layout<T, I>() -> Layout {
        Layout::new::<NativeReadBlock<T, I>>()
    }

    /// The caller must admit native_layout::<T, I>() before calling this and
    /// supply the original funder's prepaid invoice. This generic cell orders
    /// destruction; it does not certify arbitrary caller-provided accounting.
    #[doc(hidden)]
    pub fn try_native<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T,
        prepaid_invoice: I,
    ) -> Result<Self> {
        let layout = Self::native_layout::<T, I>();
        #[cfg(feature = "test-seams")]
        payload.native_allocation_for_test(layout);
        // SAFETY: the atomic count makes this layout nonzero; layout is exact.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            // No block exists. Keep the invoice alive through payload Drop,
            // including unwinding, then refund the original admission.
            drop(payload);
            drop(prepaid_invoice);
            return Err(crate::Error::Other("reader owner allocation failed".into()));
        };
        // SAFETY: fresh aligned storage for precisely this type. All following
        // initialization and owner construction is infallible and nonallocating.
        unsafe {
            block.as_ptr().write(NativeReadBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(prepaid_invoice),
            })
        };
        Ok(Self(OwnerKind::Native(NativeOwner {
            block: block.cast(),
            vtable: &NativeReadBlock::<T, I>::VTABLE,
        })))
    }

    /// Typed companion to the existing native factory. The complete layout
    /// and original prepaid invoice are identical to the erased cell. No raw
    /// payload pointer, mutation or Weak capability leaves this module.
    #[doc(hidden)]
    pub fn try_native_typed<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T, prepaid_invoice: I,
    ) -> std::result::Result<NativeReadMemory<T>, NativeRefusal> {
        let owner = Self::try_native_result(payload, prepaid_invoice)?;
        Ok(Self::typed_native_owner::<T, I>(owner))
    }

    /// The same typed projection with the original unqualified Error terminal.
    #[doc(hidden)]
    pub fn try_native_typed_unqualified<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T, prepaid_invoice: I,
    ) -> Result<NativeReadMemory<T>> {
        let owner = Self::try_native(payload, prepaid_invoice)?;
        Ok(Self::typed_native_owner::<T, I>(owner))
    }

    // Only the factories immediately above supply an owner here, from the
    // exact T/I monomorphization. No caller can forge a typed projection.
    fn typed_native_owner<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        owner: ReadMemoryOwner,
    ) -> NativeReadMemory<T> {
        fn project<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
            owner: &ReadMemoryOwner,
        ) -> &T {
            let OwnerKind::Native(native) = &owner.0 else { unreachable!() };
            // SAFETY: this private projection is installed only beside the
            // owner returned by the matching T/I factory. No caller can
            // substitute either field. The borrow of owner retains its strong
            // count and ties this shared payload reference to that borrow.
            unsafe { &native.block.cast::<NativeReadBlock<T, I>>().as_ref().payload }
        }
        NativeReadMemory { owner, project: project::<T, I>,
            _payload: std::marker::PhantomData }
    }

    /// Native-result factory; the prepaid invoice survives payload destruction.
    #[doc(hidden)]
    pub fn try_native_result<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T,
        prepaid_invoice: I,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = Self::native_layout::<T, I>();
        #[cfg(feature = "test-seams")]
        payload.native_allocation_for_test(layout);
        // SAFETY: the atomic count makes this layout nonzero; layout is exact.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            // No block exists. Keep the invoice alive through payload Drop,
            // including unwinding, then refund the original admission.
            drop(payload);
            drop(prepaid_invoice);
            return Err(NativeRefusal::AllocatorFailed { site: "reader owner", layout });
        };
        // SAFETY: fresh aligned storage for precisely this type. All following
        // initialization and owner construction is infallible and nonallocating.
        unsafe {
            block.as_ptr().write(NativeReadBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(prepaid_invoice),
            })
        };
        Ok(Self(OwnerKind::Native(NativeOwner {
            block: block.cast(),
            vtable: &NativeReadBlock::<T, I>::VTABLE,
        })))
    }

    /// Allocate a native owner before moving its external custody into the
    /// cell. The invoice may contain an unpublished answer: allocation refusal
    /// destroys that answer before its original controls. A producer can return
    /// it separately only after allocation succeeds, without exposing raw cell
    /// storage. Unwind frees the block before dropping the external invoice.
    /// The caller prepays native_layout::<T, I>() on the original funder.
    #[doc(hidden)]
    pub fn try_native_result_with<T, I, R>(
        invoice: I,
        make: impl FnOnce(&mut I) -> (T, R),
    ) -> std::result::Result<(Self, R), NativeRefusal>
    where
        T: ReadMemoryBudget + 'static,
        I: Send + Sync + 'static,
    {
        let layout = Self::native_layout::<T, I>();
        // SAFETY: the native count makes this exact layout nonzero.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            drop(make);
            drop(invoice);
            return Err(NativeRefusal::AllocatorFailed { site: "reader owner", layout });
        };
        let mut guard = NativeFreeGuard { block: block.cast(), layout, _invoice: invoice };
        let (payload, returned) = make(&mut guard._invoice);
        let guard = ManuallyDrop::new(guard);
        // SAFETY: fresh storage is initialized once. Everything after producer
        // success is infallible; the block adopts the original outside invoice.
        unsafe {
            block.as_ptr().write(NativeReadBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(std::ptr::read(&guard._invoice)),
            });
        }
        Ok((Self(OwnerKind::Native(NativeOwner {
            block: block.cast(), vtable: &NativeReadBlock::<T, I>::VTABLE,
        })), returned))
    }

    /// Admit only a payload that explicitly supports scalar native reservation.
    /// Unsupported payloads refuse before allocating the owner cell.
    #[doc(hidden)]
    pub fn try_native_reserving<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T,
        prepaid_invoice: I,
    ) -> std::result::Result<Self, NativeRefusal> {
        if !payload.native_reserve_supported() {
            drop(payload);
            drop(prepaid_invoice);
            return Err(NativeRefusal::NativeUnsupported {
                subsystem: "reader",
                operation: "request_owner",
            });
        }
        let layout = Self::native_layout::<T, I>();
        #[cfg(feature = "test-seams")]
        payload.native_allocation_for_test(layout);
        // SAFETY: the atomic count makes this layout nonzero; layout is exact.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            // No block exists. Keep the invoice alive through payload Drop,
            // including unwinding, then refund the original admission.
            drop(payload);
            drop(prepaid_invoice);
            return Err(NativeRefusal::AllocatorFailed { site: "reader owner", layout });
        };
        // SAFETY: fresh aligned storage for precisely this type. All following
        // initialization and owner construction is infallible and nonallocating.
        unsafe {
            block.as_ptr().write(NativeReadBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(prepaid_invoice),
            })
        };
        Ok(Self(OwnerKind::Native(NativeOwner {
            block: block.cast(),
            vtable: &NativeReadBlock::<T, I>::VTABLE,
        })))
    }

    /// Lifetime-bounded observation of the original allocation identity. This
    /// scalar cannot dereference the budget or certify caller accounting.
    #[doc(hidden)]
    pub fn accounting_identity(&self) -> usize {
        match &self.0 {
            OwnerKind::Legacy(owner, _) => Arc::as_ptr(owner) as *const () as usize,
            OwnerKind::Native(owner) => owner.block.as_ptr() as usize,
        }
    }

    /// Inspect the real control count without retaining another observer alias.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn strong_count_for_test(&self) -> usize {
        match &self.0 {
            OwnerKind::Legacy(owner, _) => Arc::strong_count(owner),
            // SAFETY: this strong owner pins its correctly typed control.
            OwnerKind::Native(owner) => unsafe { (owner.vtable.strong_count)(owner.block) },
        }
    }

    /// Whether this owner is a native block (admissions on it may refuse with
    /// a `NativeRefusal`) rather than a Legacy capability.
    #[doc(hidden)]
    pub fn is_native(&self) -> bool {
        matches!(self.0, OwnerKind::Native(_))
    }

    pub fn same_owner(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (OwnerKind::Legacy(a, _), OwnerKind::Legacy(b, _)) => Arc::ptr_eq(a, b),
            (OwnerKind::Native(a), OwnerKind::Native(b)) => a.block == b.block,
            _ => false,
        }
    }
}
impl AsRef<dyn ReadMemoryBudget> for ReadMemoryOwner {
    fn as_ref(&self) -> &(dyn ReadMemoryBudget + 'static) { self }
}
impl ReadMemoryBudget for ReadMemoryOwner {
    #[cfg(feature = "test-seams")]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => Some(reader),
            OwnerKind::Native(_) => None,
        }
    }
    fn control_invoice(&self) -> Option<ReadShared<ReadCredit>> {
        match &self.0 {
            OwnerKind::Legacy(_, invoice) => invoice.clone(),
            OwnerKind::Native(_) => None,
        }
    }
    fn native_reserve_supported(&self) -> bool {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => reader.native_reserve_supported(),
            // SAFETY: the strong owner pins this exact vtable payload.
            OwnerKind::Native(owner) => unsafe { (owner.vtable.native_reserve_supported)(owner.block) },
        }
    }

    fn constrain_operation_memory(&self, limit: usize) {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => reader.constrain_operation_memory(limit),
            // SAFETY: this strong owner retains its correctly typed provider.
            OwnerKind::Native(owner) => unsafe {
                (owner.vtable.constrain_operation_memory)(owner.block, limit)
            },
        }
    }

    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => reader.try_reserve(bytes, subsystem, operation, hint),
            // SAFETY: this owner retains the correctly typed block during the call.
            OwnerKind::Native(owner) => unsafe {
                (owner.vtable.reserve)(owner.block, bytes, subsystem, operation, hint)
            },
        }
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => {
                reader.try_reserve_native(bytes, subsystem, operation, hint)
            }
            // SAFETY: this owner retains this exact provider's typed block.
            OwnerKind::Native(owner) => unsafe {
                (owner.vtable.reserve_native)(owner.block, bytes, subsystem, operation, hint)
            },
        }
    }
    fn release(&self, bytes: usize) {
        match &self.0 {
            OwnerKind::Legacy(reader, _) => reader.release(bytes),
            // SAFETY: this owner retains the correctly typed block during the call.
            OwnerKind::Native(owner) => unsafe { (owner.vtable.release)(owner.block, bytes) },
        }
    }
}

/// A charge moves with its allocation and returns exactly once. Owners place
/// this field after their payload so the payload drops before the credit.
#[derive(Debug)]
#[doc(hidden)]
pub struct ReadCredit {
    budget: ReadMemoryOwner,
    bytes: usize,
}

impl ReadCredit {
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn original_payer_for_test(&self) -> &ReadMemoryOwner { &self.budget }

    pub fn new(budget: impl Into<ReadMemoryOwner>) -> Self {
        Self {
            budget: budget.into(),
            bytes: 0,
        }
    }

    /// Adopt funding already held by this exact budget capability. The
    /// transferring owner removes these bytes without releasing them first.
    /// This constructor neither reserves nor refunds nor allocates.
    #[doc(hidden)]
    pub fn from_prepaid(budget: impl Into<ReadMemoryOwner>, bytes: usize) -> Self {
        Self::from_prepaid_owned(budget.into(), bytes)
    }

    /// Borrowed identity of this credit's original payer; never retains it.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn original_payer_identity_for_test(&self) -> usize {
        self.budget.accounting_identity()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn try_grow_for(
        &mut self,
        added: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        grow_credit(
            &self.budget,
            &mut self.bytes,
            added,
            subsystem,
            operation,
            hint,
        )
    }

    pub fn try_shrink(&mut self, returned: usize) -> Result<()> {
        shrink_credit(&self.budget, &mut self.bytes, returned)
    }

    /// Retain the exact original capability, including its external controls.
    #[doc(hidden)]
    pub fn owner(&self) -> ReadMemoryOwner { self.budget.clone() }

    /// Compare the actual retained funder without acquiring or reserving.
    #[doc(hidden)]
    pub fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.budget.same_owner(owner)
    }

    pub fn try_split_off(&mut self, bytes: usize) -> Result<Self> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| crate::Error::Other("reader split exceeds credit".into()))?;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
        })
    }

    /// Combine a producer's already admitted charge from this exact capability.
    #[doc(hidden)]
    pub fn try_merge_prepaid(&mut self, mut producer: Self) -> Result<()> {
        if !self.budget.same_owner(&producer.budget) {
            return Err(crate::Error::Other(
                "reader prepaid merge has a different capability".into(),
            ));
        }
        let bytes = self
            .bytes
            .checked_add(producer.bytes)
            .ok_or_else(|| crate::Error::Other("reader prepaid merge overflow".into()))?;
        self.bytes = bytes;
        producer.bytes = 0;
        Ok(())
    }

    pub fn transfer(mut self) {
        self.bytes = 0;
    }

    /// Passive observation of the charge held by this allocation owner.
    #[doc(hidden)]
    pub fn bytes_for_test(&self) -> usize {
        self.bytes
    }

    pub fn try_new(
        budget: impl Into<ReadMemoryOwner>,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<Self> {
        Self::try_new_owned(
            budget.into(),
            bytes,
            subsystem,
            operation,
            hint,
        )
    }

    #[doc(hidden)]
    pub fn from_prepaid_owned(budget: ReadMemoryOwner, bytes: usize) -> Self {
        Self { budget, bytes }
    }

    #[doc(hidden)]
    pub fn try_new_owned(
        budget: ReadMemoryOwner,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<Self> {
        budget.try_reserve(bytes, subsystem, operation, hint)?;
        Ok(Self { budget, bytes })
    }
}

impl ReadCredit {
    #[doc(hidden)]
    pub fn try_new_owned_native(
        budget: ReadMemoryOwner,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        budget.try_reserve_native(bytes, subsystem, operation, hint)?;
        Ok(Self { budget, bytes })
    }

    /// Admit a new copy from this actual original; no prepaid amount is adopted.
    #[doc(hidden)]
    pub fn reserve_copy_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        Self::try_new_owned_native(self.budget.clone(), bytes, subsystem, operation, hint)
    }

    #[doc(hidden)]
    pub fn try_grow_for_native(
        &mut self,
        added: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        let bytes = self
            .bytes
            .checked_add(added)
            .ok_or(NativeRefusal::Other("reader charge overflow"))?;
        self.budget
            .try_reserve_native(added, subsystem, operation, hint)?;
        self.bytes = bytes;
        Ok(())
    }

    #[doc(hidden)]
    pub fn try_shrink_native(&mut self, returned: usize) -> std::result::Result<(), NativeRefusal> {
        let bytes = self
            .bytes
            .checked_sub(returned)
            .ok_or(NativeRefusal::Other("reader charge release exceeds credit"))?;
        self.budget.release(returned);
        self.bytes = bytes;
        Ok(())
    }

    #[doc(hidden)]
    pub fn try_split_off_native(
        &mut self,
        bytes: usize,
    ) -> std::result::Result<Self, NativeRefusal> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(NativeRefusal::Other("reader split exceeds credit"))?;
        let budget = self.budget.clone();
        self.bytes = remaining;
        Ok(Self { budget, bytes })
    }

    #[doc(hidden)]
    pub fn try_merge_prepaid_native(
        &mut self,
        mut producer: Self,
    ) -> std::result::Result<(), NativeRefusal> {
        if !self.budget.same_owner(&producer.budget) {
            return Err(NativeRefusal::Other(
                "reader prepaid merge has a different capability",
            ));
        }
        let bytes = self
            .bytes
            .checked_add(producer.bytes)
            .ok_or(NativeRefusal::Other("reader prepaid merge overflow"))?;
        self.bytes = bytes;
        producer.bytes = 0;
        Ok(())
    }
}

// capture969 begin
// A passive callback for the selected retained-trace payer only. No token fields.
#[cfg(feature = "test-seams")]
thread_local! {
    static TRACE_CREDIT_DROP: std::cell::Cell<(usize, Option<fn(bool, usize, usize, usize)>)> =
        const { std::cell::Cell::new((0, None)) };
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn set_trace_credit_drop_observer_for_test(
    payer: usize, callback: Option<fn(bool, usize, usize, usize)>,
) {
    TRACE_CREDIT_DROP.with(|slot| slot.set((payer, callback)));
}
// capture969 end
impl Drop for ReadCredit {
    fn drop(&mut self) {
// capture969 begin
        #[cfg(feature = "test-seams")]
        let trace_drop = TRACE_CREDIT_DROP.with(|slot| {
            let (payer, callback) = slot.get();
            if payer != 0 && payer == self.budget.accounting_identity() { callback } else { None }
        });
        #[cfg(feature = "test-seams")]
        if let Some(callback) = trace_drop {
            callback(false, self as *const Self as usize, self.budget.accounting_identity(), self.bytes);
        }
// capture969 end
        self.budget.release(self.bytes);
// capture969 begin
        #[cfg(feature = "test-seams")]
        if let Some(callback) = trace_drop {
            callback(true, self as *const Self as usize, self.budget.accounting_identity(), self.bytes);
        }
// capture969 end
    }
}

// Owned and borrowed credits share the same admission and settlement math.
fn grow_credit(
    budget: &dyn ReadMemoryBudget,
    held: &mut usize,
    added: usize,
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
) -> Result<()> {
    let bytes = held
        .checked_add(added)
        .ok_or_else(|| crate::Error::Other("reader charge overflow".into()))?;
    budget.try_reserve(added, subsystem, operation, hint)?;
    *held = bytes;
    Ok(())
}

fn shrink_credit(budget: &dyn ReadMemoryBudget, held: &mut usize, returned: usize) -> Result<()> {
    let bytes = held
        .checked_sub(returned)
        .ok_or_else(|| crate::Error::Other("reader charge release exceeds credit".into()))?;
    budget.release(returned);
    *held = bytes;
    Ok(())
}

/// Private paired Arc transport with no data or invoice raw/Weak escapes.
/// The original reservation covers both complete Arc allocation layouts.
#[doc(hidden)]
pub struct ReadPaidArc<T> {
    data: ManuallyDrop<Arc<T>>,
    invoice: PaidArcInvoice,
}
struct PaidArcInvoice(ManuallyDrop<Arc<ManuallyDrop<PaidArcCredit>>>);
struct PaidArcCredit {
    unique: std::sync::Mutex<()>,
    changed: std::sync::Condvar,
    _credit: ReadCredit,
}
fn paid_arc_layout<T>() -> Layout {
    Layout::new::<(AtomicUsize, AtomicUsize)>()
        .extend(Layout::new::<T>()).expect("Arc layout").0.pad_to_align()
}
impl Clone for PaidArcInvoice {
    fn clone(&self) -> Self { Self(ManuallyDrop::new(Arc::clone(&self.0))) }
}
impl std::ops::Deref for PaidArcInvoice {
    type Target = PaidArcCredit;
    fn deref(&self) -> &PaidArcCredit { &self.0 }
}
impl Drop for PaidArcInvoice {
    fn drop(&mut self) {
        // Every contender uses into_inner. Its winning call deallocates the
        // invoice Arc before returning the moved reservation to be destroyed.
        let invoice = unsafe { ManuallyDrop::take(&mut self.0) };
        if let Some(credit) = Arc::into_inner(invoice) {
            drop(ManuallyDrop::into_inner(credit));
        }
    }
}
impl<T> ReadPaidArc<T> {
    pub fn bytes() -> usize {
        paid_arc_layout::<T>().size()
            + paid_arc_layout::<ManuallyDrop<PaidArcCredit>>().size()
    }
    /// Infallible allocation has the same allocator-error behavior as Arc::new.
    /// Admission and exact invoice splitting happen before entering this method.
    pub fn from_prepaid(value: T, credit: ReadCredit) -> Self {
        if credit.bytes() != Self::bytes() {
            drop(value);
            drop(credit);
            panic!("paired Arc invoice differs from layout");
        }
        let invoice = PaidArcInvoice(ManuallyDrop::new(Arc::new(ManuallyDrop::new(
            PaidArcCredit { unique: std::sync::Mutex::new(()),
                changed: std::sync::Condvar::new(), _credit: credit },
        ))));
        Self { data: ManuallyDrop::new(Arc::new(value)), invoice }
    }
    /// Standing controls reuse their allocation only after every old alias has
    /// physically retired. No Weak exists; every paired drop signals this lock.
    pub fn wait_unique(&mut self) -> &mut T {
        let mut lock = self.invoice.unique.lock().unwrap_or_else(|p| p.into_inner());
        while Arc::get_mut(&mut self.data).is_none() {
            lock = self.invoice.changed.wait(lock).unwrap_or_else(|p| p.into_inner());
        }
        drop(lock);
        Arc::get_mut(&mut self.data).expect("all paired aliases retired")
    }
}
impl<T> Clone for ReadPaidArc<T> {
    fn clone(&self) -> Self {
        Self { data: ManuallyDrop::new(Arc::clone(&self.data)),
            invoice: self.invoice.clone() }
    }
}
impl<T> std::ops::Deref for ReadPaidArc<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.data }
}
impl<T: Debug> Debug for ReadPaidArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <T as Debug>::fmt(self, f)
    }
}
impl<T> Drop for ReadPaidArc<T> {
    fn drop(&mut self) {
        let lock = self.invoice.unique.lock().unwrap_or_else(|p| p.into_inner());
        // The invoice field outlives data payload destruction and physical
        // deallocation, including unwinding from a payload destructor.
        drop(unsafe { ManuallyDrop::take(&mut self.data) });
        self.invoice.changed.notify_all();
        drop(lock);
    }
}

/// One typed, weakless shared allocation. The external invoice survives payload
/// destruction and physical block deallocation; cloning only retains this cell.
#[doc(hidden)]
pub struct ReadShared<T, I = ReadCredit> {
    block: NonNull<ReadSharedBlock<T, I>>,
}
struct ReadSharedBlock<T, I = ReadCredit> {
    owners: AtomicUsize,
    payload: ManuallyDrop<T>,
    invoice: ManuallyDrop<I>,
}
// SAFETY: a cross-thread handle requires both payload transfer and immutable
// concurrent access to be safe. The count is the only unsynchronized mutation.
unsafe impl<T: Send + Sync, I: Send + Sync> Send for ReadShared<T, I> {}
unsafe impl<T: Send + Sync, I: Send + Sync> Sync for ReadShared<T, I> {}

impl<T, I> ReadShared<T, I> {
    pub fn layout() -> Layout {
        Layout::new::<ReadSharedBlock<T, I>>()
    }
}

impl<T, I> ReadShared<T, I> {
    /// Transfer one genuine strong count into a private packed handle. This
    /// pointer owns the native cell and is never an Arc or a weak observer.
    #[doc(hidden)]
    pub fn into_raw(self) -> *const T {
        let shared = ManuallyDrop::new(self);
        std::ptr::addr_of!(unsafe { shared.block.as_ref() }.payload).cast::<T>()
    }

    /// Recover the exact strong count transferred by `into_raw`.
    ///
    /// # Safety
    /// `payload` must name a live strong count transferred by
    /// `ReadShared::<T, I>::into_raw` for these exact T and I. Dropping the recovered
    /// value consumes that count exactly once. A temporary borrow for cloning
    /// must use ManuallyDrop while its real strong owner remains alive.
    /// Retired pointers and pointers from observers are forbidden.
    #[doc(hidden)]
    pub unsafe fn from_raw(payload: *const T) -> Self {
        // SAFETY: the caller transfers a live count. The offset is that of our
        // own concrete native block, never a guessed standard-library layout.
        let block = unsafe { payload.cast::<u8>().sub(
            std::mem::offset_of!(ReadSharedBlock<T, I>, payload)) };
        Self { block: unsafe { NonNull::new_unchecked(block.cast_mut().cast()) } }
    }
}

impl<T> ReadShared<T> {
    /// Borrow the immutable payload while this strong handle retains its cell.
    /// The raw projection avoids non-const trait Deref in const consumers.
    pub(crate) const fn payload_ref(&self) -> &T {
        // SAFETY: every published handle retains an initialized payload, and
        // ManuallyDrop<T> has the same layout and validity as T.
        unsafe { &*std::ptr::addr_of!((*self.block.as_ptr()).payload).cast::<T>() }
    }

    /// Borrow the one original invoice without acquiring another owner or cell.
    #[doc(hidden)]
    pub fn credit(&self) -> &ReadCredit {
        // SAFETY: this strong handle keeps the immutable invoice alive.
        unsafe { &self.block.as_ref().invoice }
    }

    /// Adopt an exact prepaid control invoice before allocating. No child is
    /// acquired and the caller's funder is retained through the final free.
    pub fn try_from_prepaid(payload: T, invoice: ReadCredit) -> crate::Result<Self> {
        let layout = Self::layout();
        if invoice.bytes() != layout.size() {
            // Keep the invoice alive through payload Drop, including panic.
            drop(payload);
            drop(invoice);
            return Err(crate::Error::Other(
                "shared control invoice differs from layout".into(),
            ));
        }
        // SAFETY: exact aligned layout, nonzero because it includes the count.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T>>()) else {
            drop(payload);
            drop(invoice);
            return Err(crate::Error::Other(
                "cannot allocate admitted shared control".into(),
            ));
        };
        // SAFETY: initialize fresh storage once, before exposing any handle.
        unsafe {
            block.as_ptr().write(ReadSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { block })
    }

    /// Native-result companion with the same layout and destruction order.
    #[doc(hidden)]
    pub fn try_from_prepaid_native(
        payload: T,
        invoice: ReadCredit,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = Self::layout();
        if invoice.bytes() != layout.size() {
            // Keep the invoice alive through payload Drop, including panic.
            drop(payload);
            drop(invoice);
            return Err(NativeRefusal::Other(
                "shared control invoice differs from layout",
            ));
        }
        // SAFETY: exact aligned layout, nonzero because it includes the count.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T>>()) else {
            drop(payload);
            drop(invoice);
            return Err(NativeRefusal::AllocatorFailed { site: "admitted shared control", layout });
        };
        // SAFETY: initialize fresh storage once, before exposing any handle.
        unsafe {
            block.as_ptr().write(ReadSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(invoice),
            });
        }
        Ok(Self { block })
    }
}
impl<T> ReadShared<T> {
    /// Validate and allocate the exact cell before invoking a fallible producer.
    /// Each allocation nested in T must retain its own original invoice.
    #[doc(hidden)]
    pub fn try_from_prepaid_with(
        invoice: ReadCredit,
        make: impl FnOnce() -> crate::Result<T>,
    ) -> crate::Result<Self> {
        let layout = Self::layout();
        if invoice.bytes() != layout.size() {
            drop(make);
            drop(invoice);
            return Err(crate::Error::Other(
                "shared control invoice differs from layout".into(),
            ));
        }
        // SAFETY: the shared header makes this exact layout nonzero.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T>>()) else {
            drop(make);
            drop(invoice);
            return Err(crate::Error::Other(
                "cannot allocate admitted shared control".into(),
            ));
        };
        let guard = NativeFreeGuard {
            block: block.cast(),
            layout,
            _invoice: invoice,
        };
        // Err and panic free the uninitialized block before settling its invoice.
        let payload = make()?;
        let guard = ManuallyDrop::new(guard);
        // SAFETY: after producer success, transfer this sole invoice and block
        // without a fallible operation; the published cell owns their finalizer.
        unsafe {
            block.as_ptr().write(ReadSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(std::ptr::read(&guard._invoice)),
            });
        }
        Ok(Self { block })
    }

    /// Native companion of `try_from_prepaid_with`: a layout mismatch or an
    /// allocation failure refuses without an owned diagnostic; a producer
    /// error is returned unchanged after the unpublished block is freed.
    #[doc(hidden)]
    pub fn try_from_prepaid_with_native<E>(
        invoice: ReadCredit,
        make: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<Self, NativeProduce<E>> {
        Self::try_from_prepaid_with_native_inner(invoice, make, false)
    }

    /// A single original invoice covers both this cell and its nested payload.
    /// The caller admits those payload allocations before constructing them.
    #[doc(hidden)]
    pub fn try_from_prepaid_payload_with_native<E>(
        invoice: ReadCredit,
        make: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<Self, NativeProduce<E>> {
        Self::try_from_prepaid_with_native_inner(invoice, make, true)
    }

    fn try_from_prepaid_with_native_inner<E>(
        invoice: ReadCredit,
        make: impl FnOnce() -> std::result::Result<T, E>,
        includes_payload: bool,
    ) -> std::result::Result<Self, NativeProduce<E>> {
        let layout = Self::layout();
        if invoice.bytes() < layout.size()
            || (!includes_payload && invoice.bytes() != layout.size())
        {
            drop(make);
            drop(invoice);
            return Err(NativeProduce::Refused(NativeRefusal::Other(
                "shared control invoice differs from layout",
            )));
        }
        // SAFETY: the shared header makes this exact layout nonzero.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T>>()) else {
            drop(make);
            drop(invoice);
            return Err(NativeProduce::Refused(NativeRefusal::AllocatorFailed { site: "admitted shared control", layout }));
        };
        let guard = NativeFreeGuard {
            block: block.cast(),
            layout,
            _invoice: invoice,
        };
        // Err and panic free the uninitialized block before settling its invoice.
        let payload = make().map_err(NativeProduce::Producer)?;
        let guard = ManuallyDrop::new(guard);
        // SAFETY: as in `try_from_prepaid_with`.
        unsafe {
            block.as_ptr().write(ReadSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(std::ptr::read(&guard._invoice)),
            });
        }
        Ok(Self { block })
    }
}

/// A shared cell's original debit, with any inline lifetime bookkeeping.
/// Metadata is part of the admitted cell layout, never another allocation.
#[doc(hidden)]
pub trait ReadSharedInvoice: Send + Sync {
    fn credit(&self) -> &ReadCredit;
}
impl ReadSharedInvoice for ReadCredit {
    fn credit(&self) -> &ReadCredit { self }
}

/// An exact admitted shared cell, allocated before its producer publishes.
/// Uninitialized storage never escapes; Drop frees it before refunding credit.
#[doc(hidden)]
pub struct ReadPrepared<T, I = ReadCredit> {
    guard: NativeFreeGuard<I>,
    _payload: std::marker::PhantomData<T>,
}
// SAFETY: an unpublished cell has a unique allocation and invoice. Moving
// or sharing its typed custody follows the eventual payload bounds.
unsafe impl<T: Send, I: Send> Send for ReadPrepared<T, I> {}
unsafe impl<T: Sync, I: Sync> Sync for ReadPrepared<T, I> {}
impl<T, I> Debug for ReadPrepared<T, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadPrepared").finish_non_exhaustive()
    }
}
impl<T> ReadShared<T> {
    #[doc(hidden)]
    pub fn try_prepare_native(
        invoice: ReadCredit,
    ) -> std::result::Result<ReadPrepared<T>, NativeRefusal> {
        Self::try_prepare_invoice_native(invoice)
    }
}
impl<T, I> ReadShared<T, I> {
    /// Adopt an original non-reader invoice, already admitted for `layout()`.
    /// This is the same physical-free guard as reader-credit cells; no scalar
    /// reader admission or new budget is inferred from the representation.
    #[doc(hidden)]
    pub fn try_from_invoice_native(payload: T, invoice: I)
        -> std::result::Result<Self, NativeRefusal> {
        let layout = Self::layout();
        // SAFETY: the strong count makes the exact native layout nonzero.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T, I>>()) else {
            drop(payload);
            drop(invoice);
            return Err(NativeRefusal::AllocatorFailed { site: "funded shared control", layout });
        };
        // SAFETY: initialize fresh exact storage once before publishing it.
        unsafe { block.as_ptr().write(ReadSharedBlock {
            owners: AtomicUsize::new(1), payload: ManuallyDrop::new(payload),
            invoice: ManuallyDrop::new(invoice),
        }); }
        Ok(Self { block })
    }

    /// The caller prepays this exact layout, including its invoice metadata.
    /// Custody stays external through failure, payload Drop and physical free.
    #[doc(hidden)]
    pub fn try_prepare_invoice_native(
        invoice: I,
    ) -> std::result::Result<ReadPrepared<T, I>, NativeRefusal>
    where I: ReadSharedInvoice,
    {
        let layout = Self::layout();
        if invoice.credit().bytes() != layout.size() {
            return Err(NativeRefusal::Other("shared control invoice differs from layout"));
        }
        // SAFETY: the count makes the exact admitted layout nonzero.
        let Some(block) = NonNull::new(unsafe { alloc(layout) }) else {
            return Err(NativeRefusal::AllocatorFailed {
                site: "prepared shared control",
                layout,
            });
        };
        Ok(ReadPrepared {
            guard: NativeFreeGuard {
                block: block.cast(),
                layout,
                _invoice: invoice,
            },
            _payload: std::marker::PhantomData,
        })
    }
}
impl<T, I> ReadPrepared<T, I> {
    /// Borrow the original invoice; observing it neither retains nor allocates.
    #[doc(hidden)]
    pub fn invoice(&self) -> &I { &self.guard._invoice }

    /// All fallible work finished before the producer ran.
    pub fn publish(self, payload: T) -> ReadShared<T, I> {
        let prepared = ManuallyDrop::new(self);
        let block = prepared.guard.block.cast::<ReadSharedBlock<T, I>>();
        // SAFETY: this unique exact allocation is initialized once, moving
        // its invoice to the normal ReadShared physical-free finalizer.
        unsafe {
            block.as_ptr().write(ReadSharedBlock {
                owners: AtomicUsize::new(1),
                payload: ManuallyDrop::new(payload),
                invoice: ManuallyDrop::new(std::ptr::read(&prepared.guard._invoice)),
            });
        }
        ReadShared { block }
    }
}

// Private, allocation-free erasure for the cross-crate cursor vocabulary.
// No pointer, mutable projection or owning payload is exposed to callers.
impl<T: crate::read_contract::CursorPageStorage, I: ReadSharedInvoice + 'static> ReadShared<T, I> {
    pub(crate) fn into_cursor_page_handle(self) -> crate::read_contract::CursorPageHandle {
        let shared = ManuallyDrop::new(self);
        crate::read_contract::CursorPageHandle {
            block: shared.block.cast(),
            view: Self::cursor_view,
            retain: Self::cursor_retain,
            release: Self::cursor_release,
            encoding_owner: Self::cursor_encoding_owner,
        }
    }
    unsafe fn cursor_encoding_owner(block: NonNull<()>) -> ReadMemoryOwner {
        // The live erased cursor handle retains this exact native cell and
        // its original external invoice; this only clones that capability.
        let cell = unsafe { block.cast::<ReadSharedBlock<T, I>>().as_ref() };
        cell.invoice.credit().owner()
    }
    unsafe fn cursor_view(
        block: NonNull<()>,
    ) -> &'static dyn crate::read_contract::CursorPageStorage {
        // SAFETY: the erased handle retains this concrete initialized cell.
        unsafe {
            &*std::ptr::addr_of!((*block.cast::<ReadSharedBlock<T, I>>().as_ptr()).payload).cast::<T>()
        }
    }
    unsafe fn cursor_retain(block: NonNull<()>) {
        let shared = ManuallyDrop::new(Self {
            block: block.cast(),
        });
        let _retained = ManuallyDrop::new(Self::clone(&shared));
    }
    unsafe fn cursor_release(block: NonNull<()>) {
        drop(Self {
            block: block.cast(),
        });
    }
}

impl<T, I> Debug for ReadShared<T, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadShared").field("block", &self.block).finish_non_exhaustive()
    }
}
impl<T, I> Clone for ReadShared<T, I> {
    fn clone(&self) -> Self {
        // SAFETY: this strong handle prevents concurrent final destruction.
        let count = unsafe { &self.block.as_ref().owners };
        if count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value < isize::MAX as usize).then_some(value + 1)
            })
            .is_err()
        {
            std::process::abort();
        }
        Self { block: self.block }
    }
}
impl<T, I> std::ops::Deref for ReadShared<T, I> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a strong handle retains the initialized immutable payload.
        unsafe { &self.block.as_ref().payload }
    }
}
impl<T, I> Drop for ReadShared<T, I> {
    fn drop(&mut self) {
        let ptr = self.block.as_ptr();
        // SAFETY: each strong holder consumes its count exactly once.
        if unsafe { (*ptr).owners.fetch_sub(1, Ordering::Release) } != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // SAFETY: final owner uniquely moves the invoice outside the block.
        let invoice = unsafe { ManuallyDrop::take(&mut (*ptr).invoice) };
        let guard = NativeFreeGuard {
            block: self.block.cast(),
            layout: Self::layout(),
            _invoice: invoice,
        };
        // SAFETY: payload is dropped once. The physical-free guard settles the
        // original invoice after deallocation even if payload Drop panics.
        unsafe { ManuallyDrop::drop(&mut (*ptr).payload) };
        drop(guard);
    }
}

/// A private erased strong handle that forwards to the original ReadShared
/// allocation. Its vtable is monomorphized per callback type and is static:
/// making, cloning, calling, and dropping this handle never allocates.
pub(crate) struct NativeCancellationCallback {
    block: NonNull<()>,
    vtable: &'static NativeCancellationCallbackVtable,
}

// SAFETY: every erased handle originates from ReadShared<F> where F is Send +
// Sync. Its monomorphized vtable only reconstructs that same ReadShared<F> to
// call, clone, or drop it; no typed raw pointer escapes this private erasure.
unsafe impl Send for NativeCancellationCallback {}
unsafe impl Sync for NativeCancellationCallback {}

struct NativeCancellationCallbackVtable {
    call: unsafe fn(NonNull<()>),
    clone_callback: unsafe fn(NonNull<()>) -> NativeCancellationCallback,
    drop_callback: unsafe fn(NonNull<()>),
}

impl NativeCancellationCallback {
    pub(crate) fn call(&self) {
        // SAFETY: every callback handle owns one ReadShared<F> strong count;
        // its static vtable names exactly that F and keeps the cell live.
        unsafe { (self.vtable.call)(self.block) };
    }
}

impl Clone for NativeCancellationCallback {
    fn clone(&self) -> Self {
        // SAFETY: this handle retains the concrete ReadShared<F> allocation.
        unsafe { (self.vtable.clone_callback)(self.block) }
    }
}

impl Drop for NativeCancellationCallback {
    fn drop(&mut self) {
        // SAFETY: this consumes exactly this erased strong handle. ReadShared's
        // finalizer frees its cell before it settles the original invoice.
        unsafe { (self.vtable.drop_callback)(self.block) };
    }
}

impl<F: Fn() + Send + Sync + 'static> ReadShared<F> {
    const CANCELLATION_CALLBACK_VTABLE: NativeCancellationCallbackVtable =
        NativeCancellationCallbackVtable {
            call: Self::call_cancellation_callback,
            clone_callback: Self::clone_cancellation_callback,
            drop_callback: Self::drop_cancellation_callback,
        };

    unsafe fn call_cancellation_callback(block: NonNull<()>) {
        // SAFETY: the erased handle's vtable fixes F and its strong count
        // retains this block while the immutable callback is invoked.
        unsafe { (block.cast::<ReadSharedBlock<F>>().as_ref().payload)() };
    }

    unsafe fn clone_cancellation_callback(block: NonNull<()>) -> NativeCancellationCallback {
        let shared: ReadShared<F> = ReadShared {
            block: block.cast(),
        };
        let clone = shared.clone();
        std::mem::forget(shared);
        clone.into_cancellation_callback()
    }

    unsafe fn drop_cancellation_callback(block: NonNull<()>) {
        // SAFETY: this restores exactly the ownership moved out by
        // into_cancellation_callback, then delegates final-drop ordering.
        drop(ReadShared::<F> {
            block: block.cast(),
        });
    }

    pub(crate) fn into_cancellation_callback(self) -> NativeCancellationCallback {
        // Moving the field out without Drop transfers the sole ReadShared
        // count to this handle; no second cell, Box, or control is created.
        let shared = ManuallyDrop::new(self);
        NativeCancellationCallback {
            block: shared.block.cast(),
            vtable: &Self::CANCELLATION_CALLBACK_VTABLE,
        }
    }
}

/// A boxed payload whose allocation is freed before its admitted credit.
/// Callers reserve the concrete layout before allocating and coerce any
/// trait-object Box before constructing this move-only owner.
#[doc(hidden)]
pub struct OwnedReadBox<T: ?Sized> {
    payload: Box<T>,
    _credit: ReadCredit,
}

impl<T: ?Sized> OwnedReadBox<T> {
    pub fn new(payload: Box<T>, credit: ReadCredit) -> Self {
        Self {
            payload,
            _credit: credit,
        }
    }
    #[allow(clippy::should_implement_trait)] // The owner projects its payload while retaining the read credit.
    pub fn as_ref(&self) -> &T {
        &self.payload
    }
}
impl<T: ?Sized> std::ops::Deref for OwnedReadBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T: ?Sized> OwnedReadBox<T> {
    #[doc(hidden)]
    pub fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self._credit.is_funded_by(owner)
    }
}

impl<T> OwnedReadBox<T> {
    /// The payload's nested allocations must already have independent invoices.
    #[doc(hidden)]
    pub fn try_from_prepaid(payload: T, invoice: ReadCredit) -> crate::Result<Self> {
        Self::try_from_prepaid_with(invoice, move || Ok(payload))
    }

    /// Allocate the concrete payload only after validating its original invoice.
    #[doc(hidden)]
    pub fn try_from_prepaid_with(
        invoice: ReadCredit,
        make: impl FnOnce() -> crate::Result<T>,
    ) -> crate::Result<Self> {
        let (payload, credit) = try_prepaid_box_with(invoice, make)?;
        Ok(Self {
            payload,
            _credit: credit,
        })
    }

    /// Native companion of `try_from_prepaid_with`.
    #[doc(hidden)]
    pub fn try_from_prepaid_with_native<E>(
        invoice: ReadCredit,
        make: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<Self, NativeProduce<E>> {
        let (payload, credit) = try_prepaid_box_with_native(invoice, make)?;
        Ok(Self {
            payload,
            _credit: credit,
        })
    }
}

// This private guard owns only uninitialized storage. Its invoice is outside
// the allocation and field destruction follows physical free, including unwind.
struct ReadBoxInitGuard<T> {
    block: NonNull<T>,
    layout: Layout,
    invoice: ReadCredit,
}
impl<T> Drop for ReadBoxInitGuard<T> {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            // SAFETY: this is the exact allocation made for T, still unpublished.
            unsafe { dealloc(self.block.as_ptr().cast(), self.layout) };
        }
    }
}

pub(crate) fn try_prepaid_box_with<T>(
    invoice: ReadCredit,
    make: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<(Box<T>, ReadCredit)> {
    let layout = Layout::new::<T>();
    if invoice.bytes() != layout.size() {
        // Captured owners are destroyed while the original invoice is live.
        drop(make);
        drop(invoice);
        return Err(crate::Error::Other(
            "boxed payload invoice differs from layout".into(),
        ));
    }
    let block = if layout.size() == 0 {
        // Box requires an aligned nonnull pointer even when no bytes are allocated.
        NonNull::<T>::dangling()
    } else {
        // SAFETY: exact nonzero concrete layout, admitted before allocation.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<T>()) else {
            drop(make);
            drop(invoice);
            return Err(crate::Error::Other(
                "cannot allocate admitted boxed payload".into(),
            ));
        };
        block
    };
    let guard = ReadBoxInitGuard {
        block,
        layout,
        invoice,
    };
    let payload = make()?;
    let guard = ManuallyDrop::new(guard);
    // SAFETY: initialize once and immediately transfer storage to Box. Neither
    // this write nor ownership transfer invokes user code or allocates.
    unsafe {
        block.as_ptr().write(payload);
        Ok((
            Box::from_raw(block.as_ptr()),
            std::ptr::read(&guard.invoice),
        ))
    }
}

/// Native companion of `try_prepaid_box_with`: refusals carry no owned
/// diagnostic; a producer error is returned unchanged after the unpublished
/// storage is freed and before its invoice settles.
fn try_prepaid_box_with_native<T, E>(
    invoice: ReadCredit,
    make: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<(Box<T>, ReadCredit), NativeProduce<E>> {
    let layout = Layout::new::<T>();
    if invoice.bytes() != layout.size() {
        drop(make);
        drop(invoice);
        return Err(NativeProduce::Refused(NativeRefusal::Other(
            "boxed payload invoice differs from layout",
        )));
    }
    let block = if layout.size() == 0 {
        NonNull::<T>::dangling()
    } else {
        // SAFETY: exact nonzero concrete layout, admitted before allocation.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<T>()) else {
            drop(make);
            drop(invoice);
            return Err(NativeProduce::Refused(NativeRefusal::AllocatorFailed { site: "admitted boxed payload", layout }));
        };
        block
    };
    let guard = ReadBoxInitGuard {
        block,
        layout,
        invoice,
    };
    let payload = make().map_err(NativeProduce::Producer)?;
    let guard = ManuallyDrop::new(guard);
    // SAFETY: as in `try_prepaid_box_with`.
    unsafe {
        block.as_ptr().write(payload);
        Ok((
            Box::from_raw(block.as_ptr()),
            std::ptr::read(&guard.invoice),
        ))
    }
}

/// A concrete admitted frame, pinned before publication, with its invoice
/// outside the allocation. Nested owners retain their independent invoices.
#[doc(hidden)]
pub struct ReadOwnedFuture<'a, T> {
    payload: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>,
    _credit: ReadCredit,
}
impl<'a, T> ReadOwnedFuture<'a, T> {
    #[doc(hidden)]
    pub fn try_new_with<F>(
        owner: ReadMemoryOwner,
        make: impl FnOnce() -> crate::Result<F>,
    ) -> crate::Result<Self>
    where
        F: std::future::Future<Output = T> + Send + 'a,
    {
        // F includes all inline wrapper fields and captured owner handles.
        // An outer operation wrapper must be inside F, never allocated first.
        let invoice = ReadCredit::try_new_owned(
            owner,
            Layout::new::<F>().size(),
            "reader",
            "native owned future",
            "admit the concrete pinned frame before its producer",
        )?;
        let (payload, credit) = try_prepaid_box_with(invoice, make)?;
        let payload: Box<dyn std::future::Future<Output = T> + Send + 'a> = payload;
        // SAFETY: Box owns the initialized frame, which has not yet been polled.
        // No API moves it out after this pin; coercion and pinning do not allocate.
        let payload = unsafe { std::pin::Pin::new_unchecked(payload) };
        Ok(Self {
            payload,
            _credit: credit,
        })
    }

    /// Native companion of `try_new_with`: the frame's admission refuses with
    /// a `NativeRefusal`; a producer error is returned unchanged.
    #[doc(hidden)]
    pub fn try_new_with_native<F, E>(
        owner: ReadMemoryOwner,
        make: impl FnOnce() -> std::result::Result<F, E>,
    ) -> std::result::Result<Self, NativeProduce<E>>
    where
        F: std::future::Future<Output = T> + Send + 'a,
    {
        let invoice = ReadCredit::try_new_owned_native(
            owner,
            Layout::new::<F>().size(),
            "reader",
            "native owned future",
            "admit the concrete pinned frame before its producer",
        )
        .map_err(NativeProduce::Refused)?;
        let (payload, credit) = try_prepaid_box_with_native(invoice, make)?;
        let payload: Box<dyn std::future::Future<Output = T> + Send + 'a> = payload;
        // SAFETY: as in `try_new_with`.
        let payload = unsafe { std::pin::Pin::new_unchecked(payload) };
        Ok(Self {
            payload,
            _credit: credit,
        })
    }

    #[doc(hidden)]
    pub fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self._credit.is_funded_by(owner)
    }
}
impl<'a, T> std::future::Future for ReadOwnedFuture<'a, T> {
    type Output = T;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<T> {
        // Moving the pointer and invoice cannot move the heap-pinned frame.
        self.get_mut().payload.as_mut().poll(cx)
    }
}

/// The wake payload is kept in its original, independently admitted shared cell.
#[doc(hidden)]
pub trait NativeReadWake: Send + Sync + 'static {
    fn wake_read(&self);
}
/// An opaque strong alias of an original paid wake cell. Only the typed
/// ReadShared factory installs these private callbacks. Clone does nothing
/// except retain its atomic count: it cannot call payload, provider or Waker
/// code, allocate, or acquire another memory owner. Wake and final Drop must
/// run outside consumer locks because those can invoke payload/provider code.
#[doc(hidden)]
pub struct NativeReadWakeLease {
    block: NonNull<()>,
    retain: unsafe fn(NonNull<()>),
    release: unsafe fn(NonNull<()>),
    wake: unsafe fn(NonNull<()>),
}
// SAFETY: the sole factory requires NativeReadWake: Send + Sync and retains
// the original cell for every lease; private callbacks preserve that type.
unsafe impl Send for NativeReadWakeLease {}
unsafe impl Sync for NativeReadWakeLease {}
impl Clone for NativeReadWakeLease {
    fn clone(&self) -> Self {
        // SAFETY: this live strong lease keeps its factory-typed cell alive.
        unsafe { (self.retain)(self.block) };
        Self {
            block: self.block,
            retain: self.retain,
            release: self.release,
            wake: self.wake,
        }
    }
}
impl NativeReadWakeLease {
    #[doc(hidden)]
    pub fn wake_read(&self) {
        // SAFETY: a borrowed lease retains the original paid cell on unwind.
        unsafe { (self.wake)(self.block) };
    }
}
impl Drop for NativeReadWakeLease {
    fn drop(&mut self) {
        // SAFETY: consume this lease's count once, using the original finalizer.
        unsafe { (self.release)(self.block) };
    }
}

impl<T: NativeReadWake> ReadShared<T> {
    #[doc(hidden)]
    pub fn into_native_wake_lease(self) -> NativeReadWakeLease {
        let shared = ManuallyDrop::new(self);
        NativeReadWakeLease {
            block: shared.block.cast(),
            retain: Self::retain_native_wake_lease,
            release: Self::release_native_wake_lease,
            wake: Self::wake_native_wake_lease,
        }
    }

    unsafe fn retain_native_wake_lease(block: NonNull<()>) {
        let shared = ManuallyDrop::new(Self {
            block: block.cast::<ReadSharedBlock<T>>(),
        });
        // Retain exactly one count without invoking any T code or allocator.
        let _retained = ManuallyDrop::new(ReadShared::clone(&shared));
    }
    unsafe fn release_native_wake_lease(block: NonNull<()>) {
        drop(Self {
            block: block.cast::<ReadSharedBlock<T>>(),
        });
    }
    unsafe fn wake_native_wake_lease(block: NonNull<()>) {
        let shared = ManuallyDrop::new(Self {
            block: block.cast::<ReadSharedBlock<T>>(),
        });
        shared.wake_read();
    }

    const READ_WAKER_VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
        Self::clone_read_waker,
        Self::wake_read_waker,
        Self::wake_read_waker_by_ref,
        Self::drop_read_waker,
    );

    fn into_read_raw_waker(self) -> std::task::RawWaker {
        let shared = ManuallyDrop::new(self);
        std::task::RawWaker::new(shared.block.as_ptr().cast(), &Self::READ_WAKER_VTABLE)
    }

    unsafe fn read_waker_handle(data: *const ()) -> Self {
        // SAFETY: only our raw waker installs this exact typed cell and vtable.
        Self {
            block: unsafe { NonNull::new_unchecked(data as *mut ReadSharedBlock<T>) },
        }
    }

    unsafe fn clone_read_waker(data: *const ()) -> std::task::RawWaker {
        // Borrow the source count; cloning acquires exactly one new count.
        let shared = ManuallyDrop::new(unsafe { Self::read_waker_handle(data) });
        ReadShared::clone(&shared).into_read_raw_waker()
    }

    unsafe fn wake_read_waker(data: *const ()) {
        // This consuming wake owns one count even if the wake payload panics.
        let shared = unsafe { Self::read_waker_handle(data) };
        shared.wake_read();
    }

    unsafe fn wake_read_waker_by_ref(data: *const ()) {
        // A borrowed wake leaves the source count unchanged, including unwind.
        let shared = ManuallyDrop::new(unsafe { Self::read_waker_handle(data) });
        shared.wake_read();
    }

    unsafe fn drop_read_waker(data: *const ()) {
        // Restore the transferred strong count and reuse its original finalizer.
        drop(unsafe { Self::read_waker_handle(data) });
    }

    #[doc(hidden)]
    pub fn into_read_waker(self) -> std::task::Waker {
        // SAFETY: callbacks retain/consume original strong counts consistently;
        // T: Send + Sync makes all callback access safe across threads.
        unsafe { std::task::Waker::from_raw(self.into_read_raw_waker()) }
    }
}

// The two private hosts exercise actual native cells and pinned membership.
#[cfg(test)]
pub(crate) mod native_control_tests {
    use super::*;
    use std::alloc::{GlobalAlloc, System};
    use std::cell::Cell;
    use std::sync::Mutex;

    pub(crate) static SERIAL: Mutex<()> = Mutex::new(());
    thread_local! { static CAPTURE: Cell<bool> = const { Cell::new(false) }; }
    #[derive(Clone, Copy)]
    struct Invoice {
        bytes: usize,
        operation: &'static str,
        reserve: usize,
        refund: usize,
        cell: usize,
        allocation_attempted: bool,
    }
    const EMPTY_INVOICE: Invoice = Invoice {
        bytes: 0,
        operation: "",
        reserve: 0,
        refund: 0,
        cell: usize::MAX,
        allocation_attempted: false,
    };
    #[derive(Clone, Copy)]
    struct Physical {
        pointer: usize,
        size: usize,
        align: usize,
        alloc: usize,
        free: usize,
        before: usize,
        after: usize,
        parent: usize,
    }
    const EMPTY_PHYSICAL: Physical = Physical {
        pointer: 0,
        size: 0,
        align: 0,
        alloc: 0,
        free: 0,
        before: 0,
        after: 0,
        parent: 0,
    };
    struct Kernel {
        active: bool,
        spy: usize,
        parent: usize,
        tick: usize,
        invoices: [Invoice; 256],
        physical: [Physical; 256],
        ni: usize,
        np: usize,
        null: Option<Layout>,
        nulls: usize,
        violations: usize,
    }
    impl Kernel {
        const fn new() -> Self {
            Self {
                active: false,
                spy: 0,
                parent: 0,
                tick: 0,
                invoices: [EMPTY_INVOICE; 256],
                physical: [EMPTY_PHYSICAL; 256],
                ni: 0,
                np: 0,
                null: None,
                nulls: 0,
                violations: 0,
            }
        }
        fn step(&mut self) -> usize {
            self.tick += 1;
            self.tick
        }
        fn used(&self) -> usize {
            // The watch owns an Arc until every watched cell and thread is gone.
            unsafe { (*(self.spy as *const Spy)).used.load(Ordering::SeqCst) }
        }
    }
    static KERNEL: Mutex<Kernel> = Mutex::new(Kernel::new());
    struct SystemWatch;
    #[global_allocator]
    static ALLOCATOR: SystemWatch = SystemWatch;
    unsafe impl GlobalAlloc for SystemWatch {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if !CAPTURE.try_with(Cell::get).unwrap_or(false) {
                return unsafe { System.alloc(layout) };
            }
            let mut k = KERNEL.lock().unwrap();
            if !k.active {
                return unsafe { System.alloc(layout) };
            }
            // Only an actual pending invoice admits a watched allocation.
            let pending = (0..k.ni).rev().find(|&i| {
                k.invoices[i].bytes == layout.size()
                    && k.invoices[i].refund == 0
                    && k.invoices[i].cell == usize::MAX
                    && !k.invoices[i].allocation_attempted
            });
            let Some(i) = pending else {
                return unsafe { System.alloc(layout) };
            };
            k.invoices[i].allocation_attempted = true;
            if k.null == Some(layout) {
                k.null = None;
                k.nulls += 1;
                return std::ptr::null_mut();
            }
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                let n = k.np;
                if n == k.physical.len() {
                    std::process::abort();
                }
                let alloc = k.step();
                k.physical[n] = Physical {
                    pointer: pointer as usize,
                    size: layout.size(),
                    align: layout.align(),
                    alloc,
                    parent: k.parent,
                    ..EMPTY_PHYSICAL
                };
                k.invoices[i].cell = n;
                k.np += 1;
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            let mut k = KERNEL.lock().unwrap();
            let n = if k.active {
                (0..k.np)
                    .find(|&n| k.physical[n].pointer == pointer as usize && k.physical[n].free == 0)
            } else {
                None
            };
            if let Some(n) = n {
                let before = k.used();
                let required: usize = k.physical[..k.np]
                    .iter()
                    .filter(|p| p.free == 0)
                    .map(|p| p.size)
                    .sum();
                if before < required
                    || layout.size() != k.physical[n].size
                    || layout.align() != k.physical[n].align
                {
                    k.violations += 1;
                }
                unsafe { System.dealloc(pointer, layout) };
                let after = k.used();
                let free = k.step();
                k.physical[n].before = before;
                k.physical[n].after = after;
                k.physical[n].free = free;
                if before != after {
                    k.violations += 1;
                }
            } else {
                unsafe { System.dealloc(pointer, layout) };
            }
        }
    }
    #[derive(Debug)]
    pub(crate) struct Spy {
        pub(crate) used: AtomicUsize,
        refuse: Mutex<Option<&'static str>>,
        pub(crate) payload_drops: AtomicUsize,
    }
    impl ReadMemoryBudget for Spy {
        fn try_reserve(
            &self,
            bytes: usize,
            _: &'static str,
            operation: &'static str,
            _: &'static str,
        ) -> Result<()> {
            if *self.refuse.lock().unwrap() == Some(operation) {
                return Err(crate::Error::Other("deliberate admission refusal".into()));
            }
            self.used.fetch_add(bytes, Ordering::SeqCst);
            let mut k = KERNEL.lock().unwrap();
            if k.active && k.spy == self as *const Self as usize && bytes != 0 {
                let n = k.ni;
                if n == k.invoices.len() {
                    std::process::abort();
                }
                let reserve = k.step();
                k.invoices[n] = Invoice {
                    bytes,
                    operation,
                    reserve,
                    ..EMPTY_INVOICE
                };
                k.ni += 1;
            }
            Ok(())
        }
        fn release(&self, bytes: usize) {
            let mut k = KERNEL.lock().unwrap();
            if k.active && k.spy == self as *const Self as usize && bytes != 0 {
                // Prefer the physically freed invoice. Pending admissions have
                // no cell (refusal/null); no synthetic prepaid charge is used.
                let n = (0..k.ni)
                    .find(|&i| {
                        let r = k.invoices[i];
                        r.bytes == bytes
                            && r.refund == 0
                            && r.cell != usize::MAX
                            && k.physical[r.cell].free != 0
                    })
                    .or_else(|| {
                        (0..k.ni)
                            .rev()
                            .find(|&i| k.invoices[i].bytes == bytes && k.invoices[i].refund == 0)
                    });
                if let Some(n) = n {
                    let refund = k.step();
                    k.invoices[n].refund = refund;
                } else {
                    k.violations += 1;
                }
            }
            let old = self.used.fetch_sub(bytes, Ordering::SeqCst);
            if old < bytes {
                std::process::abort();
            }
        }
    }
    #[derive(Debug)]
    struct Provider {
        spy: Arc<Spy>,
        panic_drop: bool,
    }
    impl ReadMemoryBudget for Provider {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            self.spy.try_reserve(bytes, subsystem, operation, hint)
        }
        fn release(&self, bytes: usize) {
            self.spy.release(bytes);
        }
    }
    impl Drop for Provider {
        fn drop(&mut self) {
            self.spy.payload_drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_drop {
                panic!("native provider payload panic");
            }
        }
    }
    pub(crate) struct Watch {
        pub(crate) spy: Arc<Spy>,
        parent: ReadMemoryOwner,
        row: &'static str,
    }
    impl Watch {
        pub(crate) fn new(row: &'static str) -> Self {
            let spy = Arc::new(Spy {
                used: AtomicUsize::new(0),
                refuse: Mutex::new(None),
                payload_drops: AtomicUsize::new(0),
            });
            let parent = ReadMemoryOwner::from_legacy(spy.clone());
            let mut k = KERNEL.lock().unwrap();
            assert!(!k.active);
            *k = Kernel::new();
            k.active = true;
            k.spy = Arc::as_ptr(&spy) as usize;
            k.parent = parent.accounting_identity();
            Self { spy, parent, row }
        }
        pub(crate) fn allocating<T>(&self, f: impl FnOnce() -> T) -> T {
            struct Restore(bool);
            impl Drop for Restore {
                fn drop(&mut self) {
                    CAPTURE.with(|c| c.set(self.0));
                }
            }
            let _restore = Restore(CAPTURE.with(|c| c.replace(true)));
            f()
        }
        pub(crate) fn refuse(&self, op: Option<&'static str>) {
            *self.spy.refuse.lock().unwrap() = op;
        }
        pub(crate) fn null(&self, layout: Layout) {
            KERNEL.lock().unwrap().null = Some(layout);
        }
        pub(crate) fn native(&self, panic_drop: bool) -> ReadMemoryOwner {
            self.allocating(|| {
                let invoice = ReadCredit::try_new_owned(
                    self.parent.clone(),
                    ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(),
                    "reader",
                    "native provider",
                    "original parent invoice",
                )
                .unwrap();
                assert!(invoice.is_funded_by(&self.parent));
                let owner = ReadMemoryOwner::try_native(
                    Provider {
                        spy: self.spy.clone(),
                        panic_drop,
                    },
                    invoice,
                )
                .unwrap();
                if let OwnerKind::Native(n) = &owner.0 {
                    // The strong owner retains this exact typed block.
                    let block = unsafe {
                        n.block
                            .cast::<NativeReadBlock<Provider, ReadCredit>>()
                            .as_ref()
                    };
                    assert!(block.invoice.is_funded_by(&self.parent));
                    assert_eq!(
                        block.invoice.bytes(),
                        ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size()
                    );
                    assert!(Arc::ptr_eq(&block.payload.spy, &self.spy));
                } else {
                    panic!("native provider must use its original native allocation");
                }
                owner
            })
        }
        pub(crate) fn assert_shared<T>(&self, shared: &ReadShared<T>, owner: &ReadMemoryOwner) {
            let block = unsafe { shared.block.as_ref() };
            assert!(block.invoice.is_funded_by(owner));
            assert_eq!(block.invoice.bytes(), ReadShared::<T>::layout().size());
            let k = KERNEL.lock().unwrap();
            let observed = k.physical[..k.np].iter().any(|p| {
                p.pointer == shared.block.as_ptr() as usize
                    && p.size == ReadShared::<T>::layout().size()
                    && p.align == ReadShared::<T>::layout().align()
            });
            drop(k);
            assert!(observed);
        }
        pub(crate) fn assert_callback<F>(
            &self,
            callback: &NativeCancellationCallback,
            owner: &ReadMemoryOwner,
        ) {
            // The caller supplies the same F just registered through the real
            // generic production constructor; the callback handle retains it.
            let block = unsafe { callback.block.cast::<ReadSharedBlock<F>>().as_ref() };
            assert!(block.invoice.is_funded_by(owner));
            assert_eq!(block.invoice.bytes(), ReadShared::<F>::layout().size());
            let k = KERNEL.lock().unwrap();
            let observed = k.physical[..k.np].iter().any(|p| {
                p.pointer == callback.block.as_ptr() as usize
                    && p.size == ReadShared::<F>::layout().size()
                    && p.align == ReadShared::<F>::layout().align()
            });
            drop(k);
            assert!(observed);
        }
        pub(crate) fn cells(&self) -> usize {
            KERNEL.lock().unwrap().np
        }
        pub(crate) fn frees(&self) -> usize {
            let k = KERNEL.lock().unwrap();
            k.physical[..k.np].iter().filter(|p| p.free != 0).count()
        }
        pub(crate) fn finish(&self) {
            assert_eq!(
                self.spy.used.load(Ordering::SeqCst),
                0,
                "original parent must settle exactly"
            );
            let (invoices, physical, ni, np, nulls, violations) = {
                let k = KERNEL.lock().unwrap();
                (k.invoices, k.physical, k.ni, k.np, k.nulls, k.violations)
            };
            for (i, p) in physical[..np].iter().enumerate() {
                let r = invoices[..ni].iter().find(|r| r.cell == i).unwrap();
                assert_eq!(p.parent, self.parent.accounting_identity());

                assert_eq!(p.before, p.after);
                // Parsed by the evidence gate, not a production diagnostic.
                println!(
                    "ALLOCATOR_ROW row={:?} pointer={} size={} align={} parent={} operation={:?} reserve={} alloc={} free={} refund={} debit_before={} debit_after={}",
                    self.row,
                    p.pointer,
                    p.size,
                    p.align,
                    p.parent,
                    r.operation,
                    r.reserve,
                    p.alloc,
                    p.free,
                    r.refund,
                    p.before,
                    p.after
                );
            }
            for r in &invoices[..ni] {
                assert!(r.refund > r.reserve);
            }
            println!(
                "PROOF_ROW row={:?} invoices={} allocations={} physical_frees={} nulls={} settled=0",
                self.row,
                ni,
                np,
                physical[..np].iter().filter(|p| p.free != 0).count(),
                nulls
            );
            assert_eq!(
                violations, 0,
                "original invoices must remain debited through System.dealloc"
            );
            for r in &invoices[..ni] {
                if r.cell != usize::MAX {
                    let p = physical[r.cell];
                    assert!(
                        r.reserve < p.alloc && p.alloc < p.free && p.free < r.refund,
                        "physical free must precede original invoice refund: {}",
                        r.operation
                    );
                }
            }
        }
    }
    impl Drop for Watch {
        fn drop(&mut self) {
            let mut k = KERNEL.lock().unwrap_or_else(|p| p.into_inner());
            k.active = false;
        }
    }
    #[derive(Debug)]
    struct Payload {
        spy: Arc<Spy>,
        panic_drop: bool,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.spy.payload_drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_drop {
                panic!("shared payload panic");
            }
        }
    }
    #[test]
    fn native_shared_control_ownership_rows() {
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        for panic_drop in [false, true] {
            let w = Watch::new(if panic_drop {
                "owner-payload-panic"
            } else {
                "owner-concurrent-clones"
            });
            let owner = w.native(panic_drop);
            let identity = owner.accounting_identity();
            std::thread::scope(|s| {
                for _ in 0..4 {
                    let c = owner.clone();
                    s.spawn(move || {
                        let d = c.clone();
                        assert_eq!(d.accounting_identity(), identity);
                        assert!(c.same_owner(&d));
                        drop(c);
                        drop(d);
                    });
                }
            });
            assert_eq!(w.frees(), 0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
            assert_eq!(result.is_err(), panic_drop);
            assert_eq!(w.spy.payload_drops.load(Ordering::SeqCst), 1);
            w.finish();
        }
        for panic_drop in [false, true] {
            let w = Watch::new(if panic_drop {
                "shared-payload-panic"
            } else {
                "shared-concurrent-clones"
            });
            let owner = w.native(false);
            let shared = w.allocating(|| {
                let invoice = ReadCredit::try_new_owned(
                    owner.clone(),
                    ReadShared::<Payload>::layout().size(),
                    "reader",
                    "shared payload",
                    "admit actual layout",
                )
                .unwrap();
                assert!(invoice.is_funded_by(&owner));
                ReadShared::try_from_prepaid(
                    Payload {
                        spy: w.spy.clone(),
                        panic_drop,
                    },
                    invoice,
                )
                .unwrap()
            });
            w.assert_shared(&shared, &owner);
            let ptr = shared.block.as_ptr() as usize;
            std::thread::scope(|s| {
                for _ in 0..4 {
                    let c = shared.clone();
                    s.spawn(move || {
                        let d = c.clone();
                        assert_eq!(d.block.as_ptr() as usize, ptr);
                        drop(c);
                        drop(d);
                    });
                }
            });
            drop(owner);
            assert_eq!(w.frees(), 0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(shared)));
            assert_eq!(result.is_err(), panic_drop);
            assert_eq!(w.spy.payload_drops.load(Ordering::SeqCst), 2);
            w.finish();
        }
        for owner_null in [false, true] {
            let w = Watch::new(if owner_null {
                "owner-null-allocation"
            } else {
                "shared-null-allocation"
            });
            if owner_null {
                w.null(ReadMemoryOwner::native_layout::<Provider, ReadCredit>());
                let result = w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(
                        w.parent.clone(),
                        ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(),
                        "reader",
                        "native provider",
                        "original invoice",
                    )
                    .unwrap();
                    ReadMemoryOwner::try_native(
                        Provider {
                            spy: w.spy.clone(),
                            panic_drop: false,
                        },
                        invoice,
                    )
                });
                assert!(result.is_err());
                assert_eq!(w.cells(), 0);
            } else {
                let owner = w.native(false);
                w.null(ReadShared::<Payload>::layout());
                let result = w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(
                        owner.clone(),
                        ReadShared::<Payload>::layout().size(),
                        "reader",
                        "shared payload",
                        "actual layout",
                    )
                    .unwrap();
                    ReadShared::try_from_prepaid(
                        Payload {
                            spy: w.spy.clone(),
                            panic_drop: false,
                        },
                        invoice,
                    )
                });
                assert!(result.is_err());
                assert_eq!(w.cells(), 1);
                drop(owner);
            }
            w.finish();
        }
        for owner_null in [false, true] {
            let w = Watch::new(if owner_null {
                "owner-null-payload-panic"
            } else {
                "shared-null-payload-panic"
            });
            if owner_null {
                w.null(ReadMemoryOwner::native_layout::<Provider, ReadCredit>());
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    w.allocating(|| {
                        let invoice = ReadCredit::try_new_owned(
                            w.parent.clone(),
                            ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(),
                            "reader",
                            "native provider",
                            "original invoice",
                        )
                        .unwrap();
                        ReadMemoryOwner::try_native(
                            Provider {
                                spy: w.spy.clone(),
                                panic_drop: true,
                            },
                            invoice,
                        )
                    })
                }));
                assert!(result.is_err());
                assert_eq!(w.cells(), 0);
            } else {
                let owner = w.native(false);
                w.null(ReadShared::<Payload>::layout());
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    w.allocating(|| {
                        let invoice = ReadCredit::try_new_owned(
                            owner.clone(),
                            ReadShared::<Payload>::layout().size(),
                            "reader",
                            "shared payload",
                            "actual layout",
                        )
                        .unwrap();
                        ReadShared::try_from_prepaid(
                            Payload {
                                spy: w.spy.clone(),
                                panic_drop: true,
                            },
                            invoice,
                        )
                    })
                }));
                assert!(result.is_err());
                assert_eq!(w.cells(), 1);
                drop(owner);
            }
            w.finish();
        }
        let w = Watch::new("shared-mismatched-invoice");
        let owner = w.native(false);
        let layout = ReadShared::<Payload>::layout();
        let invoice = ReadCredit::try_new_owned(
            owner.clone(),
            layout.size() + layout.align(),
            "reader",
            "mismatched invoice",
            "refuse before allocation",
        )
        .unwrap();
        assert!(
            w.allocating(|| ReadShared::try_from_prepaid(
                Payload {
                    spy: w.spy.clone(),
                    panic_drop: false
                },
                invoice
            ))
            .is_err()
        );
        assert_eq!(w.cells(), 1);
        drop(owner);
        w.finish();
    }
}
