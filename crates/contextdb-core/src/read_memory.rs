//! Owned memory charges shared by storage and suspendable readers.
//!
//! The engine supplies the budget capability; storage cannot change its limit.
use crate::Result;
use std::alloc::{Layout, alloc, dealloc};
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// Native admission failure contains no owned diagnostic allocation.
/// This companion is opt-in and does not change the public Error contract.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub enum NativeRefusal {
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
impl std::fmt::Display for NativeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryBudgetExceeded { subsystem, operation, requested_bytes,
                available_bytes, budget_limit_bytes, hint } => write!(f,
                "memory budget exceeded: {subsystem}/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: {hint}"),
            Self::ProcessMemoryBudgetExceeded { operation, requested_bytes,
                available_bytes, budget_limit_bytes, hint } => write!(f,
                "memory budget exceeded: process_native_ceiling/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: The process memory ceiling also holds the resident insertion runtime. {hint}"),
            Self::NativeUnsupported { subsystem, operation } => write!(f,
                "native reader admission unsupported: {subsystem}/{operation}"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

#[doc(hidden)]
pub trait ReadMemoryBudget: Debug + Send + Sync {
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
        Err(NativeRefusal::NativeUnsupported { subsystem, operation })
    }
    /// Whether a native owner may use this payload for scalar admission.
    #[doc(hidden)]
    fn native_reserve_supported(&self) -> bool {
        false
    }
    fn release(&self, bytes: usize);

    /// Test observations name a forwarding capability's actual funder.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        None
    }
}

/// An opaque strong owner. Legacy owners retain their original Arc identity;
/// native owners retain one explicitly laid-out allocation without a Weak tail.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct ReadMemoryOwner(OwnerKind);

#[derive(Debug, Clone)]
enum OwnerKind {
    Legacy(Arc<dyn ReadMemoryBudget>),
    Native(NativeOwner),
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

struct NativeVtable {
    reserve_native: unsafe fn(NonNull<()>, usize, &'static str, &'static str, &'static str)
        -> std::result::Result<(), NativeRefusal>,
    reserve: unsafe fn(NonNull<()>, usize, &'static str, &'static str, &'static str) -> Result<()>,
    release: unsafe fn(NonNull<()>, usize),
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
        reserve_native: Self::reserve_native,
        reserve: Self::reserve,
        release: Self::release,
        clone_owner: Self::clone_owner,
        drop_owner: Self::drop_owner,
    };
    unsafe fn reserve_native(
        block: NonNull<()>, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        // SAFETY: the same typed block and strong owner as Legacy reserve.
        unsafe { block.cast::<Self>().as_ref().payload
            .try_reserve_native(bytes, subsystem, operation, hint) }
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

impl ReadMemoryOwner {
    pub fn from_legacy(reader: Arc<dyn ReadMemoryBudget>) -> Self {
        Self(OwnerKind::Legacy(reader))
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

    /// Native-result factory; the prepaid invoice survives payload destruction.
    #[doc(hidden)]
    pub fn try_native_result<T: ReadMemoryBudget + 'static, I: Send + Sync + 'static>(
        payload: T,
        prepaid_invoice: I,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = Self::native_layout::<T, I>();
        // SAFETY: the atomic count makes this layout nonzero; layout is exact.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            // No block exists. Keep the invoice alive through payload Drop,
            // including unwinding, then refund the original admission.
            drop(payload);
            drop(prepaid_invoice);
            return Err(NativeRefusal::Other("reader owner allocation failed"));
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
                subsystem: "reader", operation: "request_owner",
            });
        }
        let layout = Self::native_layout::<T, I>();
        // SAFETY: the atomic count makes this layout nonzero; layout is exact.
        let allocation = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(allocation.cast::<NativeReadBlock<T, I>>()) else {
            // No block exists. Keep the invoice alive through payload Drop,
            // including unwinding, then refund the original admission.
            drop(payload);
            drop(prepaid_invoice);
            return Err(NativeRefusal::Other("reader owner allocation failed"));
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
            OwnerKind::Legacy(owner) => Arc::as_ptr(owner) as *const () as usize,
            OwnerKind::Native(owner) => owner.block.as_ptr() as usize,
        }
    }

    pub fn same_owner(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (OwnerKind::Legacy(a), OwnerKind::Legacy(b)) => Arc::ptr_eq(a, b),
            (OwnerKind::Native(a), OwnerKind::Native(b)) => a.block == b.block,
            _ => false,
        }
    }
}
impl ReadMemoryBudget for ReadMemoryOwner {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        match &self.0 {
            OwnerKind::Legacy(reader) => reader.try_reserve(bytes, subsystem, operation, hint),
            // SAFETY: this owner retains the correctly typed block during the call.
            OwnerKind::Native(owner) => unsafe {
                (owner.vtable.reserve)(owner.block, bytes, subsystem, operation, hint)
            },
        }
    }
    fn try_reserve_native(
        &self, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        match &self.0 {
            OwnerKind::Legacy(reader) => reader.try_reserve_native(bytes, subsystem, operation, hint),
            // SAFETY: this owner retains this exact provider's typed block.
            OwnerKind::Native(owner) => unsafe {
                (owner.vtable.reserve_native)(owner.block, bytes, subsystem, operation, hint)
            },
        }
    }
    fn release(&self, bytes: usize) {
        match &self.0 {
            OwnerKind::Legacy(reader) => reader.release(bytes),
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
    pub fn new(budget: Arc<dyn ReadMemoryBudget>) -> Self {
        Self {
            budget: ReadMemoryOwner::from_legacy(budget),
            bytes: 0,
        }
    }

    /// Adopt funding already held by this exact budget capability. The
    /// transferring owner removes these bytes without releasing them first.
    /// This constructor neither reserves nor refunds nor allocates.
    #[doc(hidden)]
    pub fn from_prepaid(budget: Arc<dyn ReadMemoryBudget>, bytes: usize) -> Self {
        Self::from_prepaid_owned(ReadMemoryOwner::from_legacy(budget), bytes)
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
        budget: Arc<dyn ReadMemoryBudget>,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<Self> {
        Self::try_new_owned(
            ReadMemoryOwner::from_legacy(budget),
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
        budget: ReadMemoryOwner, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        budget.try_reserve_native(bytes, subsystem, operation, hint)?;
        Ok(Self { budget, bytes })
    }

    /// Admit a new copy from this actual original; no prepaid amount is adopted.
    #[doc(hidden)]
    pub fn reserve_copy_native(
        &self, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        Self::try_new_owned_native(self.budget.clone(), bytes, subsystem, operation, hint)
    }

    #[doc(hidden)]
    pub fn try_grow_for_native(
        &mut self, added: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        let bytes = self.bytes.checked_add(added)
            .ok_or(NativeRefusal::Other("reader charge overflow"))?;
        self.budget.try_reserve_native(added, subsystem, operation, hint)?;
        self.bytes = bytes;
        Ok(())
    }

    #[doc(hidden)]
    pub fn try_shrink_native(&mut self, returned: usize) -> std::result::Result<(), NativeRefusal> {
        let bytes = self.bytes.checked_sub(returned)
            .ok_or(NativeRefusal::Other("reader charge release exceeds credit"))?;
        self.budget.release(returned);
        self.bytes = bytes;
        Ok(())
    }

    #[doc(hidden)]
    pub fn try_split_off_native(&mut self, bytes: usize) -> std::result::Result<Self, NativeRefusal> {
        let remaining = self.bytes.checked_sub(bytes)
            .ok_or(NativeRefusal::Other("reader split exceeds credit"))?;
        let budget = self.budget.clone();
        self.bytes = remaining;
        Ok(Self { budget, bytes })
    }

    #[doc(hidden)]
    pub fn try_merge_prepaid_native(&mut self, mut producer: Self) -> std::result::Result<(), NativeRefusal> {
        if !self.budget.same_owner(&producer.budget) {
            return Err(NativeRefusal::Other("reader prepaid merge has a different capability"));
        }
        let bytes = self.bytes.checked_add(producer.bytes)
            .ok_or(NativeRefusal::Other("reader prepaid merge overflow"))?;
        self.bytes = bytes;
        producer.bytes = 0;
        Ok(())
    }
}

impl Drop for ReadCredit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
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

/// One typed, weakless shared allocation. The external invoice survives payload
/// destruction and physical block deallocation; cloning only retains this cell.
#[doc(hidden)]
pub struct ReadShared<T> {
    block: NonNull<ReadSharedBlock<T>>,
}
struct ReadSharedBlock<T> {
    owners: AtomicUsize,
    payload: ManuallyDrop<T>,
    invoice: ManuallyDrop<ReadCredit>,
}
// SAFETY: a cross-thread handle requires both payload transfer and immutable
// concurrent access to be safe. The count is the only unsynchronized mutation.
unsafe impl<T: Send + Sync> Send for ReadShared<T> {}
unsafe impl<T: Send + Sync> Sync for ReadShared<T> {}

impl<T> ReadShared<T> {
    pub fn layout() -> Layout {
        Layout::new::<ReadSharedBlock<T>>()
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
    pub fn try_from_prepaid_native(payload: T, invoice: ReadCredit) -> std::result::Result<Self, NativeRefusal> {
        let layout = Self::layout();
        if invoice.bytes() != layout.size() {
            // Keep the invoice alive through payload Drop, including panic.
            drop(payload);
            drop(invoice);
            return Err(NativeRefusal::Other("shared control invoice differs from layout"));
        }
        // SAFETY: exact aligned layout, nonzero because it includes the count.
        let storage = unsafe { alloc(layout) };
        let Some(block) = NonNull::new(storage.cast::<ReadSharedBlock<T>>()) else {
            drop(payload);
            drop(invoice);
            return Err(NativeRefusal::Other("cannot allocate admitted shared control"));
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
}

impl<T> Clone for ReadShared<T> {
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
impl<T> std::ops::Deref for ReadShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a strong handle retains the initialized immutable payload.
        unsafe { &self.block.as_ref().payload }
    }
}
impl<T> Drop for ReadShared<T> {
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

    unsafe fn clone_cancellation_callback(
        block: NonNull<()>,
    ) -> NativeCancellationCallback {
        let shared: ReadShared<F> = ReadShared { block: block.cast() };
        let clone = shared.clone();
        std::mem::forget(shared);
        clone.into_cancellation_callback()
    }

    unsafe fn drop_cancellation_callback(block: NonNull<()>) {
        // SAFETY: this restores exactly the ownership moved out by
        // into_cancellation_callback, then delegates final-drop ordering.
        drop(ReadShared::<F> { block: block.cast() });
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
        Ok(Self { payload, _credit: credit })
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

fn try_prepaid_box_with<T>(
    invoice: ReadCredit,
    make: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<(Box<T>, ReadCredit)> {
    let layout = Layout::new::<T>();
    if invoice.bytes() != layout.size() {
        // Captured owners are destroyed while the original invoice is live.
        drop(make);
        drop(invoice);
        return Err(crate::Error::Other("boxed payload invoice differs from layout".into()));
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
            return Err(crate::Error::Other("cannot allocate admitted boxed payload".into()));
        };
        block
    };
    let guard = ReadBoxInitGuard { block, layout, invoice };
    let payload = make()?;
    let guard = ManuallyDrop::new(guard);
    // SAFETY: initialize once and immediately transfer storage to Box. Neither
    // this write nor ownership transfer invokes user code or allocates.
    unsafe {
        block.as_ptr().write(payload);
        Ok((Box::from_raw(block.as_ptr()), std::ptr::read(&guard.invoice)))
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
        Ok(Self { payload, _credit: credit })
    }

    #[doc(hidden)]
    pub fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self._credit.is_funded_by(owner)
    }
}
impl<'a, T> std::future::Future for ReadOwnedFuture<'a, T> {
    type Output = T;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<T> {
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
        Self { block: self.block, retain: self.retain, release: self.release, wake: self.wake }
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
        let shared = ManuallyDrop::new(Self { block: block.cast::<ReadSharedBlock<T>>() });
        // Retain exactly one count without invoking any T code or allocator.
        let _retained = ManuallyDrop::new(ReadShared::clone(&shared));
    }
    unsafe fn release_native_wake_lease(block: NonNull<()>) {
        drop(Self { block: block.cast::<ReadSharedBlock<T>>() });
    }
    unsafe fn wake_native_wake_lease(block: NonNull<()>) {
        let shared = ManuallyDrop::new(Self { block: block.cast::<ReadSharedBlock<T>>() });
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
        Self { block: unsafe { NonNull::new_unchecked(data as *mut ReadSharedBlock<T>) } }
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
    const EMPTY_INVOICE: Invoice = Invoice { bytes: 0, operation: "", reserve: 0, refund: 0, cell: usize::MAX, allocation_attempted: false };
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
    const EMPTY_PHYSICAL: Physical = Physical { pointer: 0, size: 0, align: 0, alloc: 0, free: 0, before: 0, after: 0, parent: 0 };
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
            Self { active: false, spy: 0, parent: 0, tick: 0, invoices: [EMPTY_INVOICE; 256], physical: [EMPTY_PHYSICAL; 256], ni: 0, np: 0, null: None, nulls: 0, violations: 0 }
        }
        fn step(&mut self) -> usize { self.tick += 1; self.tick }
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
            if !k.active { return unsafe { System.alloc(layout) }; }
            // Only an actual pending invoice admits a watched allocation.
            let pending = (0..k.ni).rev().find(|&i| k.invoices[i].bytes == layout.size() && k.invoices[i].refund == 0 && k.invoices[i].cell == usize::MAX && !k.invoices[i].allocation_attempted);
            let Some(i) = pending else { return unsafe { System.alloc(layout) }; };
            k.invoices[i].allocation_attempted = true;
            if k.null == Some(layout) {
                k.null = None;
                k.nulls += 1;
                return std::ptr::null_mut();
            }
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                let n = k.np;
                if n == k.physical.len() { std::process::abort(); }
                let alloc = k.step();
                k.physical[n] = Physical { pointer: pointer as usize, size: layout.size(), align: layout.align(), alloc, parent: k.parent, ..EMPTY_PHYSICAL };
                k.invoices[i].cell = n;
                k.np += 1;
            }
            pointer
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            let mut k = KERNEL.lock().unwrap();
            let n = if k.active { (0..k.np).find(|&n| k.physical[n].pointer == pointer as usize && k.physical[n].free == 0) } else { None };
            if let Some(n) = n {
                let before = k.used();
                let required: usize = k.physical[..k.np].iter().filter(|p| p.free == 0).map(|p| p.size).sum();
                if before < required || layout.size() != k.physical[n].size || layout.align() != k.physical[n].align { k.violations += 1; }
                unsafe { System.dealloc(pointer, layout) };
                let after = k.used();
                let free = k.step();
                k.physical[n].before = before;
                k.physical[n].after = after;
                k.physical[n].free = free;
                if before != after { k.violations += 1; }
            } else { unsafe { System.dealloc(pointer, layout) }; }
        }
    }
    #[derive(Debug)]
    pub(crate) struct Spy {
        pub(crate) used: AtomicUsize,
        refuse: Mutex<Option<&'static str>>,
        pub(crate) payload_drops: AtomicUsize,
    }
    impl ReadMemoryBudget for Spy {
        fn try_reserve(&self, bytes: usize, _: &'static str, operation: &'static str, _: &'static str) -> Result<()> {
            if *self.refuse.lock().unwrap() == Some(operation) { return Err(crate::Error::Other("deliberate admission refusal".into())); }
            self.used.fetch_add(bytes, Ordering::SeqCst);
            let mut k = KERNEL.lock().unwrap();
            if k.active && k.spy == self as *const Self as usize && bytes != 0 {
                let n = k.ni;
                if n == k.invoices.len() { std::process::abort(); }
                let reserve = k.step();
                k.invoices[n] = Invoice { bytes, operation, reserve, ..EMPTY_INVOICE };
                k.ni += 1;
            }
            Ok(())
        }
        fn release(&self, bytes: usize) {
            let mut k = KERNEL.lock().unwrap();
            if k.active && k.spy == self as *const Self as usize && bytes != 0 {
                // Prefer the physically freed invoice. Pending admissions have
                // no cell (refusal/null); no synthetic prepaid charge is used.
                let n = (0..k.ni).find(|&i| { let r = k.invoices[i]; r.bytes == bytes && r.refund == 0 && r.cell != usize::MAX && k.physical[r.cell].free != 0 })
                    .or_else(|| (0..k.ni).rev().find(|&i| k.invoices[i].bytes == bytes && k.invoices[i].refund == 0));
                if let Some(n) = n { let refund = k.step(); k.invoices[n].refund = refund; } else { k.violations += 1; }
            }
            let old = self.used.fetch_sub(bytes, Ordering::SeqCst);
            if old < bytes { std::process::abort(); }
        }
    }
    #[derive(Debug)]
    struct Provider { spy: Arc<Spy>, panic_drop: bool }
    impl ReadMemoryBudget for Provider {
        fn try_reserve(&self, bytes: usize, subsystem: &'static str, operation: &'static str, hint: &'static str) -> Result<()> { self.spy.try_reserve(bytes, subsystem, operation, hint) }
        fn release(&self, bytes: usize) { self.spy.release(bytes); }
    }
    impl Drop for Provider {
        fn drop(&mut self) {
            self.spy.payload_drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_drop { panic!("native provider payload panic"); }
        }
    }
    pub(crate) struct Watch { pub(crate) spy: Arc<Spy>, parent: ReadMemoryOwner, row: &'static str }
    impl Watch {
        pub(crate) fn new(row: &'static str) -> Self {
            let spy = Arc::new(Spy { used: AtomicUsize::new(0), refuse: Mutex::new(None), payload_drops: AtomicUsize::new(0) });
            let parent = ReadMemoryOwner::from_legacy(spy.clone());
            let mut k = KERNEL.lock().unwrap();
            assert!(!k.active);
            *k = Kernel::new();
            k.active = true; k.spy = Arc::as_ptr(&spy) as usize; k.parent = parent.accounting_identity();
            Self { spy, parent, row }
        }
        pub(crate) fn allocating<T>(&self, f: impl FnOnce() -> T) -> T {
            struct Restore(bool);
            impl Drop for Restore { fn drop(&mut self) { CAPTURE.with(|c| c.set(self.0)); } }
            let _restore = Restore(CAPTURE.with(|c| c.replace(true)));
            f()
        }
        pub(crate) fn refuse(&self, op: Option<&'static str>) { *self.spy.refuse.lock().unwrap() = op; }
        pub(crate) fn null(&self, layout: Layout) { KERNEL.lock().unwrap().null = Some(layout); }
        pub(crate) fn native(&self, panic_drop: bool) -> ReadMemoryOwner {
            self.allocating(|| {
                let invoice = ReadCredit::try_new_owned(self.parent.clone(), ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(), "reader", "native provider", "original parent invoice").unwrap();
                assert!(invoice.is_funded_by(&self.parent));
                let owner = ReadMemoryOwner::try_native(Provider { spy: self.spy.clone(), panic_drop }, invoice).unwrap();
                if let OwnerKind::Native(n) = &owner.0 {
                    // The strong owner retains this exact typed block.
                    let block = unsafe { n.block.cast::<NativeReadBlock<Provider, ReadCredit>>().as_ref() };
                    assert!(block.invoice.is_funded_by(&self.parent));
                    assert_eq!(block.invoice.bytes(), ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size());
                    assert!(Arc::ptr_eq(&block.payload.spy, &self.spy));
                } else { panic!("native provider must use its original native allocation"); }
                owner
            })
        }
        pub(crate) fn assert_shared<T>(&self, shared: &ReadShared<T>, owner: &ReadMemoryOwner) {
            let block = unsafe { shared.block.as_ref() };
            assert!(block.invoice.is_funded_by(owner));
            assert_eq!(block.invoice.bytes(), ReadShared::<T>::layout().size());
            let k = KERNEL.lock().unwrap();
            let observed = k.physical[..k.np].iter().any(|p| p.pointer == shared.block.as_ptr() as usize && p.size == ReadShared::<T>::layout().size() && p.align == ReadShared::<T>::layout().align());
            drop(k); assert!(observed);
        }
        pub(crate) fn assert_callback<F>(&self, callback: &NativeCancellationCallback, owner: &ReadMemoryOwner) {
            // The caller supplies the same F just registered through the real
            // generic production constructor; the callback handle retains it.
            let block = unsafe { callback.block.cast::<ReadSharedBlock<F>>().as_ref() };
            assert!(block.invoice.is_funded_by(owner));
            assert_eq!(block.invoice.bytes(), ReadShared::<F>::layout().size());
            let k = KERNEL.lock().unwrap();
            let observed = k.physical[..k.np].iter().any(|p| p.pointer == callback.block.as_ptr() as usize && p.size == ReadShared::<F>::layout().size() && p.align == ReadShared::<F>::layout().align());
            drop(k); assert!(observed);
        }
        pub(crate) fn cells(&self) -> usize { KERNEL.lock().unwrap().np }
        pub(crate) fn frees(&self) -> usize { let k = KERNEL.lock().unwrap(); k.physical[..k.np].iter().filter(|p| p.free != 0).count() }
        pub(crate) fn finish(&self) {
            assert_eq!(self.spy.used.load(Ordering::SeqCst), 0, "original parent must settle exactly");
            let (invoices, physical, ni, np, nulls, violations) = { let k = KERNEL.lock().unwrap(); (k.invoices, k.physical, k.ni, k.np, k.nulls, k.violations) };
            for (i, p) in physical[..np].iter().enumerate() {
                let r = invoices[..ni].iter().find(|r| r.cell == i).unwrap();
                assert_eq!(p.parent, self.parent.accounting_identity());

                assert_eq!(p.before, p.after);
                // Parsed by the evidence gate, not a production diagnostic.
                println!("ALLOCATOR_ROW row={:?} pointer={} size={} align={} parent={} operation={:?} reserve={} alloc={} free={} refund={} debit_before={} debit_after={}", self.row, p.pointer, p.size, p.align, p.parent, r.operation, r.reserve, p.alloc, p.free, r.refund, p.before, p.after);
            }
            for r in &invoices[..ni] { assert!(r.refund > r.reserve); }
            println!("PROOF_ROW row={:?} invoices={} allocations={} physical_frees={} nulls={} settled=0", self.row, ni, np, physical[..np].iter().filter(|p| p.free != 0).count(), nulls);
            assert_eq!(violations, 0, "original invoices must remain debited through System.dealloc");
            for r in &invoices[..ni] { if r.cell != usize::MAX { let p = physical[r.cell]; assert!(r.reserve < p.alloc && p.alloc < p.free && p.free < r.refund, "physical free must precede original invoice refund: {}", r.operation); } }
        }
    }
    impl Drop for Watch {
        fn drop(&mut self) { let mut k = KERNEL.lock().unwrap_or_else(|p| p.into_inner()); k.active = false; }
    }
    #[derive(Debug)]
    struct Payload { spy: Arc<Spy>, panic_drop: bool }
    impl Drop for Payload {
        fn drop(&mut self) { self.spy.payload_drops.fetch_add(1, Ordering::SeqCst); if self.panic_drop { panic!("shared payload panic"); } }
    }
    #[test]
    fn native_shared_control_ownership_rows() {
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        for panic_drop in [false, true] {
            let w = Watch::new(if panic_drop { "owner-payload-panic" } else { "owner-concurrent-clones" });
            let owner = w.native(panic_drop);
            let identity = owner.accounting_identity();
            std::thread::scope(|s| { for _ in 0..4 { let c = owner.clone(); s.spawn(move || { let d = c.clone(); assert_eq!(d.accounting_identity(), identity); assert!(c.same_owner(&d)); drop(c); drop(d); }); } });
            assert_eq!(w.frees(), 0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
            assert_eq!(result.is_err(), panic_drop);
            assert_eq!(w.spy.payload_drops.load(Ordering::SeqCst), 1);
            w.finish();
        }
        for panic_drop in [false, true] {
            let w = Watch::new(if panic_drop { "shared-payload-panic" } else { "shared-concurrent-clones" });
            let owner = w.native(false);
            let shared = w.allocating(|| {
                let invoice = ReadCredit::try_new_owned(owner.clone(), ReadShared::<Payload>::layout().size(), "reader", "shared payload", "admit actual layout").unwrap();
                assert!(invoice.is_funded_by(&owner));
                ReadShared::try_from_prepaid(Payload { spy: w.spy.clone(), panic_drop }, invoice).unwrap()
            });
            w.assert_shared(&shared, &owner);
            let ptr = shared.block.as_ptr() as usize;
            std::thread::scope(|s| { for _ in 0..4 { let c = shared.clone(); s.spawn(move || { let d = c.clone(); assert_eq!(d.block.as_ptr() as usize, ptr); drop(c); drop(d); }); } });
            drop(owner);
            assert_eq!(w.frees(), 0);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(shared)));
            assert_eq!(result.is_err(), panic_drop);
            assert_eq!(w.spy.payload_drops.load(Ordering::SeqCst), 2);
            w.finish();
        }
        for owner_null in [false, true] {
            let w = Watch::new(if owner_null { "owner-null-allocation" } else { "shared-null-allocation" });
            if owner_null {
                w.null(ReadMemoryOwner::native_layout::<Provider, ReadCredit>());
                let result = w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(w.parent.clone(), ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(), "reader", "native provider", "original invoice").unwrap();
                    ReadMemoryOwner::try_native(Provider { spy: w.spy.clone(), panic_drop: false }, invoice)
                });
                assert!(result.is_err()); assert_eq!(w.cells(), 0);
            } else {
                let owner = w.native(false);
                w.null(ReadShared::<Payload>::layout());
                let result = w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(owner.clone(), ReadShared::<Payload>::layout().size(), "reader", "shared payload", "actual layout").unwrap();
                    ReadShared::try_from_prepaid(Payload { spy: w.spy.clone(), panic_drop: false }, invoice)
                });
                assert!(result.is_err()); assert_eq!(w.cells(), 1); drop(owner);
            }
            w.finish();
        }
        for owner_null in [false, true] {
            let w = Watch::new(if owner_null { "owner-null-payload-panic" } else { "shared-null-payload-panic" });
            if owner_null {
                w.null(ReadMemoryOwner::native_layout::<Provider, ReadCredit>());
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(w.parent.clone(), ReadMemoryOwner::native_layout::<Provider, ReadCredit>().size(), "reader", "native provider", "original invoice").unwrap();
                    ReadMemoryOwner::try_native(Provider { spy: w.spy.clone(), panic_drop: true }, invoice)
                })));
                assert!(result.is_err()); assert_eq!(w.cells(), 0);
            } else {
                let owner = w.native(false);
                w.null(ReadShared::<Payload>::layout());
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.allocating(|| {
                    let invoice = ReadCredit::try_new_owned(owner.clone(), ReadShared::<Payload>::layout().size(), "reader", "shared payload", "actual layout").unwrap();
                    ReadShared::try_from_prepaid(Payload { spy: w.spy.clone(), panic_drop: true }, invoice)
                })));
                assert!(result.is_err()); assert_eq!(w.cells(), 1); drop(owner);
            }
            w.finish();
        }
        let w = Watch::new("shared-mismatched-invoice");
        let owner = w.native(false);
        let layout = ReadShared::<Payload>::layout();
        let invoice = ReadCredit::try_new_owned(owner.clone(), layout.size() + layout.align(), "reader", "mismatched invoice", "refuse before allocation").unwrap();
        assert!(w.allocating(|| ReadShared::try_from_prepaid(Payload { spy: w.spy.clone(), panic_drop: false }, invoice)).is_err());
        assert_eq!(w.cells(), 1); drop(owner); w.finish();
    }
}
