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

#[doc(hidden)]
pub trait ReadMemoryBudget: Debug + Send + Sync {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()>;
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
        reserve: Self::reserve,
        release: Self::release,
        clone_owner: Self::clone_owner,
        drop_owner: Self::drop_owner,
    };
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
        let bytes = self
            .bytes
            .checked_add(added)
            .ok_or_else(|| crate::Error::Other("reader charge overflow".into()))?;
        self.budget.try_reserve(added, subsystem, operation, hint)?;
        self.bytes = bytes;
        Ok(())
    }

    pub fn try_shrink(&mut self, returned: usize) -> Result<()> {
        let bytes = self
            .bytes
            .checked_sub(returned)
            .ok_or_else(|| crate::Error::Other("reader charge release exceeds credit".into()))?;
        self.budget.release(returned);
        self.bytes = bytes;
        Ok(())
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

impl Drop for ReadCredit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
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
