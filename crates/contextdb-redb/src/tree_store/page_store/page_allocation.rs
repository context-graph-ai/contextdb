//! Concrete, weakless owners for the legacy page cache. No byte/control Arc can
//! escape independently of the payer. Native copies retain their separate owner.
use crate::atomic_compat::AtomicUpdate as _;
use crate::read_admission::{ReadMemoryCharge, admit_read_memory};
use crate::{Result, StorageError};
use std::alloc::{Layout, alloc, dealloc};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};

fn allocation_failed(site: &'static str, layout: Layout) -> StorageError {
    crate::native_storage_admission::native_storage_error(
        crate::native_storage_admission::NativeColdReadError::AllocatorFailed { site, layout },
    )
}

fn retain(owners: &AtomicUsize) {
    owners
        .fetch_update_compat(Ordering::Relaxed, Ordering::Relaxed, |n| {
            n.checked_add(1).filter(|n| isize::try_from(*n).is_ok())
        })
        .expect("cache allocation strong count overflow");
}

/// The counter itself has no Weak lifetime. Its genuine admission receipt, when
/// installed, is held outside its destruction and physical deallocation too.
#[derive(Clone, Copy)]
struct CacheSettlement {
    context: NonNull<()>,
    after_free: unsafe fn(NonNull<()>, &AtomicUsize),
}
// SAFETY: the installing producer guarantees a Send + Sync context whose owner
// is retained by this same counter's fixed-reserve receipt.
unsafe impl Send for CacheSettlement {}
unsafe impl Sync for CacheSettlement {}

struct CounterBlock {
    owners: AtomicUsize,
    value: AtomicUsize,
    release_generation: AtomicU64,
    dynamic_settlement: AtomicBool,
    settlement: Mutex<Option<CacheSettlement>>,
    invoice: Option<ReadMemoryCharge>,
    preparation: Option<crate::native_storage_admission::NativeStorageCredit>,
    reserve: Mutex<Option<crate::native_storage_admission::NativeStorageCredit>>,
}
pub(crate) struct CacheCounter {
    block: NonNull<CounterBlock>,
}
// SAFETY: the only shared mutation is atomic; the invoice is Send + Sync and
// accessible only to the last owner. No mutable/raw control or Weak is exposed.
unsafe impl Send for CacheCounter {}
unsafe impl Sync for CacheCounter {}
impl CacheCounter {
    #[cfg(feature = "test-seams")]
    pub(super) fn allocation_for_test(&self) -> (usize, usize, usize) {
        let layout = Self::layout();
        (self.block.as_ptr() as usize, layout.size(), layout.align())
    }
    #[cfg(feature = "test-seams")]
    pub(super) fn preparation_for_test(&self) -> usize {
        unsafe { self.block.as_ref() }
            .preparation
            .as_ref()
            .map_or(0, |p| p.identity_for_test())
    }

    #[cfg(feature = "test-seams")]
    pub(super) fn observation_for_test(&self) -> (usize, usize) {
        (self.block.as_ptr() as usize, self.load(Ordering::SeqCst))
    }

    pub(super) fn release_generation(&self) -> u64 {
        unsafe { self.block.as_ref() }
            .release_generation
            .load(Ordering::Acquire)
    }
    fn note_external_release(&self) {
        unsafe { self.block.as_ref() }
            .release_generation
            .fetch_update_compat(Ordering::Release, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .expect("cache release generation exhausted");
    }
    fn settle_after_free(&self) {
        let block = unsafe { self.block.as_ref() };
        if !block.dynamic_settlement.load(Ordering::Acquire) {
            return;
        }
        let callback = *block
            .settlement
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(callback) = callback {
            // This CachePayment still owns the counter, and the counter still
            // owns its fixed-reserve receipt, throughout the callback.
            unsafe { (callback.after_free)(callback.context, &block.value) };
        }
    }

    pub(super) fn has_fixed_reserve(&self) -> bool {
        unsafe { self.block.as_ref() }
            .reserve
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
    pub(super) fn layout() -> Layout {
        Layout::new::<CounterBlock>()
    }
    pub(super) fn new() -> Result<Self> {
        let layout = Self::layout();
        let preparation = crate::read_admission::cache_preparation_credit();
        let invoice = if preparation.is_some() {
            crate::read_admission::prepare_cache_allocation(layout)?;
            None
        } else {
            admit_read_memory(layout.size())?
        };
        let Some(block) = NonNull::new(unsafe { alloc(layout) }).map(NonNull::cast::<CounterBlock>)
        else {
            // No backing was created. Returning the actual allocator fact does
            // not allocate a diagnostic Box or reinterpret a caller refusal.
            return Err(allocation_failed("cache occupancy control", layout));
        };
        unsafe {
            block.as_ptr().write(CounterBlock {
                owners: AtomicUsize::new(1),
                value: AtomicUsize::new(0),
                release_generation: AtomicU64::new(1),
                dynamic_settlement: AtomicBool::new(false),
                settlement: Mutex::new(None),
                invoice,
                preparation,
                reserve: Mutex::new(None),
            });
        }
        Ok(Self { block })
    }
}
impl Deref for CacheCounter {
    type Target = AtomicUsize;
    fn deref(&self) -> &Self::Target {
        &unsafe { self.block.as_ref() }.value
    }
}
impl Clone for CacheCounter {
    fn clone(&self) -> Self {
        retain(&unsafe { self.block.as_ref() }.owners);
        Self { block: self.block }
    }
}
struct CounterFree {
    block: NonNull<CounterBlock>,
    _invoice: Option<ReadMemoryCharge>,
    _preparation: Option<crate::native_storage_admission::NativeStorageCredit>,
    _reserve: Option<crate::native_storage_admission::NativeStorageCredit>,
}
impl Drop for CounterFree {
    fn drop(&mut self) {
        unsafe { dealloc(self.block.as_ptr().cast(), CacheCounter::layout()) };
        // Rust drops _invoice after the block was physically freed.
    }
}
impl Drop for CacheCounter {
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
        let _free = CounterFree {
            block: self.block,
            _invoice: block.invoice.take(),
            _preparation: block.preparation.take(),
            _reserve: block
                .reserve
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        };
        unsafe { std::ptr::drop_in_place(self.block.as_ptr()) };
    }
}

/// Occupancy of one complete physical backing. Cached backing uses the fixed
/// reserve; a source-funded write keeps its genuine receipt beside this tally
/// when that reserve has no room. The tally is not an additional source invoice.
/// Its parent stays alive until the backing has been physically freed.
pub(super) struct CachePayment {
    counter: CacheCounter,
    bytes: usize,
}
impl CachePayment {
    /// Caller reserves the complete Layout under the existing cache locks.
    pub(super) fn reserve(counter: &CacheCounter, bytes: usize) -> Result<(usize, Self)> {
        // Clone before changing the count; a strong-count overflow cannot
        // strand an occupancy increment without its rollback owner.
        let owner = counter.clone();
        let previous = counter
            .fetch_update_compat(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
            })
            .map_err(|_| StorageError::ValueTooLarge(bytes))?;
        Ok((
            previous,
            Self {
                counter: owner,
                bytes,
            },
        ))
    }
    pub(super) fn counter_is(&self, counter: &CacheCounter) -> bool {
        self.counter.block == counter.block
    }
    fn has_fixed_reserve(&self) -> bool {
        self.counter.has_fixed_reserve()
    }
}
impl Drop for CachePayment {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
        self.counter.settle_after_free();
    }
}

enum PagePayer {
    // Field is owned for its destructor, never inspected or substituted.
    Source(ReadMemoryCharge),
    // Beyond the fixed reserve, the actual source receipt pays for this whole
    // write backing. Occupancy remains visible and blocks further cache growth;
    // it is not a second invoice against the fixed reserve.
    SourceCounted {
        // Owned for its destructor; never downcast, substituted or re-invoiced.
        charge: ReadMemoryCharge,
        occupancy: CachePayment,
    },
    Cache(CachePayment),
    // Legacy non-retaining admission / unscoped standalone backing remains
    // explicitly unqualified; it never masquerades as a genuine source receipt.
    Unscoped,
}
struct PageBlock {
    owners: AtomicUsize,
    #[cfg(feature = "contextdb-internal")]
    request: u64,
    // A valid allocation Layout cannot reach isize::MAX bytes. Its unused
    // high length bit records native construction without growing the header.
    len_and_origin: usize,
    checksum: Mutex<Option<u128>>,
    payer: Mutex<Option<PagePayer>>,
}

impl PageBlock {
    fn len(&self) -> usize {
        self.len_and_origin & (isize::MAX as usize)
    }
}

/// Exactly one allocation: reference control, checksum, payer and inline bytes.
/// A clone always retains that complete allocation; no Weak or byte-only owner.
pub(crate) struct PageAllocation {
    block: NonNull<PageBlock>,
}
// SAFETY: initialized bytes are immutable while shared. Mutation requires an
// exclusive handle AND strong count one; no Weak/raw owner can create a handle.
// Checksum/payer transitions use their mutexes. Both possible genuine payers are
// Send + Sync; last release uses Release/Acquire before exclusive destruction.
unsafe impl Send for PageAllocation {}
unsafe impl Sync for PageAllocation {}
impl PageAllocation {
    pub(super) fn layout(
        len: usize,
    ) -> std::result::Result<(Layout, usize), std::alloc::LayoutError> {
        Layout::new::<PageBlock>()
            .extend(Layout::array::<u8>(len)?)
            .map(|(layout, offset)| (layout.pad_to_align(), offset))
    }
    pub(super) fn quote(len: usize) -> usize {
        Self::layout(len).map_or(usize::MAX, |(layout, _)| layout.size())
    }
    pub(super) fn zeroed(len: usize, charge: Option<ReadMemoryCharge>) -> Result<Self> {
        let (layout, offset) = Self::layout(len).map_err(|_| StorageError::ValueTooLarge(len))?;
        Self::allocate(
            len,
            layout,
            offset,
            charge.map_or(PagePayer::Unscoped, PagePayer::Source),
        )
    }
    #[cfg(feature = "contextdb-internal")]
    pub(super) fn zeroed_native(
        len: usize,
    ) -> std::result::Result<Self, crate::NativeColdReadError> {
        use crate::native_storage_admission::{
            NativeColdFault, NativeColdReadError, admit_native_storage,
        };
        let (layout, _) = Self::layout(len).map_err(|_| {
            NativeColdReadError::Corrupt(NativeColdFault::new("native page layout overflow"))
        })?;
        let credit = admit_native_storage(layout)?;
        let mut page = Self::zeroed(
            len,
            Some(ReadMemoryCharge::Native(credit.into_page_charge())),
        )
        .map_err(|error| match error {
            StorageError::Native(error) => error,
            _ => unreachable!("checked native page layout"),
        })?;
        // The unique, unpublished allocation was paid by the native producer.
        unsafe { page.block.as_mut() }.len_and_origin |= !(isize::MAX as usize);
        Ok(page)
    }
    #[cfg(feature = "contextdb-internal")]
    pub(super) fn is_native(&self) -> bool {
        (unsafe { self.block.as_ref() }.len_and_origin & !(isize::MAX as usize)) != 0
    }

    fn allocate(len: usize, layout: Layout, offset: usize, payer: PagePayer) -> Result<Self> {
        let Some(block) = NonNull::new(unsafe { alloc(layout) }).map(NonNull::cast::<PageBlock>)
        else {
            return Err(allocation_failed("cache page allocation", layout));
        };
        // All operations after alloc are infallible, with no user callback.
        unsafe {
            block.as_ptr().write(PageBlock {
                owners: AtomicUsize::new(1),
                #[cfg(feature = "contextdb-internal")]
                request: crate::read_admission::cache_request(),
                len_and_origin: len,
                checksum: Mutex::new(None),
                payer: Mutex::new(Some(payer)),
            });
            std::ptr::write_bytes(block.as_ptr().cast::<u8>().add(offset), 0, len);
        }
        Ok(Self { block })
    }
    // Snapshot the CURRENT payment under its real transition lock. This is
    // observation only: it retains no payer and changes no cache state.
    #[cfg(feature = "test-seams")]
    pub(super) fn payment_for_test(&self) -> (&'static str, usize, usize) {
        let slot = unsafe { self.block.as_ref() }
            .payer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match &*slot {
            Some(PagePayer::Cache(payment)) => (
                "cache",
                payment.counter.block.as_ptr() as usize,
                payment.bytes,
            ),
            Some(PagePayer::SourceCounted { occupancy, .. }) => (
                "source_counted",
                occupancy.counter.block.as_ptr() as usize,
                occupancy.bytes,
            ),
            Some(PagePayer::Source(_)) => ("source", 0, 0),
            Some(PagePayer::Unscoped) => ("unscoped", 0, 0),
            None => ("missing", 0, 0),
        }
    }
    #[cfg(any(test, feature = "contextdb-internal"))]
    pub(super) fn checksum(&self) -> &Mutex<Option<u128>> {
        &unsafe { self.block.as_ref() }.checksum
    }
    pub(super) fn is_charged(&self) -> bool {
        matches!(
            *unsafe { self.block.as_ref() }
                .payer
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            Some(PagePayer::Source(_) | PagePayer::SourceCounted { .. })
        )
    }
    pub(super) fn counted_by(&self, counter: &CacheCounter) -> bool {
        match &*unsafe { self.block.as_ref() }
            .payer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
        {
            Some(
                PagePayer::Cache(payment)
                | PagePayer::SourceCounted {
                    occupancy: payment, ..
                },
            ) => payment.counter_is(counter),
            _ => false,
        }
    }
    /// Only backing that fits the fixed reserve relinquishes its genuine
    /// source receipt. Overlap/oversized backing keeps that original receipt
    /// and a physical occupancy tally until free, including across flush.
    pub(super) fn transfer_counted(&self, payment: CachePayment, fixed_room: bool) {
        // Capacity is not a receipt. Before the actual engine parent is
        // installed (or on an unqualified standalone backend), keep the true
        // source charge instead of refunding it into an unowned cache reserve.
        let fixed_room = fixed_room && payment.has_fixed_reserve();
        let mut slot = unsafe { self.block.as_ref() }
            .payer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let previous = slot.take();
        let refund = match previous {
            Some(PagePayer::Source(charge)) if !fixed_room => {
                *slot = Some(PagePayer::SourceCounted {
                    charge,
                    occupancy: payment,
                });
                None
            }
            Some(PagePayer::SourceCounted { charge, occupancy }) if !fixed_room => {
                *slot = Some(PagePayer::SourceCounted {
                    charge,
                    occupancy: payment,
                });
                // The destination tally was reserved before detaching this
                // old one. Preserve the same actual source receipt throughout.
                Some(PagePayer::Cache(occupancy))
            }
            previous => {
                *slot = Some(PagePayer::Cache(payment));
                previous
            }
        };
        drop(slot);
        drop(refund);
    }
    /// The new claim is already reserved. Replace before dropping the old
    /// genuine payer, so every alias stays continuously covered during transfer.
    pub(super) fn transfer(&self, payment: CachePayment) {
        self.transfer_counted(payment, true);
    }
    #[cfg(feature = "contextdb-internal")]
    pub(super) fn belongs_to_request(&self, request: u64) -> bool {
        request != 0 && unsafe { self.block.as_ref() }.request == request
    }
    pub(crate) fn strong_count(this: &Self) -> usize {
        unsafe { this.block.as_ref() }
            .owners
            .load(Ordering::Acquire)
    }
    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        left.block == right.block
    }
    pub(crate) fn as_ptr(this: &Self) -> *const () {
        this.block.as_ptr().cast()
    }
    pub(super) fn get_mut(this: &mut Self) -> Option<&mut [u8]> {
        if Self::strong_count(this) != 1 {
            return None;
        }
        let block = unsafe { this.block.as_mut() };
        *block.checksum.get_mut().unwrap_or_else(|e| e.into_inner()) = None;
        let (_, offset) = Self::layout(block.len()).expect("live page has a valid Layout");
        Some(unsafe {
            std::slice::from_raw_parts_mut(
                this.block.as_ptr().cast::<u8>().add(offset),
                block.len(),
            )
        })
    }
}
impl Deref for PageAllocation {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let len = unsafe { self.block.as_ref() }.len();
        let (_, offset) = Self::layout(len).expect("live page has a valid Layout");
        unsafe { std::slice::from_raw_parts(self.block.as_ptr().cast::<u8>().add(offset), len) }
    }
}
impl Clone for PageAllocation {
    fn clone(&self) -> Self {
        retain(&unsafe { self.block.as_ref() }.owners);
        Self { block: self.block }
    }
}
struct PageFree {
    block: NonNull<PageBlock>,
    layout: Layout,
    _payer: Option<PagePayer>,
}
impl Drop for PageFree {
    fn drop(&mut self) {
        unsafe { dealloc(self.block.as_ptr().cast(), self.layout) };
        // Only after physical free does Rust drop the actual source receipt or
        // refund occupancy and release its counter owner. Also runs on unwind.
    }
}
impl Drop for PageAllocation {
    fn drop(&mut self) {
        // Retain the actual occupancy control while this page handle is
        // still owned. After decrement another thread may free the page, so
        // no page field may be touched while publishing the release notice.
        let counter = {
            let payer = unsafe { self.block.as_ref() }
                .payer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match &*payer {
                Some(
                    PagePayer::Cache(payment)
                    | PagePayer::SourceCounted {
                        occupancy: payment, ..
                    },
                ) => Some(payment.counter.clone()),
                _ => None,
            }
        };
        let previous = unsafe { self.block.as_ref() }
            .owners
            .fetch_sub(1, Ordering::Release);
        if previous == 2
            && let Some(counter) = &counter
        {
            counter.note_external_release();
        }
        if previous != 1 {
            return;
        }
        fence(Ordering::Acquire);
        let block = unsafe { self.block.as_mut() };
        let (layout, _) = Self::layout(block.len()).expect("live page has a valid Layout");
        let payer = block
            .payer
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let _free = PageFree {
            block: self.block,
            layout,
            _payer: payer,
        };
        unsafe { std::ptr::drop_in_place(self.block.as_ptr()) };
    }
}

impl std::fmt::Debug for PageAllocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageAllocation")
            .field("bytes", &self.len())
            .field("owners", &Self::strong_count(self))
            .finish_non_exhaustive()
    }
}

/// Private engine/fork custody seam. Clones retain the actual occupancy controls,
/// including retired pages, rather than manufacturing a new reserve invoice.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
#[derive(Clone)]
pub struct CacheReserveOwner {
    read: CacheCounter,
    write: CacheCounter,
}
#[cfg(feature = "contextdb-internal")]
impl CacheReserveOwner {
    /// Settle excess reader-reserve capacity after physical page free.
    ///
    /// # Safety
    /// `context` must be a Send + Sync object owned by the reserve receipt
    /// installed in this owner. It must remain valid through that receipt's
    /// last release. The callback may refund unused funding but must not assume
    /// a statement or database handle is still alive, or access the cache.
    pub unsafe fn set_read_settlement(
        &self,
        context: NonNull<()>,
        after_free: unsafe fn(NonNull<()>, &AtomicUsize),
    ) {
        let block = unsafe { self.read.block.as_ref() };
        let mut callback = block
            .settlement
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(callback.is_none(), "cache settlement is installed once");
        *callback = Some(CacheSettlement {
            context,
            after_free,
        });
        block.dynamic_settlement.store(true, Ordering::Release);
    }

    #[cfg(feature = "test-seams")]
    pub fn control_layout_for_test(&self) -> (usize, usize) {
        let layout = CacheCounter::layout();
        (layout.size(), layout.align())
    }
    #[cfg(feature = "test-seams")]
    pub fn observation_for_test(&self) -> [(usize, usize); 2] {
        [
            self.read.observation_for_test(),
            self.write.observation_for_test(),
        ]
    }

    pub(super) fn install(
        read: &CacheCounter,
        write: &CacheCounter,
        reserve: crate::native_storage_admission::NativeStorageCredit,
    ) -> Self {
        for counter in [read, write] {
            let mut slot = unsafe { counter.block.as_ref() }
                .reserve
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            assert!(slot.is_none(), "cache reserve is installed once");
            *slot = Some(reserve.clone());
        }
        Self {
            read: read.clone(),
            write: write.clone(),
        }
    }
    /// Only synchronous handle construction runs in this private scope. Nested
    /// construction and unwind restore the previous actual owner without a Vec.
    pub fn with_recycled_cache<R>(&self, construct: impl FnOnce() -> R) -> R {
        struct Restore(Option<CacheReserveOwner>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let old = RECYCLED_CACHE.with(|slot| slot.replace(self.0.take()));
                drop(old);
            }
        }
        let _restore = Restore(RECYCLED_CACHE.with(|slot| slot.replace(Some(self.clone()))));
        construct()
    }
}
#[cfg(feature = "contextdb-internal")]
thread_local! {
    static RECYCLED_CACHE: std::cell::RefCell<Option<CacheReserveOwner>> = const { std::cell::RefCell::new(None) };
}
pub(super) fn cache_counters() -> Result<(CacheCounter, CacheCounter)> {
    #[cfg(feature = "contextdb-internal")]
    if let Some(owner) = RECYCLED_CACHE.with(|slot| slot.borrow().clone()) {
        return Ok((owner.read, owner.write));
    }
    Ok((CacheCounter::new()?, CacheCounter::new()?))
}

/// Exact-layout, weakless cache control. Preparation admits its full concrete
/// layout before construction; this owner follows every arena/write-map alias.
struct CacheSharedBlock<T> {
    owners: AtomicUsize,
    payload: std::mem::ManuallyDrop<T>,
    preparation: Option<crate::native_storage_admission::NativeStorageCredit>,
}
pub(crate) struct CacheShared<T> {
    block: NonNull<CacheSharedBlock<T>>,
}
// SAFETY: immutable payload access, checked atomic strong count, no Weak or
// raw accessor. Last release exclusively moves an unpinned Send + Sync payload.
unsafe impl<T: Send + Sync> Send for CacheShared<T> {}
unsafe impl<T: Send + Sync> Sync for CacheShared<T> {}
impl<T> CacheShared<T> {
    #[cfg(any(feature = "test-seams", all(test, feature = "contextdb-internal")))]
    pub(crate) fn allocation_for_test(&self) -> (usize, usize, usize) {
        let layout = Self::layout();
        (self.block.as_ptr() as usize, layout.size(), layout.align())
    }

    pub(crate) fn layout() -> Layout {
        Layout::new::<CacheSharedBlock<T>>()
    }
    /// The caller already admitted this exact layout through preparation.
    pub(crate) fn new_prepaid(payload: T) -> Result<Self> {
        let layout = Self::layout();
        let preparation = crate::read_admission::cache_preparation_credit();
        let Some(block) = NonNull::new(unsafe { alloc(layout) }.cast::<CacheSharedBlock<T>>())
        else {
            drop(payload);
            drop(preparation);
            return Err(allocation_failed("cache shared control", layout));
        };
        unsafe {
            block.as_ptr().write(CacheSharedBlock {
                owners: AtomicUsize::new(1),
                payload: std::mem::ManuallyDrop::new(payload),
                preparation,
            });
        };
        Ok(Self { block })
    }
}
impl<T> Clone for CacheShared<T> {
    fn clone(&self) -> Self {
        retain(&unsafe { self.block.as_ref() }.owners);
        Self { block: self.block }
    }
}
impl<T> Deref for CacheShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &unsafe { self.block.as_ref() }.payload
    }
}
struct CacheSharedFree<T> {
    block: NonNull<CacheSharedBlock<T>>,
    _preparation: Option<crate::native_storage_admission::NativeStorageCredit>,
}
impl<T> Drop for CacheSharedFree<T> {
    fn drop(&mut self) {
        unsafe { dealloc(self.block.as_ptr().cast(), CacheShared::<T>::layout()) };
    }
}
impl<T> Drop for CacheShared<T> {
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
        let _free = CacheSharedFree {
            block: self.block,
            _preparation: block.preparation.take(),
        };
        // Payload drop glue and physical backing free finish before the actual
        // receipt refunds, on ordinary last release and on destructor unwind.
        unsafe { std::mem::ManuallyDrop::drop(&mut block.payload) };
    }
}

/// Build exactly one already-admitted boxed array, without Vec growth or an
/// infallible allocator. Partial initialization has stack-only cleanup custody.
pub(crate) fn cache_array_prepaid<T>(
    len: usize,
    mut initialize: impl FnMut(usize) -> Result<T>,
) -> Result<Box<[T]>> {
    struct FreeRaw {
        pointer: NonNull<u8>,
        layout: Layout,
    }
    impl Drop for FreeRaw {
        fn drop(&mut self) {
            if self.layout.size() != 0 {
                unsafe { dealloc(self.pointer.as_ptr(), self.layout) };
            }
        }
    }
    struct Initializing<T> {
        pointer: NonNull<T>,
        initialized: usize,
        layout: Layout,
    }
    impl<T> Drop for Initializing<T> {
        fn drop(&mut self) {
            let _free = FreeRaw {
                pointer: self.pointer.cast(),
                layout: self.layout,
            };
            unsafe {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                    self.pointer.as_ptr(),
                    self.initialized,
                ));
            };
        }
    }
    let layout = Layout::array::<T>(len).map_err(|_| StorageError::ValueTooLarge(len))?;
    let pointer = if layout.size() == 0 {
        NonNull::<T>::dangling()
    } else {
        NonNull::new(unsafe { alloc(layout) }.cast::<T>())
            .ok_or_else(|| allocation_failed("cache index array", layout))?
    };
    let mut guard = Initializing {
        pointer,
        initialized: 0,
        layout,
    };
    for index in 0..len {
        let value = initialize(index)?;
        unsafe { pointer.as_ptr().add(index).write(value) };
        guard.initialized += 1;
    }
    std::mem::forget(guard);
    Ok(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(pointer.as_ptr(), len)) })
}
