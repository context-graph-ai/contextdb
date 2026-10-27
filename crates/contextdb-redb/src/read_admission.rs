//! Operation-scoped admission before allocating a source page. The caller
//! keeps admitted bytes owned until its read handles have been released.
use crate::StorageError;
use crate::atomic_compat::AtomicUpdate as _;
use std::{cell::RefCell, io, sync::Arc};
use std::{marker::PhantomData, rc::Rc};
type Admission = Arc<dyn Fn(usize) -> io::Result<()> + Send + Sync>;

/// A charge a caller's admission hands back for one page. The page the read
/// cache does not keep carries it outside destruction and physical free of its
/// complete backing. The private page handle has no independently escaping byte
/// allocation or Weak control. The opaque receipt is never replaced by Native
/// credit or a fabricated parent. This legacy opaque Box does not establish
/// that its own allocation remains paid through physical deallocation.
pub type ReadPageCharge = Box<dyn Send + Sync>;
type ChargingAdmission = Arc<dyn Fn(usize) -> io::Result<ReadPageCharge> + Send + Sync>;

/// Private cross-crate single owner for a concrete, movable, prepaid receipt.
/// Its payload is moved out, its exact allocation freed, and only then dropped.
/// No shared, weak, raw, or borrowed payload handle can escape this owner.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub struct OwnedReadPageCharge {
    pointer: std::ptr::NonNull<()>,
    finish: unsafe fn(std::ptr::NonNull<()>),
}
// SAFETY: construction requires the actual concrete T: Send + Sync + 'static.
// No accessor exposes T; moving the single owner transfers its sole allocation.
#[cfg(feature = "contextdb-internal")]
unsafe impl Send for OwnedReadPageCharge {}
#[cfg(feature = "contextdb-internal")]
unsafe impl Sync for OwnedReadPageCharge {}
#[cfg(feature = "contextdb-internal")]
impl OwnedReadPageCharge {
    /// The caller prepays `Layout::new::<T>()` from the receipt's ORIGINAL budget
    /// before entry and includes that charge in T. This allocates only that
    /// concrete layout. A foreign Box payload's contents are not moved/freed by
    /// this protocol and never acquire a physical-funding certificate here.
    /// This is an ownership mechanism, not a certificate of caller funding.
    /// On allocator null, T is dropped with no backing to free; the actual
    /// failed Layout is returned without constructing diagnostic backing.
    pub fn try_new_prepaid<T: Send + Sync + 'static>(value: T) -> Result<Self, std::alloc::Layout> {
        let layout = std::alloc::Layout::new::<T>();
        let pointer = if layout.size() == 0 {
            std::ptr::NonNull::<T>::dangling()
        } else {
            let Some(pointer) =
                std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) }.cast::<T>())
            else {
                return Err(layout);
            };
            pointer
        };
        // No fallible/user operation occurs after allocation and before custody.
        unsafe { pointer.as_ptr().write(value) };
        Ok(Self {
            pointer: pointer.cast(),
            finish: finish_owned_charge::<T>,
        })
    }
}
#[cfg(feature = "contextdb-internal")]
unsafe fn finish_owned_charge<T: Send + Sync + 'static>(pointer: std::ptr::NonNull<()>) {
    let pointer = pointer.cast::<T>();
    // Read MOVES the authentic receipt to this stack. It is not dropped in the
    // allocation. No user destructor runs until backing is physically freed.
    let value = unsafe { pointer.as_ptr().read() };
    let layout = std::alloc::Layout::new::<T>();
    if layout.size() != 0 {
        unsafe { std::alloc::dealloc(pointer.as_ptr().cast(), layout) };
    }
    // Even if T::drop unwinds, backing is already gone and cannot free twice.
    drop(value);
}
#[cfg(feature = "contextdb-internal")]
impl Drop for OwnedReadPageCharge {
    fn drop(&mut self) {
        unsafe { (self.finish)(self.pointer) };
    }
}

// Preserve opaque foreign callers exactly, without silently treating their Box
// as the concrete free-before-refund protocol used by the closed engine factory.
pub(crate) enum ReadMemoryCharge {
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Legacy(ReadPageCharge),
    #[cfg(feature = "contextdb-internal")]
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Native(crate::native_storage_admission::NativePageCharge),
    #[cfg(feature = "contextdb-internal")]
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Owned(OwnedReadPageCharge),
}

enum ActiveAdmission {
    #[cfg(feature = "contextdb-internal")]
    Owned(Arc<dyn Fn(usize) -> Result<OwnedReadPageCharge, StorageError> + Send + Sync>),
    Scoped(Admission),
    Charging(ChargingAdmission),
    StorageScoped(Arc<dyn Fn(usize) -> Result<(), StorageError> + Send + Sync>),
    StorageCharging(Arc<dyn Fn(usize) -> Result<ReadPageCharge, StorageError> + Send + Sync>),
}

thread_local! { static ACTIVE: RefCell<Vec<ActiveAdmission>> = const { RefCell::new(Vec::new()) }; }

struct ResetAdmission;
impl Drop for ResetAdmission {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            active.borrow_mut().pop();
        });
    }
}

/// Execute synchronous source reads with admission before each cache-miss
/// allocation. Nested operations restore the enclosing admission on unwind.
#[doc(hidden)]
pub fn with_read_memory_admission<R>(admit: Admission, operation: impl FnOnce() -> R) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Scoped(admit)));
    let _reset = ResetAdmission;
    operation()
}

/// Like [`with_read_memory_admission`], but the admission returns a charge
/// for each page. Backing transfers to the cache's fixed budget only while
/// that reserve covers it. An uncached page, or source-funded write/flush
/// overlap beyond that reserve, retains the genuine receipt until transfer
/// becomes covered or its complete backing is physically freed.
#[doc(hidden)]
pub fn with_read_page_charge<R>(admit: ChargingAdmission, operation: impl FnOnce() -> R) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Charging(admit)));
    let _reset = ResetAdmission;
    operation()
}

/// Like the legacy scoped admission, retaining a caller's typed storage refusal.
#[doc(hidden)]
pub fn with_read_memory_admission_storage<R>(
    admit: Arc<dyn Fn(usize) -> Result<(), StorageError> + Send + Sync>,
    operation: impl FnOnce() -> R,
) -> R {
    ACTIVE.with(|active| {
        active
            .borrow_mut()
            .push(ActiveAdmission::StorageScoped(admit));
    });
    let _reset = ResetAdmission;
    operation()
}

/// Like the legacy charging admission, retaining a caller's typed storage refusal.
#[doc(hidden)]
pub fn with_read_page_charge_storage<R>(
    admit: Arc<dyn Fn(usize) -> Result<ReadPageCharge, StorageError> + Send + Sync>,
    operation: impl FnOnce() -> R,
) -> R {
    ACTIVE.with(|active| {
        active
            .borrow_mut()
            .push(ActiveAdmission::StorageCharging(admit));
    });
    let _reset = ResetAdmission;
    operation()
}

/// Closed engine callback; legacy boxed callbacks keep their original wrappers.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub fn with_owned_read_page_charge_storage<R>(
    admit: Arc<dyn Fn(usize) -> Result<OwnedReadPageCharge, StorageError> + Send + Sync>,
    operation: impl FnOnce() -> R,
) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Owned(admit)));
    let _reset = ResetAdmission;
    operation()
}

#[cfg(feature = "contextdb-internal")]
pub(crate) fn has_read_memory_admission() -> bool {
    ACTIVE.with(|active| !active.borrow().is_empty())
}

pub(crate) fn admit_read_memory(bytes: usize) -> Result<Option<ReadMemoryCharge>, StorageError> {
    ACTIVE.with(|active| match active.borrow().last() {
        #[cfg(feature = "contextdb-internal")]
        Some(ActiveAdmission::Owned(admit)) => {
            admit(bytes).map(|charge| Some(ReadMemoryCharge::Owned(charge)))
        }
        Some(ActiveAdmission::Scoped(admit)) => {
            admit(bytes).map(|()| None).map_err(StorageError::from)
        }
        Some(ActiveAdmission::Charging(admit)) => admit(bytes)
            .map(|charge| Some(ReadMemoryCharge::Legacy(charge)))
            .map_err(StorageError::from),
        Some(ActiveAdmission::StorageScoped(admit)) => admit(bytes).map(|()| None),
        Some(ActiveAdmission::StorageCharging(admit)) => {
            admit(bytes).map(|charge| Some(ReadMemoryCharge::Legacy(charge)))
        }
        None => Ok(None),
    })
}

// The TLS slot owns no storage or capacity. Every linked node is heap-stable
// and kept alive by thread-bound guards, including guards returned to callers.
thread_local! {
    static SCAN_TAIL: std::cell::Cell<Option<std::ptr::NonNull<ScanScopeNode>>> = const { std::cell::Cell::new(None) };
}

/// Fallback growth for a legacy scan without declared admission. Declared
/// readers instead use the capacity-limited shared page cache.
pub(crate) const SCAN_GROWTH_ALLOWANCE: usize = 16 * 1024;

#[derive(Clone)]
enum ScanState {
    // Public legacy constructors retain their unqualified allocation behavior.
    Legacy(Arc<std::sync::atomic::AtomicUsize>),
    #[cfg(feature = "contextdb-internal")]
    Native {
        // Only immutable owned admission crosses threads. Scope links never do.
        admission: crate::NativeStorageAdmission,
    },
}

/// Shared scan state across resumed steps. Legacy scans retain their finite
/// growth allowance; native scans retain only the original admission used to
/// fund each thread-bound entry. Shared-cache capacity bounds native page reuse.
#[doc(hidden)]
#[derive(Clone)]
pub struct ScanAllowance(ScanState);

impl Default for ScanAllowance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanAllowance {
    /// A fresh legacy allowance of `SCAN_GROWTH_ALLOWANCE` (16 KiB).
    /// Its public infallible allocation remains unqualified for native funding.
    pub fn new() -> Self {
        Self(ScanState::Legacy(Arc::new(
            std::sync::atomic::AtomicUsize::new(SCAN_GROWTH_ALLOWANCE),
        )))
    }

    /// Retain the active original reader admission without allocating scan state.
    /// Each later native entry separately prepays its concrete linked-node Layout.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn try_new_native() -> Result<Self, StorageError> {
        let admission = crate::NativeStorageAdmission::retain_current()?;
        Ok(Self(ScanState::Native { admission }))
    }

    /// Enter one-pass scan reads on this thread until the guard drops.
    /// This legacy infallible entry does not certify the node's funding.
    pub fn enter(&self) -> ScanReads {
        let allowance = self.clone();
        let layout = std::alloc::Layout::new::<ScanScopeNode>();
        let pointer = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) })
            .map(std::ptr::NonNull::cast::<ScanScopeNode>);
        let Some(pointer) = pointer else {
            std::alloc::handle_alloc_error(layout);
        };
        Self::link_entry(allowance, pointer, ScanNodeInvoice::Legacy)
    }

    /// Each distinct native entry owns a separate genuinely prepaid node, even
    /// when this allowance is already active under another allowance or thread.
    /// Null/refusal returns the actual scalar native cause before linking.
    #[cfg(feature = "contextdb-internal")]
    #[doc(hidden)]
    pub fn try_enter_native(&self) -> Result<ScanReads, StorageError> {
        use crate::native_storage_admission::{admit_native_storage, native_storage_error};
        let ScanState::Native { admission } = &self.0 else {
            return Err(native_storage_error(
                crate::NativeColdReadError::AdmissionRefused,
            ));
        };
        let allowance = self.clone();
        let layout = std::alloc::Layout::new::<ScanScopeNode>();
        let invoice = crate::with_native_storage_admission(admission.clone(), || {
            admit_native_storage(layout)
        })
        .map_err(native_storage_error)?;
        let pointer = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) })
            .map(std::ptr::NonNull::cast::<ScanScopeNode>);
        let Some(pointer) = pointer else {
            // No node exists. The genuine invoice settles on this error return.
            return Err(native_storage_error(
                crate::NativeColdReadError::AllocatorFailed {
                    site: "native scan scope",
                    layout,
                },
            ));
        };
        Ok(Self::link_entry(
            allowance,
            pointer,
            ScanNodeInvoice::Native(invoice),
        ))
    }

    fn link_entry(
        allowance: Self,
        pointer: std::ptr::NonNull<ScanScopeNode>,
        invoice: ScanNodeInvoice,
    ) -> ScanReads {
        // All retains happened before allocation. No fallible/user operation
        // follows allocation before linking and thread-bound guard custody.
        SCAN_TAIL.with(|tail| {
            let previous = tail.get();
            unsafe {
                pointer.as_ptr().write(ScanScopeNode {
                    owners: std::cell::Cell::new(1),
                    previous: std::cell::Cell::new(previous),
                    next: std::cell::Cell::new(None),
                    allowance,
                    invoice,
                });
            };
            if let Some(previous) = previous {
                unsafe { previous.as_ref() }.next.set(Some(pointer));
            }
            tail.set(Some(pointer));
        });
        ScanReads {
            node: pointer,
            _thread: PhantomData,
        }
    }
}

// The receipt is moved out with the complete node and dropped only after free.
enum ScanNodeInvoice {
    Legacy,
    #[cfg(feature = "contextdb-internal")]
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    Native(crate::NativeStorageCredit),
}
struct ScanScopeNode {
    owners: std::cell::Cell<usize>,
    previous: std::cell::Cell<Option<std::ptr::NonNull<ScanScopeNode>>>,
    next: std::cell::Cell<Option<std::ptr::NonNull<ScanScopeNode>>>,
    allowance: ScanAllowance,
    #[allow(
        dead_code,
        reason = "KEEP-GUARD: held for its Drop; releases what it holds when the owner drops"
    )]
    invoice: ScanNodeInvoice,
}

/// Declared reads use the shared cache capacity; unscoped reads use the
/// fallback allowance (see [`with_scan_reads`]). A node stays linked
/// until its last guard drops; dropping an older guard cannot pop a newer entry.
#[doc(hidden)]
pub struct ScanReads {
    node: std::ptr::NonNull<ScanScopeNode>,
    _thread: PhantomData<Rc<()>>,
}

impl ScanReads {
    fn retain_current(node: std::ptr::NonNull<ScanScopeNode>) -> Self {
        // Only called with the current thread's linked live node. This new
        // guard is itself !Send + !Sync, so the Cell and links stay thread-local.
        let owners = &unsafe { node.as_ref() }.owners;
        let next = owners
            .get()
            .checked_add(1)
            .filter(|n| isize::try_from(*n).is_ok())
            .expect("scan scope strong count overflow");
        owners.set(next);
        Self {
            node,
            _thread: PhantomData,
        }
    }
}

impl Drop for ScanReads {
    fn drop(&mut self) {
        let node = unsafe { self.node.as_ref() };
        let owners = node.owners.get();
        if owners != 1 {
            node.owners.set(owners - 1);
            return;
        }
        // Unlink this exact node, including an interior/out-of-order release.
        // Its neighbors all retain their own live thread-bound guard owners.
        SCAN_TAIL.with(|tail| {
            let previous = node.previous.get();
            let next = node.next.get();
            if let Some(previous) = previous {
                unsafe { previous.as_ref() }.next.set(next);
            }
            if let Some(next) = next {
                unsafe { next.as_ref() }.previous.set(previous);
            } else {
                tail.set(previous);
            }
        });
        // No user destructor runs while any TLS pointer still names this node.
        let node = unsafe { self.node.as_ptr().read() };
        unsafe {
            std::alloc::dealloc(
                self.node.as_ptr().cast(),
                std::alloc::Layout::new::<ScanScopeNode>(),
            );
        };
        drop(node); // allowance and real node invoice settle after physical free
    }
}

/// Enter one-pass scan reads until the guard drops, drawing on the enclosing
/// scan's allowance when one is active and on a fresh allowance otherwise.
/// A nested helper retains the SAME current node without allocating or moving
/// its position in the active list, including inside an ordinary native scan.
#[doc(hidden)]
pub fn scan_reads() -> ScanReads {
    match SCAN_TAIL.with(std::cell::Cell::get) {
        Some(node) => ScanReads::retain_current(node),
        None => ScanAllowance::new().enter(),
    }
}

/// Execute a scan using the shared cache under its declared admission and
/// capacity. Unpinned pages can be recycled by a larger stream. Without
/// declared admission the existing finite fallback growth allowance applies.
#[doc(hidden)]
pub fn with_scan_reads<R>(operation: impl FnOnce() -> R) -> R {
    let _scan = scan_reads();
    operation()
}

// Declared readers already admit source pages and the shared cache enforces
// its own capacity. Reuse that bounded cache instead of imposing an unrelated
// per-scan growth constant or retaining a second set of pinned page handles.
pub(crate) fn declared_scan_reads_active() -> bool {
    if ACTIVE.with(|active| !active.borrow().is_empty()) {
        return true;
    }
    #[cfg(feature = "contextdb-internal")]
    return SCAN_TAIL.with(|tail| {
        tail.get().is_some_and(|node| {
            matches!(
                &unsafe { node.as_ref() }.allowance.0,
                ScanState::Native { .. }
            )
        })
    });
    #[cfg(not(feature = "contextdb-internal"))]
    false
}

pub(crate) fn scan_reads_active() -> bool {
    SCAN_TAIL.with(|tail| tail.get().is_some())
}

/// Take `bytes` of the innermost legacy scan's finite fallback allowance.
pub(crate) fn take_scan_growth(bytes: usize) -> bool {
    use std::sync::atomic::Ordering;
    SCAN_TAIL.with(|tail| {
        tail.get().is_some_and(|node| {
            #[allow(
                clippy::infallible_destructuring_match,
                reason = "Internal builds also match native admission without a legacy counter."
            )]
            let remaining = match &unsafe { node.as_ref() }.allowance.0 {
                ScanState::Legacy(remaining) => remaining,
                #[cfg(feature = "contextdb-internal")]
                ScanState::Native { .. } => return false,
            };
            remaining
                .fetch_update_compat(Ordering::AcqRel, Ordering::Acquire, |left| {
                    left.checked_sub(bytes)
                })
                .is_ok()
        })
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

// Cache reconstruction is separate from source-page admission: preparing an
// index must not recurse into the page producer or select its payer.
#[cfg(feature = "contextdb-internal")]
thread_local! {
    static CACHE_PREPARATION: RefCell<Option<(crate::NativeStorageAdmission, bool)>> = const { RefCell::new(None) };
}
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub fn with_cache_preparation_admission<R>(
    admission: crate::NativeStorageAdmission,
    prepare: impl FnOnce() -> R,
) -> R {
    with_cache_preparation_scope(admission, false, prepare)
}

/// Direct-source preparation retains its genuine aggregate invoice with native
/// backing. Writer preparation keeps its existing temporary handoff semantics.
#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub fn with_retained_cache_preparation_admission<R>(
    admission: crate::NativeStorageAdmission,
    prepare: impl FnOnce() -> R,
) -> R {
    with_cache_preparation_scope(admission, true, prepare)
}
#[cfg(feature = "contextdb-internal")]
fn with_cache_preparation_scope<R>(
    admission: crate::NativeStorageAdmission,
    retain: bool,
    prepare: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<(crate::NativeStorageAdmission, bool)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = CACHE_PREPARATION.with(|slot| slot.replace(self.0.take()));
            drop(previous);
        }
    }
    let _restore = Restore(CACHE_PREPARATION.with(|slot| slot.replace(Some((admission, retain)))));
    prepare()
}

/// Clone the actual preparation owner without new admission or a receipt cell.
/// Legacy/unscoped construction returns None, never a fabricated native payer.
pub(crate) fn cache_preparation_credit()
-> Option<crate::native_storage_admission::NativeStorageCredit> {
    #[cfg(feature = "contextdb-internal")]
    {
        CACHE_PREPARATION.with(|slot| {
            slot.borrow()
                .as_ref()
                .and_then(|(owner, retain)| retain.then(|| owner.cache_preparation_credit()))
        })
    }
    #[cfg(not(feature = "contextdb-internal"))]
    {
        None
    }
}

pub(crate) fn prepare_cache_allocation(layout: std::alloc::Layout) -> Result<(), StorageError> {
    #[cfg(feature = "contextdb-internal")]
    if let Some((owner, _)) = CACHE_PREPARATION.with(|slot| slot.borrow().clone()) {
        // The actual outer preparation owner retains the aggregate original
        // invoice until fixed custody is installed, including constructor unwind.
        owner.reserve_cache_preparation(layout)?;
    }
    #[cfg(not(feature = "contextdb-internal"))]
    let _ = layout; // no fabricated receipt in the ordinary upstream backend
    Ok(())
}

/// Passive node identities and the actual production legacy allowance debit, confined
/// to test-seams. These methods allocate nothing and retain no owner or TLS slot.
#[cfg(all(feature = "contextdb-internal", feature = "test-seams"))]
impl ScanAllowance {
    #[doc(hidden)]
    pub fn node_layout_for_test() -> std::alloc::Layout {
        std::alloc::Layout::new::<ScanScopeNode>()
    }
    #[doc(hidden)]
    pub fn current_node_for_test() -> usize {
        SCAN_TAIL.with(|tail| tail.get().map_or(0, |node| node.as_ptr() as usize))
    }
    #[doc(hidden)]
    pub fn take_current_growth_for_test(bytes: usize) -> bool {
        take_scan_growth(bytes)
    }
}
#[cfg(all(feature = "contextdb-internal", feature = "test-seams"))]
impl ScanReads {
    #[doc(hidden)]
    pub fn allocation_observation_for_test(&self) -> (usize, std::alloc::Layout, usize) {
        let node = unsafe { self.node.as_ref() };
        let receipt = match &node.invoice {
            ScanNodeInvoice::Native(invoice) => invoice.receipt_identity_for_test(),
            ScanNodeInvoice::Legacy => 0,
        };
        (
            self.node.as_ptr() as usize,
            std::alloc::Layout::new::<ScanScopeNode>(),
            receipt,
        )
    }
}

// Cache misses are attributed to the enclosing engine request without retaining
// a reader, callback or allocation. Existing hits retain their original tag.
#[cfg(feature = "contextdb-internal")]
thread_local! {
    static CACHE_REQUEST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static RETIRING_CACHE_REQUEST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
#[cfg(feature = "contextdb-internal")]
static NEXT_CACHE_REQUEST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[cfg(feature = "contextdb-internal")]
#[doc(hidden)]
pub struct ReadCacheRequest {
    id: u64,
    previous: u64,
    committed: bool,
    _thread: PhantomData<Rc<()>>,
}
#[cfg(feature = "contextdb-internal")]
impl ReadCacheRequest {
    pub fn enter() -> Self {
        use std::sync::atomic::Ordering;
        let id = NEXT_CACHE_REQUEST
            .fetch_update_compat(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .expect("cache request identity exhausted");
        let previous = CACHE_REQUEST.with(|slot| slot.replace(id));
        Self {
            id,
            previous,
            committed: false,
            _thread: PhantomData,
        }
    }
    pub fn commit(&mut self) {
        self.committed = true;
    }
    pub fn release(&self, release: impl FnOnce()) {
        struct Restore(u64);
        impl Drop for Restore {
            fn drop(&mut self) {
                RETIRING_CACHE_REQUEST.with(|slot| slot.set(self.0));
            }
        }
        let _restore = Restore(
            RETIRING_CACHE_REQUEST
                .with(|slot| slot.replace(if self.committed { 0 } else { self.id })),
        );
        release();
    }
    pub fn retiring() -> Option<u64> {
        RETIRING_CACHE_REQUEST.with(|slot| match slot.get() {
            0 => None,
            id => Some(id),
        })
    }
}
#[cfg(feature = "contextdb-internal")]
impl Drop for ReadCacheRequest {
    fn drop(&mut self) {
        CACHE_REQUEST.with(|slot| slot.set(self.previous));
    }
}
#[cfg(feature = "contextdb-internal")]
pub(crate) fn cache_request() -> u64 {
    CACHE_REQUEST.with(std::cell::Cell::get)
}

/// Physical page work observed independently of admission callbacks.
/// Original-source native totals include their dirty pages and are priced once.
#[cfg(feature = "test-seams")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageAllocationWorkForTest {
    pub fresh_dirty_pages: u64,
    pub fresh_dirty_page_bytes: usize,
    pub native_pages: u64,
    pub native_page_bytes: usize,
    pub other_native_pages: u64,
    pub other_native_page_bytes: usize,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static PAGE_ALLOCATION_WORK: std::cell::Cell<Option<PageAllocationWorkForTest>> = const { std::cell::Cell::new(None) };
}

/// Observe successful physical page constructions on this operation's thread.
/// Reusing backing and admitting memory do not increment this observation.
#[cfg(feature = "test-seams")]
pub fn observe_page_allocations_for_test<T>(
    operation: impl FnOnce() -> T,
) -> (T, PageAllocationWorkForTest) {
    struct Restore(Option<PageAllocationWorkForTest>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PAGE_ALLOCATION_WORK.with(|slot| {
                let nested = slot.get().unwrap_or_default();
                slot.set(self.0.map(|parent| PageAllocationWorkForTest {
                    fresh_dirty_pages: parent.fresh_dirty_pages + nested.fresh_dirty_pages,
                    fresh_dirty_page_bytes: parent.fresh_dirty_page_bytes
                        + nested.fresh_dirty_page_bytes,
                    native_pages: parent.native_pages + nested.native_pages,
                    native_page_bytes: parent.native_page_bytes + nested.native_page_bytes,
                    other_native_pages: parent.other_native_pages + nested.other_native_pages,
                    other_native_page_bytes: parent.other_native_page_bytes
                        + nested.other_native_page_bytes,
                }));
            });
        }
    }
    let restore = Restore(
        PAGE_ALLOCATION_WORK.with(|slot| slot.replace(Some(PageAllocationWorkForTest::default()))),
    );
    let answer = operation();
    let count = PAGE_ALLOCATION_WORK.with(|slot| slot.get().unwrap());
    drop(restore);
    (answer, count)
}

#[cfg(feature = "test-seams")]
fn page_invoice_bytes_for_test(backing_bytes: usize) -> usize {
    #[cfg(feature = "contextdb-internal")]
    let receipt_bytes = crate::native_storage_admission::receipt_overhead_for_test();
    #[cfg(not(feature = "contextdb-internal"))]
    let receipt_bytes = 0;
    backing_bytes + receipt_bytes
}

#[cfg(feature = "test-seams")]
pub(crate) fn record_dirty_page_allocation_for_test(backing_bytes: usize, native: bool) {
    PAGE_ALLOCATION_WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.fresh_dirty_pages += 1;
            work.fresh_dirty_page_bytes += if native {
                page_invoice_bytes_for_test(backing_bytes)
            } else {
                backing_bytes
            };
            slot.set(Some(work));
        }
    });
}

#[cfg(all(feature = "test-seams", feature = "contextdb-internal"))]
pub(crate) fn record_native_page_allocation_for_test(backing_bytes: usize) {
    PAGE_ALLOCATION_WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            let invoice_bytes = page_invoice_bytes_for_test(backing_bytes);
            if invoice_bytes > backing_bytes {
                // Only the installed original source can name its concrete
                // separately allocated receipt. Other reader pages remain
                // covered by their existing reader/work observations.
                work.native_pages += 1;
                work.native_page_bytes += invoice_bytes;
            } else {
                work.other_native_pages += 1;
                work.other_native_page_bytes += backing_bytes;
            }
            slot.set(Some(work));
        }
    });
}
