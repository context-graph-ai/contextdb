//! Operation-scoped admission before allocating a source page. The caller
//! keeps admitted bytes owned until its read handles have been released.
use std::{cell::RefCell, io, sync::Arc};
use std::{marker::PhantomData, rc::Rc};
type Admission = Arc<dyn Fn(usize) -> io::Result<()> + Send + Sync>;

/// A charge a caller's admission hands back for one page. The page the read
/// cache does not keep carries it and drops it when its last holder drops the
/// page, so the caller's charge lasts exactly as long as the page does.
pub type ReadPageCharge = Box<dyn Send + Sync>;
type ChargingAdmission = Arc<dyn Fn(usize) -> io::Result<ReadPageCharge> + Send + Sync>;

enum ActiveAdmission {
    Scoped(Admission),
    Charging(ChargingAdmission),
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
/// for each page. A page the read cache keeps is released from its charge at
/// once, because the cache's own budget holds it; a page the cache does not
/// keep carries the charge until its last holder drops it.
#[doc(hidden)]
pub fn with_read_page_charge<R>(admit: ChargingAdmission, operation: impl FnOnce() -> R) -> R {
    ACTIVE.with(|active| active.borrow_mut().push(ActiveAdmission::Charging(admit)));
    let _reset = ResetAdmission;
    operation()
}

pub(crate) fn admit_read_memory(bytes: usize) -> io::Result<Option<ReadPageCharge>> {
    ACTIVE.with(|active| match active.borrow().last() {
        Some(ActiveAdmission::Scoped(admit)) => admit(bytes).map(|()| None),
        Some(ActiveAdmission::Charging(admit)) => admit(bytes).map(Some),
        None => Ok(None),
    })
}

thread_local! { static SCAN_READS: RefCell<Vec<ScanAllowance>> = const { RefCell::new(Vec::new()) }; }

/// Cache growth one outermost scan may take for pages it installs into free
/// room, such as the root and branch pages every probe of the scan reuses.
pub(crate) const SCAN_GROWTH_ALLOWANCE: usize = 16 * 1024;

/// The cache growth one scan may still take for pages it installs into free
/// room. A statement that scans in several resumed steps keeps one allowance
/// across them by re-entering with the same value.
#[doc(hidden)]
#[derive(Clone)]
pub struct ScanAllowance(Arc<std::sync::atomic::AtomicUsize>);

impl Default for ScanAllowance {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanAllowance {
    /// A fresh allowance of `SCAN_GROWTH_ALLOWANCE` (16 KiB).
    pub fn new() -> Self {
        Self(Arc::new(std::sync::atomic::AtomicUsize::new(
            SCAN_GROWTH_ALLOWANCE,
        )))
    }

    /// Enter one-pass scan reads on this thread, drawing on this allowance,
    /// until the guard drops.
    pub fn enter(&self) -> ScanReads {
        SCAN_READS.with(|active| active.borrow_mut().push(self.clone()));
        ScanReads {
            _thread: PhantomData,
        }
    }
}

/// While held, this thread's cache misses grow the read cache by at most the
/// scan's remaining allowance (see [`with_scan_reads`]).
#[doc(hidden)]
pub struct ScanReads {
    _thread: PhantomData<Rc<()>>,
}

impl Drop for ScanReads {
    fn drop(&mut self) {
        SCAN_READS.with(|active| {
            active.borrow_mut().pop();
        });
    }
}

/// Enter one-pass scan reads until the guard drops, drawing on the enclosing
/// scan's allowance when one is active and on a fresh allowance otherwise.
#[doc(hidden)]
pub fn scan_reads() -> ScanReads {
    let enclosing = SCAN_READS.with(|active| active.borrow().last().cloned());
    enclosing.unwrap_or_default().enter()
}

/// Execute a one-pass scan with a bounded cache footprint: its misses may
/// install into free cache room up to a small growth allowance (the pages
/// every probe of the scan reuses); past it a miss first evicts as many bytes
/// as it needs and is installed only if that made room, otherwise it lives
/// only while its reader holds it. Hits are still served. A whole-partition
/// scan then grows the cache by at most the allowance.
#[doc(hidden)]
pub fn with_scan_reads<R>(operation: impl FnOnce() -> R) -> R {
    let _scan = scan_reads();
    operation()
}

pub(crate) fn scan_reads_active() -> bool {
    SCAN_READS.with(|active| !active.borrow().is_empty())
}

/// Take `bytes` of the innermost scan's growth allowance, if that much remains.
pub(crate) fn take_scan_growth(bytes: usize) -> bool {
    use std::sync::atomic::Ordering;
    SCAN_READS.with(|active| {
        active.borrow().last().is_some_and(|allowance| {
            allowance
                .0
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
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
