//! Fixed engine workers retain their original invoices through join and unmap.
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner, ReadShared};
use contextdb_core::{Error, Result};
use std::sync::Mutex;

#[cfg(target_os = "linux")]
mod native {
    use super::*;
    use std::ffi::c_void;

    // The existing Rust worker stack size; the additional page is an explicit
    // guard because pthread does not protect caller-supplied stacks.
    const STACK_BYTES: usize = 2 * 1024 * 1024;

    fn os_error(code: i32) -> Error {
        Error::FundedEngine(contextdb_core::error::FundedEngineError::from_os_error(
            std::num::NonZeroI32::new(code)
                .unwrap_or(std::num::NonZeroI32::new(libc::EIO).unwrap()),
        ))
    }

    struct Stack {
        address: *mut c_void,
        bytes: usize,
        _invoice: ReadCredit,
    }
    unsafe impl Send for Stack {}
    impl Drop for Stack {
        fn drop(&mut self) {
            // Only an unstarted worker or a joined worker can own this value.
            // Do not refund an extent the OS has not actually unmapped.
            if unsafe { libc::munmap(self.address, self.bytes) } != 0 {
                std::process::abort();
            }
        }
    }
    struct Body<F> {
        run: Mutex<Option<F>>,
        panicked: std::sync::atomic::AtomicBool,
    }
    extern "C" fn enter<F: FnOnce() + Send + 'static>(pointer: *mut c_void) -> *mut c_void {
        // The creator transferred exactly this thread's strong count.
        let body = unsafe { ReadShared::<Body<F>>::from_raw(pointer.cast()) };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let run = body.run.lock().unwrap_or_else(|p| p.into_inner()).take();
            if let Some(run) = run {
                run();
            }
        }));
        body.panicked
            .store(result.is_err(), std::sync::atomic::Ordering::Release);
        // The creator keeps a second count until pthread_join has completed.
        // No unwind may cross the C thread boundary, including panic cleanup.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result))).is_err() {
            std::process::abort();
        }
        std::ptr::null_mut()
    }
    unsafe fn release_body<F>(pointer: *const c_void) -> bool {
        let body = unsafe { ReadShared::<Body<F>>::from_raw(pointer.cast()) };
        body.panicked.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) struct OwnedWorker {
        thread: Option<libc::pthread_t>,
        body: *const c_void,
        release_body: unsafe fn(*const c_void) -> bool,
        stack: Option<Stack>,
    }
    unsafe impl Send for OwnedWorker {}
    impl std::fmt::Debug for OwnedWorker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OwnedWorker")
                .field("joined", &self.thread.is_none())
                .finish()
        }
    }
    impl OwnedWorker {
        pub(crate) fn spawn<F: FnOnce() + Send + 'static>(
            owner: ReadMemoryOwner,
            run: F,
        ) -> Result<Self> {
            let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            if page <= 0 {
                return Err(os_error(libc::EINVAL));
            }
            let page = page as usize;
            let bytes = STACK_BYTES
                .checked_add(page)
                .ok_or_else(|| os_error(libc::EOVERFLOW))?;
            let body_invoice = ReadCredit::try_new_owned(
                owner.clone(),
                ReadShared::<Body<F>>::layout().size(),
                "worker",
                "worker_control",
                "Raise MEMORY_LIMIT for the fixed engine workers.",
            )?;
            let stack_invoice = ReadCredit::try_new_owned(
                owner,
                bytes,
                "worker",
                "worker_stack",
                "Raise MEMORY_LIMIT for the fixed engine workers.",
            )?;
            let body = ReadShared::try_from_prepaid(
                Body {
                    run: Mutex::new(Some(run)),
                    panicked: std::sync::atomic::AtomicBool::new(false),
                },
                body_invoice,
            )?;
            let address = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    bytes,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_STACK,
                    -1,
                    0,
                )
            };
            if address == libc::MAP_FAILED {
                return Err(os_error(
                    std::io::Error::last_os_error()
                        .raw_os_error()
                        .unwrap_or(libc::EIO),
                ));
            }
            let stack = Stack {
                address,
                bytes,
                _invoice: stack_invoice,
            };
            if unsafe { libc::mprotect(address, page, libc::PROT_NONE) } != 0 {
                return Err(os_error(
                    std::io::Error::last_os_error()
                        .raw_os_error()
                        .unwrap_or(libc::EIO),
                ));
            }
            let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
            let code = unsafe { libc::pthread_attr_init(attributes.as_mut_ptr()) };
            if code != 0 {
                return Err(os_error(code));
            }
            let mut attributes = unsafe { attributes.assume_init() };
            let code = unsafe {
                libc::pthread_attr_setstack(
                    &mut attributes,
                    address.cast::<u8>().add(page).cast(),
                    STACK_BYTES,
                )
            };
            if code != 0 {
                unsafe {
                    libc::pthread_attr_destroy(&mut attributes);
                }
                return Err(os_error(code));
            }
            let thread_body = body.clone().into_raw();
            let mut thread = std::mem::MaybeUninit::<libc::pthread_t>::uninit();
            let code = unsafe {
                libc::pthread_create(
                    thread.as_mut_ptr(),
                    &attributes,
                    enter::<F>,
                    thread_body.cast_mut().cast(),
                )
            };
            unsafe {
                libc::pthread_attr_destroy(&mut attributes);
            }
            if code != 0 {
                drop(unsafe { ReadShared::<Body<F>>::from_raw(thread_body) });
                return Err(os_error(code));
            }
            Ok(Self {
                thread: Some(unsafe { thread.assume_init() }),
                body: body.into_raw().cast(),
                release_body: release_body::<F>,
                stack: Some(stack),
            })
        }
        fn finish(&mut self) -> std::result::Result<(), ()> {
            let Some(thread) = self.thread.take() else {
                return Ok(());
            };
            if unsafe { libc::pthread_join(thread, std::ptr::null_mut()) } != 0 {
                // A failed join proves no physical-free boundary. Termination
                // keeps us from unmapping a possibly live worker's stack.
                std::process::abort();
            }
            let panicked = unsafe { (self.release_body)(self.body) };
            drop(self.stack.take());
            if panicked { Err(()) } else { Ok(()) }
        }
        pub(crate) fn join(mut self) -> std::result::Result<(), ()> {
            self.finish()
        }
    }
    impl Drop for OwnedWorker {
        fn drop(&mut self) {
            let _ = self.finish();
        }
    }

    #[cfg(all(test, feature = "test-seams"))]
    mod independent_tests {
        use super::*;
        use crate::memory_accounting::MemoryAccountant;
        use crate::persistence::content_identity::proof_allocation_oracle_tests as physical;
        use contextdb_core::read_memory::{NativeRefusal, ReadMemoryBudget};
        use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};

        static PHASE: AtomicUsize = AtomicUsize::new(0);
        static STACK_PAGE: AtomicUsize = AtomicUsize::new(0);
        static ENTERED: AtomicUsize = AtomicUsize::new(0);
        fn page_bytes() -> usize { unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize } }
        fn worker_body() {
            let marker = 0_u8;
            let page = page_bytes();
            STACK_PAGE.store((&marker as *const u8 as usize) / page * page, Ordering::Release);
            ENTERED.fetch_add(1, Ordering::SeqCst);
            PHASE.store(1, Ordering::Release);
            while PHASE.load(Ordering::Acquire) == 1 { std::hint::spin_loop(); }
            if PHASE.load(Ordering::Acquire) == 3 { panic!("application test callback unwinds"); }
            std::hint::black_box(marker);
        }
        fn threads() -> usize { std::fs::read_dir("/proc/self/task").unwrap().count() }
        fn unmapped(address: usize) -> bool {
            let mut residency = 0_u8;
            let rc = unsafe { libc::mincore(address as *mut libc::c_void, page_bytes(), &mut residency) };
            rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOMEM)
        }
        #[derive(Debug)]
        struct Payer {
            actual: Arc<MemoryAccountant>,
            checks: AtomicUsize,
            early: AtomicBool,
        }
        impl ReadMemoryBudget for Payer {
            fn try_reserve(&self, bytes: usize, subsystem: &'static str, operation: &'static str, hint: &'static str) -> Result<()> {
                ReadMemoryBudget::try_reserve(&*self.actual, bytes, subsystem, operation, hint)
            }
            fn try_reserve_native(&self, bytes: usize, subsystem: &'static str, operation: &'static str, hint: &'static str) -> std::result::Result<(), NativeRefusal> {
                ReadMemoryBudget::try_reserve_native(&*self.actual, bytes, subsystem, operation, hint)
            }
            fn release(&self, bytes: usize) {
                if bytes == STACK_BYTES + page_bytes() {
                    let address = STACK_PAGE.load(Ordering::Acquire);
                    if address != 0 {
                        self.checks.fetch_add(1, Ordering::SeqCst);
                        self.early.fetch_or(!unmapped(address), Ordering::SeqCst);
                    }
                }
                ReadMemoryBudget::release(&*self.actual, bytes);
            }
        }
        struct ReleaseBody;
        impl Drop for ReleaseBody { fn drop(&mut self) { PHASE.store(2, Ordering::Release); } }
        struct FreeWatch;
        impl Drop for FreeWatch { fn drop(&mut self) { physical::disarm_source_free_watch(); } }

        // Promise: startup refusal leaves no worker, and joined/dropped/unwound
        // workers refund their original control/stack only after physical free.
        // No prior mapped-worker test exists. Decisive mutants: admit the stack
        // after mmap, refund before munmap, or forget Stack after pthread_join.
        #[test]
        #[serial_test::serial]
        fn fixed_worker_startup_and_physical_release_rows() {
            let control = ReadShared::<Body<fn()>>::layout();
            let stack = STACK_BYTES + page_bytes();
            let before = threads();
            for limit in [control.size() - 1, control.size() + stack - 1] {
                ENTERED.store(0, Ordering::SeqCst);
                let actual = Arc::new(MemoryAccountant::with_budget(limit));
                let error = match OwnedWorker::spawn(actual.clone().into(), worker_body as fn()) {
                    Err(error) => error,
                    Ok(worker) => {
                        while PHASE.load(Ordering::Acquire) != 1 { std::thread::yield_now(); }
                        PHASE.store(2, Ordering::Release);
                        drop(worker);
                        panic!("the original under-limit owner must refuse worker startup");
                    }
                };
                let Error::MemoryBudgetExceeded { operation, requested_bytes, budget_limit_bytes, .. } = error else {
                    panic!("startup denial must preserve the original memory refusal: {error:?}");
                };
                let (name, bytes) = if limit < control.size() { ("worker_control", control.size()) } else { ("worker_stack", stack) };
                assert_eq!(operation.to_string(), name);
                assert_eq!(requested_bytes, bytes);
                assert_eq!(budget_limit_bytes, limit);
                assert_eq!(actual.usage().used, 0);
                assert_eq!(ENTERED.load(Ordering::SeqCst), 0);
                assert_eq!(threads(), before);
            }
            for ending in ["join", "drop", "unwind"] {
                STACK_PAGE.store(0, Ordering::SeqCst);
                ENTERED.store(0, Ordering::SeqCst);
                PHASE.store(0, Ordering::SeqCst);
                let payer = Arc::new(Payer { actual: Arc::new(MemoryAccountant::with_budget(control.size() + stack)), checks: AtomicUsize::new(0), early: AtomicBool::new(false) });
                let counter = crate::memory_accounting::AccountantHandle::from_host(payer.actual.clone()).used_counter_for_observation();
                let (worker, requests, count) = physical::source_requests_for_test(counter, || OwnedWorker::spawn(payer.clone().into(), worker_body as fn()));
                let worker = worker.unwrap();
                while PHASE.load(Ordering::Acquire) != 1 { std::thread::yield_now(); }
                let _release = ReleaseBody;
                assert_eq!(threads(), before + 1);
                assert_eq!(payer.actual.usage().used, control.size() + stack);
                assert!(!unmapped(STACK_PAGE.load(Ordering::Acquire)));
                let request = requests[..count].iter().copied().find(|(pointer, bytes, align)|
                    *bytes == control.size() && *align == control.align()
                        && *pointer <= worker.body as usize && (worker.body as usize) < *pointer + *bytes)
                    .expect("actual original control allocation containing the worker body");
                physical::watch_source_free_for_test(&[request], counter);
                let watch = FreeWatch;
                PHASE.store(if ending == "unwind" { 3 } else { 2 }, Ordering::Release);
                if ending == "drop" { drop(worker); } else { assert_eq!(worker.join().is_err(), ending == "unwind"); }
                let free = physical::source_free_for_test(1)[0];
                assert_eq!(free.2, 1, "original body allocation physically freed once");
                assert_eq!(free.0, control.size() + stack, "original body and stack invoices at free entry");
                assert_eq!(free.1, free.0, "body invoice remains paid through actual System.dealloc");
                assert_eq!(payer.checks.load(Ordering::SeqCst), 1);
                assert!(!payer.early.load(Ordering::SeqCst), "stack invoice refunded while its actual page was still mapped");
                assert!(unmapped(STACK_PAGE.load(Ordering::Acquire)));
                assert_eq!(payer.actual.usage().used, 0);
                assert_eq!(threads(), before);
                drop(watch);
            }
        }
    }
}
#[cfg(target_os = "linux")]
pub(crate) use native::OwnedWorker;

#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub(crate) struct OwnedWorker(std::thread::JoinHandle<()>);
#[cfg(not(target_os = "linux"))]
impl OwnedWorker {
    pub(crate) fn spawn<F: FnOnce() + Send + 'static>(_: ReadMemoryOwner, run: F) -> Result<Self> {
        std::thread::Builder::new()
            .spawn(run)
            .map(Self)
            .map_err(|_| {
                Error::NativeEngineOther(contextdb_core::read_memory::NativeRefusal::Other(
                    "worker launch failed",
                ))
            })
    }
    pub(crate) fn join(self) -> std::result::Result<(), ()> {
        self.0.join().map_err(|_| ())
    }
}
