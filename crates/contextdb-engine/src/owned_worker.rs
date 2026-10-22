//! Worker payloads retain their original invoices through join and unmap.
//! Intrinsic thread stacks are accounted by the whole-process limit.
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
    }
    unsafe impl Send for Stack {}
    impl Drop for Stack {
        fn drop(&mut self) {
            // Only an unstarted worker or a joined worker can own this value.
            // Preserve physical stack cleanup even though stacks are process-accounted.
            if unsafe { libc::munmap(self.address, self.bytes) } != 0 {
                std::process::abort();
            }
        }
    }
    struct FinishNode {
        next: *mut FinishNode,
        worker: Option<OwnedWorker>,
        cleanup: Option<crate::database::event_bus::EventBusResourceCleanup>,
    }
    // The queue lock exclusively owns linked nodes; the worker's creator
    // count keeps the node alive through the external join.
    struct FinishQueue {
        head: *mut FinishNode,
        tail: *mut FinishNode,
    }
    unsafe impl Send for FinishQueue {}
    static FINISH_QUEUE: (Mutex<FinishQueue>, std::sync::Condvar) = (
        Mutex::new(FinishQueue {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
        }),
        std::sync::Condvar::new(),
    );
    static FINISHER: Mutex<Option<OwnedWorker>> = Mutex::new(None);

    #[derive(Debug)]
    struct ProcessResidue;
    struct ResidueInvoice(usize);
    impl Drop for ResidueInvoice {
        fn drop(&mut self) {
            crate::memory_accounting::refund_open_residue(self.0);
        }
    }
    impl contextdb_core::read_memory::ReadMemoryBudget for ProcessResidue {
        fn try_reserve(
            &self,
            bytes: usize,
            _: &'static str,
            operation: &'static str,
            _: &'static str,
        ) -> Result<()> {
            crate::memory_accounting::admit_open_residue(bytes, operation)
        }
        fn release(&self, bytes: usize) {
            crate::memory_accounting::refund_open_residue(bytes);
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ensure_finisher() -> Result<()> {
        let mut finisher = FINISHER.lock().unwrap_or_else(|p| p.into_inner());
        if finisher.is_some() {
            return Ok(());
        }
        let bytes = ReadMemoryOwner::native_layout::<ProcessResidue, ResidueInvoice>().size();
        crate::memory_accounting::admit_open_residue(bytes, "worker_finisher_owner")?;
        let owner = ReadMemoryOwner::try_native(ProcessResidue, ResidueInvoice(bytes))?;
        // The static retains this original worker, including all invoices,
        // for the process lifetime. It is never described as refunded.
        *finisher = Some(OwnedWorker::spawn_inner(owner, finish_loop, false)?);
        Ok(())
    }
    fn finish_loop() {
        loop {
            let (mut worker, cleanup) = {
                let mut queue = FINISH_QUEUE.0.lock().unwrap_or_else(|p| p.into_inner());
                while queue.head.is_null() {
                    queue = FINISH_QUEUE
                        .1
                        .wait(queue)
                        .unwrap_or_else(|p| p.into_inner());
                }
                // Node storage belongs to the original prepaid body and cannot
                // be freed until this worker's join below releases its count.
                let node = unsafe { &mut *queue.head };
                queue.head = node.next;
                if queue.head.is_null() {
                    queue.tail = std::ptr::null_mut();
                }
                (
                    node.worker.take().expect("published worker"),
                    node.cleanup.take(),
                )
            };
            // A node's cleanup must not take down the only process finisher.
            // Join/unmap failures still abort; completion is reached only after
            // the original worker has physically released its storage.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _ = worker.finish();
                drop(worker);
                if let Some(cleanup) = cleanup {
                    cleanup.complete();
                }
            }));
            // The panic hook reports the failure. Even a hostile panic payload
            // must not unwind out of the finisher's permanent loop.
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(outcome))).is_err() {
                std::process::abort();
            }
        }
    }
    struct Body<F> {
        finish: std::cell::UnsafeCell<FinishNode>,
        run: Mutex<Option<F>>,
        panicked: std::sync::atomic::AtomicBool,
    }
    unsafe impl<F: Send> Sync for Body<F> {}
    unsafe impl<F: Send> Send for Body<F> {}
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
        finish_node: *mut FinishNode,
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
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        pub(crate) fn spawn<F: FnOnce() + Send + 'static>(
            owner: ReadMemoryOwner,
            run: F,
        ) -> Result<Self> {
            Self::spawn_inner(owner, run, true)
        }
        // Only ensure_finisher uses the nonrecursive bootstrap path while
        // holding FINISHER. All other workers require its live lease.
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn spawn_inner<F: FnOnce() + Send + 'static>(
            owner: ReadMemoryOwner,
            run: F,
            needs_finisher: bool,
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
            // Engine closure and queue data stay prepaid; the intrinsic stack,
            // thread control block and TLS belong to the whole-process limit.
            if needs_finisher {
                ensure_finisher()?;
            }
            let body = ReadShared::try_from_prepaid(
                Body {
                    finish: std::cell::UnsafeCell::new(FinishNode {
                        next: std::ptr::null_mut(),
                        worker: None,
                        cleanup: None,
                    }),
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
            let stack = Stack { address, bytes };
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
            let finish_node = body.finish.get();
            Ok(Self {
                thread: Some(unsafe { thread.assume_init() }),
                body: body.into_raw().cast(),
                release_body: release_body::<F>,
                stack: Some(stack),
                finish_node,
            })
        }
        pub(crate) fn is_current(&self) -> bool {
            self.thread.is_some_and(|thread| unsafe {
                libc::pthread_equal(thread, libc::pthread_self()) != 0
            })
        }
        pub(crate) fn defer_cleanup(
            mut self,
            cleanup: crate::database::event_bus::EventBusResourceCleanup,
        ) {
            // This handle uniquely owns its node until queue publication.
            unsafe {
                (*self.finish_node).cleanup = Some(cleanup);
            }
            self.handoff();
        }
        fn handoff(&mut self) {
            let mut queue = FINISH_QUEUE.0.lock().unwrap_or_else(|p| p.into_inner());
            let worker = Self {
                thread: self.thread.take(),
                body: self.body,
                release_body: self.release_body,
                stack: self.stack.take(),
                finish_node: self.finish_node,
            };
            unsafe {
                let node = &mut *self.finish_node;
                node.worker = Some(worker);
                node.next = std::ptr::null_mut();
                if queue.tail.is_null() {
                    queue.head = self.finish_node;
                } else {
                    (*queue.tail).next = self.finish_node;
                }
                queue.tail = self.finish_node;
            }
            FINISH_QUEUE.1.notify_one();
        }
        fn finish(&mut self) -> std::result::Result<(), ()> {
            if self.is_current() {
                self.handoff();
                return Ok(());
            }
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
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        static PHASE: AtomicUsize = AtomicUsize::new(0);
        static STACK_PAGE: AtomicUsize = AtomicUsize::new(0);
        static ENTERED: AtomicUsize = AtomicUsize::new(0);
        fn page_bytes() -> usize {
            unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
        }
        fn worker_body() {
            let marker = 0_u8;
            let page = page_bytes();
            STACK_PAGE.store(
                (&marker as *const u8 as usize) / page * page,
                Ordering::Release,
            );
            ENTERED.fetch_add(1, Ordering::SeqCst);
            PHASE.store(1, Ordering::Release);
            while PHASE.load(Ordering::Acquire) == 1 {
                std::hint::spin_loop();
            }
            if PHASE.load(Ordering::Acquire) == 3 {
                panic!("application test callback unwinds");
            }
            std::hint::black_box(marker);
        }
        fn threads() -> usize {
            std::fs::read_dir("/proc/self/task").unwrap().count()
        }
        fn unmapped(address: usize) -> bool {
            let mut residency = 0_u8;
            let rc = unsafe {
                libc::mincore(address as *mut libc::c_void, page_bytes(), &mut residency)
            };
            rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOMEM)
        }
        struct ReleaseBody;
        impl Drop for ReleaseBody {
            fn drop(&mut self) {
                PHASE.store(2, Ordering::Release);
            }
        }
        struct FreeWatch;
        impl Drop for FreeWatch {
            fn drop(&mut self) {
                physical::disarm_source_free_watch();
            }
        }

        // Promise: engine closure/handoff data stays paid through physical
        // free; intrinsic stacks belong to the whole-process limit. A data
        // admission refusal starts no worker. Decisive mutants: charge a stack,
        // refund the body before deallocation, or skip unmap after join.
        #[test]
        #[serial_test::serial]
        fn fixed_worker_startup_and_physical_release_rows() {
            let control = ReadShared::<Body<fn()>>::layout();
            let before = threads();
            {
                let limit = control.size() - 1;
                ENTERED.store(0, Ordering::SeqCst);
                let actual = Arc::new(MemoryAccountant::with_budget(limit));
                let error = match OwnedWorker::spawn(actual.clone().into(), worker_body as fn()) {
                    Err(error) => error,
                    Ok(worker) => {
                        while PHASE.load(Ordering::Acquire) != 1 {
                            std::thread::yield_now();
                        }
                        PHASE.store(2, Ordering::Release);
                        drop(worker);
                        panic!("the original under-limit owner must refuse worker startup");
                    }
                };
                let Error::MemoryBudgetExceeded {
                    operation,
                    requested_bytes,
                    budget_limit_bytes,
                    ..
                } = error
                else {
                    panic!("startup denial must preserve the original memory refusal: {error:?}");
                };
                assert_eq!(operation.to_string(), "worker_control");
                assert_eq!(requested_bytes, control.size());
                assert_eq!(budget_limit_bytes, limit);
                assert_eq!(actual.usage().used, 0);
                assert_eq!(ENTERED.load(Ordering::SeqCst), 0);
                assert_eq!(threads(), before);
            }
            // A cold process starts its one permanent finisher only after
            // successful admission. Repeating bootstrap must add no worker.
            let finisher_was_started = FINISHER.lock().unwrap().is_some();
            ensure_finisher().expect("start the process finisher after refusal controls");
            let warm = before + usize::from(!finisher_was_started);
            assert_eq!(threads(), warm);
            ensure_finisher().expect("reuse the same process finisher");
            assert_eq!(threads(), warm);
            for ending in ["join", "drop", "unwind"] {
                STACK_PAGE.store(0, Ordering::SeqCst);
                ENTERED.store(0, Ordering::SeqCst);
                PHASE.store(0, Ordering::SeqCst);
                // Exactly the engine body budget admits startup: no intrinsic
                // mapped stack, thread control block or TLS charge is allowed.
                let payer = Arc::new(MemoryAccountant::with_budget(control.size()));
                let counter = crate::memory_accounting::AccountantHandle::from_host(payer.clone())
                    .used_counter_for_observation();
                let (worker, requests, count) = physical::source_requests_for_test(counter, || {
                    OwnedWorker::spawn(payer.clone().into(), worker_body as fn())
                });
                let worker = worker.unwrap();
                while PHASE.load(Ordering::Acquire) != 1 {
                    std::thread::yield_now();
                }
                let _release = ReleaseBody;
                assert_eq!(threads(), warm + 1);
                assert_eq!(payer.usage().used, control.size());
                assert!(!unmapped(STACK_PAGE.load(Ordering::Acquire)));
                let request = requests[..count]
                    .iter()
                    .copied()
                    .find(|(pointer, bytes, align)| {
                        *bytes == control.size()
                            && *align == control.align()
                            && *pointer <= worker.body as usize
                            && (worker.body as usize) < *pointer + *bytes
                    })
                    .expect("actual original control allocation containing the worker body");
                physical::watch_source_free_for_test(&[request], counter);
                let watch = FreeWatch;
                PHASE.store(if ending == "unwind" { 3 } else { 2 }, Ordering::Release);
                if ending == "drop" {
                    drop(worker);
                } else {
                    assert_eq!(worker.join().is_err(), ending == "unwind");
                }
                let free = physical::source_free_for_test(1)[0];
                assert_eq!(free.2, 1, "original body allocation physically freed once");
                assert_eq!(
                    free.0,
                    control.size(),
                    "engine body invoice at free entry; intrinsic stack is process-accounted"
                );
                assert_eq!(
                    free.1, free.0,
                    "body invoice remains paid through actual System.dealloc"
                );
                assert!(unmapped(STACK_PAGE.load(Ordering::Acquire)));
                assert_eq!(payer.usage().used, 0);
                assert_eq!(threads(), warm);
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
    pub(crate) fn is_current(&self) -> bool {
        self.0.thread().id() == std::thread::current().id()
    }
    pub(crate) fn join(self) -> std::result::Result<(), ()> {
        self.0.join().map_err(|_| ())
    }
}
#[cfg(target_os = "linux")]
pub(crate) use native::ensure_finisher;

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CurrentThreadId(u64);
#[cfg(target_os = "linux")]
static NEXT_THREAD_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
#[cfg(target_os = "linux")]
thread_local! {
    // Drop-free static TLS is inside each owned worker's paid supplied-stack
    // extent. No Rust Thread handle or heap-backed TLS is materialized.
    static THREAD_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
#[cfg(not(target_os = "linux"))]
pub(crate) type CurrentThreadId = std::thread::ThreadId;
pub(crate) fn current_thread_id() -> CurrentThreadId {
    #[cfg(target_os = "linux")]
    {
        THREAD_ID.with(|id| {
            let mut value = id.get();
            if value == 0 {
                value = NEXT_THREAD_ID
                    .fetch_update(
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                        |next| next.checked_add(1),
                    )
                    .expect("thread identity exhausted");
                id.set(value);
            }
            CurrentThreadId(value)
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::thread::current().id()
    }
}
