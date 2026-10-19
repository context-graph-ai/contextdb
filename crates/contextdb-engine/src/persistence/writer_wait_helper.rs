//! A writer's wait for starting readers that its caller can end early.
//!
//! A blocking lock request cannot be told to stop. So the requests are made by
//! a minimal helper task that shares this process's descriptor table: each
//! lock it is granted lands on the writer's own open file description, the
//! one the writer keeps. The writer itself sleeps exactly once, on three
//! things the kernel can make ready -- the helper's exit (its pidfd), the
//! caller's cancellation, and the caller's own deadline clock -- and decides
//! once, in a fixed order, what that one wake means.
//!
//! The helper runs nothing but raw system calls: no allocation, no runtime,
//! no destructors, no `errno`, and every signal blocked from before it exists,
//! so no handler of the host can ever run in it. It makes each lock request
//! once and never repeats one; its exit status is its answer.

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::sync::Arc;

use super::physical_admission::WriterAcquireRefusal;

const HELPER_STACK_BYTES: usize = 64 * 1024;
/// Exit statuses the helper answers with. `0` means every requested lock is
/// held on the shared description.
const EXIT_PARENT_GONE: i32 = 2;
const EXIT_SETUP_FAILED: i32 = 3;
const EXIT_LOCK_FAILED_BASE: i32 = 10;

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn raw3(number: i64, a: i64, b: i64, c: i64) -> i64 {
    let answer: i64;
    // SAFETY: the caller supplies a valid system call; `syscall` clobbers
    // rcx and r11 only.
    unsafe {
        std::arch::asm!(
            "syscall",
            inlateout("rax") number => answer,
            in("rdi") a,
            in("rsi") b,
            in("rdx") c,
            lateout("rcx") _,
            lateout("r11") _,
            options(nostack)
        );
    }
    answer
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn raw3(number: i64, a: i64, b: i64, c: i64) -> i64 {
    let answer: i64;
    // SAFETY: the caller supplies a valid system call.
    unsafe {
        std::arch::asm!(
            "svc 0",
            in("x8") number,
            inlateout("x0") a => answer,
            in("x1") b,
            in("x2") c,
            options(nostack)
        );
    }
    answer
}

/// What the helper is to request, prepared by the parent before it exists.
#[repr(C)]
struct HelperPlan {
    descriptor: RawFd,
    parent: i32,
    count: usize,
    offsets: [i64; 3],
    lock_type: libc::c_short,
}

#[inline(always)]
unsafe fn helper_exit(status: i32) -> ! {
    // SAFETY: exit_group never returns.
    unsafe {
        raw3(libc::SYS_exit_group, i64::from(status), 0, 0);
        std::hint::unreachable_unchecked()
    }
}

extern "C" fn helper_main(argument: *mut c_void) -> libc::c_int {
    // SAFETY: the parent passes a pointer to a plan that this task's copy of
    // the address space holds unchanged; nothing below allocates or unwinds.
    unsafe {
        let plan = &*(argument as *const HelperPlan);
        if raw3(
            libc::SYS_prctl,
            i64::from(libc::PR_SET_PDEATHSIG),
            i64::from(libc::SIGKILL),
            0,
        ) < 0
        {
            helper_exit(EXIT_SETUP_FAILED);
        }
        if raw3(libc::SYS_getppid, 0, 0, 0) != i64::from(plan.parent) {
            helper_exit(EXIT_PARENT_GONE);
        }
        let mut index = 0;
        while index < plan.count && index < 3 {
            let mut request: libc::flock = std::mem::zeroed();
            request.l_type = plan.lock_type;
            request.l_whence = libc::SEEK_SET as libc::c_short;
            request.l_start = *plan.offsets.get_unchecked(index) as libc::off_t;
            request.l_len = 1;
            let answer = raw3(
                libc::SYS_fcntl,
                i64::from(plan.descriptor),
                i64::from(libc::F_OFD_SETLKW),
                std::ptr::addr_of_mut!(request) as i64,
            );
            if answer < 0 {
                helper_exit(EXIT_LOCK_FAILED_BASE.saturating_add((-answer) as i32));
            }
            index += 1;
        }
        helper_exit(0)
    }
}

/// Every signal blocked on this thread, the original mask restored on drop.
struct SignalsBlocked {
    previous: libc::sigset_t,
}

impl SignalsBlocked {
    fn now() -> io::Result<Self> {
        // SAFETY: both sets are initialized before use.
        unsafe {
            let mut all: libc::sigset_t = std::mem::zeroed();
            libc::sigfillset(&mut all);
            let mut previous: libc::sigset_t = std::mem::zeroed();
            let answered = libc::pthread_sigmask(libc::SIG_SETMASK, &all, &mut previous);
            if answered != 0 {
                return Err(io::Error::from_raw_os_error(answered));
            }
            Ok(Self { previous })
        }
    }
}

impl Drop for SignalsBlocked {
    fn drop(&mut self) {
        // SAFETY: `previous` was filled by pthread_sigmask.
        unsafe {
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.previous, std::ptr::null_mut());
        }
    }
}

/// A started helper: its pidfd and its private stack. Dropping it without
/// a confirmed reap is a bug the caller prevents (see `abort`).
pub(crate) struct Helper {
    pidfd: OwnedFd,
    pid: i32,
    stack: *mut c_void,
    // Kept alive until the helper is reaped: it reads this plan.
    _plan: std::mem::ManuallyDrop<Box<HelperPlan>>,
    _read_funding: std::mem::ManuallyDrop<Option<ReadWaitFunding>>,
    _read_descriptor: std::mem::ManuallyDrop<Option<File>>,
}

impl std::fmt::Debug for Helper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Helper")
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

// SAFETY: the stack pointer is only unmapped by the owner after a confirmed
// reap; nothing else dereferences it in this process.
unsafe impl Send for Helper {}

impl Helper {
    fn spawn(file: &File, offsets: &[i64]) -> io::Result<Self> {
        Self::spawn_mode(file, offsets, libc::F_WRLCK as libc::c_short)
    }

    fn spawn_mode(file: &File, offsets: &[i64], lock_type: libc::c_short) -> io::Result<Self> {
        let mut plan = Box::new(HelperPlan {
            descriptor: file.as_raw_fd(),
            // SAFETY: getpid cannot fail.
            parent: unsafe { libc::getpid() },
            count: offsets.len().min(3),
            offsets: [0; 3],
            lock_type,
        });
        plan.offsets[..plan.count].copy_from_slice(&offsets[..plan.count]);
        // SAFETY: a private anonymous mapping; checked below.
        let stack = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                HELPER_STACK_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_STACK,
                -1,
                0,
            )
        };
        if stack == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        #[cfg(any(test, feature = "test-seams"))]
        HELPER_STACKS_MAPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut pidfd: libc::c_int = -1;
        let spawned = {
            let _blocked = SignalsBlocked::now()?;
            // SAFETY: the stack is a fresh writable mapping and grows down
            // from its top; the plan outlives the helper (it is kept in the
            // returned value until reap); flags request a shared descriptor
            // table, a pidfd, and no exit signal to the host.
            unsafe {
                libc::clone(
                    helper_main,
                    (stack as *mut u8).add(HELPER_STACK_BYTES) as *mut c_void,
                    libc::CLONE_FILES | libc::CLONE_PIDFD,
                    std::ptr::addr_of_mut!(*plan) as *mut c_void,
                    std::ptr::addr_of_mut!(pidfd),
                )
            }
        };
        if spawned == -1 {
            let error = io::Error::last_os_error();
            // SAFETY: the mapping was made above and nothing uses it.
            unsafe {
                libc::munmap(stack, HELPER_STACK_BYTES);
            }
            #[cfg(any(test, feature = "test-seams"))]
            HELPER_STACKS_MAPPED.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return Err(error);
        }
        Ok(Self {
            // SAFETY: CLONE_PIDFD stored a fresh descriptor this call owns.
            pidfd: unsafe { OwnedFd::from_raw_fd(pidfd) },
            pid: spawned,
            stack,
            _plan: std::mem::ManuallyDrop::new(plan),
            _read_funding: std::mem::ManuallyDrop::new(None),
            _read_descriptor: std::mem::ManuallyDrop::new(None),
        })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid as u32
    }

    /// Reap an exited (or killed) helper; with `wait` false, only if it has
    /// already exited. Answers its exit status, or None if it is still alive.
    fn reap(&self, wait: bool) -> io::Result<Option<ReapedStatus>> {
        let _blocked = SignalsBlocked::now()?;
        // SAFETY: an all-zero siginfo is valid output space.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let options = libc::WEXITED | libc::__WALL | if wait { 0 } else { libc::WNOHANG };
        // SAFETY: P_PIDFD with our own pidfd; `info` is valid output space.
        let answered = unsafe {
            libc::waitid(
                libc::P_PIDFD,
                self.pidfd.as_raw_fd() as libc::id_t,
                &mut info,
                options,
            )
        };
        if answered == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                // Reaped elsewhere: only a task that has died can be.
                return Ok(Some(ReapedStatus::Unknown));
            }
            return Err(error);
        }
        // SAFETY: waitid filled `info` for a reaped child; si_pid 0 means
        // WNOHANG found nothing to reap.
        let (pid, code, status) = unsafe { (info.si_pid(), info.si_code, info.si_status()) };
        if pid == 0 {
            return Ok(None);
        }
        Ok(Some(if code == libc::CLD_EXITED {
            ReapedStatus::Exited(status)
        } else {
            ReapedStatus::Killed
        }))
    }

    fn kill(&self) -> io::Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if KILL_FAILS.with(std::cell::Cell::get) {
            return Err(io::Error::from_raw_os_error(libc::EPERM));
        }
        // SAFETY: our own pidfd and a signal number.
        let answered = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.pidfd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<c_void>(),
                0,
            )
        };
        if answered == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    fn release_stack(&mut self) {
        if !self.stack.is_null() {
            // SAFETY: the helper is reaped, so nothing runs on this stack.
            unsafe {
                libc::munmap(self.stack, HELPER_STACK_BYTES);
            }
            self.stack = std::ptr::null_mut();
            #[cfg(any(test, feature = "test-seams"))]
            HELPER_STACKS_MAPPED.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReapedStatus {
    Exited(i32),
    Killed,
    Unknown,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static KILL_FAILS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Helper stacks this process has mapped and not yet unmapped.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) static HELPER_STACKS_MAPPED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The outcome of ending a helper that must not finish its requests.
pub(crate) enum Ended {
    /// It is dead and reaped; nothing of it remains.
    Confirmed,
    /// Its death could not be confirmed. Everything it could still lock
    /// must stay held by this process.
    Unconfirmed(Helper),
}

/// Kill and reap, with exactly one non-blocking check when the kill itself
/// failed. Never a blocking wait behind a helper that may still be alive.
fn end(mut helper: Helper) -> Ended {
    match helper.kill() {
        Ok(()) => match helper.reap(true) {
            Ok(Some(_)) => {
                helper.release_stack();
                Ended::Confirmed
            }
            _ => Ended::Unconfirmed(helper),
        },
        Err(_) => match helper.reap(false) {
            Ok(Some(_)) => {
                helper.release_stack();
                Ended::Confirmed
            }
            _ => Ended::Unconfirmed(helper),
        },
    }
}

impl Drop for Helper {
    fn drop(&mut self) {
        if !self.stack.is_null()
            && let Ok(Some(_)) = self.reap(false)
        {
            self.release_stack();
        }
        if self.stack.is_null() {
            // The helper cannot access its plan/descriptor after confirmed
            // death. Free those before returning their original payer's credit.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self._plan);
                std::mem::ManuallyDrop::drop(&mut self._read_descriptor);
                std::mem::ManuallyDrop::drop(&mut self._read_funding);
            }
        }
        // An unconfirmed helper retains its plan, exact descriptor and payer
        // even if unwinding bypasses normal quarantine insertion.
    }
}

/// A pipe the cancellation listener and the deadline clock write one byte to.
struct WakePipe {
    read: OwnedFd,
    write: OwnedFd,
}

impl WakePipe {
    fn new() -> io::Result<Self> {
        let mut ends = [0; 2];
        // SAFETY: valid output array.
        if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pipe2 returned two fresh descriptors.
        Ok(unsafe {
            Self {
                read: OwnedFd::from_raw_fd(ends[0]),
                write: OwnedFd::from_raw_fd(ends[1]),
            }
        })
    }

    fn poke(&self) {
        let byte = 1u8;
        // SAFETY: one byte from a valid buffer; a full pipe already wakes.
        unsafe {
            libc::write(self.write.as_raw_fd(), std::ptr::addr_of!(byte).cast(), 1);
        }
    }
}

/// Sleep once on the helper's exit and the wake pipe, every signal blocked
/// for the duration, with an optional timeout. One `ppoll`, whatever it says.
fn park_once(pidfd: RawFd, wake: RawFd, timeout_ms: Option<u64>) -> io::Result<()> {
    let mut watched = [
        libc::pollfd {
            fd: pidfd,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let timeout = timeout_ms.map(|ms| libc::timespec {
        tv_sec: (ms / 1000).min(i64::MAX as u64) as libc::time_t,
        tv_nsec: ((ms % 1000) * 1_000_000) as libc::c_long,
    });
    // SAFETY: an initialized full set.
    let mut all: libc::sigset_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigfillset(&mut all);
    }
    // SAFETY: valid arrays and set; the timeout pointer is null or valid.
    let answered = unsafe {
        libc::ppoll(
            watched.as_mut_ptr(),
            2,
            timeout
                .as_ref()
                .map_or(std::ptr::null(), |timeout| timeout as *const libc::timespec),
            &all,
        )
    };
    if answered == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The native-park adapter the caller's deadline clock drives: its timeout
/// becomes this park's ppoll timeout, and its wake lease pokes the pipe.
struct PipePark {
    pidfd: RawFd,
    read: RawFd,
    wake: contextdb_core::read_memory::ReadShared<PipeWake>,
}

struct PipeWake {
    write: RawFd,
}

impl contextdb_core::read_memory::NativeReadWake for PipeWake {
    fn wake_read(&self) {
        let byte = 1u8;
        // SAFETY: one byte to a pipe whose write end outlives the park.
        unsafe {
            libc::write(self.write, std::ptr::addr_of!(byte).cast(), 1);
        }
    }
}

impl contextdb_core::read_memory::NativeReadWake for PipePark {
    fn wake_read(&self) {
        contextdb_core::read_memory::NativeReadWake::wake_read(&*self.wake);
    }
}

impl contextdb_core::read_contract::NativeDriverPark for PipePark {
    fn park(&self) -> contextdb_core::Result<()> {
        park_once(self.pidfd, self.read, None)
            .map_err(|error| contextdb_core::Error::Other((error.to_string()).into()))
    }

    fn park_for_ms(&self, ms: u64) -> contextdb_core::Result<()> {
        park_once(self.pidfd, self.read, Some(ms))
            .map_err(|error| contextdb_core::Error::Other((error.to_string()).into()))
    }

    fn wake_lease(&self) -> contextdb_core::read_memory::NativeReadWakeLease {
        self.wake.clone().into_native_wake_lease()
    }
}

thread_local! {
    static PREFLIGHT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Prove, once per thread, that this thread can start a helper, SIGKILL it
/// and reap it: the exact sequence an ended wait relies on. A seccomp filter
/// that permits signal 0 but not SIGKILL fails here, not mid-wait.
///
/// The preflight helper can always end WITHOUT the kill: it blocks on a byte
/// this thread holds, so if the kill is refused, giving that byte back lets
/// the helper be granted it and exit by itself, and the reap that follows is
/// certain. No outcome leaves a child behind.
fn preflight(store_path: &std::path::Path) -> bool {
    #[cfg(any(test, feature = "test-seams"))]
    let kill_denied = super::read_persistence_test_scaffold::wait_fault_for_test(store_path)
        == Some(super::read_persistence_test_scaffold::WaitFault::PreflightFails);
    #[cfg(not(any(test, feature = "test-seams")))]
    let kill_denied = false;
    let _ = store_path;
    if !kill_denied && let Some(known) = PREFLIGHT.with(std::cell::Cell::get) {
        return known;
    }
    let proven = (|| -> io::Result<bool> {
        let (holder, asker) = tempfile_for_preflight()?;
        let mut held: libc::flock = unsafe { std::mem::zeroed() };
        held.l_type = libc::F_WRLCK as libc::c_short;
        held.l_whence = libc::SEEK_SET as libc::c_short;
        held.l_len = 1;
        // SAFETY: valid request on our own descriptor.
        if unsafe { libc::fcntl(holder.as_raw_fd(), libc::F_OFD_SETLK, &mut held) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut helper = Helper::spawn(&asker, &[0])?;
        #[cfg(any(test, feature = "test-seams"))]
        super::read_persistence_test_scaffold::note_reader_release_block_for_test(
            super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::HelperSpawned {
                pid: helper.pid(),
            },
        );
        #[cfg(any(test, feature = "test-seams"))]
        let _denied = KillFault::arm(kill_denied);
        let killed = helper.kill().is_ok();
        if !killed {
            // Let the helper be granted the byte and exit on its own.
            held.l_type = libc::F_UNLCK as libc::c_short;
            // SAFETY: as above.
            unsafe {
                libc::fcntl(holder.as_raw_fd(), libc::F_OFD_SETLK, &mut held);
            }
        }
        // Certain either way: killed, or granted and exiting.
        let reaped = helper.reap(true)?;
        helper.release_stack();
        #[cfg(any(test, feature = "test-seams"))]
        super::read_persistence_test_scaffold::note_reader_release_block_for_test(
            super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::Reaped,
        );
        Ok(killed && reaped.is_some())
    })()
    .unwrap_or(false);
    if !kill_denied {
        PREFLIGHT.with(|cell| cell.set(Some(proven)));
    }
    proven
}

/// Two descriptions of one anonymous file: the first holds a byte, the
/// helper asks for it through the second and so blocks.
fn tempfile_for_preflight() -> io::Result<(File, File)> {
    let name = std::ffi::CString::new("contextdb-writer-wait-preflight")
        .map_err(|_| io::Error::other("preflight name"))?;
    // SAFETY: a valid name; MFD_CLOEXEC.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fresh descriptor.
    let first = unsafe { File::from_raw_fd(fd) };
    let path = format!("/proc/self/fd/{fd}");
    let second = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    Ok((first, second))
}

/// How a cancellable wait ended.
pub(crate) enum WaitEnded {
    /// Every requested lock is held on the writer's description.
    Granted,
    Refused(WriterAcquireRefusal),
    /// The helper's death could not be confirmed: the caller must keep the
    /// writer's description (and so everything it holds) for as long as the
    /// helper may live.
    Quarantine(Helper),
}

/// Request `offsets` exclusive on `file` through a helper, sleeping exactly
/// once until they are granted, the caller cancels, or its deadline arrives.
pub(crate) fn wait_for_helper(
    file: &File,
    offsets: &[i64],
    stop: Option<&contextdb_core::read_contract::OwnerReadCancellation>,
    deadline: Option<(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    store_path: &std::path::Path,
) -> WaitEnded {
    wait_for_helper_mode(file, offsets, stop, deadline, store_path, None)
}

struct ReadWaitFunding {
    memory: Arc<crate::read_image_memory::ImageMemory>,
    bytes: usize,
}
impl Drop for ReadWaitFunding {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

pub(crate) fn wait_for_shared_hold(
    file: &File,
    stop: &contextdb_core::read_contract::OwnerReadCancellation,
    deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
    store_path: &std::path::Path,
    memory: Arc<crate::read_image_memory::ImageMemory>,
) -> WaitEnded {
    wait_for_helper_mode(
        file,
        &[super::physical_admission::READER_HOLD_OFFSET],
        Some(stop),
        Some(deadline),
        store_path,
        Some(memory),
    )
}

fn wait_for_helper_mode(
    file: &File,
    offsets: &[i64],
    stop: Option<&contextdb_core::read_contract::OwnerReadCancellation>,
    deadline: Option<(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    store_path: &std::path::Path,
    read_memory: Option<Arc<crate::read_image_memory::ImageMemory>>,
) -> WaitEnded {
    let funding = if let Some(memory) = &read_memory {
        let bytes = HELPER_STACK_BYTES
            + std::mem::size_of::<HelperPlan>()
            + std::mem::size_of::<WakePipe>()
            + std::mem::size_of::<PipeFutureWake>()
            + 16 * std::mem::size_of::<usize>();
        if let Err(error) = memory.reserve(bytes) {
            return WaitEnded::Refused(WriterAcquireRefusal::Io(io::Error::other(
                error.to_string(),
            )));
        }
        Some(ReadWaitFunding {
            memory: Arc::clone(memory),
            bytes,
        })
    } else {
        None
    };
    if stop.is_some_and(|stop| stop.is_cancelled()) {
        return WaitEnded::Refused(WriterAcquireRefusal::Cancelled);
    }
    if deadline
        .as_ref()
        .is_some_and(|(clock, deadline)| clock.now_ms() >= *deadline)
    {
        return WaitEnded::Refused(WriterAcquireRefusal::DeadlineExpired);
    }
    if !preflight(store_path) {
        return WaitEnded::Refused(WriterAcquireRefusal::CancellableWaitUnsupported);
    }
    #[cfg(any(test, feature = "test-seams"))]
    let _kill_fault = KillFault::arm(
        super::read_persistence_test_scaffold::wait_fault_for_test(store_path)
            == Some(
                super::read_persistence_test_scaffold::WaitFault::KillFailsAndHelperUnconfirmed,
            ),
    );
    let pipe = match WakePipe::new() {
        Ok(pipe) => Arc::new(pipe),
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Io(error)),
    };
    // Armed before the helper exists, so a cancellation at any later instant
    // is already a readable byte when the one park begins.
    let _listener = stop.map(|stop| {
        let pipe = Arc::clone(&pipe);
        stop.tell_on_cancel(move || pipe.poke())
    });
    let descriptor = if read_memory.is_some() {
        match file.try_clone() {
            Ok(file) => Some(file),
            Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Io(error)),
        }
    } else {
        None
    };
    let mut helper = match Helper::spawn_mode(
        descriptor.as_ref().unwrap_or(file),
        offsets,
        if read_memory.is_some() {
            libc::F_RDLCK as libc::c_short
        } else {
            libc::F_WRLCK as libc::c_short
        },
    ) {
        Ok(helper) => helper,
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Io(error)),
    };
    *helper._read_descriptor = descriptor;
    *helper._read_funding = funding;
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::note_reader_release_block_for_test(
        super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::HelperSpawned {
            pid: helper.pid(),
        },
    );
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::note_reader_release_block_for_test(
        super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::ParkEntered,
    );
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::note_writer_admission_for_test(
        store_path,
        super::read_persistence_test_scaffold::WriterAdmissionEvent::Waiting,
    );
    let parked = if let Some(memory) = &read_memory {
        park_read(&helper, &pipe, stop, deadline.as_ref(), memory)
    } else {
        park(&helper, &pipe, deadline.as_ref())
    };
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::note_reader_release_block_for_test(
        super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::ParkReturned,
    );
    let cancelled = || stop.is_some_and(|stop| stop.is_cancelled());
    let expired = || {
        deadline
            .as_ref()
            .is_some_and(|(clock, deadline)| clock.now_ms() >= *deadline)
    };
    let abort = |helper: Helper, refusal: WriterAcquireRefusal| match end(helper) {
        Ended::Confirmed => WaitEnded::Refused(refusal),
        Ended::Unconfirmed(helper) => WaitEnded::Quarantine(helper),
    };
    if let Err(error) = parked {
        return abort(helper, WriterAcquireRefusal::Io(error));
    }
    if cancelled() {
        return abort(helper, WriterAcquireRefusal::Cancelled);
    }
    match helper.reap(false) {
        Ok(Some(status)) => {
            let mut helper = helper;
            helper.release_stack();
            #[cfg(any(test, feature = "test-seams"))]
            super::read_persistence_test_scaffold::note_reader_release_block_for_test(
                super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::Reaped,
            );
            #[cfg(any(test, feature = "test-seams"))]
            super::read_persistence_test_scaffold::writer_wait_linearization_point_for_test(
                store_path,
            );
            // The linearization point: a cancellation or expiry observed
            // here wins over a completed acquisition, which the caller then
            // gives back; after this check the acquisition stands.
            #[cfg(any(test, feature = "test-seams"))]
            super::read_persistence_test_scaffold::note_reader_release_block_for_test(
                super::read_persistence_test_scaffold::ReaderReleaseBlockForTest::Linearized,
            );
            match status {
                ReapedStatus::Exited(0) if cancelled() => {
                    WaitEnded::Refused(WriterAcquireRefusal::Cancelled)
                }
                ReapedStatus::Exited(0) if expired() => {
                    WaitEnded::Refused(WriterAcquireRefusal::DeadlineExpired)
                }
                ReapedStatus::Exited(0) => WaitEnded::Granted,
                other => WaitEnded::Refused(WriterAcquireRefusal::HelperFailed(format!(
                    "the writer's acquisition helper ended without the store: {other:?}"
                ))),
            }
        }
        Ok(None) if expired() => abort(helper, WriterAcquireRefusal::DeadlineExpired),
        Ok(None) => abort(
            helper,
            WriterAcquireRefusal::Io(io::Error::other(
                "the writer's wait woke before its helper, its caller or its deadline",
            )),
        ),
        Err(error) => abort(helper, WriterAcquireRefusal::Io(error)),
    }
}

struct PipeFutureWake(Arc<WakePipe>);
impl std::task::Wake for PipeFutureWake {
    fn wake(self: Arc<Self>) {
        self.0.poke();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.poke();
    }
}

fn park_read(
    helper: &Helper,
    pipe: &Arc<WakePipe>,
    stop: Option<&contextdb_core::read_contract::OwnerReadCancellation>,
    deadline: Option<&(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    memory: &Arc<crate::read_image_memory::ImageMemory>,
) -> io::Result<()> {
    let Some((clock, deadline)) = deadline else {
        return park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None);
    };
    let owner = contextdb_core::read_memory::ReadMemoryOwner::from_legacy(
        memory
            .source_box_budget()
            .map_err(|error| io::Error::other(error))?,
    );
    if let Some(source) = clock
        .try_native_source(&owner)
        .map_err(|error| io::Error::other(error))?
    {
        if !source.is_funded_by(&owner) {
            return Err(io::Error::other(
                "deadline source has different memory ownership",
            ));
        }
        let layout = contextdb_core::read_memory::ReadShared::<PipeWake>::layout();
        let invoice = contextdb_core::read_memory::ReadCredit::try_new_owned(
            owner,
            layout.size(),
            "reader",
            "publication wait",
            "admit the reader deadline wake",
        )
        .map_err(|error| io::Error::other(error))?;
        let wake = contextdb_core::read_memory::ReadShared::try_from_prepaid(
            PipeWake {
                write: pipe.write.as_raw_fd(),
            },
            invoice,
        )
        .map_err(|error| io::Error::other(error))?;
        let park = PipePark {
            pidfd: helper.pidfd.as_raw_fd(),
            read: pipe.read.as_raw_fd(),
            wake,
        };
        return source
            .park_until(clock.now_ms(), *deadline, &park)
            .map_err(|error| io::Error::other(error));
    }
    // The future belongs to this same clock and absolute deadline. Its waker
    // feeds the kernel wait; only a clock wake causes an event-driven repoll.
    let mut future = clock.wait_until(*deadline);
    let bytes = std::mem::size_of_val(&*future);
    memory
        .reserve(bytes)
        .map_err(|error| io::Error::other(error))?;
    let funding = ReadWaitFunding {
        memory: Arc::clone(memory),
        bytes,
    };
    let waker = std::task::Waker::from(Arc::new(PipeFutureWake(Arc::clone(pipe))));
    let mut context = std::task::Context::from_waker(&waker);
    let result = loop {
        // Drain before polling, so a wake during poll remains readable.
        let mut byte = [0u8; 64];
        unsafe {
            libc::read(pipe.read.as_raw_fd(), byte.as_mut_ptr().cast(), byte.len());
        }
        if stop.is_some_and(|stop| stop.is_cancelled()) || clock.now_ms() >= *deadline {
            break Ok(());
        }
        if future.as_mut().poll(&mut context).is_ready() {
            break Ok(());
        }
        if let Err(error) = park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None) {
            break Err(error);
        }
        let mut event = libc::pollfd {
            fd: helper.pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        if unsafe { libc::poll(&mut event, 1, 0) } > 0 {
            break Ok(());
        }
    };
    drop(future);
    drop(waker);
    drop(funding);
    result
}

fn park(
    helper: &Helper,
    pipe: &Arc<WakePipe>,
    deadline: Option<&(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
) -> io::Result<()> {
    let Some((clock, deadline)) = deadline else {
        return park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None);
    };
    let owner = contextdb_core::read_memory::ReadMemoryOwner::from_legacy(
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(64 * 1024)
            .child()
            .map_err(|error| io::Error::other(error.to_string()))?,
    );
    let binding = contextdb_core::read_contract::NativeClockBinding::try_select(clock, &owner)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "the caller's deadline clock offers no native park",
            )
        })?;
    let layout = contextdb_core::read_memory::ReadShared::<PipeWake>::layout();
    let invoice = contextdb_core::read_memory::ReadCredit::try_new_owned(
        owner,
        layout.size(),
        "writer",
        "reader wait wake",
        "admit the writer's deadline wake",
    )
    .map_err(|error| io::Error::other(error.to_string()))?;
    let wake = contextdb_core::read_memory::ReadShared::try_from_prepaid(
        PipeWake {
            write: pipe.write.as_raw_fd(),
        },
        invoice,
    )
    .map_err(|error| io::Error::other(error.to_string()))?;
    let park = PipePark {
        pidfd: helper.pidfd.as_raw_fd(),
        read: pipe.read.as_raw_fd(),
        wake,
    };
    binding
        .park_until(*deadline, &park)
        .map_err(|error| io::Error::other(error.to_string()))
}

/// For one wait, a kill that fails and a helper whose death cannot be seen.
#[cfg(any(test, feature = "test-seams"))]
struct KillFault;

#[cfg(any(test, feature = "test-seams"))]
impl KillFault {
    fn arm(armed: bool) -> Self {
        KILL_FAILS.with(|cell| cell.set(armed));
        Self
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for KillFault {
    fn drop(&mut self) {
        KILL_FAILS.with(|cell| cell.set(false));
    }
}

/// Stores whose writer's helper could not be confirmed dead: the writer's
/// description stays open here, holding every byte the helper could still be
/// granted, until a later open or close of the same store sees it gone.
type Quarantined = std::collections::HashMap<(u64, u64), Vec<(File, Helper)>>;

static QUARANTINE: std::sync::OnceLock<std::sync::Mutex<Quarantined>> = std::sync::OnceLock::new();

fn quarantined() -> std::sync::MutexGuard<'static, Quarantined> {
    QUARANTINE
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Keep `file` and `helper` until the helper is confirmed dead.
pub(crate) fn enter_quarantine(identity: (u64, u64), file: File, helper: Helper) {
    quarantined()
        .entry(identity)
        .or_default()
        .push((file, helper));
}

/// Whether this store is still quarantined: one non-blocking death check.
/// A confirmed death releases the retained description and with it every
/// lock it held. Serialized by the registry's lock, so no second helper is
/// started for a store whose first one is unresolved.
pub(crate) fn still_quarantined(identity: (u64, u64)) -> bool {
    let mut registry = quarantined();
    let Some(helpers) = registry.get_mut(&identity) else {
        return false;
    };
    helpers.retain_mut(|(_, helper)| {
        if let Ok(Some(_)) = helper.reap(false) {
            helper.release_stack();
            false
        } else {
            true
        }
    });
    if helpers.is_empty() {
        registry.remove(&identity);
        false
    } else {
        true
    }
}
