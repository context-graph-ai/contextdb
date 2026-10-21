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

use super::physical_admission::{
    HelperExit as ReapedStatus, HelperFailure, WaitIo, WriterAcquireRefusal,
};
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner, ReadShared};

fn wait_credit(owner: &ReadMemoryOwner, bytes: usize) -> Result<ReadCredit, WriterAcquireRefusal> {
    ReadCredit::try_new_owned(
        owner.clone(),
        bytes,
        "storage",
        "handoff_wait",
        "Admit the original operation's wait storage.",
    )
    .map_err(WriterAcquireRefusal::Memory)
}

struct HelperAdmission {
    plan: contextdb_core::read_memory::ReadPrepared<HelperPlan>,
    stack: HelperStack,
}
impl HelperAdmission {
    fn new(owner: &ReadMemoryOwner) -> Result<Self, WriterAcquireRefusal> {
        let plan = wait_credit(owner, ReadShared::<HelperPlan>::layout().size())?;
        let stack = wait_credit(owner, HELPER_STACK_BYTES)?;
        let plan = ReadShared::try_prepare_native(plan).map_err(|error| {
            WriterAcquireRefusal::Memory(contextdb_core::Error::NativeEngineOther(error))
        })?;
        let stack = HelperStack::map(stack).map_err(WriterAcquireRefusal::Io)?;
        Ok(Self { plan, stack })
    }
}

/// The invoice remains outside the mapping until munmap completes, including
/// setup failure and unwinding before a helper has started.
struct HelperStack {
    address: *mut c_void,
    _credit: ReadCredit,
}
impl HelperStack {
    fn map(credit: ReadCredit) -> io::Result<Self> {
        // SAFETY: a private anonymous mapping of the exact admitted extent.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                HELPER_STACK_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_STACK,
                -1,
                0,
            )
        };
        if address == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        #[cfg(any(test, feature = "test-seams"))]
        HELPER_STACKS_MAPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Self {
            address,
            _credit: credit,
        })
    }
}
impl Drop for HelperStack {
    fn drop(&mut self) {
        // SAFETY: unpublished, failed launch, or confirmed reaped helper only.
        unsafe {
            libc::munmap(self.address, HELPER_STACK_BYTES);
        }
        #[cfg(any(test, feature = "test-seams"))]
        HELPER_STACKS_MAPPED.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

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
    // Parent-only links and retained resources occupy the same exact admitted
    // plan allocation. The raw helper reads only the request fields above.
    quarantine: std::sync::Mutex<QuarantineSlot>,
}

#[derive(Default)]
struct QuarantineSlot {
    identity: (u64, u64),
    retained: Option<(File, Helper)>,
    next: Option<ReadShared<HelperPlan>>,
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
    stack: std::mem::ManuallyDrop<Option<HelperStack>>,
    // Kept alive until the helper is reaped: it reads this plan.
    _plan: std::mem::ManuallyDrop<ReadShared<HelperPlan>>,
    _read_memory: std::mem::ManuallyDrop<Option<crate::read_image_memory::ImageMemoryHandle>>,
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
    fn spawn_mode(
        file: &File,
        offsets: &[i64],
        lock_type: libc::c_short,
        admission: HelperAdmission,
    ) -> io::Result<Self> {
        let mut plan = HelperPlan {
            descriptor: file.as_raw_fd(),
            // SAFETY: getpid cannot fail.
            parent: unsafe { libc::getpid() },
            count: offsets.len().min(3),
            offsets: [0; 3],
            lock_type,
            quarantine: std::sync::Mutex::new(QuarantineSlot::default()),
        };
        plan.offsets[..plan.count].copy_from_slice(&offsets[..plan.count]);
        let plan = admission.plan.publish(plan);
        let stack = admission.stack;
        let mut pidfd: libc::c_int = -1;
        let spawned = {
            let _blocked = SignalsBlocked::now()?;
            // SAFETY: a fresh writable stack; the immutable shared plan is
            // retained until reap. The helper has its own copied address space.
            unsafe {
                libc::clone(
                    helper_main,
                    (stack.address as *mut u8).add(HELPER_STACK_BYTES) as *mut c_void,
                    libc::CLONE_FILES | libc::CLONE_PIDFD,
                    std::ptr::from_ref(&*plan) as *mut c_void,
                    std::ptr::addr_of_mut!(pidfd),
                )
            }
        };
        if spawned == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // SAFETY: CLONE_PIDFD stored a fresh descriptor this call owns.
            pidfd: unsafe { OwnedFd::from_raw_fd(pidfd) },
            pid: spawned,
            stack: std::mem::ManuallyDrop::new(Some(stack)),
            _plan: std::mem::ManuallyDrop::new(plan),
            _read_memory: std::mem::ManuallyDrop::new(None),
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
        // Only called after confirmed reap: unmap before settling its invoice.
        drop(self.stack.take());
    }
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
        if self.stack.is_some()
            && let Ok(Some(_)) = self.reap(false)
        {
            self.release_stack();
        }
        if self.stack.is_none() {
            // The helper cannot access its plan/descriptor after confirmed
            // death. Free those before returning their original payer's credit.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self._plan);
                std::mem::ManuallyDrop::drop(&mut self._read_descriptor);
                std::mem::ManuallyDrop::drop(&mut self._read_memory);
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
    wake: ReadShared<WakePipe>,
}

impl contextdb_core::read_memory::NativeReadWake for PipePark {
    fn wake_read(&self) {
        contextdb_core::read_memory::NativeReadWake::wake_read(&*self.wake);
    }
}

// park_once returns only last_os_error after a failed raw syscall. Keep the
// actual errno in the existing native Other bridge, with no String or I/O box.
fn native_park_error(error: io::Error) -> contextdb_core::Error {
    let errno =
        std::num::NonZeroI32::new(error.raw_os_error().expect("failed ppoll has an OS error"))
            .expect("failed ppoll has nonzero errno");
    contextdb_core::Error::FundedEngine(contextdb_core::error::FundedEngineError::from_os_error(
        errno,
    ))
}

impl contextdb_core::read_contract::NativeDriverPark for PipePark {
    fn park(&self) -> contextdb_core::Result<()> {
        park_once(self.pidfd, self.read, None).map_err(native_park_error)
    }

    fn park_for_ms(&self, ms: u64) -> contextdb_core::Result<()> {
        park_once(self.pidfd, self.read, Some(ms)).map_err(native_park_error)
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
fn preflight(
    store_path: &std::path::Path,
    owner: &ReadMemoryOwner,
) -> Result<bool, WriterAcquireRefusal> {
    #[cfg(any(test, feature = "test-seams"))]
    let kill_denied = super::read_persistence_test_scaffold::wait_fault_for_test(store_path)
        == Some(super::read_persistence_test_scaffold::WaitFault::PreflightFails);
    #[cfg(not(any(test, feature = "test-seams")))]
    let kill_denied = false;
    let _ = store_path;
    if !kill_denied && let Some(known) = PREFLIGHT.with(std::cell::Cell::get) {
        return Ok(known);
    }
    // Admission failures are real memory refusals, never platform refusals.
    let admission = HelperAdmission::new(owner)?;
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
        let mut helper =
            Helper::spawn_mode(&asker, &[0], libc::F_WRLCK as libc::c_short, admission)?;
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
    Ok(proven)
}

/// Two descriptions of one anonymous file: the first holds a byte, the
/// helper asks for it through the second and so blocks.
fn tempfile_for_preflight() -> io::Result<(File, File)> {
    let name = c"contextdb-writer-wait-preflight";
    // SAFETY: a valid name; MFD_CLOEXEC.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fresh descriptor.
    let first = unsafe { File::from_raw_fd(fd) };
    // Format a nonnegative descriptor entirely on the caller's stack.
    use std::io::Write as _;
    use std::os::unix::ffi::OsStrExt as _;
    let mut path = [0u8; b"/proc/self/fd/".len() + RawFd::BITS as usize];
    let capacity = path.len();
    let mut remaining = &mut path[..];
    write!(&mut remaining, "/proc/self/fd/{fd}")?;
    let written = capacity - remaining.len();
    let second = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(std::ffi::OsStr::from_bytes(&path[..written]))?;
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
    payer: Option<&crate::memory_accounting::StoreAccountantHandle>,
) -> WaitEnded {
    wait_for_helper_mode(file, offsets, stop, deadline, store_path, None, payer)
}

struct ReadWaitFunding {
    memory: crate::read_image_memory::ImageMemoryHandle,
    bytes: usize,
}
impl Drop for ReadWaitFunding {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

// The fallback API still returns an already allocated opaque future. Once its
// existing reservation succeeds, keep physical-free ordering on unwind too.
struct ReadWaitFuture<'a> {
    future: contextdb_core::read_contract::DeadlineWait<'a>,
    _funding: ReadWaitFunding,
}

pub(crate) fn wait_for_shared_hold(
    file: &File,
    stop: &contextdb_core::read_contract::OwnerReadCancellation,
    deadline: (Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64),
    store_path: &std::path::Path,
    memory: crate::read_image_memory::ImageMemoryHandle,
) -> WaitEnded {
    wait_for_helper_mode(
        file,
        &[super::physical_admission::READER_HOLD_OFFSET],
        Some(stop),
        Some(deadline),
        store_path,
        Some(memory),
        None,
    )
}

fn wait_for_helper_mode(
    file: &File,
    offsets: &[i64],
    stop: Option<&contextdb_core::read_contract::OwnerReadCancellation>,
    deadline: Option<(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    store_path: &std::path::Path,
    read_memory: Option<crate::read_image_memory::ImageMemoryHandle>,
    writer_payer: Option<&crate::memory_accounting::StoreAccountantHandle>,
) -> WaitEnded {
    if stop.is_some_and(|stop| stop.is_cancelled()) {
        return WaitEnded::Refused(WriterAcquireRefusal::Cancelled);
    }
    if deadline
        .as_ref()
        .is_some_and(|(clock, deadline)| clock.now_ms() >= *deadline)
    {
        return WaitEnded::Refused(WriterAcquireRefusal::DeadlineExpired);
    }
    let owner = match &read_memory {
        Some(memory) => memory
            .reader_budget()
            .ok_or_else(|| {
                contextdb_core::Error::Other(
                    "reader publication wait has no original reader payer".into(),
                )
            }),
        None => writer_payer
            .ok_or_else(|| {
                contextdb_core::Error::Other("writer wait has no original writer payer".into())
            })
            .and_then(|payer| {
                payer
                    .wait_memory_owner()
                    .map_err(contextdb_core::Error::NativeEngineOther)
            }),
    };
    let owner = match owner {
        Ok(owner) => owner,
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Memory(error)),
    };
    match preflight(store_path, &owner) {
        Ok(true) => {}
        Ok(false) => return WaitEnded::Refused(WriterAcquireRefusal::CancellableWaitUnsupported),
        Err(refusal) => return WaitEnded::Refused(refusal),
    }
    #[cfg(any(test, feature = "test-seams"))]
    let _kill_fault = KillFault::arm(
        super::read_persistence_test_scaffold::wait_fault_for_test(store_path)
            == Some(
                super::read_persistence_test_scaffold::WaitFault::KillFailsAndHelperUnconfirmed,
            ),
    );
    let prepared_pipe =
        wait_credit(&owner, ReadShared::<WakePipe>::layout().size()).and_then(|invoice| {
            ReadShared::try_prepare_native(invoice).map_err(|error| {
                WriterAcquireRefusal::Memory(contextdb_core::Error::NativeEngineOther(error))
            })
        });
    let prepared_pipe = match prepared_pipe {
        Ok(pipe) => pipe,
        Err(refusal) => return WaitEnded::Refused(refusal),
    };
    let pipe = match WakePipe::new() {
        Ok(pipe) => prepared_pipe.publish(pipe),
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Io(error)),
    };
    // Register before the helper exists. Even a host-created cancellation
    // token charges this engine-created node and callback to the operation.
    let _listener = match stop
        .map(|stop| {
            let pipe = pipe.clone();
            stop.try_tell_on_cancel_with_owner(move || pipe.poke(), owner.clone())
        })
        .transpose()
    {
        Ok(listener) => listener,
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Memory(error)),
    };
    let admission = match HelperAdmission::new(&owner) {
        Ok(admission) => admission,
        Err(refusal) => return WaitEnded::Refused(refusal),
    };
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
        admission,
    ) {
        Ok(helper) => helper,
        Err(error) => return WaitEnded::Refused(WriterAcquireRefusal::Io(error)),
    };
    *helper._read_descriptor = descriptor;
    *helper._read_memory = read_memory.clone();
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
        park_read(&helper, &pipe, stop, deadline.as_ref(), memory, &owner)
    } else {
        park(&helper, &pipe, deadline.as_ref(), &owner)
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
    if let Err(refusal) = parked {
        return abort(helper, refusal);
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
                other => {
                    WaitEnded::Refused(WriterAcquireRefusal::HelperFailed(HelperFailure(other)))
                }
            }
        }
        Ok(None) if expired() => abort(helper, WriterAcquireRefusal::DeadlineExpired),
        Ok(None) => abort(
            helper,
            WriterAcquireRefusal::NativeIo(WaitIo::Static {
                kind: io::ErrorKind::Other,
                reason: "the writer's wait woke before its helper, its caller or its deadline",
            }),
        ),
        Err(error) => abort(helper, WriterAcquireRefusal::Io(error)),
    }
}

impl contextdb_core::read_memory::NativeReadWake for WakePipe {
    fn wake_read(&self) {
        self.poke();
    }
}

fn park_read(
    helper: &Helper,
    pipe: &ReadShared<WakePipe>,
    stop: Option<&contextdb_core::read_contract::OwnerReadCancellation>,
    deadline: Option<&(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    memory: &crate::read_image_memory::ImageMemoryHandle,
    owner: &ReadMemoryOwner,
) -> Result<(), WriterAcquireRefusal> {
    let Some((clock, deadline)) = deadline else {
        return park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None)
            .map_err(WriterAcquireRefusal::Io);
    };
    if let Some(source) = clock
        .try_native_source(owner)
        .map_err(WriterAcquireRefusal::Memory)?
    {
        if !source.is_funded_by(owner) {
            return Err(WriterAcquireRefusal::NativeIo(WaitIo::Static {
                kind: io::ErrorKind::Other,
                reason: "deadline source has different memory ownership",
            }));
        }
        let park = PipePark {
            pidfd: helper.pidfd.as_raw_fd(),
            read: pipe.read.as_raw_fd(),
            wake: pipe.clone(),
        };
        return source
            .park_until(clock.now_ms(), *deadline, &park)
            .map_err(WriterAcquireRefusal::Memory);
    }
    // The future belongs to this same clock and absolute deadline. Its waker
    // feeds the kernel wait; only a clock wake causes an event-driven repoll.
    let future = clock.wait_until(*deadline);
    let bytes = std::mem::size_of_val(&*future);
    memory
        .reserve(bytes)
        .map_err(WriterAcquireRefusal::Memory)?;
    let mut future = ReadWaitFuture {
        future,
        _funding: ReadWaitFunding {
            memory: memory.clone(),
            bytes,
        },
    };
    let waker = pipe.clone().into_read_waker();
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
        if future.future.as_mut().poll(&mut context).is_ready() {
            break Ok(());
        }
        if let Err(error) = park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None) {
            break Err(WriterAcquireRefusal::Io(error));
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
    result
}

fn park(
    helper: &Helper,
    pipe: &ReadShared<WakePipe>,
    deadline: Option<&(Arc<dyn contextdb_core::read_contract::DeadlineClock>, u64)>,
    owner: &ReadMemoryOwner,
) -> Result<(), WriterAcquireRefusal> {
    let Some((clock, deadline)) = deadline else {
        return park_once(helper.pidfd.as_raw_fd(), pipe.read.as_raw_fd(), None)
            .map_err(WriterAcquireRefusal::Io);
    };
    let binding = contextdb_core::read_contract::NativeClockBinding::try_select(clock, owner)
        .map_err(|error| match error {
            error @ contextdb_core::Error::MemoryBudgetExceeded { .. }
            | error @ contextdb_core::Error::NativeEngineOther(
                contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded { .. }
                | contextdb_core::read_memory::NativeRefusal::ProcessMemoryBudgetExceeded { .. }
                | contextdb_core::read_memory::NativeRefusal::AllocatorFailed { .. },
            ) => WriterAcquireRefusal::Memory(error),
            _ => WriterAcquireRefusal::NativeIo(WaitIo::Static {
                kind: io::ErrorKind::Unsupported,
                reason: "the caller's deadline clock offers no native park",
            }),
        })?;
    let park = PipePark {
        pidfd: helper.pidfd.as_raw_fd(),
        read: pipe.read.as_raw_fd(),
        wake: pipe.clone(),
    };
    binding
        .park_until(*deadline, &park)
        .map_err(|error| WriterAcquireRefusal::NativeIo(WaitIo::Clock(error)))
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
// Only the endpoints are static. Every link and retained entry is prepaid inside
// its helper's actual plan cell, before mmap/clone. No registry capacity or
// failure-time allocation is needed, and no global payer funds these entries.
type QuarantineLink = Option<ReadShared<HelperPlan>>;
struct Quarantined {
    head: QuarantineLink,
    tail: QuarantineLink,
}

static QUARANTINE: std::sync::Mutex<Quarantined> = std::sync::Mutex::new(Quarantined {
    head: None,
    tail: None,
});

fn quarantined() -> std::sync::MutexGuard<'static, Quarantined> {
    QUARANTINE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Keep `file` and `helper` until the helper is confirmed dead. Publishing the
/// prepaid slot only moves owners and links; even an exhausted payer can do it.
pub(crate) fn enter_quarantine(identity: (u64, u64), file: File, helper: Helper) {
    let plan = ReadShared::clone(&helper._plan);
    let mut registry = quarantined();
    {
        let mut slot = plan.quarantine.lock().unwrap_or_else(|p| p.into_inner());
        slot.identity = identity;
        slot.retained = Some((file, helper));
    }
    // Constant-time append preserves the existing per-store insertion order.
    // Both static handles only retain the same paid cells; neither allocates.
    if let Some(tail) = &registry.tail {
        tail.quarantine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next = Some(plan.clone());
    } else {
        registry.head = Some(plan.clone());
    }
    registry.tail = Some(plan);
}

/// Whether this store is still quarantined: one non-blocking death check per
/// retained helper of this store, serialized with insertion. Other stores'
/// helpers are never polled. A detached node has no next link, so final free
/// cannot recursively destroy the registry's remaining chain.
pub(crate) fn still_quarantined(identity: (u64, u64)) -> bool {
    let mut registry = quarantined();
    let mut previous: QuarantineLink = None;
    let mut current = registry.head.clone();
    let mut remains = false;
    while let Some(plan) = current {
        let mut slot = plan.quarantine.lock().unwrap_or_else(|p| p.into_inner());
        let mut remove = false;
        if slot.identity == identity {
            if let Some((_, helper)) = &mut slot.retained {
                if let Ok(Some(_)) = helper.reap(false) {
                    helper.release_stack();
                    remove = true;
                } else {
                    remains = true;
                }
            }
        }
        current = slot.next.clone();
        if remove {
            let next = slot.next.take();
            if next.is_none() {
                registry.tail = previous.clone();
            }
            if let Some(previous) = &previous {
                previous
                    .quarantine
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .next = next;
            } else {
                registry.head = next;
            }
            // Break the rooted self-reference before dropping the helper. Its
            // confirmed-death Drop releases the plan, descriptor and reader
            // owner; this traversal's plan handle retains the original credit
            // until the node's final physical deallocation.
            let retained = slot.retained.take();
            drop(slot);
            drop(retained);
        } else {
            drop(slot);
            previous = Some(plan);
        }
    }
    remains
}
