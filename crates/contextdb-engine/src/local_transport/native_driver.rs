//! The admitted caller-thread futex shared by local reads and native transport.
use contextdb_core::read_memory::{ReadMemoryOwner, ReadShared};
use std::sync::atomic::{AtomicU32, Ordering};
pub(crate) struct NativeCallerWake {
    ready: AtomicU32,
}

impl NativeCallerWake {
    pub(crate) fn try_new_owned(owner: ReadMemoryOwner) -> contextdb_core::Result<ReadShared<Self>> {
        let invoice = contextdb_core::read_memory::ReadCredit::try_new_owned(
            owner, ReadShared::<Self>::layout().size(), "reader", "local_driver_wake",
            "Raise the declared reader memory limit.")?;
        ReadShared::try_from_prepaid_native(Self { ready: AtomicU32::new(0) }, invoice)
            .map_err(contextdb_core::Error::NativeEngineOther)
    }

    pub(crate) fn park_once(&self, timeout_ms: Option<u64>) -> contextdb_core::Result<()> {
        if self.ready.swap(0, Ordering::AcqRel) != 0 {
            return Ok(());
        }
        // Relative FUTEX_WAIT uses CLOCK_MONOTONIC. The entire timespec
        // stays on this already-owned caller's stack through the syscall.
        // Cap only to the platform's representable relative interval; the
        // next operation poll rereads the supplied clock after any return.
        let timeout = timeout_ms.map(|ms| libc::timespec {
            tv_sec: libc::time_t::try_from(ms / 1_000).unwrap_or(libc::time_t::MAX),
            tv_nsec: ((ms % 1_000) * 1_000_000) as libc::c_long,
        });
        let timeout_pointer = timeout
            .as_ref()
            .map_or(std::ptr::null(), |value| value as *const libc::timespec);
        // SAFETY: the paid shared borrow keeps the aligned word live. A
        // concurrent wake stores one before FUTEX_WAKE; expected zero
        // prevents losing that wake between our swap and kernel enqueue.
        let result = unsafe {
            libc::syscall(
                libc::SYS_futex,
                self.ready.as_ptr(),
                libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                0u32,
                timeout_pointer,
            )
        };
        if result == -1 {
            // No diagnostic allocation, retry loop, worker or timer registry.
            let error = unsafe { *libc::__errno_location() };
            if error != libc::EAGAIN && error != libc::EINTR && error != libc::ETIMEDOUT {
                return Err(contextdb_core::Error::ReadSessionNotImplemented);
            }
        }
        Ok(())
    }
}

impl contextdb_core::read_memory::NativeReadWake for NativeCallerWake {
    fn wake_read(&self) {
        self.ready.store(1, Ordering::Release);
        // SAFETY: this callback retains the same paid cell; wake neither
        // copies a host waker nor creates a scheduler/task/channel node.
        unsafe {
            libc::syscall(
                libc::SYS_futex,
                self.ready.as_ptr(),
                libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                1i32,
            );
        }
    }
}

// Inline adapter only: the original paid cell remains the sole physical
// control. Its handle is part of the admitted drain frame, with no std Arc
// wrapper or second allocation. A local type keeps the companion impls
// within Rust's trait coherence rules.
pub(crate) struct NativeCallerPark {
    pub(crate) wake: ReadShared<NativeCallerWake>,
}
impl NativeCallerPark {
    pub(crate) fn try_new(owner: ReadMemoryOwner) -> contextdb_core::Result<Self> {
        let invoice = contextdb_core::read_memory::ReadCredit::try_new_owned(
            owner, ReadShared::<NativeCallerWake>::layout().size(),
            "reader", "local_driver_wake", "Raise the declared reader memory limit.")?;
        let wake = ReadShared::try_from_prepaid_native(
            NativeCallerWake { ready: AtomicU32::new(0) }, invoice)
            .map_err(contextdb_core::Error::NativeEngineOther)?;
        Ok(Self { wake })
    }
    pub(crate) fn waker(&self) -> std::task::Waker {
        self.wake.clone().into_read_waker()
    }
}
impl contextdb_core::read_memory::NativeReadWake for NativeCallerPark {
    fn wake_read(&self) {
        contextdb_core::read_memory::NativeReadWake::wake_read(&*self.wake);
    }
}
impl contextdb_core::read_contract::NativeDriverPark for NativeCallerPark {
    fn park(&self) -> contextdb_core::Result<()> {
        self.wake.park_once(None)
    }
    fn park_for_ms(&self, ms: u64) -> contextdb_core::Result<()> {
        self.wake.park_once(Some(ms))
    }
    fn wake_lease(&self) -> contextdb_core::read_memory::NativeReadWakeLease {
        self.wake.clone().into_native_wake_lease()
    }
}
