//! Immediate, non-queueing owner admission and its RAII slot lease.

use super::{OwnerReadScaffoldError, OwnerReadScaffoldResult};
use crate::local_transport::OwnerAdmissionCounters;
use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, ReadFailure, ReadFailureDetail, ReadFailureKind,
    ReadLimits,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// One immediate owner-reader slot gate. Admission deliberately has no waiter,
/// queue, condition variable, or sender. The one stored waker belongs only to
/// shutdown: the thread draining slots must be told when the final lease—or a
/// reservation that lost the close race—puts its slot back.
#[derive(Debug)]
pub struct OwnerAdmission {
    limits: OwnerReadLimits,
    accepting: AtomicBool,
    active_readers: AtomicU64,
    drain_waiter: Mutex<Option<Waker>>,
}

impl OwnerAdmission {
    pub fn new(limits: OwnerReadLimits) -> OwnerReadScaffoldResult<Arc<Self>> {
        limits.validate()?;
        Ok(Arc::new(Self {
            limits,
            accepting: AtomicBool::new(true),
            active_readers: AtomicU64::new(0),
            drain_waiter: Mutex::new(None),
        }))
    }

    /// Compute effective limits only through the shared field-by-field seam.
    pub fn effective_limits(
        requested: ReadLimits,
        owner: OwnerReadLimits,
    ) -> OwnerReadScaffoldResult<ReadLimits> {
        Ok(ReadLimits::stricter_of(requested, owner.limits)?)
    }

    pub const fn configured_limits(&self) -> OwnerReadLimits {
        self.limits
    }

    pub fn counters(&self) -> OwnerAdmissionCounters {
        OwnerAdmissionCounters {
            capacity: self.limits.concurrency,
            active_readers: self.active_readers.load(Ordering::SeqCst),
        }
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }

    /// Attempt one non-blocking acquisition after authentication and effective
    /// limit calculation. A closed gate returns `OwnerNotServing`; an open gate
    /// with no free slot returns the existing typed `OwnerAtCapacity` refusal.
    pub fn try_acquire(
        self: &Arc<Self>,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
    ) -> OwnerReadScaffoldResult<RequestLease> {
        self.try_acquire_with_post_reservation(effective_limits, cancellation, || {})
    }

    fn try_acquire_with_post_reservation(
        self: &Arc<Self>,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
        after_reservation: impl FnOnce(),
    ) -> OwnerReadScaffoldResult<RequestLease> {
        if !self.is_accepting() {
            return Err(owner_not_serving());
        }
        let capacity = self.limits.concurrency;
        let reserved =
            self.active_readers
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    if active < capacity {
                        Some(active + 1)
                    } else {
                        None
                    }
                });
        if reserved.is_err() {
            return Err(OwnerReadScaffoldError::Refused(
                ReadFailure::new(ReadFailureKind::OwnerAtCapacity, ReadFailureDetail::None)
                    .expect("owner-at-capacity refusal carries the canonical empty detail"),
            ));
        }
        after_reservation();
        // `accepting` and the slot counter are separate atomics. Shutdown may
        // close the gate after the first check but before this reservation, so
        // recheck after the CAS and roll the slot back before returning. This
        // makes a completed close_to_new_work an absolute fence: no request
        // which races across it can remain admitted.
        if !self.is_accepting() {
            self.release_slot();
            return Err(owner_not_serving());
        }
        Ok(RequestLease::acquired(
            Arc::clone(self),
            effective_limits,
            cancellation,
        ))
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn try_acquire_with_post_reservation_hook_for_test(
        self: &Arc<Self>,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
        after_reservation: impl FnOnce(),
    ) -> OwnerReadScaffoldResult<RequestLease> {
        self.try_acquire_with_post_reservation(effective_limits, cancellation, after_reservation)
    }

    pub fn close_to_new_work(&self) -> OwnerReadScaffoldResult<()> {
        self.accepting.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn poll_drained(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.active_readers.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(());
        }
        let mut waiter = self.drain_waiter.lock().expect("owner drain waiter");
        waiter.replace(context.waker().clone());
        if self.active_readers.load(Ordering::SeqCst) == 0 {
            waiter.take();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn poll_drained_for_test(&self, context: &mut Context<'_>) -> Poll<()> {
        self.poll_drained(context)
    }

    fn release_slot(&self) {
        let released =
            self.active_readers
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    active.checked_sub(1)
                });
        if matches!(released, Ok(1))
            && let Some(waker) = self.drain_waiter.lock().expect("owner drain waiter").take()
        {
            waker.wake();
        }
    }
}

fn owner_not_serving() -> OwnerReadScaffoldError {
    OwnerReadScaffoldError::Refused(
        ReadFailure::new(ReadFailureKind::OwnerNotServing, ReadFailureDetail::None)
            .expect("owner-not-serving refusal carries the canonical empty detail"),
    )
}

/// One admitted request. A cursor moves this lease into its retained entry;
/// every other path drops it when the request reaches a terminal outcome.
#[derive(Debug)]
pub struct RequestLease {
    admission: Arc<OwnerAdmission>,
    effective_limits: ReadLimits,
    cancellation: OwnerReadCancellation,
    released: bool,
}

impl RequestLease {
    pub(crate) fn acquired(
        admission: Arc<OwnerAdmission>,
        effective_limits: ReadLimits,
        cancellation: OwnerReadCancellation,
    ) -> Self {
        Self {
            admission,
            effective_limits,
            cancellation,
            released: false,
        }
    }

    pub const fn effective_limits(&self) -> ReadLimits {
        self.effective_limits
    }

    pub const fn cancellation(&self) -> &OwnerReadCancellation {
        &self.cancellation
    }

    pub fn release(mut self) {
        self.release_once();
    }

    fn release_once(&mut self) {
        if !self.released {
            self.admission.release_slot();
            self.released = true;
        }
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.release_once();
    }
}
