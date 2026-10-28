//! Bounded test-owned observations of real native page custody.
//! Producers enqueue scalar facts only. Guard callbacks run during explicit
//! drain, after storage locks have been released. Tokens own no page or payer.
use crate::NativeColdReadError;
use std::{
    alloc::Layout,
    cell::RefCell,
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug)]
pub struct NativePageBackingForTest {
    pub allocation: usize,
    pub payload: usize,
    pub layout: Layout,
    pub payload_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct NativePagePaymentForTest {
    pub source_receipt: Option<usize>,
    pub fixed_cache_counter: Option<usize>,
    pub fixed_cache_receipt: Option<usize>,
    /// None requires a join to the genuine receipt's original admission.
    pub original_funder: Option<usize>,
    pub charged_bytes: usize,
    pub strong_handles: usize,
}
#[derive(Clone, Copy, Debug)]
pub enum NativePageCustodyEventForTest {
    Allocated {
        backing: NativePageBackingForTest,
        payment: NativePagePaymentForTest,
    },
    PaymentTransferred {
        backing: NativePageBackingForTest,
        old: NativePagePaymentForTest,
        new: NativePagePaymentForTest,
    },
    Installed {
        allocation: usize,
        cache: usize,
        offset: u64,
        len: usize,
    },
    Rejected {
        allocation: usize,
        cache: usize,
        offset: u64,
        len: usize,
    },
    MapHandleEvicted {
        allocation: usize,
        cache: usize,
        strong_handles: usize,
    },
    BeforePhysicalFree {
        backing: NativePageBackingForTest,
        payment: NativePagePaymentForTest,
    },
    AfterPhysicalFree {
        backing: NativePageBackingForTest,
        payment: NativePagePaymentForTest,
    },
    PaymentReleased {
        allocation: usize,
        receipt: usize,
        original_funder: Option<usize>,
        bytes: usize,
    },
    OccupancyReleased {
        allocation: usize,
        counter: usize,
        fixed_cache_receipt: Option<usize>,
        fixed: bool,
        bytes: usize,
    },
    FixedReserveAdmitted {
        receipt: usize,
        original_funder: usize,
        bytes: usize,
        control: Layout,
    },
}
#[derive(Clone, Copy, Debug)]
pub struct NativeColdReadAttemptForTest {
    pub backend: usize,
    pub cache: usize,
    pub offset: u64,
    pub len: usize,
    pub backing: NativePageBackingForTest,
    pub source_receipt: Option<usize>,
}
pub trait NativePageObserverForTest: Send + Sync {
    fn observe(&self, event: NativePageCustodyEventForTest);
    /// Borrow the producer's actual result; calling retain clones that real page alias.
    fn page_returned(&self, _retain: &dyn Fn() -> crate::CachePageForTest) {}
    fn before_cold_read(
        &self,
        _attempt: &NativeColdReadAttemptForTest,
    ) -> Option<NativeColdReadError> {
        None
    }
    fn after_cold_read(
        &self,
        _attempt: &NativeColdReadAttemptForTest,
        _result: Result<(), &NativeColdReadError>,
    ) {
    }
}
#[derive(Clone, Copy, Debug)]
pub struct NativePageCaptureCompletenessForTest {
    pub recorded: usize,
    pub delivered: usize,
    pub pending: usize,
    pub live_backings: usize,
    pub live_payments: usize,
    pub overflow: bool,
    pub closed_lost_events: bool,
}
impl NativePageCaptureCompletenessForTest {
    pub fn complete(self) -> bool {
        !self.overflow
            && !self.closed_lost_events
            && self.pending == 0
            && self.recorded == self.delivered
            && self.live_backings == 0
            && self.live_payments == 0
    }
}
struct Ring {
    events: VecDeque<NativePageCustodyEventForTest>,
    bound: usize,
    recorded: usize,
    delivered: usize,
    overflow: bool,
    closed: bool,
    closed_lost: bool,
}
struct Capture {
    observer: Arc<dyn NativePageObserverForTest>,
    ring: Mutex<Ring>,
    backings: AtomicUsize,
    payments: AtomicUsize,
    draining: AtomicBool,
}
thread_local! { static CURRENT: RefCell<Option<Arc<Capture>>> = const { RefCell::new(None) }; }
#[derive(Clone, Default)]
pub(crate) struct PageObservation(Option<Arc<Capture>>);
impl PageObservation {
    pub(crate) fn current() -> Self {
        Self(CURRENT.with(|slot| slot.borrow().clone()))
    }
    pub(crate) fn emit(&self, event: NativePageCustodyEventForTest) {
        let Some(capture) = &self.0 else { return };
        let mut ring = capture.ring.lock().unwrap_or_else(|e| e.into_inner());
        if ring.closed {
            ring.closed_lost = true;
            return;
        }
        if ring.events.len() == ring.bound {
            ring.overflow = true;
            return;
        }
        if let Some(next) = ring.recorded.checked_add(1) {
            ring.recorded = next;
        } else {
            ring.overflow = true;
            return;
        }
        ring.events.push_back(event);
    }
    pub(crate) fn backing_opened(&self) {
        if let Some(c) = &self.0 {
            c.backings.fetch_add(1, Ordering::AcqRel);
        }
    }
    pub(crate) fn backing_closed(&self) {
        if let Some(c) = &self.0 {
            c.backings.fetch_sub(1, Ordering::AcqRel);
        }
    }
    pub(crate) fn payment_opened(&self) {
        if let Some(c) = &self.0 {
            c.payments.fetch_add(1, Ordering::AcqRel);
        }
    }
    pub(crate) fn payment_closed(&self) {
        if let Some(c) = &self.0 {
            c.payments.fetch_sub(1, Ordering::AcqRel);
        }
    }
    pub(crate) fn page_returned(&self, retain: &dyn Fn() -> crate::CachePageForTest) {
        if let Some(c) = &self.0 {
            c.observer.page_returned(retain);
        }
    }
    pub(crate) fn before_cold_read(
        &self,
        attempt: &NativeColdReadAttemptForTest,
    ) -> Option<NativeColdReadError> {
        self.0
            .as_ref()
            .and_then(|c| c.observer.before_cold_read(attempt))
    }
    pub(crate) fn after_cold_read(
        &self,
        attempt: &NativeColdReadAttemptForTest,
        result: Result<(), &NativeColdReadError>,
    ) {
        if let Some(c) = &self.0 {
            c.observer.after_cold_read(attempt, result);
        }
    }
}
/// The test retains this handle until all real aliases have been released.
/// Closing with live backings/payments is an incomplete capture.
pub struct NativePageObservationForTest(Arc<Capture>);
impl NativePageObservationForTest {
    pub fn drain(&self) {
        if self.0.draining.swap(true, Ordering::AcqRel) {
            self.0
                .ring
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .overflow = true;
            return;
        }
        struct Draining<'a>(&'a AtomicBool);
        impl Drop for Draining<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _draining = Draining(&self.0.draining);
        loop {
            let event = self
                .0
                .ring
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .events
                .pop_front();
            let Some(event) = event else { break };
            self.0.observer.observe(event);
            let mut ring = self.0.ring.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(next) = ring.delivered.checked_add(1) {
                ring.delivered = next;
            } else {
                ring.overflow = true;
            }
        }
    }
    pub fn completeness(&self) -> NativePageCaptureCompletenessForTest {
        let ring = self.0.ring.lock().unwrap_or_else(|e| e.into_inner());
        NativePageCaptureCompletenessForTest {
            recorded: ring.recorded,
            delivered: ring.delivered,
            pending: ring.events.len(),
            live_backings: self.0.backings.load(Ordering::Acquire),
            live_payments: self.0.payments.load(Ordering::Acquire),
            overflow: ring.overflow,
            closed_lost_events: ring.closed_lost,
        }
    }
    pub fn close(&self) -> NativePageCaptureCompletenessForTest {
        self.drain();
        self.0.ring.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
        self.completeness()
    }
}
pub fn observe_native_pages_for_test<R>(
    observer: Arc<dyn NativePageObserverForTest>,
    capacity: usize,
    run: impl FnOnce() -> R,
) -> (R, NativePageObservationForTest) {
    let capture = Arc::new(Capture {
        observer,
        ring: Mutex::new(Ring {
            events: VecDeque::with_capacity(capacity),
            bound: capacity,
            recorded: 0,
            delivered: 0,
            overflow: false,
            closed: false,
            closed_lost: false,
        }),
        backings: AtomicUsize::new(0),
        payments: AtomicUsize::new(0),
        draining: AtomicBool::new(false),
    });
    struct Installed(Option<Arc<Capture>>);
    impl Drop for Installed {
        fn drop(&mut self) {
            CURRENT.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _installed = Installed(CURRENT.with(|slot| slot.replace(Some(capture.clone()))));
    (run(), NativePageObservationForTest(capture))
}

pub(crate) struct PaymentObservation {
    observation: PageObservation,
    allocation: usize,
    receipt: usize,
    pub(crate) original_funder: Option<usize>,
    bytes: usize,
}
impl PaymentObservation {
    pub(crate) fn new(
        observation: PageObservation,
        allocation: usize,
        receipt: usize,
        original_funder: Option<usize>,
        bytes: usize,
    ) -> Self {
        observation.payment_opened();
        Self {
            observation,
            allocation,
            receipt,
            original_funder,
            bytes,
        }
    }
    /// Called only after the existing genuine release operation has returned.
    pub(crate) fn released(self) {
        self.observation
            .emit(NativePageCustodyEventForTest::PaymentReleased {
                allocation: self.allocation,
                receipt: self.receipt,
                original_funder: self.original_funder,
                bytes: self.bytes,
            });
        self.observation.payment_closed();
    }
}

impl PaymentObservation {
    pub(crate) fn occupancy_released(
        self,
        counter: usize,
        fixed_cache_receipt: Option<usize>,
        fixed: bool,
    ) {
        self.observation
            .emit(NativePageCustodyEventForTest::OccupancyReleased {
                allocation: self.allocation,
                counter,
                fixed_cache_receipt,
                fixed,
                bytes: self.bytes,
            });
        self.observation.payment_closed();
    }
}
/// Held after the original producer's actual reserve/control receipts, so its
/// Drop reports their completed refund, never an occupancy estimate.
pub struct NativeFixedReserveObservationForTest(PaymentObservation);
impl NativeFixedReserveObservationForTest {
    pub fn actual_release_bytes(&mut self, bytes: usize) {
        self.0.bytes = bytes;
    }
}
impl Drop for NativeFixedReserveObservationForTest {
    fn drop(&mut self) {
        self.0
            .observation
            .emit(NativePageCustodyEventForTest::PaymentReleased {
                allocation: self.0.allocation,
                receipt: self.0.receipt,
                original_funder: self.0.original_funder,
                bytes: self.0.bytes,
            });
        self.0.observation.payment_closed();
    }
}
/// Called by the original reserve producer only after its real receipt exists.
/// This reports scalar facts; it performs no admission or payer transfer.
pub fn observe_fixed_reserve_for_test(
    receipt: usize,
    original_funder: usize,
    bytes: usize,
    control: Layout,
) -> NativeFixedReserveObservationForTest {
    let observation = PageObservation::current();
    observation.emit(NativePageCustodyEventForTest::FixedReserveAdmitted {
        receipt,
        original_funder,
        bytes,
        control,
    });
    NativeFixedReserveObservationForTest(PaymentObservation::new(
        observation,
        receipt,
        receipt,
        Some(original_funder),
        bytes,
    ))
}

#[derive(Clone, Copy, Debug)]
pub struct NativeCacheOccupancyForTest {
    pub cache: usize,
    pub limit: usize,
    pub index_bytes: usize,
    pub read_held_bytes: usize,
    pub read_fixed_bytes: usize,
    pub write_held_bytes: usize,
    pub write_fixed_bytes: usize,
    pub read_counter: usize,
    pub write_counter: usize,
    pub read_fixed_receipt: Option<usize>,
    pub write_fixed_receipt: Option<usize>,
}
