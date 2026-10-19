//! Private engine capture transport; Native data never escape their invoice.
use contextdb_core::{Lsn, SnapshotId};
use std::any::Any;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Weak};

type Erased = dyn Any + Send + Sync;

struct Invoice {
    credit: Box<Erased>,
    retire: fn(Box<Erased>),
    bytes: usize,
}
impl Invoice {
    fn retire<C: Any + Send + Sync>(credit: Box<Erased>) {
        // End the Box owner's scope before dropping the moved reservation.
        // A move alone is insufficient: shallow Box cleanup is scope-bound.
        let credit = {
            let boxed = credit
                .downcast::<C>()
                .unwrap_or_else(|_| unreachable!("invoice type"));
            *boxed
        };
        drop(credit);
    }
}

// No weak, raw, deref-to-Arc, or into-Arc door exists for this owner.
struct InvoiceHandle(ManuallyDrop<Arc<ManuallyDrop<Invoice>>>);
impl Clone for InvoiceHandle {
    fn clone(&self) -> Self {
        Self(ManuallyDrop::new(Arc::clone(&self.0)))
    }
}
impl Drop for InvoiceHandle {
    fn drop(&mut self) {
        // SAFETY: the only move from this ManuallyDrop, in its destructor.
        let arc = unsafe { ManuallyDrop::take(&mut self.0) };
        // Every contender uses into_inner. The winner receives the invoice
        // only after its Arc allocation has been freed, including in a race.
        if let Some(invoice) = Arc::into_inner(arc) {
            let Invoice { credit, retire, .. } = ManuallyDrop::into_inner(invoice);
            retire(credit);
        }
    }
}

/// Field order releases the data count before its invoice, also during unwind.
#[doc(hidden)]
pub struct NativeCapture {
    data: Arc<Erased>,
    invoice: InvoiceHandle,
}
#[doc(hidden)]
pub struct NativeCaptureWeak {
    data: Weak<Erased>,
    invoice: InvoiceHandle,
}
impl NativeCapture {
    /// Reserve this exact sum from the original before from_prepaid allocates.
    pub fn allocation_bytes<T, C>() -> Option<usize> {
        crate::staged_source::source_arc_layout::<T>()
            .size()
            .checked_add(crate::staged_source::source_arc_layout::<ManuallyDrop<Invoice>>().size())?
            .checked_add(std::mem::size_of::<C>())
    }
    pub fn from_prepaid<T: Any + Send + Sync, C: Any + Send + Sync>(
        value: T,
        credit: C,
        bytes: usize,
    ) -> Self {
        let invoice = InvoiceHandle(ManuallyDrop::new(Arc::new(ManuallyDrop::new(Invoice {
            credit: Box::new(credit),
            retire: Invoice::retire::<C>,
            bytes,
        }))));
        Self {
            data: Arc::new(value),
            invoice,
        }
    }
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.data.downcast_ref()
    }
    pub fn downgrade(&self) -> NativeCaptureWeak {
        let invoice = self.invoice.clone();
        NativeCaptureWeak {
            data: Arc::downgrade(&self.data),
            invoice,
        }
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
    pub fn data_ptr(&self) -> *const () {
        Arc::as_ptr(&self.data).cast::<()>()
    }
    pub fn retained_bytes(&self) -> usize {
        self.invoice.0.bytes
    }
}
impl Clone for NativeCapture {
    fn clone(&self) -> Self {
        let invoice = self.invoice.clone();
        Self {
            data: self.data.clone(),
            invoice,
        }
    }
}
impl NativeCaptureWeak {
    pub fn upgrade(&self) -> Option<NativeCapture> {
        let invoice = self.invoice.clone();
        Some(NativeCapture {
            data: self.data.upgrade()?,
            invoice,
        })
    }
    pub fn strong_count(&self) -> usize {
        self.data.strong_count()
    }
    pub fn data_ptr(&self) -> *const () {
        self.data.as_ptr().cast::<()>()
    }
}
impl Clone for NativeCaptureWeak {
    fn clone(&self) -> Self {
        let invoice = self.invoice.clone();
        Self {
            data: self.data.clone(),
            invoice,
        }
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub enum CaptureHandle {
    Legacy(Arc<Erased>),
    Native(NativeCapture),
}
#[doc(hidden)]
#[derive(Clone, Default)]
pub enum CaptureWeak {
    #[default]
    Empty,
    Legacy(Weak<Erased>),
    Native(NativeCaptureWeak),
}
impl CaptureHandle {
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        match self {
            Self::Legacy(data) => data.downcast_ref(),
            Self::Native(data) => data.downcast_ref(),
        }
    }
    pub fn downgrade(&self) -> CaptureWeak {
        match self {
            Self::Legacy(data) => CaptureWeak::Legacy(Arc::downgrade(data)),
            Self::Native(data) => CaptureWeak::Native(data.downgrade()),
        }
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Legacy(a), Self::Legacy(b)) => Arc::ptr_eq(a, b),
            (Self::Native(a), Self::Native(b)) => a.ptr_eq(b),
            _ => false,
        }
    }
    pub fn data_ptr(&self) -> *const () {
        match self {
            Self::Legacy(data) => Arc::as_ptr(data).cast::<()>(),
            Self::Native(data) => data.data_ptr(),
        }
    }
}
impl std::fmt::Debug for CaptureHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CapturedRoot")
            .field(&self.data_ptr())
            .finish()
    }
}
impl CaptureWeak {
    pub fn upgrade(&self) -> Option<CaptureHandle> {
        match self {
            Self::Empty => None,
            Self::Legacy(data) => data.upgrade().map(CaptureHandle::Legacy),
            Self::Native(data) => data.upgrade().map(CaptureHandle::Native),
        }
    }
    pub fn strong_count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Legacy(data) => data.strong_count(),
            Self::Native(data) => data.strong_count(),
        }
    }
    pub fn data_ptr(&self) -> *const () {
        match self {
            Self::Empty => std::ptr::null(),
            Self::Legacy(data) => data.as_ptr().cast::<()>(),
            Self::Native(data) => data.data_ptr(),
        }
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct EnginePublishedRead {
    pub snapshot: SnapshotId,
    pub lsn: Lsn,
    pub handle: Option<CaptureHandle>,
}
impl From<crate::PublishedRead> for EnginePublishedRead {
    fn from(read: crate::PublishedRead) -> Self {
        Self {
            snapshot: read.snapshot,
            lsn: read.lsn,
            handle: read.handle.map(CaptureHandle::Legacy),
        }
    }
}
impl EnginePublishedRead {
    pub(crate) fn legacy(&self) -> Option<crate::PublishedRead> {
        let handle = match &self.handle {
            Some(CaptureHandle::Native(_)) => return None,
            Some(CaptureHandle::Legacy(data)) => Some(data.clone()),
            None => None,
        };
        Some(crate::PublishedRead {
            snapshot: self.snapshot,
            lsn: self.lsn,
            handle,
        })
    }
}

impl std::fmt::Debug for EnginePublishedRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedRead").field("snapshot", &self.snapshot)
            .field("lsn", &self.lsn).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod paired_capture_contract_tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Barrier, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Trace {
        events: Mutex<Vec<&'static str>>,
        payload_drops: AtomicUsize,
        credit_drops: AtomicUsize,
        early_credit: AtomicUsize,
    }
    impl Trace {
        fn assert_counts(&self, payload: usize, credit: usize) {
            assert_eq!(self.payload_drops.load(Ordering::SeqCst), payload);
            assert_eq!(self.credit_drops.load(Ordering::SeqCst), credit);
            assert_eq!(self.early_credit.load(Ordering::SeqCst), 0);
        }
        fn assert_finished(&self) {
            self.assert_counts(1, 1);
            assert_eq!(*self.events.lock().unwrap(), ["payload", "credit"]);
        }
    }
    struct Payload {
        trace: Arc<Trace>,
        gate: Option<(Arc<Barrier>, Arc<Barrier>)>,
        unwind: bool,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.trace.payload_drops.fetch_add(1, Ordering::SeqCst);
            self.trace.events.lock().unwrap().push("payload");
            self.trace.early_credit.fetch_add(
                self.trace.credit_drops.load(Ordering::SeqCst), Ordering::SeqCst);
            if let Some((entered, resume)) = &self.gate {
                entered.wait();
                resume.wait();
                self.trace.early_credit.fetch_add(
                    self.trace.credit_drops.load(Ordering::SeqCst), Ordering::SeqCst);
            }
            if self.unwind { std::panic::panic_any("payload destructor probe"); }
        }
    }
    struct Credit(Arc<Trace>);
    impl Drop for Credit {
        fn drop(&mut self) {
            self.0.credit_drops.fetch_add(1, Ordering::SeqCst);
            self.0.events.lock().unwrap().push("credit");
        }
    }
    fn capture(trace: &Arc<Trace>, unwind: bool,
        gate: Option<(Arc<Barrier>, Arc<Barrier>)>) -> NativeCapture {
        let bytes = NativeCapture::allocation_bytes::<Payload, Credit>().unwrap();
        NativeCapture::from_prepaid(Payload { trace: trace.clone(), gate, unwind },
            Credit(trace.clone()), bytes)
    }
    fn invoice_owners(capture: &NativeCaptureWeak) -> usize {
        Arc::strong_count(&capture.invoice.0)
    }

    // Frame: data destruction at last strong, credit at last weak; successful
    // upgrade owns both; failed upgrade must return its temporary invoice count.
    #[test]
    fn paired_capture_last_strong_last_weak_and_upgrade_sequence() {
        let trace = Arc::new(Trace::default());
        let root = capture(&trace, false, None);
        let root_clone = root.clone();
        let weak = root.downgrade();
        let weak_clone = weak.clone();
        let upgraded = weak.upgrade().expect("live strong root upgrades");
        assert!(root.ptr_eq(&upgraded));
        assert_eq!(root.data_ptr(), weak.data_ptr());
        assert_eq!(weak.strong_count(), 3);
        assert_eq!(invoice_owners(&weak), 5);
        drop(root);
        drop(root_clone);
        trace.assert_counts(0, 0);
        assert_eq!(weak.strong_count(), 1);
        assert_eq!(invoice_owners(&weak), 3);
        drop(upgraded);
        trace.assert_counts(1, 0);
        assert_eq!(weak.strong_count(), 0);
        assert_eq!(invoice_owners(&weak), 2);
        for _ in 0..3 {
            assert!(weak.upgrade().is_none());
            assert_eq!(invoice_owners(&weak), 2, "failed upgrade leaked an invoice count");
            trace.assert_counts(1, 0);
        }
        drop(weak);
        trace.assert_counts(1, 0);
        assert_eq!(invoice_owners(&weak_clone), 1);
        drop(weak_clone);
        trace.assert_finished();
    }

    // Frame: the enum's Native borrowed downcast retains the paid owner.
    #[test]
    fn paired_capture_native_borrowed_downcast_keeps_its_invoice() {
        let trace = Arc::new(Trace::default());
        let owner = CaptureHandle::Native(capture(&trace, false, None));
        let retained = owner.clone();
        let weak = owner.downgrade();
        let upgraded = weak.upgrade().expect("Native enum upgrades");
        assert!(matches!(&upgraded, CaptureHandle::Native(_)));
        assert!(owner.ptr_eq(&upgraded));
        drop(owner);
        drop(upgraded);
        {
            let borrowed = retained.downcast_ref::<Payload>().expect("actual payload");
            assert!(Arc::ptr_eq(&borrowed.trace, &trace));
            assert!(retained.downcast_ref::<u64>().is_none());
            trace.assert_counts(0, 0);
            match &retained {
                CaptureHandle::Native(native) => assert_eq!(native.retained_bytes(),
                    NativeCapture::allocation_bytes::<Payload, Credit>().unwrap()),
                _ => panic!("Native downcast changed transport"),
            }
        }
        drop(retained);
        trace.assert_counts(1, 0);
        assert!(weak.upgrade().is_none());
        drop(weak);
        trace.assert_finished();
    }

    fn concurrent_final_drops(weak_tail: bool) {
        let trace = Arc::new(Trace::default());
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let root = capture(&trace, false, Some((entered.clone(), resume.clone())));
        let weak = weak_tail.then(|| root.downgrade());
        let contender = root.clone();
        let start = Arc::new(Barrier::new(3));
        let a_start = start.clone();
        let a = std::thread::spawn(move || { a_start.wait(); drop(root); });
        let b_start = start.clone();
        let b = std::thread::spawn(move || { b_start.wait(); drop(contender); });
        start.wait();
        entered.wait(); // Last payload destructor is parked; no timing oracle.
        let parked_counts = (trace.payload_drops.load(Ordering::SeqCst),
            trace.credit_drops.load(Ordering::SeqCst));
        resume.wait();
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(parked_counts, (1, 0));
        if let Some(weak) = weak {
            trace.assert_counts(1, 0);
            assert!(weak.upgrade().is_none());
            let contender = weak.clone();
            let start = Arc::new(Barrier::new(3));
            let a_start = start.clone();
            let a = std::thread::spawn(move || { a_start.wait(); drop(weak); });
            let b_start = start.clone();
            let b = std::thread::spawn(move || { b_start.wait(); drop(contender); });
            start.wait();
            a.join().unwrap();
            b.join().unwrap();
        }
        trace.assert_finished();
    }
    // Frame: final invoice election must neither leak nor refund during the
    // payload destructor. Separate rows cover last-strong and last-weak races.
    #[test]
    fn paired_capture_concurrent_final_strong_and_weak_drops() {
        concurrent_final_drops(false);
        concurrent_final_drops(true);
    }

    // Frame: field cleanup during payload unwind must retain weak-tail credit
    // and retire exactly once when no weak tail remains.
    #[test]
    fn paired_capture_payload_unwind_retires_credit_after_payload() {
        for weak_tail in [false, true] {
            let trace = Arc::new(Trace::default());
            let root = capture(&trace, true, None);
            let weak = weak_tail.then(|| root.downgrade());
            let panic = catch_unwind(AssertUnwindSafe(|| drop(root)))
                .expect_err("payload destructor must unwind");
            assert_eq!(panic.downcast_ref::<&'static str>(), Some(&"payload destructor probe"));
            if let Some(weak) = weak {
                trace.assert_counts(1, 0);
                assert!(weak.upgrade().is_none());
                assert_eq!(invoice_owners(&weak), 1);
                drop(weak);
            }
            trace.assert_finished();
        }
    }
}
