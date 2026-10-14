use std::alloc::Layout;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, OnceLock};

// The process aggregate: native runtime and named open residue plus every writer
// counter that has joined it. A counter joins at its first writer registration
// and stays a member for its lifetime, so retirement never touches the
// aggregate; each byte enters once (a member increment, or the join) and
// leaves once (the matching decrement, or the counter's final drop).
static PROCESS_WRITER_AGGREGATE: AtomicUsize = AtomicUsize::new(0);

// The high bit classifies a fixed process-owned open residue without growing
// the inherited control layouts (writer decode 232 B; received registry 104 B).
pub(crate) const OPEN_RESIDUE_TAG: usize = 1 << (usize::BITS - 1);

pub(crate) fn admit_open_residue(bytes: usize, operation: &str) -> contextdb_core::Result<()> {
    assert!(bytes < OPEN_RESIDUE_TAG);
    let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
    let ceiling = domain.ceiling.unwrap_or_else(inherited_process_ceiling);
    aggregate_try_reserve(bytes, ceiling).map_err(|available| {
        contextdb_core::Error::MemoryBudgetExceeded {
            subsystem: "process_native_ceiling".to_string(),
            operation: operation.to_string(),
            requested_bytes: bytes,
            available_bytes: available,
            budget_limit_bytes: ceiling,
            hint: "The inherited process ceiling funds fixed writer open residue.".to_string(),
        }
    })
}

pub(crate) fn refund_open_residue(bytes: usize) {
    aggregate_refund(bytes);
}

fn aggregate_refund(bytes: usize) {
    if bytes != 0 {
        PROCESS_WRITER_AGGREGATE
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(bytes)
            })
            .map_or_else(
                |_| refund_invariant_broken("process aggregate refund is owned"),
                |_| (),
            );
    }
}

/// Reserve on the process aggregate against the inherited ceiling. This CAS is
/// the linearization point: from its success the bytes count in the aggregate.
fn aggregate_try_reserve(bytes: usize, ceiling: usize) -> Result<(), usize> {
    PROCESS_WRITER_AGGREGATE
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
            held.checked_add(bytes).filter(|next| *next <= ceiling)
        })
        .map(|_| ())
        .map_err(|held| ceiling.saturating_sub(held))
}

// A writer's `used` counter: the byte count first (`repr(C)`), so the word at
// the counter's address is exactly its bytes, and the aggregate membership
// beside it. Membership is set once, at the first writer registration, under
// the domain WRITE gate. Every counter mutation either holds the domain read
// gate (admission, release, retained charge, control refund), runs on the
// activation thread that holds the write gate, or belongs to an adapter that
// exists only after its writer registered -- so no mutation runs concurrently
// with the join, and the join counts exactly the bytes it marks. No `Deref`:
// every mutation names whether it moves the process aggregate.
#[derive(Debug, Default)]
#[repr(C)]
pub(crate) struct WriterCounter {
    used: AtomicUsize,
    aggregate_member: AtomicBool,
}
impl WriterCounter {
    pub(crate) fn load(&self, ordering: Ordering) -> usize {
        self.used.load(ordering)
    }
    fn is_member(&self) -> bool {
        self.aggregate_member.load(Ordering::SeqCst)
    }
    /// Local admission CAS on the byte value.
    fn compare_exchange_local(&self, current: usize, next: usize) -> bool {
        self.used
            .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
    /// Undo a local reservation whose aggregate CAS failed or never ran.
    /// Those bytes never entered the aggregate, so ONLY the local counter is
    /// decremented -- the one member decrement that skips the aggregate.
    fn rollback_local_only(&self, bytes: usize) {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_sub(bytes)
            })
            .map_or_else(
                |_| refund_invariant_broken("local rollback is owned"),
                |_| (),
            );
    }
    /// Unconditional charge (retained startup floors); a member's bytes join
    /// the aggregate without a ceiling check, as the local charge has none.
    fn charge(&self, bytes: usize) {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes)
            })
            .expect("retained ownership cannot overflow its counter");
        if self.is_member() {
            PROCESS_WRITER_AGGREGATE.fetch_add(bytes, Ordering::SeqCst);
        }
    }
    /// Return owned bytes; a member's bytes leave the aggregate once, here.
    pub(crate) fn release_owned(&self, bytes: usize) -> Result<(), usize> {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_sub(bytes)
            })?;
        if self.is_member() {
            aggregate_refund(bytes);
        }
        Ok(())
    }
    /// Saturating release; returns the byte value before it. Only the bytes
    /// actually removed from the counter leave the aggregate.
    fn release_saturating(&self, bytes: usize) -> usize {
        let before = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                Some(used.saturating_sub(bytes))
            })
            .expect("release update always supplies a value");
        if self.is_member() {
            aggregate_refund(bytes.min(before));
        }
        before
    }
    /// Join the aggregate at the first writer registration (write gate held).
    fn join_aggregate(&self) {
        if !self.aggregate_member.swap(true, Ordering::SeqCst) {
            PROCESS_WRITER_AGGREGATE.fetch_add(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
        }
    }
}
impl Drop for WriterCounter {
    fn drop(&mut self) {
        // A residual member charge leaves with its counter, so a leaked
        // charge never pins process headroom after its owner is gone.
        if *self.aggregate_member.get_mut() {
            aggregate_refund(*self.used.get_mut());
        }
    }
}

// Every private counter owner participates in the same final-consumer protocol.
// No raw Arc or Weak leaves this handle. The last contender frees the native
// Arc block before destroying the moved counter and settling aggregate residue.
#[derive(Debug, Clone)]
pub(crate) struct WriterCounterHandle(ManuallyDrop<Arc<ManuallyDrop<WriterCounter>>>);
impl WriterCounterHandle {
    fn new(counter: WriterCounter) -> Self {
        Self(ManuallyDrop::new(Arc::new(ManuallyDrop::new(counter))))
    }
    pub(crate) fn as_ptr(&self) -> *const WriterCounter {
        Arc::as_ptr(&self.0).cast()
    }
}
impl std::ops::Deref for WriterCounterHandle {
    type Target = WriterCounter;
    fn deref(&self) -> &WriterCounter {
        &self.0
    }
}
impl Drop for WriterCounterHandle {
    fn drop(&mut self) {
        // Move the owned Arc out without adding a contender or dropping twice.
        let owner = unsafe { ManuallyDrop::take(&mut self.0) };
        if let Some(counter) = Arc::into_inner(owner) {
            drop(ManuallyDrop::into_inner(counter));
        }
    }
}

const ACCOUNTANT_ORIGIN: usize = 0b100;
const ACCOUNTANT_TAGS: usize = 0b111;
const _: () = assert!(std::mem::align_of::<MemoryAccountant>() >= 8);

// One strong ownership word. Standard mode preserves supplied host and reader
// Arc/Weak identity. Only engine-created writers use restricted native mode.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct AccountantHandle(NonNull<MemoryAccountant>);
impl AccountantHandle {
    pub(crate) fn from_host(accountant: Arc<MemoryAccountant>) -> Self {
        Self::from_standard(accountant)
    }
    // Standard representation preserves existing reader-image funding and
    // identity. It does not declare those allocations host-funded or writers.
    fn from_standard(accountant: Arc<MemoryAccountant>) -> Self {
        Self(NonNull::new(Arc::into_raw(accountant).cast_mut()).expect("live accountant"))
    }
    fn is_engine(&self) -> bool {
        self.0.addr().get() & ACCOUNTANT_ORIGIN != 0
    }
    pub(crate) fn as_ptr(&self) -> *const MemoryAccountant {
        self.0
            .as_ptr()
            .map_addr(|address| address & !ACCOUNTANT_TAGS)
    }
    // Checked clone for genuine standard-Arc observation only. Engine writer
    // callers cannot manufacture an unrestricted Arc or Weak from this seam.
    pub(crate) fn standard_arc(&self) -> Option<Arc<MemoryAccountant>> {
        if self.is_engine() {
            return None;
        }
        let raw = self.as_ptr();
        Some(unsafe {
            Arc::increment_strong_count(raw);
            Arc::from_raw(raw)
        })
    }
    fn into_owned_raw(self) -> *mut MemoryAccountant {
        ManuallyDrop::new(self).0.as_ptr()
    }
    unsafe fn from_owned_raw(raw: *mut MemoryAccountant) -> Self {
        Self(NonNull::new(raw).expect("owned accountant"))
    }
    fn backing_bytes(&self) -> usize {
        if self.is_engine() {
            engine_writer_backing_bytes()
        } else {
            0
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn writer_fixed_ownership(&self) -> (usize, usize, usize) {
        let (node, _, adapters) = (**self).writer_fixed_ownership();
        (node, self.backing_bytes(), adapters)
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn native_backing_for_test(&self) -> (bool, [(usize, Layout); 3]) {
        (
            self.is_engine(),
            [
                (
                    self.as_ptr() as usize,
                    writer_adapter_type_layout::<MemoryAccountant>(),
                ),
                (
                    Arc::as_ptr(&self.limit) as usize,
                    writer_adapter_type_layout::<AtomicUsize>(),
                ),
                (
                    self.used.as_ptr() as usize,
                    writer_adapter_type_layout::<WriterCounter>(),
                ),
            ],
        )
    }
    fn engine(limit: Option<usize>) -> contextdb_core::Result<Self> {
        let counter = WriterCounter::default();
        let backing_bytes = engine_writer_backing_bytes();
        // Existing unavoidable startup retention, charged and joined BEFORE
        // any of the accountant's three native control allocations exist.
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        counter.charge(backing_bytes);
        counter.join_aggregate();
        drop(domain);
        let used = WriterCounterHandle::new(counter);
        let limit_control = Arc::new(AtomicUsize::new(limit.unwrap_or(0)));
        let accountant = MemoryAccountant::with_controls(limit_control, used, limit);
        #[cfg(any(test, feature = "test-seams"))]
        {
            accountant
                .high_water_for_test
                .store(backing_bytes, Ordering::SeqCst);
            accountant
                .admitted_bytes_for_test
                .store(backing_bytes, Ordering::SeqCst);
            accountant
                .retained_charge_bytes_for_test
                .store(backing_bytes, Ordering::SeqCst);
            note_budget_ledger("charge_retained", backing_bytes, backing_bytes);
        }
        let raw = Arc::into_raw(Arc::new(ManuallyDrop::new(accountant)))
            .cast_mut()
            .cast::<MemoryAccountant>();
        Ok(Self(
            NonNull::new(raw.map_addr(|address| address | ACCOUNTANT_ORIGIN))
                .expect("native accountant"),
        ))
    }
}
impl From<Arc<MemoryAccountant>> for AccountantHandle {
    fn from(accountant: Arc<MemoryAccountant>) -> Self {
        Self::from_standard(accountant)
    }
}
impl std::ops::Deref for AccountantHandle {
    type Target = MemoryAccountant;
    fn deref(&self) -> &MemoryAccountant {
        unsafe { &*self.as_ptr() }
    }
}
impl AsRef<MemoryAccountant> for AccountantHandle {
    fn as_ref(&self) -> &MemoryAccountant {
        self
    }
}
impl Clone for AccountantHandle {
    fn clone(&self) -> Self {
        unsafe {
            if self.is_engine() {
                Arc::increment_strong_count(self.as_ptr().cast::<ManuallyDrop<MemoryAccountant>>());
            } else {
                Arc::increment_strong_count(self.as_ptr());
            }
        }
        Self(self.0)
    }
}
impl Drop for AccountantHandle {
    fn drop(&mut self) {
        if !self.is_engine() {
            unsafe {
                drop(Arc::from_raw(self.as_ptr()));
            }
            return;
        }
        let owner =
            unsafe { Arc::from_raw(self.as_ptr().cast::<ManuallyDrop<MemoryAccountant>>()) };
        if let Some(accountant) = Arc::into_inner(owner) {
            let accountant = ManuallyDrop::into_inner(accountant);
            let counter = accountant.used.clone();
            // Native accountant block is already freed; the stack payload
            // frees its unique limit block before the original-counter refund.
            drop(accountant);
            let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
            let refund = counter.release_owned(
                writer_adapter_type_layout::<MemoryAccountant>().size()
                    + writer_adapter_type_layout::<AtomicUsize>().size(),
            );
            if !std::thread::panicking() {
                refund.expect("native accountant backing refund is owned");
            }
            drop(domain);
            drop(counter);
        }
    }
}
// SAFETY: immutable ownership tags and Arc's Send/Sync pointee bounds; raw
// custody is never used to expose mutable access or an engine Arc/Weak.
unsafe impl Send for AccountantHandle {}
unsafe impl Sync for AccountantHandle {}

impl contextdb_tx::staged_source::RetainedControlSource for AccountantHandle {
    fn retained_control_identity(
        &self,
        kind: contextdb_tx::staged_source::RetainedControlKind,
    ) -> Option<contextdb_tx::staged_source::RetainedControlIdentity> {
        if !self.is_engine() {
            return None;
        }
        use contextdb_tx::staged_source::{RetainedControlIdentity, RetainedControlKind};
        Some(match kind {
            RetainedControlKind::Accountant => RetainedControlIdentity::from_borrowed(&**self),
            RetainedControlKind::Limit => RetainedControlIdentity::from_borrowed(&*self.limit),
            RetainedControlKind::Counter => RetainedControlIdentity::from_borrowed(&*self.used),
            RetainedControlKind::WriterRegistration => return None,
        })
    }
}
impl contextdb_tx::staged_source::RetainedControlSource for WriterUse {
    fn retained_control_identity(
        &self,
        kind: contextdb_tx::staged_source::RetainedControlKind,
    ) -> Option<contextdb_tx::staged_source::RetainedControlIdentity> {
        use contextdb_tx::staged_source::{
            RetainedControlIdentity, RetainedControlKind, RetainedControlSource,
        };
        if kind != RetainedControlKind::WriterRegistration {
            return self.accountant.retained_control_identity(kind);
        }
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        let record = domain.lookup_in(self.stable_identity)?;
        if record.epoch != self.epoch
            || record.users == 0
            || !Arc::ptr_eq(&record.limit, &self.accountant.limit)
            || record.used.as_ptr() != self.accountant.used.as_ptr()
        {
            return None;
        }
        // The use pins this actual typed Box record after the gate is released.
        Some(RetainedControlIdentity::from_borrowed(record))
    }
}
impl AccountantHandle {
    fn visit_retained_source_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        // Supplied host backing has its original Arc/Weak lifetime and is not
        // charged as native engine backing. Its census remains unknown.
        if !self.is_engine() {
            return None;
        }
        use contextdb_tx::staged_source::{RetainedControl, RetainedControlKind};
        visit(RetainedControl::from_borrowed(
            RetainedControlKind::Accountant,
            self,
            writer_adapter_type_layout::<MemoryAccountant>().size(),
        ));
        visit(RetainedControl::from_borrowed(
            RetainedControlKind::Limit,
            self,
            writer_adapter_type_layout::<AtomicUsize>().size(),
        ));
        visit(RetainedControl::from_borrowed(
            RetainedControlKind::Counter,
            self,
            writer_adapter_type_layout::<WriterCounter>().size(),
        ));
        Some(())
    }
}
impl WriterUse {
    fn visit_retained_source_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        {
            let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
            let record = domain.lookup_in(self.stable_identity)?;
            if record.epoch != self.epoch
                || record.users == 0
                || !Arc::ptr_eq(&record.limit, &self.accountant.limit)
                || record.used.as_ptr() != self.accountant.used.as_ptr()
            {
                return None;
            }
        }
        // The borrowed use pins this actual record across the callbacks. Drop
        // the domain guard first: a repeated census walk may visit it again.
        self.accountant.visit_retained_source_controls(visit)?;
        use contextdb_tx::staged_source::{RetainedControl, RetainedControlKind};
        visit(RetainedControl::from_borrowed(
            RetainedControlKind::WriterRegistration,
            self,
            Layout::new::<WriterRegistration>().size(),
        ));
        Some(())
    }
}
fn engine_writer_backing_bytes() -> usize {
    writer_adapter_type_layout::<MemoryAccountant>().size()
        + writer_adapter_type_layout::<AtomicUsize>().size()
        + writer_adapter_type_layout::<WriterCounter>().size()
}

// Native backing remains funded through its physical allocation lifetime.
// These rows use the existing allocator and allocation-free observer storage.
#[cfg(test)]
pub(crate) fn native_allocation_lifetime_rows_for_test() {
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    fn native() -> (AccountantHandle, [(usize, usize, usize); 3], usize) {
        let (owner, requests, count) =
            oracle::native_requests_for_test(|| AccountantHandle::engine(None).unwrap());
        assert_eq!(
            count, 3,
            "the engine constructor makes exactly its three backing allocations"
        );
        let (engine, controls) = owner.native_backing_for_test();
        assert!(engine);
        // Actual constructor order: counter, limit, accountant. Compare each
        // independent allocator invoice to its concrete typed backing layout.
        for (request, control) in requests.iter().zip([controls[2], controls[1], controls[0]]) {
            assert_eq!(
                (request.1, request.2),
                (control.1.size(), control.1.align())
            );
            assert!(request.0 < control.0 && control.0 < request.0 + request.1);
        }
        let backing = requests.iter().map(|request| request.1).sum::<usize>();
        assert_eq!(owner.usage().used, backing);
        oracle::watch_native_free_for_test(requests, controls[2].0);
        (owner, requests, backing)
    }
    let aggregate = process_aggregate_snapshot().0;
    let (owner, requests, backing) = native();
    let counter_bytes = requests[0].1;
    // A real activation refuses before the allocation/admission callback when
    // its requested native payload exceeds the currently available process room.
    let writer = WriterBudget::from_handle(owner.clone()).unwrap();
    let activation_used = owner.usage().used;
    let activation_aggregate = process_aggregate_snapshot().0;
    let activation_epoch = owner.writer_registration_observation_for_test().2;
    let called = std::cell::Cell::new(false);
    set_process_ceiling_for_test(Some(activation_aggregate));
    let refused =
        owner.activate_resident_insertion_runtime(1, Layout::new::<usize>().size(), 0, 0, || {
            called.set(true);
            Ok(())
        });
    set_process_ceiling_for_test(None);
    assert!(!refused.unwrap());
    assert!(
        !called.get(),
        "process refusal precedes the payload allocation callback"
    );
    assert_eq!(owner.usage().used, activation_used);
    assert_eq!(process_aggregate_snapshot().0, activation_aggregate);
    assert_eq!(
        owner.writer_registration_observation_for_test().2,
        activation_epoch
    );
    assert_eq!(oracle::native_free_observation_for_test(), [(0, 0, 0); 3]);
    let cancelled =
        owner.activate_resident_insertion_runtime(1, Layout::new::<usize>().size(), 0, 0, || {
            Err(contextdb_core::Error::Other(String::new()))
        });
    assert!(cancelled.is_err());
    assert_eq!(owner.usage().used, activation_used);
    assert_eq!(process_aggregate_snapshot().0, activation_aggregate);
    assert_eq!(
        owner.writer_registration_observation_for_test().2,
        activation_epoch
    );
    drop(writer);
    assert_eq!(owner.usage().used, backing);
    println!(
        "NATIVE_ACTIVATION_WITNESS preallocation_process_refusal=true callback_not_called=true cancelled_callback_preserves_counter_and_epoch=true"
    );
    let mut previous_epoch = 0;
    for _ in 0..3 {
        let writer = WriterBudget::from_handle(owner.clone()).unwrap();
        let registration = owner.writer_registration_observation_for_test();
        assert_ne!(registration.2, previous_epoch);
        previous_epoch = registration.2;
        assert_eq!(owner.usage().used, backing + registration.1);
        drop(writer);
        assert_eq!(owner.writer_registration_observation_for_test().2, 0);
        assert_eq!(
            owner.usage().used,
            backing,
            "retiring an epoch cannot refund live native backing"
        );
        assert_eq!(process_aggregate_snapshot().0, aggregate + backing);
    }
    // Every contender owns the same production handle and consumes its own Arc
    // through the production final-consumer protocol. Barriers, not polling,
    // arrange simultaneous last-owner drops.
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let owner = owner.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                drop(owner);
            })
        })
        .collect();
    drop(owner);
    assert_eq!(oracle::native_free_observation_for_test(), [(0, 0, 0); 3]);
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    let free = oracle::native_free_observation_for_test();
    assert_eq!(
        free[2],
        (backing, backing, 1),
        "accountant physical free precedes its original-counter refund"
    );
    assert_eq!(
        free[1],
        (backing, backing, 1),
        "limit physical free precedes its original-counter refund"
    );
    assert_eq!(
        free[0],
        (aggregate + counter_bytes, aggregate + counter_bytes, 1),
        "counter backing is physically freed before its aggregate residual refunds"
    );
    assert_eq!(process_aggregate_snapshot().0, aggregate);
    println!(
        "NATIVE_BACKING_WITNESS requests={requests:?} backing={backing} free={free:?} concurrent_final_owners=2 epochs=3 original_funder=true final_aggregate_baseline=true"
    );

    let (owner, requests, backing) = native();
    let payload_layout = Layout::new::<usize>();
    let credit =
        ControlCredit::admit(&owner, payload_layout.size(), "native_late_control_proof").unwrap();
    let payload = Box::new(7usize);
    let refund = credit.prepare();
    let counter = owner.used.clone();
    assert_eq!(
        refund.counter.as_ptr(),
        counter.as_ptr(),
        "the late credit holds its actual original funder"
    );
    drop(owner);
    let free = oracle::native_free_observation_for_test();
    assert_eq!(
        free[2],
        (
            backing + payload_layout.size(),
            backing + payload_layout.size(),
            1
        )
    );
    assert_eq!(
        free[1],
        (
            backing + payload_layout.size(),
            backing + payload_layout.size(),
            1
        )
    );
    assert_eq!(
        free[0].2, 0,
        "counter-only refund custody still retains its real backing"
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        requests[0].1 + payload_layout.size()
    );
    assert_eq!(
        process_aggregate_snapshot().0,
        aggregate + requests[0].1 + payload_layout.size()
    );
    oracle::watch_registry_free_for_test(
        (&*payload as *const usize) as usize,
        counter.as_ptr() as usize,
    );
    drop(payload);
    assert_eq!(
        oracle::registry_free_observation_for_test(true),
        (
            requests[0].1 + payload_layout.size(),
            requests[0].1 + payload_layout.size(),
            true
        ),
        "the original counter funds the actual late payload through System.dealloc return"
    );
    drop(refund);
    assert_eq!(counter.load(Ordering::SeqCst), requests[0].1);
    drop(counter);
    assert_eq!(
        oracle::native_free_observation_for_test()[0],
        (aggregate + requests[0].1, aggregate + requests[0].1, 1)
    );
    assert_eq!(process_aggregate_snapshot().0, aggregate);
    println!(
        "NATIVE_LATE_COUNTER_WITNESS requests={requests:?} original_funder=true accountant_and_limit_physically_freed=true counter_only_retained=true final_aggregate_baseline=true"
    );

    for unwind in [false, true] {
        let (owner, requests, backing) = native();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = owner;
            if unwind {
                panic!("native cleanup proof");
            }
        }));
        assert_eq!(result.is_err(), unwind);
        let free = oracle::native_free_observation_for_test();
        assert_eq!(free[2], (backing, backing, 1));
        assert_eq!(free[1], (backing, backing, 1));
        assert_eq!(
            free[0],
            (aggregate + requests[0].1, aggregate + requests[0].1, 1)
        );
        assert_eq!(process_aggregate_snapshot().0, aggregate);
        println!(
            "NATIVE_CLEANUP_WITNESS unwind={unwind} physical_free_before_refund=true final_aggregate_baseline=true"
        );
    }

    let (host, requests, count) =
        oracle::native_requests_for_test(|| Arc::new(MemoryAccountant::no_limit()));
    assert_eq!(count, 3);
    let handle = AccountantHandle::from_host(host.clone());
    let (engine, controls) = handle.native_backing_for_test();
    assert!(
        !engine,
        "a supplied host Arc keeps its original representation"
    );
    for (request, control) in requests.iter().zip([controls[1], controls[2], controls[0]]) {
        // Host construction creates limit before counter; engine construction
        // above creates counter before limit.
        assert_eq!(
            (request.1, request.2),
            (control.1.size(), control.1.align())
        );
        assert!(request.0 < control.0 && control.0 < request.0 + request.1);
    }
    assert_eq!(
        handle.backing_bytes(),
        0,
        "host-supplied backing never becomes an engine debit"
    );
    assert!(Arc::ptr_eq(&handle.standard_arc().unwrap(), &host));
    let weak = Arc::downgrade(&host);
    let retained = host.clone();
    let writer = WriterBudget::from_accountant(host.clone()).unwrap();
    drop(writer);
    assert_eq!(host.usage().used, 0);
    assert_eq!(host.writer_registration_observation_for_test().2, 0);
    // Observe aggregate only: after standard strong-zero the counter is gone,
    // while the accountant Arc header can still be held by its genuine Weak.
    oracle::watch_native_free_for_test(requests, 0);
    drop(handle);
    drop(host);
    assert_eq!(oracle::native_free_observation_for_test(), [(0, 0, 0); 3]);
    assert!(Arc::ptr_eq(&weak.upgrade().unwrap(), &retained));
    drop(retained);
    assert!(weak.upgrade().is_none());
    let free = oracle::native_free_observation_for_test();
    assert_eq!(free[0], (aggregate, aggregate, 1));
    assert_eq!(free[1], (aggregate, aggregate, 1));
    assert_eq!(
        free[2].2, 0,
        "strong-zero destroys the payload but Weak still owns the Arc header"
    );
    drop(weak);
    assert_eq!(
        oracle::native_free_observation_for_test()[2],
        (aggregate, aggregate, 1)
    );
    assert_eq!(process_aggregate_snapshot().0, aggregate);
    println!(
        "HOST_ARC_WEAK_WITNESS requests={requests:?} engine_backing_debit=0 identity_preserved=true payload_destroyed_at_strong_zero=true header_physically_freed_only_at_weak_zero=true"
    );
}

/// The inherited whole-process memory limit: the effective cgroup v2
/// `memory.max` (the minimum along this process's cgroup path), else the
/// host's physical memory. It is never absent. Writer ceilings govern only
/// each writer's own working memory.
fn inherited_process_ceiling() -> usize {
    #[cfg(any(test, feature = "test-seams"))]
    if let Some(ceiling) = *PROCESS_CEILING_FOR_TEST.lock() {
        return ceiling;
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(ceiling) = cgroup_memory_max() {
            return ceiling;
        }
        let mut contents = [0_u8; 4096];
        if let Some(read) = read_linux_file(b"/proc/meminfo\0", &mut contents)
            && let Some(rest) = contents[..read]
                .split(|byte| *byte == b'\n')
                .find_map(|line| line.strip_prefix(b"MemTotal:"))
            && let Some(kib) = parse_ascii_usize(rest)
            && let Some(total) = kib.checked_mul(1024)
        {
            return total;
        }
    }
    #[cfg(unix)]
    {
        // SAFETY: sysconf has no preconditions and reads process-global values.
        let (pages, page) = unsafe {
            (
                libc::sysconf(libc::_SC_PHYS_PAGES),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        if pages > 0 && page > 0 {
            return (pages as usize).saturating_mul(page as usize);
        }
    }
    usize::MAX
}

#[cfg(target_os = "linux")]
fn cgroup_memory_max() -> Option<usize> {
    const ROOT: &[u8] = b"/sys/fs/cgroup";
    const LIMIT: &[u8] = b"/memory.max\0";
    let mut membership = [0_u8; 4096];
    let read = read_linux_file(b"/proc/self/cgroup\0", &mut membership)?;
    let path = membership[..read]
        .split(|byte| *byte == b'\n')
        .find_map(|line| line.strip_prefix(b"0::"))?;
    let path = trim_ascii(path);
    let mut current = [0_u8; 4096];
    current[..ROOT.len()].copy_from_slice(ROOT);
    let mut current_len = ROOT.len();
    let mut limit: Option<usize> = None;
    let visit = |dir: &[u8], limit: &mut Option<usize>| {
        let mut file = [0_u8; 4096];
        let full_len = dir.len().checked_add(LIMIT.len())?;
        if full_len > file.len() {
            return None;
        }
        file[..dir.len()].copy_from_slice(dir);
        file[dir.len()..full_len].copy_from_slice(LIMIT);
        let mut contents = [0_u8; 64];
        let read = read_linux_file(&file[..full_len], &mut contents)?;
        let bytes = parse_ascii_usize(&contents[..read])?;
        *limit = Some(limit.map_or(bytes, |held| held.min(bytes)));
        Some(())
    };
    let _ = visit(&current[..current_len], &mut limit);
    for component in path
        .split(|byte| *byte == b'/')
        .filter(|part| !part.is_empty())
    {
        let next_len = current_len.checked_add(1 + component.len())?;
        if next_len >= current.len() {
            return limit;
        }
        current[current_len] = b'/';
        current[current_len + 1..next_len].copy_from_slice(component);
        current_len = next_len;
        let _ = visit(&current[..current_len], &mut limit);
    }
    limit
}

#[cfg(target_os = "linux")]
fn read_linux_file(path: &[u8], destination: &mut [u8]) -> Option<usize> {
    if path.last() != Some(&0) {
        return None;
    }
    // SAFETY: `path` is NUL-terminated, `destination` is writable for its
    // declared length, and the descriptor is closed on every successful open.
    let descriptor = unsafe { libc::open(path.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if descriptor < 0 {
        return None;
    }
    let read = unsafe {
        libc::read(
            descriptor,
            destination.as_mut_ptr().cast(),
            destination.len(),
        )
    };
    unsafe { libc::close(descriptor) };
    (read >= 0).then_some(read as usize)
}

#[cfg(target_os = "linux")]
fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

#[cfg(target_os = "linux")]
fn parse_ascii_usize(bytes: &[u8]) -> Option<usize> {
    let mut value = 0_usize;
    let mut saw_digit = false;
    for byte in trim_ascii(bytes) {
        if !byte.is_ascii_digit() {
            break;
        }
        saw_digit = true;
        value = value.checked_mul(10)?.checked_add((byte - b'0') as usize)?;
    }
    saw_digit.then_some(value)
}

#[cfg(any(test, feature = "test-seams"))]
static PROCESS_CEILING_FOR_TEST: parking_lot::Mutex<Option<usize>> = parking_lot::Mutex::new(None);

/// Test seam: fix the inherited process ceiling for a later activation, and
/// for an already active runtime.
#[cfg(any(test, feature = "test-seams"))]
#[allow(dead_code)] // Keep the process ceiling control available to focused accounting tests.
pub(crate) fn set_process_ceiling_for_test(ceiling: Option<usize>) {
    *PROCESS_CEILING_FOR_TEST.lock() = ceiling;
    let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
    if domain.native.is_some() {
        domain.ceiling = Some(ceiling.unwrap_or_else(inherited_process_ceiling));
    }
}

// Allocator callbacks can run under the process-domain gate. This test-only
// atomic observation avoids recursively acquiring that gate and allocates nothing.
#[cfg(test)]
pub(crate) fn process_aggregate_bytes_for_test() -> usize {
    PROCESS_WRITER_AGGREGATE.load(Ordering::SeqCst)
}

/// The whole-process ceiling writers are bounded by: the recorded domain
/// ceiling, else the inherited one. It is the effective limit of a writer
/// that declares none.
pub(crate) fn effective_process_ceiling() -> usize {
    PROCESS_WRITER_BUDGET_DOMAIN
        .read()
        .ceiling
        .unwrap_or_else(inherited_process_ceiling)
}

/// (aggregate bytes, recorded process ceiling) for receipts and rows.
#[allow(dead_code)] // Keep the aggregate receipt seam available to memory-accounting tests.
pub(crate) fn process_aggregate_snapshot() -> (usize, Option<usize>) {
    let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
    (
        PROCESS_WRITER_AGGREGATE.load(Ordering::SeqCst),
        domain.ceiling,
    )
}

// Ordinary writer mutations share this gate. Runtime activation alone takes
// its exclusive side. The build lease is independent and never an admission gate.
#[derive(Debug)]
struct ProcessNativeReservation {
    bytes: usize,
}
#[derive(Debug)]
struct ResidentInsertionRuntime {
    width: usize,
    funder_identity: usize,
    reservation: ProcessNativeReservation,
}
#[derive(Debug)]
struct WriterRegistration {
    stable_identity: usize,
    epoch: usize,
    users: usize,
    limit: Arc<AtomicUsize>,
    used: WriterCounterHandle,
    // Observational backing attribution; only the final native handle refunds it.
    engine_backing_bytes: AtomicUsize,
    adapter_control_bytes: AtomicUsize,
    next: Option<Box<WriterRegistration>>,
}
#[derive(Debug)]
struct ProcessWriterBudgetDomain {
    native: Option<ResidentInsertionRuntime>,
    // Recorded once at the first activation: the inherited process ceiling.
    ceiling: Option<usize>,
    writers: Option<Box<WriterRegistration>>,
    next_identity: usize,
}
impl ProcessWriterBudgetDomain {
    fn lookup_in(&self, stable_identity: usize) -> Option<&WriterRegistration> {
        let mut next = self.writers.as_deref();
        while let Some(record) = next {
            if record.stable_identity == stable_identity {
                return Some(record);
            }
            next = record.next.as_deref();
        }
        None
    }
    fn lookup_mut_in(&mut self, stable_identity: usize) -> Option<&mut WriterRegistration> {
        let mut next = self.writers.as_deref_mut();
        while let Some(record) = next {
            if record.stable_identity == stable_identity {
                return Some(record);
            }
            next = record.next.as_deref_mut();
        }
        None
    }
    fn retain_in(&mut self, stable_identity: usize, epoch: usize) {
        let record = self
            .lookup_mut_in(stable_identity)
            .expect("live use has its registration");
        assert_eq!(record.epoch, epoch, "live use cannot change epoch");
        record.users = record
            .users
            .checked_add(1)
            .expect("writer use count overflow");
    }
    fn release_in(&mut self, accountant: &MemoryAccountant, stable_identity: usize, epoch: usize) {
        let mut slot = &mut self.writers;
        while slot.is_some() {
            if slot
                .as_ref()
                .is_some_and(|record| record.stable_identity == stable_identity)
            {
                let record = slot.as_mut().expect("live registration");
                assert_eq!(record.epoch, epoch, "live use cannot change epoch");
                record.users = record.users.checked_sub(1).expect("writer use underflow");
                if record.users != 0 {
                    return;
                }
                let mut record = slot.take().expect("last-use registration");
                *slot = record.next.take();
                // Retirement frees/refunds only the registration. Native
                // accountant and counter blocks have independent final owners.
                drop(record);
                accountant.release_under_exclusive_gate(Layout::new::<WriterRegistration>().size());
                return;
            }
            slot = &mut slot.as_mut().expect("registration exists").next;
        }
        panic!("live writer use lost its registration");
    }
}
static PROCESS_WRITER_BUDGET_DOMAIN: parking_lot::RwLock<ProcessWriterBudgetDomain> =
    parking_lot::RwLock::new(ProcessWriterBudgetDomain {
        native: None,
        ceiling: None,
        writers: None,
        next_identity: 1,
    });
pub(crate) static RESIDENT_INSERTION_BUILD_LEASE: parking_lot::Mutex<()> =
    parking_lot::Mutex::new(());

// Explicit engine custody. The accountant's OnceLock is a stable association
// key; the active epoch and count live only in the admitted domain record.
#[derive(Debug)]
pub(crate) struct WriterUse {
    accountant: AccountantHandle,
    stable_identity: usize,
    epoch: usize,
}
impl WriterUse {
    pub(crate) fn retain_if_writer(accountant: &AccountantHandle) -> Option<Self> {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let stable_identity = accountant.writer_identity.get().copied()?;
        let epoch = domain.lookup_in(stable_identity)?.epoch;
        domain.retain_in(stable_identity, epoch);
        Some(Self {
            accountant: accountant.clone(),
            stable_identity,
            epoch,
        })
    }
}
impl Clone for WriterUse {
    fn clone(&self) -> Self {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        domain.retain_in(self.stable_identity, self.epoch);
        Self {
            accountant: self.accountant.clone(),
            stable_identity: self.stable_identity,
            epoch: self.epoch,
        }
    }
}
impl Drop for WriterUse {
    fn drop(&mut self) {
        PROCESS_WRITER_BUDGET_DOMAIN.write().release_in(
            &self.accountant,
            self.stable_identity,
            self.epoch,
        );
    }
}

// A provenance-preserving one-word strong Arc projection. A tagged handle
// contributes exactly one active engine use; an untagged handle never acquires
// one on clone, even if a writer subsequently registers the same accountant.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StoreAccountantHandle(NonNull<MemoryAccountant>);
const _: () = assert!(std::mem::align_of::<MemoryAccountant>() >= 8);
impl StoreAccountantHandle {
    /// Project the exact pinned original use, never rediscover a writer epoch.
    pub(crate) fn from_writer_use(original: &WriterUse) -> Self {
        let accountant = original.accountant.clone();
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        assert_eq!(
            accountant.writer_identity.get().copied(),
            Some(original.stable_identity),
            "original use has its accountant key"
        );
        assert_ne!(original.stable_identity, 0, "writer identity is nonzero");
        assert_ne!(original.epoch, 0, "writer epoch is nonzero");
        let record = domain
            .lookup_in(original.stable_identity)
            .expect("original use pins its record");
        assert_eq!(record.epoch, original.epoch, "original use pins its epoch");
        assert!(
            Arc::ptr_eq(&record.limit, &accountant.limit),
            "original record has its accountant limit"
        );
        assert_eq!(
            record.used.as_ptr(),
            accountant.used.as_ptr(),
            "original record has its accountant counter"
        );
        domain.retain_in(original.stable_identity, original.epoch);
        let raw = NonNull::new(accountant.into_owned_raw()).expect("owned accountant is non-null");
        Self(raw.map_addr(|address| {
            std::num::NonZeroUsize::new(address.get() | 1).expect("tagged accountant is non-null")
        }))
    }
    /// An original nonwriter stays untagged even after later registration.
    pub(crate) fn from_original_nonwriter(accountant: &AccountantHandle) -> Self {
        Self(
            NonNull::new(accountant.clone().into_owned_raw())
                .expect("owned accountant is non-null"),
        )
    }
    fn decoded(&self) -> NonNull<MemoryAccountant> {
        self.0.map_addr(|address| {
            std::num::NonZeroUsize::new(address.get() & !ACCOUNTANT_TAGS)
                .expect("Arc pointer is non-null")
        })
    }
    pub(crate) fn is_writer(&self) -> bool {
        self.0.addr().get() & 1 != 0
    }
    pub(crate) fn retain_if_writer(accountant: &AccountantHandle) -> Option<Self> {
        // Construct and check the tag from the same exclusive-gate snapshot.
        let handle = Self::new(accountant.clone());
        handle.is_writer().then_some(handle)
    }
    pub(crate) fn same_allocation(&self, other: &Self) -> bool {
        self.decoded() == other.decoded()
    }
    pub(crate) fn new(accountant: AccountantHandle) -> Self {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let active = accountant
            .writer_identity
            .get()
            .copied()
            .and_then(|key| domain.lookup_in(key).map(|record| (key, record.epoch)));
        if let Some((key, epoch)) = active {
            domain.retain_in(key, epoch);
        }
        let raw = NonNull::new(accountant.into_owned_raw()).expect("Arc pointer is non-null");
        Self(if active.is_some() {
            raw.map_addr(|address| {
                std::num::NonZeroUsize::new(address.get() | 1)
                    .expect("tagged Arc pointer is non-null")
            })
        } else {
            raw
        })
    }
    // CDB127 returned-loader slot projection. The raw word owns this exact
    // handle's Arc/use; moving it preserves the attach-time role without a
    // second discovery, allocation, or writer count.
    pub(crate) fn into_owned_raw(self) -> *mut MemoryAccountant {
        let handle = std::mem::ManuallyDrop::new(self);
        handle.0.as_ptr()
    }
    /// Consume exactly one original tagged strong/use ownership.
    /// The caller must remove any enclosing slot-state bits first.
    pub(crate) unsafe fn from_owned_raw(raw: *mut MemoryAccountant) -> Self {
        Self(NonNull::new(raw).expect("owned accountant pointer is non-null"))
    }
    /// Borrow one immutable raw role while its source slot or ready winner
    /// keeps the same accountant and (for a writer) original epoch live.
    pub(crate) unsafe fn borrow_owned_raw(raw: *mut MemoryAccountant) -> Self {
        // This stack projection never consumes the slot's ownership. Clone
        // increments the Arc first; checked-retain panic rolls that Arc back.
        let original = std::mem::ManuallyDrop::new(unsafe { Self::from_owned_raw(raw) });
        Self::clone(&original)
    }
    // A resident cache carries this original role in its existing word;
    // admission and escaping chunk clones must never rediscover a later role.
    pub(crate) fn try_allocate_for_original(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for_provenance(bytes, subsystem, operation, hint, Some(self))
    }
    pub(crate) fn same_accountant(&self, accountant: &AccountantHandle) -> bool {
        std::ptr::eq(self.decoded().as_ptr(), accountant.as_ptr())
    }
    pub(crate) fn as_handle(&self) -> AccountantHandle {
        let original = ManuallyDrop::new(unsafe {
            AccountantHandle::from_owned_raw(self.0.as_ptr().map_addr(|address| address & !1))
        });
        AccountantHandle::clone(&original)
    }
    pub(crate) fn vector_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        let accountant = self.as_handle();
        if !self.is_writer() {
            // An untagged derived owner stays a reader even after a writer
            // later registers this same accountant.
            return accountant.vector_budget_for_original_reader();
        }
        let writer_use = self.original_writer_use(&accountant);
        Ok(accountant.budget_adapter(Some(writer_use), false)?)
    }
    fn original_writer_use(&self, accountant: &AccountantHandle) -> WriterUse {
        assert!(
            self.is_writer(),
            "only an original writer retains its epoch"
        );
        {
            let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
            let stable_identity = *accountant
                .writer_identity
                .get()
                .expect("tagged handle has a stable key");
            let epoch = domain
                .lookup_in(stable_identity)
                .expect("tagged handle pins its record")
                .epoch;
            domain.retain_in(stable_identity, epoch);
            WriterUse {
                accountant: accountant.clone(),
                stable_identity,
                epoch,
            }
        }
    }
    pub(crate) fn read_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        let accountant = self.as_handle();
        let writer_use = self
            .is_writer()
            .then(|| self.original_writer_use(&accountant));
        accountant.read_budget_for_original(writer_use)
    }
}
impl std::ops::Deref for StoreAccountantHandle {
    type Target = MemoryAccountant;
    fn deref(&self) -> &Self::Target {
        // SAFETY: tags are removed without changing provenance; the handle's
        // strong count keeps this immutable shared value live.
        unsafe { self.decoded().as_ref() }
    }
}
impl Clone for StoreAccountantHandle {
    fn clone(&self) -> Self {
        // The stack Arc rolls its added count back if checked count retention
        // panics. No operation that can panic follows successful retention.
        let accountant = self.as_handle();
        if self.is_writer() {
            let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
            let key = *accountant
                .writer_identity
                .get()
                .expect("tagged handle has a stable key");
            let epoch = domain
                .lookup_in(key)
                .expect("tagged handle pins its record")
                .epoch;
            domain.retain_in(key, epoch);
        }
        let raw = NonNull::new(accountant.into_owned_raw()).expect("Arc pointer is non-null");
        Self(if self.is_writer() {
            raw.map_addr(|address| {
                std::num::NonZeroUsize::new(address.get() | 1)
                    .expect("tagged Arc pointer is non-null")
            })
        } else {
            raw
        })
    }
}
impl Drop for StoreAccountantHandle {
    fn drop(&mut self) {
        // SAFETY: exactly one into_raw ownership is consumed per handle and
        // only the decoded pointer is passed to Arc's ownership operations.
        let accountant = unsafe {
            AccountantHandle::from_owned_raw(self.0.as_ptr().map_addr(|address| address & !1))
        };
        if self.is_writer() {
            let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
            let key = *accountant
                .writer_identity
                .get()
                .expect("tagged handle has a stable key");
            let epoch = domain
                .lookup_in(key)
                .expect("tagged handle pins its record")
                .epoch;
            domain.release_in(&accountant, key, epoch);
        }
        drop(accountant);
    }
}
// SAFETY: the pointee has AccountantHandle's Send/Sync bounds. The tag is
// immutable per handle; every mutable registration operation uses the one
// exclusive process-domain gate. No raw mutable pointee access is provided.
unsafe impl Send for StoreAccountantHandle {}
unsafe impl Sync for StoreAccountantHandle {}

#[derive(Debug, Clone)]
pub(crate) struct WriterBudget {
    accountant: AccountantHandle,
    writer_use: WriterUse,
    vector_accountant: Arc<dyn contextdb_vector::MemoryBudget>,
}
impl WriterBudget {
    pub(crate) fn prepare_source_funder(
        &self,
    ) -> contextdb_core::Result<contextdb_tx::SourceFunder> {
        if !self.accountant.is_engine() {
            // Move a clone of the exact supplied standard allocation; no native
            // relabelling, replacement shell or Weak identity change occurs.
            let receiver = self
                .accountant
                .standard_arc()
                .expect("supplied accountant has its standard Arc");
            return Ok(contextdb_tx::SourceFunder::from_host(receiver));
        }
        let writer = self.writer_use.clone();
        assert_eq!(
            self.accountant.as_ptr(),
            writer.accountant.as_ptr(),
            "writer budget retains its original accountant"
        );
        let control = ControlCredit::admit_for_use(
            &writer,
            writer_adapter_type_layout::<NativeSourceProvider>().size(),
            "staged_source_provider_control",
        )?;
        let provider = NativeSourceProvider {
            accountant: Some(self.accountant.clone()),
            writer: Some(writer),
            control: Some(control),
        };
        let origin = provider.native_origin()?;
        // SAFETY: this preadmitted exact concrete provider starts with one
        // paired count; it never makes Weak or exposes a permanent raw count.
        Ok(unsafe { contextdb_tx::SourceFunder::from_prepared_native(provider, origin) })
    }
    pub(crate) fn from_accountant(
        accountant: Arc<MemoryAccountant>,
    ) -> contextdb_core::Result<Self> {
        Self::from_handle(AccountantHandle::from_host(accountant))
    }
    pub(crate) fn from_handle(accountant: AccountantHandle) -> contextdb_core::Result<Self> {
        let engine_backing_bytes = accountant.backing_bytes();
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let stable_identity = if let Some(key) = accountant.writer_identity.get() {
            *key
        } else {
            let key = domain.next_identity;
            domain.next_identity = key
                .checked_add(1)
                .ok_or_else(|| contextdb_core::Error::Other("writer identity overflow".into()))?;
            accountant
                .writer_identity
                .set(key)
                .expect("association published under the exclusive gate");
            key
        };
        let epoch = if let Some(record) = domain.lookup_in(stable_identity) {
            let epoch = record.epoch;
            assert_eq!(
                record.engine_backing_bytes.load(Ordering::SeqCst),
                engine_backing_bytes,
                "existing writer backing retains its origin"
            );
            domain.retain_in(stable_identity, epoch);
            epoch
        } else {
            let epoch = domain.next_identity;
            let next_identity = epoch
                .checked_add(1)
                .ok_or_else(|| contextdb_core::Error::Other("writer identity overflow".into()))?;
            let registration_bytes = Layout::new::<WriterRegistration>().size();
            accountant
                .used
                .load(Ordering::SeqCst)
                .checked_add(registration_bytes)
                .ok_or_else(|| {
                    contextdb_core::Error::Other("writer registration size overflow".into())
                })?;
            accountant.charge_retained_under_admission(registration_bytes);
            accountant.used.join_aggregate();
            let registration = Box::new(WriterRegistration {
                stable_identity,
                epoch,
                users: 1,
                limit: accountant.limit.clone(),
                used: accountant.used.clone(),
                engine_backing_bytes: AtomicUsize::new(engine_backing_bytes),
                adapter_control_bytes: AtomicUsize::new(0),
                next: domain.writers.take(),
            });
            domain.writers = Some(registration);
            domain.next_identity = next_identity;
            epoch
        };
        drop(domain);
        let writer_use = WriterUse {
            accountant: accountant.clone(),
            stable_identity,
            epoch,
        };
        let vector_accountant = accountant.vector_budget_for_startup()?;
        Ok(Self {
            accountant,
            writer_use,
            vector_accountant,
        })
    }
    pub(crate) fn accountant(&self) -> AccountantHandle {
        self.accountant.clone()
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        AccountantHandle,
        WriterUse,
        Arc<dyn contextdb_vector::MemoryBudget>,
    ) {
        (self.accountant, self.writer_use, self.vector_accountant)
    }
    pub(crate) fn vector_accountant(&self) -> Arc<dyn contextdb_vector::MemoryBudget> {
        self.vector_accountant.clone()
    }
}

// Pinned Rust 1.94.1 alloc::sync ArcInner layout derivation: repr(C, align(2)),
// two atomic usize counts followed by T, extended and padded. This computes
// a typed request; independent allocator observation must verify each native
// accountant, limit and counter request in its actual compilation profile.
pub(crate) fn writer_adapter_layout<T>(adapter: &T) -> Layout {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::for_value(adapter))
        .expect("adapter Arc layout overflow")
        .0
        .pad_to_align()
}
pub(crate) fn writer_adapter_type_layout<T>() -> Layout {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .expect("adapter Arc layout overflow")
        .0
        .pad_to_align()
}

// The complete workspace transition is the only operation permitted to use
// this exclusive-admission scope. No allocation/reclaimer/observer callback
// runs here; vector's ordinary transition delegates straight to the kernel.
thread_local! {
    static ACTIVATING_WRITER: std::cell::Cell<Option<(usize, usize)>> = const { std::cell::Cell::new(None) };
}
struct ActivationScope(Option<(usize, usize)>);
impl Drop for ActivationScope {
    fn drop(&mut self) {
        ACTIVATING_WRITER.with(|slot| slot.set(self.0));
    }
}
struct WriterAdmission {
    _gate: Option<parking_lot::RwLockReadGuard<'static, ProcessWriterBudgetDomain>>,
    // The inherited process ceiling this admission must also fit, once a
    // runtime recorded it. None: no aggregate check (no ceiling yet, or the
    // activation scope, whose whole post-sum was checked under the write gate).
    ceiling: Option<usize>,
}

impl WriterAdmission {
    fn process_available(&self) -> Option<usize> {
        self.ceiling
            .map(|ceiling| ceiling.saturating_sub(PROCESS_WRITER_AGGREGATE.load(Ordering::SeqCst)))
    }
}

// One envelope calculation for admission, replacement credit, fit and reports.
// The old local ownership is removed BEFORE the process debit. Unlimited
// remains unlimited; finite arithmetic cannot manufacture capacity on overflow.
struct WriterHeadroom {
    available: Option<usize>,
    #[allow(dead_code)] // Preserve the full writer headroom report for future diagnostics.
    combined_used: Option<usize>,
    within_limit: bool,
}
fn writer_headroom(limit: usize, used: usize, releasing: usize, private: usize) -> WriterHeadroom {
    let occupied = used
        .checked_sub(releasing)
        .expect("replacement credit must be owned by this accountant");
    let combined_used = Some(occupied);
    let effective = limit.saturating_add(private);
    WriterHeadroom {
        available: (limit != 0).then(|| effective.saturating_sub(occupied)),
        combined_used,
        within_limit: limit == 0 || combined_used.is_some_and(|bytes| bytes <= effective),
    }
}

fn min_available(local: Option<usize>, process: Option<usize>) -> Option<usize> {
    match (local, process) {
        (Some(local), Some(process)) => Some(local.min(process)),
        (local, None) => local,
        (None, process) => process,
    }
}

pub(crate) fn resident_insertion_runtime_snapshot() -> Option<(usize, usize, usize)> {
    PROCESS_WRITER_BUDGET_DOMAIN
        .read()
        .native
        .as_ref()
        .map(|runtime| {
            (
                runtime.width,
                runtime.reservation.bytes,
                runtime.funder_identity,
            )
        })
}

pub(crate) type VectorLoadReservationTransfer =
    Arc<dyn Fn(&MemoryAccountant, usize) -> contextdb_core::Result<()> + Send + Sync>;

struct ActiveVectorLoadReservationTransfer {
    accountant: usize,
    transfer: VectorLoadReservationTransfer,
}

thread_local! {
    static VECTOR_LOAD_RESERVATION_TRANSFERS: RefCell<Vec<ActiveVectorLoadReservationTransfer>> =
        const { RefCell::new(Vec::new()) };
}

/// Give one synchronous vector-load call a way to move its request charge to
/// the resident graph owner. The accountant identity prevents a nested load
/// for another database from adopting the wrong request's bytes.
pub(crate) fn with_vector_load_reservation_transfer<T>(
    accountant: &AccountantHandle,
    transfer: VectorLoadReservationTransfer,
    operation: impl FnOnce() -> T,
) -> T {
    VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
        transfers
            .borrow_mut()
            .push(ActiveVectorLoadReservationTransfer {
                accountant: accountant.as_ptr() as usize,
                transfer,
            });
    });
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
                transfers.borrow_mut().pop();
            });
        }
    }
    let _restore = Restore;
    operation()
}

fn adopt_vector_load_reservation(
    accountant: &MemoryAccountant,
    bytes: usize,
) -> contextdb_core::Result<bool> {
    VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
        let transfers = transfers.borrow();
        let Some(active) = transfers.last() else {
            return Ok(false);
        };
        if active.accountant != accountant as *const MemoryAccountant as usize {
            return Ok(false);
        }
        (active.transfer)(accountant, bytes)?;
        Ok(true)
    })
}

/// Test-build only: a one-shot callback slot fired inside `try_allocate` at
/// the TOCTOU window between reading `limit` and re-checking it. It lets a unit
/// test force the exact `set_budget(None)`-races-`try_allocate` interleaving
/// deterministically instead of hoping the scheduler lands it. Production
/// carries neither this field nor its fire site (mirrors the engine's
/// `#[cfg(test)]` `__maintenance_wakes` liveness counter).
#[cfg(test)]
#[derive(Default)]
struct AllocRaceHook(std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>);

#[cfg(test)]
impl std::fmt::Debug for AllocRaceHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AllocRaceHook")
    }
}

thread_local! {
    /// Private, thread-scoped headroom for a limit transition's own control write.
    ///
    /// A limit transition has to write its new value down, and that write is itself
    /// charged to the budget it is moving. Publishing the target budget first so
    /// the write fits hands every concurrent operation capacity the durable value
    /// has not committed to; persisting first under a budget too small to write
    /// under traps the operator. This is the third answer: the transitioning thread
    /// gets the capacity it is moving between, nobody else sees it, every byte it
    /// admits is still counted in `used`, and the grant ends when the scope does.
    /// It is not a privilege a caller can ask for and it is never unaccounted.
    static TRANSITION_HEADROOM: RefCell<(usize, usize)> = const { RefCell::new((0, 0)) };
}

pub(crate) struct TransitionHeadroomScope {
    previous: (usize, usize),
}

impl Drop for TransitionHeadroomScope {
    fn drop(&mut self) {
        TRANSITION_HEADROOM.with(|headroom| *headroom.borrow_mut() = self.previous);
    }
}

/// Budget enforcer for memory-constrained edge devices.
/// All methods are &self — interior mutability via atomics.
#[derive(Debug)]
#[allow(dead_code)]
#[repr(align(8))]
pub struct MemoryAccountant {
    limit: Arc<AtomicUsize>,
    used: WriterCounterHandle,
    writer_identity: OnceLock<usize>,
    #[cfg(any(test, feature = "test-seams"))]
    high_water_for_test: AtomicUsize,
    #[cfg(any(test, feature = "test-seams"))]
    admitted_bytes_for_test: AtomicUsize,
    /// Bytes taken through `charge_retained` -- the writer's own retention
    /// door, which never refuses. A read session must never pay at it.
    #[cfg(any(test, feature = "test-seams"))]
    retained_charge_bytes_for_test: AtomicUsize,
    startup_ceiling: AtomicUsize,
    has_ceiling: AtomicBool,
    idle_graph_reclaimer:
        parking_lot::Mutex<Option<std::sync::Weak<contextdb_vector::VectorStore>>>,
    #[cfg(any(test, feature = "test-seams"))]
    underflow_count: AtomicUsize,
    #[cfg(any(test, feature = "test-seams"))]
    tail_admission_failure: AtomicUsize,
    #[cfg(test)]
    alloc_race_hook: AllocRaceHook,
    /// Test-build only: the mirror of `alloc_race_hook` on the SUCCESS path,
    /// fired between the limit read and the `used` CAS. It lets a unit test
    /// land the allocator-reads-old-limit -> narrowing-publish -> CAS
    /// interleaving deterministically, which is the one a narrowing limit has
    /// to survive.
    #[cfg(test)]
    admission_race_hook: AllocRaceHook,
}

/// An ordered record of what one thread charged the budget and what it gave
/// back, so a refusal can be read as a composition rather than as a total.
///
/// Named allocation and reservation-growth entries carry their operation;
/// shrinkage and ordinary releases are untagged releases. `used_after` is the
/// accountant's whole-database total immediately after the change, which is
/// what makes the ledger answer "who is still holding this" for a single-
/// threaded sequence: a name whose allocations are never followed by the total
/// coming back down is the one holding the bytes.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetLedgerEntry {
    /// The `subsystem/operation` that charged, or `"release"`.
    pub what: String,
    pub bytes: usize,
    pub used_after: usize,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static BUDGET_LEDGER: std::cell::RefCell<Option<Vec<BudgetLedgerEntry>>> =
        const { std::cell::RefCell::new(None) };
}

/// Start recording this thread's budget traffic. Recording is off until this is
/// called, so nothing pays for the ledger on an ordinary run.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn watch_budget_ledger_for_test() {
    BUDGET_LEDGER.with(|ledger| *ledger.borrow_mut() = Some(Vec::new()));
}

/// Stop recording and take what was recorded.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn take_budget_ledger_for_test() -> Vec<BudgetLedgerEntry> {
    BUDGET_LEDGER.with(|ledger| ledger.borrow_mut().take().unwrap_or_default())
}

/// How far past what it was PRICED at an already-admitted received schema unit
/// may still go before the ordinary refusal fires.
///
/// An admitted unit installs whole or not at all, so an allocation its price
/// did not foresee cannot simply be refused half way through. It is charged
/// instead -- but only this far. What the price can miss is decided by what a
/// peer chose to send (an arriving index wider than the ones the table already
/// has is priced at the average of those), so without a ceiling here the amount
/// a declared MEMORY_LIMIT can be overshot by is a peer's choice rather than the
/// operator's. Past this the model was wrong by more than a declared limit
/// should absorb and the unit is refused instead of installed over budget.
///
/// One exact-probe bitmap's worth, which is the largest single structure one
/// arriving index adds.
pub const ADMITTED_SCHEMA_UNIT_EXCESS_BYTES: usize = 64 * 1024;

/// What ONE already-admitted received schema unit may still take past its own
/// price, and which accountant it is entitled to take it from.
///
/// One of these belongs to one unit, pushed while that unit's items install and
/// popped when they are done, so two units never read or spend each other's
/// allowance. The accountant is identified by address, the same way
/// `private_transition_headroom` identifies the accountant a thread's headroom
/// belongs to: an allocation against any OTHER accountant -- another database's
/// statement, or the detached working database an authenticated unit stages
/// through -- keeps the ordinary refusal.
#[derive(Debug)]
struct AdmittedUnitBudget {
    accountant: usize,
    excess_charged: usize,
}

thread_local! {
    /// The schema units installing on THIS thread, innermost last. A unit's
    /// items run on the thread that entered its scope, so the innermost entry
    /// is the unit an allocation belongs to.
    static ADMITTED_UNITS: std::cell::RefCell<Vec<AdmittedUnitBudget>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The headroom a whole arriving schema unit was priced at, HELD from the
/// preflight that priced it until the unit is installed or refused.
///
/// The preflight used to be a comparison: it read what was available, found the
/// unit fitted, and held nothing -- so any admission between that read and the
/// unit's first item spent the room the unit had just been promised, and the
/// unit installed anyway. The reservation is what makes the preflight's answer
/// still true when the items take it.
pub(crate) struct SchemaUnitReservation {
    accountant: AccountantHandle,
    /// Bytes THIS reservation is holding and has not handed to its own items.
    /// Owned here rather than pooled on the accountant, so a second unit's
    /// reservation can neither be spent by this one nor released by it.
    remaining: std::cell::Cell<usize>,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

impl Drop for SchemaUnitReservation {
    /// Hands back whatever THIS unit never took. Nothing, if the unit reached
    /// its items -- entering the scope hands them the whole reservation.
    fn drop(&mut self) {
        let unspent = self.remaining.replace(0);
        self.accountant.release(unspent);
    }
}

/// Held for as long as an already-admitted received schema unit is installing
/// its items against THIS accountant.
///
/// Entering it hands the unit the room the preflight reserved for it: the
/// items then take that room through the ordinary allocator, which is what
/// keeps every byte paired with whatever is holding it. Once the room is gone,
/// an allocation the unit's price did not foresee is charged unconditionally up
/// to the bounded excess above -- so the unit cannot be left half installed by
/// a model that was slightly wrong, and cannot push the store arbitrarily past
/// its declared limit by a model that was very wrong.
pub(crate) struct AdmittedSchemaUnit<'a> {
    /// Borrowed for the life of the scope, so the reservation whose room the
    /// items are spending cannot be dropped while they are spending it.
    reservation: &'a SchemaUnitReservation,
}

impl<'a> AdmittedSchemaUnit<'a> {
    /// Hand THIS unit the room its own reservation is holding, and open its own
    /// excess allowance, for as long as its items install on this thread.
    pub(crate) fn enter(reservation: &'a SchemaUnitReservation) -> Self {
        let handed = reservation.remaining.replace(0);
        reservation.accountant.release(handed);
        ADMITTED_UNITS.with(|units| {
            units.borrow_mut().push(AdmittedUnitBudget {
                accountant: std::ptr::from_ref(&*reservation.accountant) as usize,
                excess_charged: 0,
            });
        });
        Self { reservation }
    }
}

impl Drop for AdmittedSchemaUnit<'_> {
    fn drop(&mut self) {
        let _ = self.reservation;
        ADMITTED_UNITS.with(|units| {
            units.borrow_mut().pop();
        });
    }
}

#[cfg(any(test, feature = "test-seams"))]
fn note_budget_ledger(what: &str, bytes: usize, used_after: usize) {
    BUDGET_LEDGER.with(|ledger| {
        if let Some(entries) = ledger.borrow_mut().as_mut() {
            entries.push(BudgetLedgerEntry {
                what: what.to_owned(),
                bytes,
                used_after,
            });
        }
    });
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    /// Largest live-byte charge of one file-reader child (zero for a writer).
    #[cfg(any(test, feature = "test-seams"))]
    pub reader_file_high_water: u64,
    pub limit: Option<usize>,
    pub used: usize,
    pub available: Option<usize>,
    pub startup_ceiling: Option<usize>,
}

// The allocator oracle must not count a disabled ledger's formatting as a
// control allocation. Construct the identical observation only for a live
// explicitly scoped ledger; this seam is absent without test-seams.
#[cfg(any(test, feature = "test-seams"))]
fn note_budget_admission(subsystem: &str, operation: &str, bytes: usize, used_after: usize) {
    BUDGET_LEDGER.with(|ledger| {
        if let Some(entries) = ledger.borrow_mut().as_mut() {
            entries.push(BudgetLedgerEntry {
                what: format!("{subsystem}/{operation}"),
                bytes,
                used_after,
            });
        }
    });
}

// One complete fixed-control liability, separate from payload credit.
// Its final owner extracts the bounded refund capability before dropping
// payload/use fields; PendingControlRefund is then the last local destroyed.
#[derive(Debug)]
pub(crate) struct ControlCredit {
    accountant: Option<AccountantHandle>,
    bytes: usize,
    registration: Option<(usize, usize)>,
}
impl ControlCredit {
    pub(crate) fn admit_for_use(
        original: &WriterUse,
        bytes: usize,
        operation: &str,
    ) -> contextdb_core::Result<Self> {
        let handle = StoreAccountantHandle::from_writer_use(original);
        Self::admit_from_handle(
            &handle,
            bytes,
            "vector_maintenance",
            operation,
            "Raise MEMORY_LIMIT for this fixed writer control.",
        )
    }
    // CDB127 private static-map extension: registry controls inherit the
    // creating handle's provenance and the existing family's refusal door.
    // An untagged reader never acquires a later writer's receipt/epoch.
    pub(crate) fn admit_from_handle(
        accountant: &StoreAccountantHandle,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for_provenance(
            bytes,
            subsystem,
            operation,
            hint,
            Some(accountant),
        )?;
        let registration = if accountant.is_writer() {
            let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
            let key = *accountant
                .writer_identity
                .get()
                .expect("tagged handle has its original key");
            let record = domain
                .lookup_in(key)
                .expect("tagged handle pins its original epoch");
            record
                .adapter_control_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_add(bytes)
                })
                .expect("control receipt arithmetic must be owned");
            Some((key, record.epoch))
        } else {
            None
        };
        Ok(Self {
            accountant: Some(accountant.as_handle()),
            bytes,
            registration,
        })
    }
    pub(crate) fn admit_open_residue(
        accountant: &StoreAccountantHandle,
        bytes: usize,
        operation: &str,
    ) -> contextdb_core::Result<Self> {
        admit_open_residue(bytes, operation)?;
        Ok(Self {
            accountant: Some(accountant.as_handle()),
            bytes: bytes | OPEN_RESIDUE_TAG,
            registration: None,
        })
    }
    // Only constant per-assembly controls call this door. Growing write
    // payloads, including staged posting charges, retain ordinary admission.
    pub(crate) fn admit_assembly(
        accountant: &AccountantHandle,
        bytes: usize,
        operation: &str,
    ) -> contextdb_core::Result<Self> {
        let handle = StoreAccountantHandle::new(accountant.clone());
        if handle.is_writer() {
            Self::admit_open_residue(&handle, bytes, operation)
        } else {
            Self::admit(accountant, bytes, operation)
        }
    }
    pub(crate) fn admit(
        accountant: &AccountantHandle,
        bytes: usize,
        operation: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.admit_box_control(
            bytes,
            operation,
            "Raise MEMORY_LIMIT for this fixed writer control.",
        )?;
        Ok(Self {
            accountant: Some(accountant.clone()),
            bytes,
            registration: accountant.prepare_control_refund().1,
        })
    }
    pub(crate) fn from_admitted(accountant: &AccountantHandle, bytes: usize) -> Self {
        // Transfer a named portion of an existing control reservation. No
        // admission, refund or payer substitution happens at this handoff.
        accountant.change_adapter_control_receipt(bytes, true);
        Self {
            accountant: Some(accountant.clone()),
            bytes,
            registration: accountant.prepare_control_refund().1,
        }
    }
    pub(crate) fn retained_startup(accountant: &AccountantHandle, bytes: usize) -> Self {
        // Preserve the existing captured-schema startup floor: accounted
        // before allocation, but never refused solely by a persisted limit.
        accountant.charge_retained(bytes);
        accountant.change_adapter_control_receipt(bytes, true);
        Self {
            accountant: Some(accountant.clone()),
            bytes,
            registration: accountant.prepare_control_refund().1,
        }
    }
    pub(crate) fn prepare(mut self) -> PendingControlRefund {
        let accountant = self
            .accountant
            .take()
            .expect("live control credit has its original funder");
        let bytes = std::mem::take(&mut self.bytes);
        let counter = accountant.original_control_counter();
        let registration = self.registration.take();
        drop(accountant);
        PendingControlRefund {
            counter,
            bytes,
            registration,
        }
    }
}
impl Drop for ControlCredit {
    fn drop(&mut self) {
        if let Some(accountant) = self.accountant.take() {
            let counter = accountant.original_control_counter();
            let registration = self.registration.take();
            drop(accountant);
            drop(PendingControlRefund {
                counter,
                bytes: std::mem::take(&mut self.bytes),
                registration,
            });
        }
    }
}
/// A refund that does not hold is a broken invariant and panics, except on a
/// thread already unwinding from another failure: a second panic there runs
/// inside a destructor and aborts the whole process instead of reporting the
/// failure that started the unwind.
#[track_caller]
fn refund_invariant_broken(what: &str) {
    if !std::thread::panicking() {
        panic!("{what}");
    }
}

pub(crate) struct PendingControlRefund {
    counter: WriterCounterHandle,
    bytes: usize,
    registration: Option<(usize, usize)>,
}
impl Drop for PendingControlRefund {
    fn drop(&mut self) {
        if self.bytes & OPEN_RESIDUE_TAG != 0 {
            refund_open_residue(self.bytes & !OPEN_RESIDUE_TAG);
            return;
        }
        // Receipt ownership lasts through every child/use cleanup. A retired
        // original epoch has no record; a reopened epoch must never lose this
        // old control's price. The bounded counter retains its original funder.
        // A refund that runs while its thread already unwinds from another
        // failure must not panic again: that aborts the whole process instead
        // of reporting the first failure. The invariants stay hard otherwise.
        let unwinding = std::thread::panicking();
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if let Some((key, epoch)) = self.registration
            && let Some(record) = domain.lookup_in(key).filter(|record| record.epoch == epoch)
        {
            let receipt = record.adapter_control_bytes.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |used| used.checked_sub(self.bytes),
            );
            if !unwinding {
                receipt.expect("final control receipt is owned");
            }
        }
        // Leaves the process aggregate exactly once, here, whether or not
        // this epoch's registration has retired (retirement never refunds).
        let refund = self.counter.release_owned(self.bytes);
        if !unwinding {
            refund.expect("final control refund is owned");
        }
    }
}

/// Stack-only final control cleanup. All automatic accountant/use children
/// are moved here before any fallible invariant destructor can unwind. The
/// index advances before each operation; a live guard resumes at the next one.
pub(crate) struct ReservationCleanup {
    accountant: Option<AccountantHandle>,
    writer_use: Option<WriterUse>,
    payload: usize,
    refund: Option<PendingControlRefund>,
    next: u8,
}
impl ReservationCleanup {
    pub(crate) fn new(
        accountant: Option<AccountantHandle>,
        writer_use: Option<WriterUse>,
        payload: usize,
        refund: Option<PendingControlRefund>,
    ) -> Self {
        Self {
            accountant,
            writer_use,
            payload,
            refund,
            next: 0,
        }
    }
    pub(crate) fn finish(&mut self) {
        while self.next < 4 {
            let step = self.next;
            self.next += 1;
            match step {
                0 => {
                    if let Some(accountant) = &self.accountant {
                        accountant.release(std::mem::take(&mut self.payload));
                    }
                }
                1 => drop(self.writer_use.take()),
                2 => drop(self.accountant.take()),
                3 => drop(self.refund.take()),
                _ => unreachable!(),
            }
        }
    }
}
impl Drop for ReservationCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

/// An owned database-accountant charge used by suspendable read operations.
///
/// Unlike the short-lived call-site pairs used by the eager executor, this
/// reservation can grow as a pull source retains continuation state and can
/// outlive the stack frame that opened a cursor. Dropping it returns the
/// complete charge exactly once.
#[derive(Debug)]
pub(crate) struct OwnedMemoryReservation {
    accountant: Option<AccountantHandle>,
    bytes: usize,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
    own_control: Option<ControlCredit>,
}

/// Pre2-shaped credit for values whose existing enclosing owner retains the
/// writer use through payload destruction. Conversion transfers admitted
/// bytes exactly once; it never releases and readmits them.
pub(crate) struct ContainedMemoryReservation {
    accountant: AccountantHandle,
    bytes: usize,
}
impl ContainedMemoryReservation {
    pub(crate) fn from_owned(mut reservation: OwnedMemoryReservation) -> Self {
        let bytes = std::mem::take(&mut reservation.bytes);
        Self {
            accountant: reservation.accountant().clone(),
            bytes,
        }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for ContainedMemoryReservation {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

/// Admission transferred from a displaced ordinary write into mutation undo.
#[derive(Debug)]
pub(crate) struct TransferredRowReservation {
    accountant: Option<AccountantHandle>,
    bytes: AtomicUsize,
    _writer_use: Option<crate::memory_accounting::WriterUse>,
    own_control: Option<ControlCredit>,
}
impl TransferredRowReservation {
    pub(crate) fn adopt(&self, bytes: usize) {
        self.bytes.store(bytes, Ordering::Relaxed);
    }
    pub(crate) fn prepare_control(
        accountant: &AccountantHandle,
    ) -> contextdb_core::Result<ControlCredit> {
        ControlCredit::admit(
            accountant,
            writer_adapter_type_layout::<Self>().size(),
            "transferred_row_control",
        )
    }
    pub(crate) fn from_admitted_with_control(
        accountant: AccountantHandle,
        bytes: usize,
        control: ControlCredit,
    ) -> Arc<Self> {
        Arc::new(Self {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&accountant),
            accountant: Some(accountant),
            bytes: AtomicUsize::new(bytes),
            own_control: Some(control),
        })
    }
    pub(crate) fn try_from_admitted(
        accountant: AccountantHandle,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<Self>> {
        // Admission failure leaves the original payload's credit untouched.
        let control = Self::prepare_control(&accountant)?;
        Ok(Self::from_admitted_with_control(accountant, bytes, control))
    }
}
impl contextdb_tx::StagedPostingCharge for TransferredRowReservation {
    fn retained_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
    fn transfer_back(&self) -> usize {
        self.bytes.swap(0, Ordering::Relaxed)
    }
}
impl Drop for TransferredRowReservation {
    fn drop(&mut self) {
        let refund = self.own_control.take().map(ControlCredit::prepare);
        let mut cleanup = ReservationCleanup::new(
            self.accountant.take(),
            self._writer_use.take(),
            self.bytes.swap(0, Ordering::Relaxed),
            refund,
        );
        cleanup.finish();
    }
}

/// A stack-scoped database-accountant charge. This is used where an existing
/// execution primitive can unwind from inside its source loop; the charge is
/// then returned even though control never reaches the primitive's ordinary
/// success/error epilogue.
pub(crate) struct ScopedMemoryReservation<'a> {
    accountant: &'a MemoryAccountant,
    bytes: usize,
}

impl<'a> ScopedMemoryReservation<'a> {
    pub(crate) fn try_new_for(
        accountant: &'a MemoryAccountant,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(Self { accountant, bytes })
    }
}

impl Drop for ScopedMemoryReservation<'_> {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

// Ordered staged postings retain the same engine admission owner.
#[derive(Debug)]
struct NativeCopyCharge {
    grant: contextdb_tx::OriginalCopyGrant,
}

impl contextdb_tx::StagedPostingCharge for NativeCopyCharge {
    fn retained_bytes(&self) -> usize {
        self.grant.retained_bytes()
    }
    fn reserve_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        self.grant.reserve_terminal_copy(bytes)
    }
}

impl contextdb_tx::StagedPostingCharge for OwnedMemoryReservation {
    fn retained_control_bytes(&self) -> Option<usize> {
        Some(self.own_control.as_ref().map_or(0, |control| control.bytes))
    }
    fn visit_retained_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        self.visit_retained_source_controls(visit)
    }
    fn reserve_private_original_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<contextdb_tx::OriginalCopyGrant> {
        self.reserve_original_grant(bytes)
    }
    fn retained_bytes(&self) -> usize {
        self.bytes()
    }
    fn reserve_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        // Legacy/public output remains a standard Arc. Native internal holders
        // use reserve_private_original_copy and retain the opaque grant instead.
        let owner = if let Some(original) = &self._writer_use {
            Self::try_new_arc_for_original(
                original,
                bytes,
                "relational",
                "statement_overlay",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            Self::try_new_arc_for_nonwriter(
                self.accountant(),
                bytes,
                "relational",
                "statement_overlay",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        Ok(owner)
    }
    fn reserve_terminal_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        let grant = self.reserve_original_grant(bytes)?;
        Ok(Arc::new(NativeCopyCharge { grant }))
    }
}

impl OwnedMemoryReservation {
    /// Call only after election has physically freed this reservation's ArcInner.
    /// The payload and its original writer registration remain untouched.
    fn finish_freed_enclosing_control(&mut self) {
        drop(self.own_control.take());
    }
    fn visit_retained_source_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        let writer = self._writer_use.as_ref()?;
        if writer.accountant.as_ptr() != self.accountant().as_ptr() {
            return None;
        }
        if let Some(control) = &self.own_control
            && (control
                .accountant
                .as_ref()
                .is_none_or(|owner| owner.as_ptr() != self.accountant().as_ptr())
                || control.registration != Some((writer.stable_identity, writer.epoch)))
        {
            return None;
        }
        writer.visit_retained_source_controls(visit)
    }
    /// Actual original reservation kernel: header control first, payload next,
    /// concrete allocation last. No epoch discovery follows payload admission.
    pub(crate) fn try_new_arc_for_original(
        original: &WriterUse,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Arc<Self>> {
        let writer = original.clone();
        let projection = StoreAccountantHandle::from_writer_use(&writer);
        let control = ControlCredit::admit_for_use(
            &writer,
            writer_adapter_type_layout::<Self>().size(),
            "reservation_control",
        )?;
        if control.bytes != writer_adapter_type_layout::<Self>().size()
            || control.registration != Some((writer.stable_identity, writer.epoch))
            || control
                .accountant
                .as_ref()
                .is_none_or(|owner| owner.as_ptr() != writer.accountant.as_ptr())
        {
            return Err(contextdb_core::Error::Other(
                "original reservation control origin mismatch".into(),
            ));
        }
        let accountant = projection.as_handle();
        projection.try_allocate_for_original(bytes, subsystem, operation, hint)?;
        let value = Self {
            accountant: Some(accountant),
            bytes,
            _writer_use: Some(writer),
            own_control: Some(control),
        };
        Ok(Arc::new(value))
    }
    fn try_new_arc_for_nonwriter(
        original: &AccountantHandle,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Arc<Self>> {
        let projection = StoreAccountantHandle::from_original_nonwriter(original);
        let control = ControlCredit::admit_from_handle(
            &projection,
            writer_adapter_type_layout::<Self>().size(),
            "vector_maintenance",
            "reservation_control",
            "Raise MEMORY_LIMIT for this fixed writer control.",
        )?;
        if control.bytes != writer_adapter_type_layout::<Self>().size()
            || control.registration.is_some()
            || control
                .accountant
                .as_ref()
                .is_none_or(|owner| owner.as_ptr() != original.as_ptr())
        {
            return Err(contextdb_core::Error::Other(
                "nonwriter reservation control origin mismatch".into(),
            ));
        }
        let accountant = projection.as_handle();
        projection.try_allocate_for_original(bytes, subsystem, operation, hint)?;
        let value = Self {
            accountant: Some(accountant),
            bytes,
            _writer_use: None,
            own_control: Some(control),
        };
        Ok(Arc::new(value))
    }
    /// Only the genuine original reservation can supply an opaque native O grant.
    fn reserve_original_grant(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<contextdb_tx::OriginalCopyGrant> {
        if !self.accountant().is_engine() {
            return <Self as contextdb_tx::StagedPostingCharge>::reserve_copy(self, bytes)
                .map(contextdb_tx::OriginalCopyGrant::from_host);
        }
        let writer = self._writer_use.as_ref().ok_or_else(|| {
            contextdb_core::Error::Other("native original copy requires its held writer use".into())
        })?;
        if writer.accountant.as_ptr() != self.accountant().as_ptr()
            || writer.stable_identity == 0
            || writer.epoch == 0
        {
            return Err(contextdb_core::Error::Other(
                "native original copy origin mismatch".into(),
            ));
        }
        let origin = contextdb_tx::NativeSourceOrigin::new(
            self.accountant().as_ptr() as usize,
            contextdb_tx::SourceLease {
                identity: writer.stable_identity,
                epoch: writer.epoch,
            },
        );
        let receiver = Self::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "statement_overlay",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        // SAFETY: the kernel checks the held original, preadmits its actual
        // header, allocates exactly Self, and returns its sole private count.
        Ok(unsafe { contextdb_tx::OriginalCopyGrant::from_native(receiver, origin) })
    }
    fn source_invoice_for(
        &mut self,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::SourceCharge> {
        let writer = self._writer_use.as_ref().ok_or_else(|| {
            contextdb_core::Error::Other(
                "native source transfer requires its held writer use".into(),
            )
        })?;
        let lease = contextdb_tx::SourceLease {
            identity: writer.stable_identity,
            epoch: writer.epoch,
        };
        if !self.accountant().is_engine()
            || writer.accountant.as_ptr() != self.accountant().as_ptr()
            || lease.identity == 0
            || lease.epoch == 0
            || !funder.matches_native_origin(self.accountant().as_ptr() as usize, lease)
        {
            return Err(contextdb_core::Error::Other(
                "native source transfer origin mismatch".into(),
            ));
        }
        let charge = contextdb_tx::SourceCharge::adopt_funded(funder.clone(), self.bytes, lease)?;
        self.bytes = 0;
        Ok(charge)
    }
    pub(crate) fn into_source_row_invoice_for(
        &mut self,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::OriginalRowInvoice> {
        self.source_invoice_for(funder)
            .map(contextdb_tx::OriginalRowInvoice::from_charge)
    }
    pub(crate) fn into_source_vector_invoice_for(
        &mut self,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::OriginalVectorInvoice> {
        self.source_invoice_for(funder)
            .map(contextdb_tx::OriginalVectorInvoice::from_charge)
    }
    /// Auxiliary bodies transfer the original reservation rather than a
    /// post-mutation byte estimate.
    pub(crate) fn into_source_aux_invoice_for(
        &mut self,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::OriginalAuxInvoice> {
        self.source_invoice_for(funder)
            .map(contextdb_tx::OriginalAuxInvoice::from_charge)
    }
    pub(crate) fn accountant(&self) -> &AccountantHandle {
        self.accountant
            .as_ref()
            .expect("live reservation has its accountant")
    }
    pub(crate) fn with_enclosing_arc_control<T>(
        mut self,
        retained_startup: bool,
    ) -> contextdb_core::Result<Self> {
        assert!(
            self.own_control.is_none(),
            "one reservation owns one enclosing control"
        );
        let bytes = writer_adapter_type_layout::<T>().size();
        self.own_control = Some(if retained_startup {
            ControlCredit::retained_startup(self.accountant(), bytes)
        } else {
            ControlCredit::admit(self.accountant(), bytes, "captured_schema_control")?
        });
        Ok(self)
    }
    pub(crate) fn into_admitted_arc(mut self) -> contextdb_core::Result<Arc<Self>> {
        let layout = writer_adapter_layout(&self);
        self.own_control = Some(ControlCredit::admit(
            self.accountant(),
            layout.size(),
            "reservation_control",
        )?);
        Ok(Arc::new(self))
    }
    pub(crate) fn into_admitted_mutex_arc(
        self,
    ) -> contextdb_core::Result<Arc<parking_lot::Mutex<Self>>> {
        let mut value = parking_lot::Mutex::new(self);
        let layout = writer_adapter_layout(&value);
        let reservation = value.get_mut();
        reservation.own_control = Some(ControlCredit::admit(
            reservation.accountant(),
            layout.size(),
            "reservation_mutex_control",
        )?);
        Ok(Arc::new(value))
    }
    pub(crate) fn from_admitted(accountant: AccountantHandle, bytes: usize) -> Self {
        Self {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
            accountant: Some(accountant),
            bytes,
            own_control: None,
        }
    }
    pub(crate) fn transfer_to_staged(mut self) {
        self.bytes = 0;
    }

    pub(crate) fn new(accountant: AccountantHandle) -> Self {
        Self {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
            accountant: Some(accountant),
            bytes: 0,
            own_control: None,
        }
    }

    /// Grow a held reservation under a name the store's limit can report.
    ///
    /// The unlabelled `try_grow` below reports a refusal as the bounded read's
    /// own retain; a reservation held on behalf of one named piece of work
    /// grows here instead, so a refusal names that work.
    pub(crate) fn try_grow_for(
        &mut self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        let next = self.bytes.checked_add(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_string(),
                operation: operation.to_string(),
                requested_bytes: bytes,
                available_bytes: usize::MAX - self.bytes,
                budget_limit_bytes: usize::MAX,
                hint: hint.to_string(),
            }
        })?;
        self.accountant()
            .try_allocate_for(bytes, subsystem, operation, hint)?;
        self.bytes = next;
        Ok(())
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn try_shrink(&mut self, bytes: usize) -> contextdb_core::Result<()> {
        let remaining = self.bytes.checked_sub(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "bounded_read".to_string(),
                operation: "release".to_string(),
                requested_bytes: bytes,
                available_bytes: self.bytes,
                budget_limit_bytes: self.bytes,
                hint: "The read gave back more memory than it was holding.".to_string(),
            }
        })?;
        self.accountant().release(bytes);
        self.bytes = remaining;
        Ok(())
    }

    /// Transfer an existing charge to a store owner. The shared accountant
    /// total is deliberately unchanged; only this request stops owning it.
    #[allow(dead_code)] // Keep the explicit transfer operation beside reservation release.
    pub(crate) fn try_transfer_to_store(&mut self, bytes: usize) -> contextdb_core::Result<()> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "bounded_read".to_string(),
                operation: "transfer_vector_load_reservation".to_string(),
                requested_bytes: bytes,
                available_bytes: self.bytes,
                budget_limit_bytes: self.bytes,
                hint: "The vector load cannot transfer more memory than its request reserved."
                    .to_string(),
            }
        })?;
        Ok(())
    }
}

impl OwnedMemoryReservation {
    /// Take a reservation the store's limit can name, and hold it for as long
    /// as the guard lives.
    ///
    /// The unlabelled `try_grow` above reports a refusal as the bounded read's
    /// own retain, which tells an operator nothing about WHICH piece of work
    /// the store-wide limit stopped. A read that holds a working set on behalf
    /// of one named operation reserves it here instead, so the refusal carries
    /// that operation's name the way the same work reports it on any other
    /// path.
    pub(crate) fn try_new_for(
        accountant: AccountantHandle,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(Self {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
            accountant: Some(accountant),
            bytes,
            own_control: None,
        })
    }
}

impl Drop for OwnedMemoryReservation {
    fn drop(&mut self) {
        let refund = self.own_control.take().map(ControlCredit::prepare);
        let mut cleanup = ReservationCleanup::new(
            self.accountant.take(),
            self._writer_use.take(),
            std::mem::take(&mut self.bytes),
            refund,
        );
        cleanup.finish();
    }
}

impl MemoryAccountant {
    /// Hand loaded graph residency from an operation to its retention owner.
    /// Admit first so the live graph is never unfunded. A failed source split
    /// rolls back this destination admission and leaves the source untouched.
    pub(crate) fn transfer_vector_load_reservation(
        &self,
        bytes: usize,
        release_reader: impl FnOnce() -> contextdb_core::Result<()>,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for(
            bytes,
            "vector_generation",
            "retain_loaded_graph",
            "Raise MEMORY_LIMIT to retain the loaded vector generation.",
        )?;
        if let Err(error) = release_reader() {
            self.release(bytes);
            return Err(error);
        }
        Ok(())
    }

    /// No budget enforcement. All allocations succeed. Default behavior.
    pub fn no_limit() -> Self {
        Self::with_initial_budget(None, false)
    }

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self::with_initial_budget(Some(bytes), true)
    }

    /// Restore a persisted live setting without establishing a startup ceiling.
    pub(crate) fn with_runtime_budget(bytes: usize) -> Self {
        Self::with_initial_budget(Some(bytes), false)
    }

    fn with_initial_budget(limit: Option<usize>, startup: bool) -> Self {
        Self::with_controls(
            Arc::new(AtomicUsize::new(limit.unwrap_or(0))),
            WriterCounterHandle::new(WriterCounter::default()),
            limit.filter(|_| startup),
        )
    }
    fn with_controls(
        limit: Arc<AtomicUsize>,
        used: WriterCounterHandle,
        startup: Option<usize>,
    ) -> Self {
        Self {
            limit,
            used,
            #[cfg(any(test, feature = "test-seams"))]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(startup.unwrap_or(0)),
            has_ceiling: AtomicBool::new(startup.is_some()),
            writer_identity: OnceLock::new(),
            idle_graph_reclaimer: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            underflow_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            tail_admission_failure: AtomicUsize::new(0),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
            #[cfg(test)]
            admission_race_hook: AllocRaceHook::default(),
        }
    }

    /// Run `operation` on this thread with `bytes` of private headroom above
    /// the published budget. Nothing about the published budget changes, so no
    /// other thread's allocation is admitted by one byte more than before.
    pub(crate) fn with_transition_headroom<T>(
        &self,
        bytes: usize,
        operation: impl FnOnce() -> T,
    ) -> T {
        let token = std::ptr::from_ref(self) as usize;
        let previous = TRANSITION_HEADROOM
            .with(|headroom| std::mem::replace(&mut *headroom.borrow_mut(), (token, bytes)));
        let _scope = TransitionHeadroomScope { previous };
        operation()
    }

    /// What this thread may allocate above the published budget for the limit
    /// transition it is performing, and zero for every other thread and every
    /// other accountant.
    fn private_transition_headroom(&self) -> usize {
        let token = std::ptr::from_ref(self) as usize;
        TRANSITION_HEADROOM
            .try_with(|headroom| {
                let (owner, bytes) = *headroom.borrow();
                if owner == token { bytes } else { 0 }
            })
            .unwrap_or(0)
    }

    pub(crate) fn writer_budget(bytes: usize) -> contextdb_core::Result<WriterBudget> {
        Self::new_engine_writer(Some(bytes))
    }
    pub(crate) fn writer_no_limit() -> contextdb_core::Result<WriterBudget> {
        Self::new_engine_writer(None)
    }
    fn new_engine_writer(limit: Option<usize>) -> contextdb_core::Result<WriterBudget> {
        WriterBudget::from_handle(AccountantHandle::engine(limit)?)
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn writer_fixed_ownership(&self) -> (usize, usize, usize) {
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        self.writer_identity
            .get()
            .and_then(|key| domain.lookup_in(*key))
            .map_or((0, 0, 0), |record| {
                (
                    Layout::new::<WriterRegistration>().size(),
                    record.engine_backing_bytes.load(Ordering::Relaxed),
                    record.adapter_control_bytes.load(Ordering::Relaxed),
                )
            })
    }
    fn change_adapter_control_receipt(&self, bytes: usize, adding: bool) {
        assert!(
            self.exclusive_activation().is_none(),
            "control allocation is outside borrowed activation"
        );
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if let Some(record) = self
            .writer_identity
            .get()
            .and_then(|key| domain.lookup_in(*key))
        {
            record
                .adapter_control_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    if adding {
                        current.checked_add(bytes)
                    } else {
                        current.checked_sub(bytes)
                    }
                })
                .expect("adapter control receipt arithmetic must be owned");
        }
    }
    // A ControlCredit already captured its original registration. Refund
    // custody needs only that accountant's source counter, never a new epoch.
    fn original_control_counter(&self) -> WriterCounterHandle {
        assert!(
            self.exclusive_activation().is_none(),
            "control cleanup is outside borrowed activation"
        );
        self.used.clone()
    }
    fn prepare_control_refund(&self) -> (WriterCounterHandle, Option<(usize, usize)>) {
        assert!(
            self.exclusive_activation().is_none(),
            "control cleanup is outside borrowed activation"
        );
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        let registration = self
            .writer_identity
            .get()
            .and_then(|key| domain.lookup_in(*key).map(|record| (*key, record.epoch)));
        (self.used.clone(), registration)
    }
    pub(crate) fn retain_box_control_refund(&self, bytes: usize) -> WriterCounterHandle {
        self.change_adapter_control_receipt(bytes, false);
        self.used.clone()
    }
    pub(crate) fn admit_box_control(
        &self,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for(bytes, "vector_maintenance", operation, hint)?;
        self.change_adapter_control_receipt(bytes, true);
        Ok(())
    }
    fn release_under_exclusive_gate(&self, bytes: usize) {
        if self.used.release_owned(bytes).is_err() {
            refund_invariant_broken("exclusive lifecycle refund must be owned");
        }
    }
    fn exclusive_activation(&self) -> Option<usize> {
        let address = self as *const Self as usize;
        ACTIVATING_WRITER.with(|slot| {
            slot.get()
                .filter(|(id, _)| *id == address)
                .map(|(_, bytes)| bytes)
        })
    }
    // Private admissions derived from counted custody must use its original
    // tag. The retained tagged handle prevents epoch replacement; a reader
    // handle intentionally ignores any later association of this accountant.
    fn writer_admission_for_provenance(
        &self,
        provenance: Option<&StoreAccountantHandle>,
    ) -> WriterAdmission {
        let Some(handle) = provenance else {
            return self.writer_admission();
        };
        assert!(
            std::ptr::eq(self, &**handle),
            "admission retains its original accountant"
        );
        assert!(
            self.exclusive_activation().is_none(),
            "registry admission is outside borrowed activation"
        );
        let gate = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if handle.is_writer() {
            let key = *self
                .writer_identity
                .get()
                .expect("tagged handle has its original key");
            gate.lookup_in(key)
                .expect("tagged handle pins its original epoch");
        }
        let ceiling = gate.ceiling.filter(|_| self.used.is_member());
        WriterAdmission {
            _gate: Some(gate),
            ceiling,
        }
    }
    fn writer_admission(&self) -> WriterAdmission {
        if self.exclusive_activation().is_some() {
            return WriterAdmission {
                _gate: None,
                ceiling: None,
            };
        }
        let gate = PROCESS_WRITER_BUDGET_DOMAIN.read();
        let ceiling = gate.ceiling.filter(|_| self.used.is_member());
        WriterAdmission {
            _gate: Some(gate),
            ceiling,
        }
    }
    // Exclusive activation validates ALL live records and admits the requesting
    // build's complete local peak before publishing the one permanent owner.
    // The closure only changes the existing workspace token/counters.
    pub(crate) fn activate_resident_insertion_runtime(
        &self,
        width: usize,
        native_bytes: usize,
        old_workspace: usize,
        complete_peak: usize,
        admit_workspace: impl FnOnce() -> contextdb_core::Result<()>,
    ) -> contextdb_core::Result<bool> {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let identity = *self
            .writer_identity
            .get()
            .expect("resident insertion requires a stable association");
        domain
            .lookup_in(identity)
            .expect("resident insertion requires a retained writer use");
        if domain.native.is_some() {
            return Ok(false);
        }
        let mut next = domain.writers.as_deref();
        while let Some(record) = next {
            let limit = record.limit.load(Ordering::SeqCst);
            let used = record.used.load(Ordering::SeqCst);
            // Activation has no private transition allowance on other writers.
            let releasing = if record.stable_identity == identity {
                old_workspace
            } else {
                0
            };
            let requested = if record.stable_identity == identity {
                complete_peak
            } else {
                0
            };
            let local = used
                .checked_sub(releasing)
                .and_then(|bytes| bytes.checked_add(requested))
                .ok_or_else(|| {
                    contextdb_core::Error::Other("runtime activation envelope overflow".into())
                })?;
            // Writer limits govern each writer's own working memory only.
            if !writer_headroom(limit, local, 0, 0).within_limit {
                return Ok(false);
            }
            next = record.next.as_deref();
        }
        // The reservation is process memory: the post-activation aggregate,
        // with this build's peak replacing its old workspace, must fit the
        // inherited whole-process ceiling, never a writer's limit.
        let ceiling = domain.ceiling.unwrap_or_else(inherited_process_ceiling);
        let after = PROCESS_WRITER_AGGREGATE
            .load(Ordering::SeqCst)
            .checked_sub(old_workspace)
            .and_then(|bytes| bytes.checked_add(complete_peak))
            .and_then(|bytes| bytes.checked_add(native_bytes))
            .ok_or_else(|| {
                contextdb_core::Error::Other("runtime activation combined size overflow".into())
            })?;
        if after > ceiling {
            return Ok(false);
        }
        let previous = ACTIVATING_WRITER
            .with(|slot| slot.replace(Some((self as *const Self as usize, native_bytes))));
        let scope = ActivationScope(previous);
        admit_workspace()?;
        drop(scope);
        PROCESS_WRITER_AGGREGATE.fetch_add(native_bytes, Ordering::SeqCst);
        domain.ceiling = Some(ceiling);
        domain.native = Some(ResidentInsertionRuntime {
            width,
            funder_identity: self.used.as_ptr() as usize,
            reservation: ProcessNativeReservation {
                bytes: native_bytes,
            },
        });
        // Drop the exclusive gate before bodies, spawn, joins, export or callbacks.
        drop(domain);
        Ok(true)
    }

    /// A fit probe may replace its own workspace, but must subtract the
    /// retained process debit after returning only that workspace's credit.
    pub(crate) fn available_after_releasing(&self, bytes: usize) -> Option<usize> {
        let admission = self.writer_admission();
        let local = writer_headroom(
            self.limit.load(Ordering::SeqCst),
            self.used.load(Ordering::SeqCst),
            bytes,
            self.private_transition_headroom(),
        )
        .available;
        // The released workspace is this member's own aggregate share.
        let process = admission
            .process_available()
            .map(|room| room.saturating_add(bytes));
        min_available(local, process)
    }

    /// Step 2 of admission, run only for bytes this writer's own limit has
    /// already admitted (step 1). A member's bytes join the process aggregate;
    /// once a ceiling is recorded, they must fit it. A failed aggregate CAS
    /// rolls back ONLY the local counter: those bytes never entered the
    /// aggregate, the one member decrement that does not refund it.
    fn admit_to_process_aggregate(
        &self,
        admission: &WriterAdmission,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        if !self.used.is_member() {
            return Ok(());
        }
        let Some(ceiling) = admission.ceiling else {
            PROCESS_WRITER_AGGREGATE.fetch_add(bytes, Ordering::SeqCst);
            return Ok(());
        };
        if let Err(available) = aggregate_try_reserve(bytes, ceiling) {
            self.used.rollback_local_only(bytes);
            return Err(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "process_native_ceiling".to_string(),
                operation: operation.to_string(),
                requested_bytes: bytes,
                available_bytes: available,
                budget_limit_bytes: ceiling,
                hint: format!(
                    "The process memory ceiling also holds the resident insertion runtime. {hint}"
                ),
            });
        }
        Ok(())
    }

    /// Attempt to allocate bytes. CAS-based, no TOCTOU.
    pub fn try_allocate(&self, bytes: usize) -> contextdb_core::Result<()> {
        self.try_allocate_with_provenance(bytes, None)
    }
    fn try_allocate_with_provenance(
        &self,
        bytes: usize,
        provenance: Option<&StoreAccountantHandle>,
    ) -> contextdb_core::Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        let admission = self.writer_admission_for_provenance(provenance);
        #[cfg(test)]
        let mut admission = admission;
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let published = self.limit.load(Ordering::SeqCst);
            let limit = if published == 0 {
                0
            } else {
                published.saturating_add(self.private_transition_headroom())
            };
            if limit != 0 {
                let available = writer_headroom(limit, used, 0, 0)
                    .available
                    .unwrap_or(usize::MAX - used);
                if bytes > available {
                    // TOCTOU window: `limit` was loaded above; a concurrent
                    // `set_budget(None)` may have cleared it since. The re-check
                    // below closes that window. In test builds a hook lets a
                    // unit test land exactly that interleaving deterministically.
                    #[cfg(test)]
                    if self.fire_accounting_hook_without_gate_for_provenance(
                        &mut admission,
                        provenance,
                        false,
                    ) {
                        continue;
                    }
                    if self.limit.load(Ordering::SeqCst) != published {
                        continue;
                    }
                    return Err(contextdb_core::Error::MemoryBudgetExceeded {
                        subsystem: "memory".to_string(),
                        operation: "allocate".to_string(),
                        requested_bytes: bytes,
                        available_bytes: available,
                        budget_limit_bytes: limit,
                        hint:
                            "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT."
                                .to_string(),
                    });
                }
            }

            let Some(next) = used.checked_add(bytes) else {
                return Err(contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: "memory".to_string(),
                    operation: "allocate".to_string(),
                    requested_bytes: bytes,
                    available_bytes: usize::MAX - used,
                    budget_limit_bytes: usize::MAX,
                    hint: "Reduce retained data or working-set size.".to_string(),
                });
            };
            // The interleaving a narrowing limit has to survive: this
            // allocator has already read `published` and decided the request
            // fits it; a concurrent narrowing publishes a smaller limit right
            // here, and the CAS below still runs. In test builds a hook lands
            // exactly that order deterministically.
            #[cfg(test)]
            if self.fire_accounting_hook_without_gate_for_provenance(
                &mut admission,
                provenance,
                true,
            ) {
                continue;
            }
            if self.used.compare_exchange_local(used, next) {
                // Admission and publication share ONE linearization boundary,
                // and it is this load. An admission is not SUCCESSFUL until it
                // has been validated against a published limit read that
                // happens-after its own CAS. A narrowing that linearizes before
                // this load is therefore always seen: the CAS is undone and the
                // loop produces the ordinary typed refusal under the new limit.
                // A narrowing that linearizes after it is a publication this
                // admission genuinely preceded. Either way no request returns
                // success having been admitted under a limit already superseded,
                // and `used` is never left above the published ceiling by an
                // allocator that read a stale one.
                let current = self.limit.load(Ordering::SeqCst);
                let admissible = if current == 0 {
                    0
                } else {
                    current.saturating_add(self.private_transition_headroom())
                };
                if writer_headroom(admissible, used, 0, 0)
                    .available
                    .is_some_and(|available| bytes > available)
                {
                    // Still inside local admission: the aggregate step below
                    // has not run, so these bytes never entered it.
                    self.used.rollback_local_only(bytes);
                    continue;
                }
                self.admit_to_process_aggregate(
                    &admission,
                    bytes,
                    "allocate",
                    "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT.",
                )?;
                #[cfg(any(test, feature = "test-seams"))]
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                #[cfg(any(test, feature = "test-seams"))]
                self.admitted_bytes_for_test
                    .fetch_add(bytes, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    /// Replace one caller-owned reservation without releasing its protected
    /// capacity or charging both phases. The caller retains `old_bytes` on
    /// refusal and changes its ownership token only after success.
    pub(crate) fn transition_reservation(
        &self,
        old_bytes: usize,
        new_bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        let admission = self.writer_admission();
        #[cfg(test)]
        let mut admission = admission;
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let others = used
                .checked_sub(old_bytes)
                .expect("a reservation transition must own its entire old charge");
            let published = self.limit.load(Ordering::SeqCst);
            let limit = if published == 0 {
                0
            } else {
                published.saturating_add(self.private_transition_headroom())
            };
            let available = writer_headroom(limit, used, old_bytes, 0)
                .available
                .unwrap_or(usize::MAX - others);
            // A shrinking phase always remains admitted, including after a
            // budget is narrowed below already-retained ownership.
            if new_bytes > old_bytes && new_bytes > available {
                #[cfg(test)]
                if self.fire_accounting_hook_without_gate(&mut admission, false) {
                    continue;
                }
                if self.limit.load(Ordering::SeqCst) != published {
                    continue;
                }
                return Err(contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: new_bytes - old_bytes,
                    available_bytes: writer_headroom(limit, used, 0, 0)
                        .available
                        .unwrap_or(usize::MAX - used),
                    budget_limit_bytes: if limit == 0 { usize::MAX } else { limit },
                    hint: hint.to_owned(),
                });
            }
            let next = others.checked_add(new_bytes).ok_or_else(|| {
                contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: new_bytes.saturating_sub(old_bytes),
                    available_bytes: usize::MAX - used,
                    budget_limit_bytes: usize::MAX,
                    hint: hint.to_owned(),
                }
            })?;
            #[cfg(test)]
            if self.fire_accounting_hook_without_gate(&mut admission, true) {
                continue;
            }
            if !self.used.compare_exchange_local(used, next) {
                continue;
            }
            // Match allocation's post-CAS limit linearization. Only growth
            // can invalidate admission; undo exactly its positive delta so
            // concurrent charges stay intact and the old owner stays held.
            let current = self.limit.load(Ordering::SeqCst);
            let admissible = if current == 0 {
                0
            } else {
                current.saturating_add(self.private_transition_headroom())
            };
            if new_bytes > old_bytes
                && writer_headroom(admissible, used, old_bytes, 0)
                    .available
                    .is_some_and(|available| new_bytes > available)
            {
                // Still inside local admission; the growth never entered the aggregate.
                self.used.rollback_local_only(new_bytes - old_bytes);
                continue;
            }
            if new_bytes > old_bytes {
                self.admit_to_process_aggregate(
                    &admission,
                    new_bytes - old_bytes,
                    operation,
                    hint,
                )?;
            } else if self.used.is_member() {
                aggregate_refund(old_bytes - new_bytes);
            }
            #[cfg(any(test, feature = "test-seams"))]
            {
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                self.admitted_bytes_for_test
                    .fetch_add(new_bytes.saturating_sub(old_bytes), Ordering::SeqCst);
                if new_bytes < old_bytes {
                    note_budget_ledger("release", old_bytes - new_bytes, next);
                } else {
                    note_budget_admission(subsystem, operation, new_bytes - old_bytes, next);
                }
            }
            return Ok(());
        }
    }

    /// Charge bytes the store is holding now, whether or not the budget has
    /// room for them.
    ///
    /// The in-memory change log and the transaction manager's commit index
    /// exist the moment a commit is durable: refusing them at that point
    /// would mean a write that is on disk and not in memory. So the charge is
    /// unconditional, and the ordinary typed refusal fires on the NEXT
    /// admission instead -- which is the whole point, because until these
    /// were charged what stopped an import that outgrew a declared
    /// MEMORY_LIMIT was the machine's out-of-memory kill, not the engine.
    pub fn charge_retained(&self, bytes: usize) {
        let _admission = self.writer_admission();
        self.charge_retained_under_admission(bytes);
    }
    fn charge_retained_under_admission(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used.charge(bytes);
        #[cfg(any(test, feature = "test-seams"))]
        {
            self.high_water_for_test
                .fetch_max(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
            self.admitted_bytes_for_test
                .fetch_add(bytes, Ordering::SeqCst);
            self.retained_charge_bytes_for_test
                .fetch_add(bytes, Ordering::SeqCst);
            note_budget_ledger("charge_retained", bytes, self.used.load(Ordering::SeqCst));
        }
    }

    /// An allocation inside an admitted schema unit that the ordinary budget
    /// has already refused -- so that unit has spent everything it was priced
    /// at and is now asking for more.
    ///
    /// It is charged, unconditionally, up to
    /// [`ADMITTED_SCHEMA_UNIT_EXCESS_BYTES`] FOR THAT UNIT. Past that this
    /// answers `false` and the ordinary typed refusal fires, which is what
    /// keeps the overshoot a peer can cause at that constant rather than at
    /// whatever that peer chose to send. The allowance is read from the unit
    /// installing on this thread, and only if that unit was priced against THIS
    /// accountant, so one unit exhausting its allowance never refuses another's
    /// next allocation.
    fn charge_admitted_unit_excess(&self, bytes: usize) -> bool {
        let token = std::ptr::from_ref(self) as usize;
        let charged = ADMITTED_UNITS
            .try_with(|units| {
                let mut units = units.borrow_mut();
                let Some(budget) = units.last_mut() else {
                    return false;
                };
                if budget.accountant != token
                    || bytes
                        > ADMITTED_SCHEMA_UNIT_EXCESS_BYTES.saturating_sub(budget.excess_charged)
                {
                    return false;
                }
                budget.excess_charged = budget.excess_charged.saturating_add(bytes);
                true
            })
            .unwrap_or(false);
        if charged {
            self.charge_retained(bytes);
        }
        charged
    }

    /// Return freed bytes to the budget.
    pub fn release(&self, bytes: usize) {
        let _admission = self.writer_admission();
        self.release_under_admission(bytes);
    }
    fn release_under_admission(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let released = self.used.release_saturating(bytes);
        #[cfg(any(test, feature = "test-seams"))]
        if bytes > released {
            self.underflow_count.fetch_add(1, Ordering::SeqCst);
        }
        #[cfg(any(test, feature = "test-seams"))]
        note_budget_ledger("release", bytes, self.used.load(Ordering::SeqCst));
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = released;
    }

    pub(crate) fn register_idle_graph_reclaimer(
        &self,
        vector: &Arc<contextdb_vector::VectorStore>,
    ) {
        *self.idle_graph_reclaimer.lock() = Some(Arc::downgrade(vector));
    }

    /// Runtime budget adjustment. None removes limit.
    /// Returns Err if new limit exceeds startup ceiling.
    pub fn set_budget(&self, limit: Option<usize>) -> contextdb_core::Result<()> {
        let _admission = self.writer_admission();
        if self.has_ceiling.load(Ordering::SeqCst) {
            let ceiling = self.startup_ceiling.load(Ordering::SeqCst);
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(contextdb_core::Error::Other(format!(
                        "memory limit {bytes} exceeds startup ceiling {ceiling}"
                    )));
                }
                None => {
                    return Err(contextdb_core::Error::Other(
                        "cannot remove memory limit when a startup ceiling is set".to_string(),
                    ));
                }
                _ => {}
            }
        }

        match limit {
            Some(bytes) => {
                self.limit.store(bytes, Ordering::SeqCst);
            }
            None => {
                self.limit.store(0, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    /// Start the high-water mark again from what is admitted now.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn reset_high_water_for_test(&self) {
        self.high_water_for_test
            .store(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Peak successfully admitted bytes, including allocations already released.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn high_water_for_test(&self) -> usize {
        self.high_water_for_test.load(Ordering::SeqCst)
    }

    /// Successful byte admissions at this accountant, on every calling thread.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn admitted_bytes_for_test(&self) -> usize {
        self.admitted_bytes_for_test.load(Ordering::SeqCst)
    }

    /// Bytes this accountant took through `charge_retained`, the writer's
    /// unconditional retention door. A read session's accountant must report
    /// zero here: a committed image never appends to a change log, and every
    /// byte of the image it does hold was already admitted when it was
    /// decoded.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn retained_charge_bytes_for_test(&self) -> usize {
        self.retained_charge_bytes_for_test.load(Ordering::SeqCst)
    }

    pub(crate) fn writer_envelope_usage(&self) -> contextdb_core::Result<(usize, usize, usize)> {
        // Report-only: this writer's bytes, the process runtime reservation,
        // and their sum. The reservation is owned by the process, not by
        // this writer's headroom.
        let admission = self.writer_admission();
        let used = self.used.load(Ordering::SeqCst);
        let native = admission
            ._gate
            .as_ref()
            .and_then(|gate| gate.native.as_ref())
            .map_or(0, |runtime| runtime.reservation.bytes);
        let combined = used.checked_add(native).ok_or_else(|| {
            contextdb_core::Error::Other("writer report combined size overflow".into())
        })?;
        Ok((used, native, combined))
    }

    /// Snapshot of current memory state.
    pub fn usage(&self) -> MemoryUsage {
        let admission = self.writer_admission();
        let limit = match self.limit.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        };
        let used = self.used.load(Ordering::SeqCst);
        let startup_ceiling = self
            .has_ceiling
            .load(Ordering::SeqCst)
            .then(|| self.startup_ceiling.load(Ordering::SeqCst));
        MemoryUsage {
            #[cfg(any(test, feature = "test-seams"))]
            reader_file_high_water: 0,
            limit,
            used,
            available: min_available(
                writer_headroom(limit.unwrap_or(0), used, 0, 0).available,
                admission.process_available(),
            ),
            startup_ceiling,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn underflow_count_for_test(&self) -> usize {
        self.underflow_count.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_tail_admission_failure_for_test(&self) {
        self.tail_admission_failure.store(1, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn tail_admission_failure_consumed_for_test(&self) -> bool {
        self.tail_admission_failure.load(Ordering::SeqCst) == 2
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_current_identity_admission_failure_for_test(&self) {
        self.tail_admission_failure.store(3, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn current_identity_admission_failure_consumed_for_test(&self) -> bool {
        self.tail_admission_failure.load(Ordering::SeqCst) == 4
    }

    /// Evict idle graphs of partitions the statement on this thread is not
    /// searching until `short` more bytes are free, for a graph it is about
    /// to load. Returns how many graphs were evicted.
    pub(crate) fn make_room_for_graph_load(&self, short: usize) -> usize {
        self.idle_graph_reclaimer
            .lock()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map_or(0, |vector| vector.make_room_for_graph_load(short))
    }

    pub fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for_provenance(bytes, subsystem, operation, hint, None)
    }
    fn available_for_provenance(
        &self,
        provenance: Option<&StoreAccountantHandle>,
    ) -> Option<usize> {
        let admission = self.writer_admission_for_provenance(provenance);
        min_available(
            writer_headroom(
                self.limit.load(Ordering::SeqCst),
                self.used.load(Ordering::SeqCst),
                0,
                0,
            )
            .available,
            admission.process_available(),
        )
    }
    fn try_allocate_for_provenance(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
        provenance: Option<&StoreAccountantHandle>,
    ) -> contextdb_core::Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if operation == "current_identity_delta"
            && self
                .tail_admission_failure
                .compare_exchange(3, 4, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_owned(),
                operation: operation.to_owned(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: "Injected current vector identity publication admission exhaustion."
                    .to_owned(),
            });
        }
        #[cfg(any(test, feature = "test-seams"))]
        if operation == "prepare_hnsw_tail"
            && self
                .tail_admission_failure
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_owned(),
                operation: operation.to_owned(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: "Injected fresh-tail admission exhaustion.".to_owned(),
            });
        }
        let mut allocation = self.try_allocate_with_provenance(bytes, provenance);
        #[cfg(any(test, feature = "test-seams"))]
        if allocation.is_ok() {
            note_budget_admission(
                subsystem,
                operation,
                bytes,
                self.used.load(Ordering::SeqCst),
            );
        }
        // A saved graph whose topology does not fit is admitted once the
        // serving pages already resident give way to it: they are re-readable,
        // the topology is what makes its partition searchable at all.
        // Idle graphs of partitions the statement is not searching go first;
        // serving pages give way only after them.
        if allocation.is_err() && operation == "load_retained_vector_generation" {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            if let Some(vector) = vector {
                let short = || {
                    bytes.saturating_sub(
                        self.available_for_provenance(provenance)
                            .unwrap_or(usize::MAX),
                    )
                };
                if vector.make_room_for_graph_load(short()) != 0 {
                    allocation = self.try_allocate_with_provenance(bytes, provenance);
                }
                if allocation.is_err() && vector.give_back_serving_pages(short()) != 0 {
                    allocation = self.try_allocate_with_provenance(bytes, provenance);
                }
            }
        }
        // Serving pages are a cache a read can always refill, so nothing is
        // refused while they hold the memory it needs: they give way to any
        // allocation exactly as they give way to a graph load. The give-back
        // only tries its locks, so it is safe from inside any caller.
        if allocation.is_err() && operation != "load_retained_vector_generation" {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let short = bytes.saturating_sub(
                self.available_for_provenance(provenance)
                    .unwrap_or(usize::MAX),
            );
            if vector.is_some_and(|vector| vector.give_back_serving_pages(short) != 0) {
                allocation = self.try_allocate_with_provenance(bytes, provenance);
            }
        }
        if allocation.is_err() && matches!(operation, "read/table_scan" | "read/index_range") {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let graphs = vector
                .as_ref()
                .map_or(0, |vector| vector.evict_idle_graphs());
            if graphs != 0 {
                allocation = self.try_allocate_with_provenance(bytes, provenance);
            }
        }
        // A received schema unit that was already ADMITTED AS A WHOLE is
        // applying against THIS accountant, and an allocation it did not model
        // does not fit. It draws on the reservation the preflight is holding
        // for it, and past that on a BOUNDED excess.
        //
        // The unit was priced and admitted before its first item mutated
        // anything, so a limit genuinely too small for it was already refused
        // there, leaving the receiver's old complete state. Refusing at the
        // first byte the model got slightly wrong would instead leave a proper
        // subset of an arriving schema installed and durable -- the one outcome
        // the design's "Atomic schema installation" row forbids outright -- and
        // there is no way back: a received DROP TABLE cannot be taken back at
        // all, and compensating DDL written to undo it lands in the REPLICATED
        // log, so one node's memory shortfall would propagate a schema revert
        // to every peer.
        //
        // But the model's error is driven by what a PEER sent, so absorbing it
        // without a ceiling would hand the peer the operator's declared limit.
        // Past `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES` the ordinary typed refusal
        // fires: the limit the operator declared bounds the process, and the
        // most a unit can take past what it was priced at is that constant.
        if allocation.is_err() && self.charge_admitted_unit_excess(bytes) {
            return Ok(());
        }
        allocation.map_err(|err| match err {
            contextdb_core::Error::MemoryBudgetExceeded {
                requested_bytes,
                budget_limit_bytes,
                available_bytes,
                ..
            } => contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_string(),
                operation: operation.to_string(),
                requested_bytes,
                budget_limit_bytes,
                available_bytes,
                hint: hint.to_string(),
            },
            other => other,
        })
    }
}

// No Weak is created for this private adapter. Its sole factory returns an
// ordinary Arc; clones/coercions retain the same control allocation.
#[derive(Debug)]
pub(crate) struct WriterVectorBudget {
    accountant: Option<AccountantHandle>,
    layout: Layout,
    _writer_use: Option<WriterUse>,
}
impl WriterVectorBudget {
    fn accountant(&self) -> &AccountantHandle {
        self.accountant
            .as_ref()
            .expect("live adapter has its accountant")
    }
}
impl Drop for WriterVectorBudget {
    fn drop(&mut self) {
        let accountant = self.accountant.take().expect("adapter has its accountant");
        if self.layout.size() == 0 {
            drop(self._writer_use.take());
            return;
        }
        // Alignment one is a receipt discriminator only, never an allocator
        // layout: the concrete Arc has pointer alignment. Reusing this field
        // keeps the fixed assembly adapter at its inherited 64 B footprint.
        if self.layout.align() == 1 {
            drop(self._writer_use.take());
            drop(accountant);
            refund_open_residue(self.layout.size());
            return;
        }
        let refund = accountant.retain_box_control_refund(self.layout.size());
        // Registration/payload cleanup and accountant destruction finish before
        // the fixed ArcInner refund. No Weak exists for this allocation.
        drop(self._writer_use.take());
        drop(accountant);
        let released = refund.release_owned(self.layout.size());
        // Never a second panic while unwinding from another failure.
        if !std::thread::panicking() {
            released.expect("adapter control refund is owned");
        }
        // The bounded original counter owner is the last remaining liability.
    }
}
impl contextdb_core::read_memory::ReadMemoryBudget for WriterVectorBudget {
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.accountant()
            .try_allocate_for_native(bytes, subsystem, operation, hint)
    }

    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.accountant()
            .try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.accountant().release(bytes);
    }
}
impl contextdb_vector::MemoryBudget for WriterVectorBudget {
    fn stable_funder_identity(&self) -> usize {
        self.accountant().as_ptr() as usize
    }
    fn transition_reservation_for(
        &self,
        old: usize,
        new: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        contextdb_vector::MemoryBudget::transition_reservation_for(
            &**self.accountant(),
            old,
            new,
            subsystem,
            operation,
            hint,
        )
    }
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        self.accountant()
            .try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.accountant().release(bytes);
    }
    fn available_bytes(&self) -> Option<usize> {
        self.accountant().usage().available
    }
    fn adopt_caller_reservation(&self, bytes: usize) -> contextdb_core::Result<bool> {
        adopt_vector_load_reservation(self.accountant(), bytes)
    }
}
impl MemoryAccountant {
    #[cfg(test)]
    pub(crate) fn writer_registration_observation_for_test(
        &self,
    ) -> (usize, usize, usize, usize, usize) {
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if let Some(record) = self
            .writer_identity
            .get()
            .and_then(|key| domain.lookup_in(*key))
        {
            return (
                record as *const WriterRegistration as usize,
                Layout::new::<WriterRegistration>().size(),
                record.epoch,
                record.users,
                self.used.as_ptr() as usize,
            );
        }
        (0, 0, 0, 0, self.used.as_ptr() as usize)
    }
    #[cfg(test)]
    pub(crate) fn writer_vector_adapter_type_layout_for_test() -> Layout {
        writer_adapter_type_layout::<WriterVectorBudget>()
    }
}
impl AccountantHandle {
    /// Hold this unit's admitted bytes until it installs or refuses.
    pub(crate) fn reserve_schema_unit(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<SchemaUnitReservation> {
        self.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(SchemaUnitReservation {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(self.clone())),
            accountant: self.clone(),
            remaining: std::cell::Cell::new(bytes),
        })
    }

    #[cfg(test)]
    pub(crate) fn writer_vector_adapter_layout_for_test(&self) -> Layout {
        crate::persistence::returned_loader_rows_for_test();
        crate::persistence::content_identity::proof_allocation_oracle_tests::r1_mapped_control_rows(
        );
        let adapter = WriterVectorBudget {
            accountant: Some(self.clone()),
            layout: Layout::new::<()>(),
            _writer_use: WriterUse::retain_if_writer(self),
        };
        writer_adapter_layout(&adapter)
    }
    fn budget_adapter(
        &self,
        writer_use: Option<WriterUse>,
        open_residue: bool,
    ) -> contextdb_core::Result<Arc<WriterVectorBudget>> {
        let mut adapter = WriterVectorBudget {
            accountant: Some(self.clone()),
            layout: Layout::new::<()>(),
            _writer_use: writer_use,
        };
        let layout = writer_adapter_layout(&adapter);
        if open_residue {
            assert!(layout.align() > 1);
            admit_open_residue(layout.size(), "assembly_budget_adapter_open_residue")?;
            adapter.layout =
                Layout::from_size_align(layout.size(), 1).expect("receipt size is already valid");
        } else {
            self.try_allocate_for(
                layout.size(),
                "writer",
                "adapter_control",
                "Raise MEMORY_LIMIT for the writer adapter.",
            )?;
            adapter.layout = layout;
            self.change_adapter_control_receipt(layout.size(), true);
        }
        Ok(Arc::new(adapter))
    }
    pub(crate) fn vector_budget_for_original_reader(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        match self.standard_arc() {
            Some(arc) => Ok(arc),
            None => Ok(self.budget_adapter(None, false)?),
        }
    }
    fn vector_budget_for_startup(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        let writer_use =
            WriterUse::retain_if_writer(self).expect("construction root retains the writer epoch");
        Ok(self.budget_adapter(Some(writer_use), true)?)
    }
    pub(crate) fn vector_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(Some(writer_use), false)?),
            None => match self.standard_arc() {
                Some(arc) => Ok(arc),
                None => Ok(self.budget_adapter(None, false)?),
            },
        }
    }
    pub(crate) fn assembly_read_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(Some(writer_use), true)?),
            None => match self.standard_arc() {
                Some(arc) => Ok(arc),
                None => Ok(self.budget_adapter(None, false)?),
            },
        }
    }
    pub(crate) fn read_budget_for_original(
        &self,
        writer_use: Option<WriterUse>,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        if writer_use.is_none()
            && let Some(arc) = self.standard_arc()
        {
            return Ok(arc);
        }
        Ok(self.budget_adapter(writer_use, false)?)
    }
    pub(crate) fn read_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(Some(writer_use), false)?),
            None => match self.standard_arc() {
                Some(arc) => Ok(arc),
                None => Ok(self.budget_adapter(None, false)?),
            },
        }
    }
}

impl contextdb_vector::MemoryBudget for MemoryAccountant {
    fn transition_reservation_for(
        &self,
        old_bytes: usize,
        new_bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        let mut transition =
            self.transition_reservation(old_bytes, new_bytes, subsystem, operation, hint);
        // Activation's short exclusive scope never invokes a reclaimer.
        if self.exclusive_activation().is_some() {
            return transition;
        }
        // Workspace growth preserves ordinary admission's cache give-back:
        // these pages can be filled again and their release is independent
        // of the caller's still-held old reservation.
        if transition.is_err() {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let short = new_bytes
                .saturating_sub(old_bytes)
                .saturating_sub(self.usage().available.unwrap_or(usize::MAX));
            if vector.is_some_and(|vector| vector.give_back_serving_pages(short) != 0) {
                transition =
                    self.transition_reservation(old_bytes, new_bytes, subsystem, operation, hint);
            }
        }
        transition
    }

    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        MemoryAccountant::try_allocate_for(self, bytes, subsystem, operation, hint)
    }

    fn release(&self, bytes: usize) {
        MemoryAccountant::release(self, bytes);
    }

    fn available_bytes(&self) -> Option<usize> {
        self.usage().available
    }

    fn adopt_caller_reservation(&self, bytes: usize) -> contextdb_core::Result<bool> {
        adopt_vector_load_reservation(self, bytes)
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for MemoryAccountant {
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.try_allocate_for_native(bytes, subsystem, operation, hint)
    }

    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        MemoryAccountant::release(self, bytes);
    }
}

#[cfg(test)]
impl MemoryAccountant {
    // The existing hooks can join a thread that mutates this accountant.
    // Release the shared gate before invoking them and refresh the native
    // snapshot afterward. If activation changed it, price again before CAS.
    // Exclusive activation performs only the counter/token transition and
    // never calls these arbitrary test callbacks while holding its gate.
    fn fire_accounting_hook_without_gate(
        &self,
        admission: &mut WriterAdmission,
        success: bool,
    ) -> bool {
        self.fire_accounting_hook_without_gate_for_provenance(admission, None, success)
    }
    #[cfg(test)]
    fn fire_accounting_hook_without_gate_for_provenance(
        &self,
        admission: &mut WriterAdmission,
        provenance: Option<&StoreAccountantHandle>,
        success: bool,
    ) -> bool {
        if self.exclusive_activation().is_some() {
            return false;
        }
        let ceiling_before = admission.ceiling;
        drop(admission._gate.take());
        if success {
            self.fire_admission_race_hook();
        } else {
            self.fire_alloc_race_hook();
        }
        *admission = self.writer_admission_for_provenance(provenance);
        admission.ceiling != ceiling_before
    }

    /// Test-build only: install the one-shot interleaving hook.
    fn set_alloc_race_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.alloc_race_hook.0.lock().unwrap() = Some(hook);
    }

    /// Test-build only: fire (and consume) the interleaving hook, if installed.
    fn fire_alloc_race_hook(&self) {
        let hook = self.alloc_race_hook.0.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Test-build only: install the one-shot hook that runs between the limit
    /// read and the `used` CAS of a request that fits the limit it read.
    pub(crate) fn set_admission_race_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.admission_race_hook.0.lock().unwrap() = Some(hook);
    }

    /// Test-build only: fire (and consume) the admission interleaving hook.
    fn fire_admission_race_hook(&self) {
        let hook = self.admission_race_hook.0.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reservation_transition_at_zero_headroom_keeps_capacity_from_a_concurrent_writer() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(1_000).unwrap();
        let writer_accountant = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer_accountant.clone();
            std::thread::spawn(move || {
                assert!(
                    writer
                        .try_allocate_for(1, "writer", "insert", "test")
                        .is_err()
                );
                assert_eq!(writer.usage().used, 1_000);
            })
            .join()
            .unwrap();
        }));
        accountant
            .transition_reservation(900, 900, "vector_index", "encode", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 1_000);
        let writer_accountant = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer_accountant.clone();
            std::thread::spawn(move || {
                assert!(writer.try_allocate(400).is_err());
                assert_eq!(writer.usage().used, 1_000);
            })
            .join()
            .unwrap();
        }));
        accountant
            .transition_reservation(900, 600, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 700);
        accountant.try_allocate(300).unwrap();
        #[cfg(any(test, feature = "test-seams"))]
        assert_eq!(accountant.high_water_for_test(), 1_000);
        accountant.release(1_000);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_retries_cas_after_a_concurrent_independent_charge() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(900).unwrap();
        let writer = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer.clone();
            std::thread::spawn(move || writer.try_allocate(50).unwrap())
                .join()
                .unwrap();
        }));
        accountant
            .transition_reservation(600, 600, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 950);
        #[cfg(any(test, feature = "test-seams"))]
        assert_eq!(accountant.high_water_for_test(), 950);
        accountant.release(600);
        assert_eq!(
            accountant.usage().used,
            350,
            "the concurrent writer's charge remains intact"
        );
        accountant.release(350);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_refusal_preserves_old_owner_and_typed_phase_fields() {
        let accountant = MemoryAccountant::with_budget(1_000);
        accountant.try_allocate(900).unwrap();
        let error = accountant
            .transition_reservation(600, 701, "vector_index", "candidate", "phase hint")
            .unwrap_err();
        match error {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => {
                assert_eq!(subsystem, "vector_index");
                assert_eq!(operation, "candidate");
                assert_eq!(requested_bytes, 101);
                assert_eq!(available_bytes, 100);
                assert_eq!(budget_limit_bytes, 1_000);
                assert_eq!(hint, "phase hint");
            }
            other => panic!("expected typed transition refusal, got {other:?}"),
        }
        assert_eq!(accountant.usage().used, 900);
        accountant
            .transition_reservation(600, 700, "vector_index", "candidate", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 1_000);
        #[cfg(any(test, feature = "test-seams"))]
        assert_eq!(accountant.high_water_for_test(), 1_000);
        accountant.release(1_000);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_rechecks_narrowed_limit_and_retains_old_charge() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(500).unwrap();
        let narrower = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let narrower = narrower.clone();
            std::thread::spawn(move || narrower.set_budget(Some(600)).unwrap())
                .join()
                .unwrap();
        }));
        assert!(matches!(
            accountant.transition_reservation(400, 700, "vector_index", "encode", "test"),
            Err(contextdb_core::Error::MemoryBudgetExceeded {
                requested_bytes: 300,
                available_bytes: 100,
                budget_limit_bytes: 600,
                ..
            })
        ));
        assert_eq!(accountant.usage().used, 500);
        // A further narrowing cannot prevent giving dead workspace back.
        accountant.set_budget(Some(200)).unwrap();
        accountant
            .transition_reservation(400, 100, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 200);
        accountant.release(200);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_retries_a_limit_removal_before_refusal() {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant.set_budget(Some(500)).unwrap();
        accountant.try_allocate(500).unwrap();
        let remover = accountant.clone();
        accountant.set_alloc_race_hook(Box::new(move || remover.set_budget(None).unwrap()));
        accountant
            .transition_reservation(400, 800, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 900);
        accountant.release(900);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    /// The TOCTOU this guards: `try_allocate` loads `limit`, finds the request
    /// over budget, and a concurrent `set_budget(None)` clears the limit before
    /// the failure returns. The re-check must observe the change and retry so
    /// the allocation succeeds under the now-unlimited budget — never a
    /// spurious `MemoryBudgetExceeded`. The hook forces exactly that
    /// interleaving; deleting the re-check makes this fail deterministically.
    #[test]
    fn set_budget_none_racing_allocate_never_spuriously_fails() {
        // A REMOVABLE runtime budget: `no_limit()` + `set_budget(Some(..))`.
        // (A `with_budget` construction sets a startup ceiling, which forbids
        // removing the limit — the race under test needs a removable one.)
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant
            .set_budget(Some(1024))
            .expect("adding a removable runtime budget must succeed");
        accountant
            .try_allocate(1024)
            .expect("filling the budget to the brim must succeed");
        assert_eq!(accountant.usage().available, Some(0));

        let racer = Arc::clone(&accountant);
        accountant.set_alloc_race_hook(Box::new(move || {
            racer
                .set_budget(None)
                .expect("removing the budget must succeed");
        }));

        accountant.try_allocate(1).expect(
            "allocate racing a concurrent set_budget(None) must observe the \
             cleared limit via the re-check and succeed, never spuriously fail",
        );
        assert!(accountant.usage().limit.is_none());
    }

    /// A reservation means the room the preflight found is still there when
    /// THAT unit's items take it, and `used` means what the store is holding.
    ///
    /// Both stop being true the moment two units share one accountant -- two
    /// stores opened on one accountant, which `open_with_config` exists to
    /// allow, or two units that change no table shape and so never meet the
    /// schema publication gate. A reservation that reads a POOL rather than its
    /// own bytes hands the first unit to enter its scope every live
    /// reservation's room, so the second unit does not have the guarantee it
    /// was given; and it then releases room it had already handed away, which
    /// leaves `used` permanently below what the store is holding and the
    /// declared limit that much less effective.
    #[test]
    fn one_schema_unit_never_spends_or_releases_another_unit_s_reserved_room() {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant
            .set_budget(Some(1_000_000))
            .expect("declare a limit both units are priced under");

        let first = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
            .reserve_schema_unit(100_000, "sync", "received_schema_unit", "")
            .expect("the first unit's price fits");
        let second = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
            .reserve_schema_unit(200_000, "sync", "received_schema_unit", "")
            .expect("the second unit's price fits too");
        assert_eq!(
            accountant.usage().used,
            300_000,
            "both reservations are held"
        );

        {
            // The first unit's items start installing. Entering hands that unit
            // the room ITS OWN reservation is holding -- never the second
            // unit's, which no item of this unit is entitled to spend.
            let _scope = AdmittedSchemaUnit::enter(&first);
            assert_eq!(
                accountant.usage().used,
                200_000,
                "entering a scope hands back the entering unit's 100,000 B and leaves the other \
                 unit's 200,000 B reserved"
            );
        }

        // The first unit is done and its reservation has nothing left to hand
        // back: its room went to its own items. Releasing anything here would
        // be releasing bytes those items are still holding.
        drop(first);
        assert_eq!(
            accountant.usage().used,
            200_000,
            "a spent reservation releases nothing, so `used` still accounts for the second unit"
        );

        drop(second);
        assert_eq!(
            accountant.usage().used,
            0,
            "and the unspent reservation releases exactly its own bytes"
        );
    }
}

/// Fork allocator ownership is independent of the redb page cache. The fork
/// must obtain this admission before opening/growing allocator metadata and
/// report actual capacities, including overlapping resize copies. Naming a
/// charge does not itself supply a structural allocator residency bound.
#[derive(Debug)]
#[allow(dead_code)] // Admission seam awaiting the fork allocator capacity oracle.
pub(crate) struct RedbAllocatorStateCharge {
    reservation: OwnedMemoryReservation,
    pub(crate) resident_bytes: usize,
    pub(crate) resize_copy_bytes: usize,
    pub(crate) region_bookkeeping_bytes: usize,
}

#[allow(dead_code)] // Consumed by the separately owned fork integration.
impl RedbAllocatorStateCharge {
    pub(crate) fn new(accountant: AccountantHandle) -> Self {
        Self {
            reservation: OwnedMemoryReservation::new(accountant),
            resident_bytes: 0,
            resize_copy_bytes: 0,
            region_bookkeeping_bytes: 0,
        }
    }

    /// The caller supplies capacities from the allocator, never file size or
    /// logical page counts. Growth refuses before the corresponding allocation;
    /// shrinking follows release of the old allocation.
    pub(crate) fn resize(
        &mut self,
        resident_bytes: usize,
        resize_copy_bytes: usize,
        region_bookkeeping_bytes: usize,
    ) -> contextdb_core::Result<()> {
        let next = resident_bytes
            .checked_add(resize_copy_bytes)
            .and_then(|bytes| bytes.checked_add(region_bookkeeping_bytes))
            .ok_or_else(|| contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "redb".into(),
                operation: "redb_allocator_state".into(),
                requested_bytes: usize::MAX,
                available_bytes: 0,
                budget_limit_bytes: usize::MAX,
                hint: "Allocator capacity accounting overflowed.".into(),
            })?;
        let held = self.reservation.bytes();
        if next > held {
            self.reservation.try_grow_for(
                next - held,
                "redb",
                "redb_allocator_state",
                "Raise MEMORY_LIMIT to admit redb allocator metadata.",
            )?;
        } else {
            self.reservation.try_shrink(held - next)?;
        }
        self.resident_bytes = resident_bytes;
        self.resize_copy_bytes = resize_copy_bytes;
        self.region_bookkeeping_bytes = region_bookkeeping_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod redb_allocator_state_tests {
    use super::*;

    #[test]
    fn allocator_capacity_owner_keeps_prior_charge_when_growth_refuses() {
        let accountant = Arc::new(MemoryAccountant::with_budget(128));
        let mut owner = RedbAllocatorStateCharge::new(Arc::clone(&accountant).into());
        owner.resize(32, 16, 8).unwrap();
        assert_eq!(accountant.usage().used, 56);
        assert!(owner.resize(64, 128, 8).is_err());
        assert_eq!(accountant.usage().used, 56);
        assert_eq!(
            (
                owner.resident_bytes,
                owner.resize_copy_bytes,
                owner.region_bookkeeping_bytes
            ),
            (32, 16, 8)
        );
        owner.resize(32, 0, 8).unwrap();
        assert_eq!(accountant.usage().used, 40);
        drop(owner);
        assert_eq!(accountant.usage().used, 0);
    }
}

/// A fixed native provider pins the actual original writer, not a current role.
/// It has no payload/root/invoice back-reference, so retained sources are acyclic.
struct NativeSourceProvider {
    accountant: Option<AccountantHandle>,
    writer: Option<WriterUse>,
    control: Option<ControlCredit>,
}
impl NativeSourceProvider {
    fn native_origin(&self) -> contextdb_core::Result<contextdb_tx::NativeSourceOrigin> {
        let accountant = self
            .accountant
            .as_ref()
            .expect("live native source accountant");
        let writer = self.writer.as_ref().expect("live native source writer");
        let control = self.control.as_ref().expect("live native source control");
        if !accountant.is_engine()
            || writer.accountant.as_ptr() != accountant.as_ptr()
            || writer.stable_identity == 0
            || writer.epoch == 0
            || accountant.writer_identity.get().copied() != Some(writer.stable_identity)
            || control
                .accountant
                .as_ref()
                .is_none_or(|owner| owner.as_ptr() != accountant.as_ptr())
            || control.bytes != writer_adapter_type_layout::<Self>().size()
            || control.registration != Some((writer.stable_identity, writer.epoch))
        {
            return Err(contextdb_core::Error::Other(
                "native source provider origin mismatch".into(),
            ));
        }
        Ok(contextdb_tx::NativeSourceOrigin::new(
            accountant.as_ptr() as usize,
            contextdb_tx::SourceLease {
                identity: writer.stable_identity,
                epoch: writer.epoch,
            },
        ))
    }
    fn paired_receiver(self: &Arc<Self>) -> contextdb_core::Result<contextdb_tx::SourceFunder> {
        let origin = self.native_origin()?;
        // SAFETY: the synchronous dispatch's paired guard dominates this
        // ordinary temporary receiver. This new count is immediately paired;
        // only that opaque count enters returned invoices and bridges.
        Ok(unsafe { contextdb_tx::SourceFunder::adopt_native_receiver(self.clone(), origin) })
    }
    fn lease(&self) -> contextdb_tx::SourceLease {
        let writer = self.writer.as_ref().expect("live native source writer");
        contextdb_tx::SourceLease {
            identity: writer.stable_identity,
            epoch: writer.epoch,
        }
    }
    fn assert_lease(&self, lease: contextdb_tx::SourceLease) {
        let original = self.lease();
        assert_eq!(
            lease.identity, original.identity,
            "source lease retains its original writer key"
        );
        assert_eq!(
            lease.epoch, original.epoch,
            "source lease retains its original writer epoch"
        );
    }
}
impl Drop for NativeSourceProvider {
    fn drop(&mut self) {
        let refund = self.control.take().map(ControlCredit::prepare);
        let mut cleanup =
            ReservationCleanup::new(self.accountant.take(), self.writer.take(), 0, refund);
        cleanup.finish();
    }
}
impl contextdb_tx::SourceFunding for NativeSourceProvider {
    fn copy_aux(
        self: Arc<Self>,
        bodies: contextdb_tx::staged_source::AuxBorrow<'_>,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedAuxBatch> {
        source_copy_aux(bodies, self.paired_receiver()?)
    }
    fn copy_sidecars(
        self: Arc<Self>,
        maps: contextdb_tx::staged_source::SidecarsBorrow<'_>,
        change: Option<(
            &str,
            contextdb_core::RowId,
            Option<(contextdb_core::Lsn, u8)>,
            Option<usize>,
        )>,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSidecars> {
        source_copy_sidecars(maps, change, self.paired_receiver()?)
    }

    fn retained_control_bytes(&self) -> Option<usize> {
        self.control.as_ref().map(|control| control.bytes)
    }
    fn visit_retained_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        self.native_origin().ok()?;
        self.writer.as_ref()?.visit_retained_source_controls(visit)
    }
    fn admit_source(&self, bytes: usize) -> contextdb_core::Result<contextdb_tx::SourceLease> {
        let original = self.writer.as_ref().expect("live native source writer");
        let projection = StoreAccountantHandle::from_writer_use(original);
        projection.try_allocate_for_original(
            bytes,
            "relational",
            "staged_source",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let lease = self.lease();
        PROCESS_WRITER_BUDGET_DOMAIN
            .write()
            .retain_in(lease.identity, lease.epoch);
        Ok(lease)
    }
    fn retain_source_lease(&self, lease: contextdb_tx::SourceLease) {
        self.assert_lease(lease);
        PROCESS_WRITER_BUDGET_DOMAIN
            .write()
            .retain_in(lease.identity, lease.epoch);
    }
    fn release_source(&self, bytes: usize, lease: contextdb_tx::SourceLease) {
        self.assert_lease(lease);
        let accountant = self
            .accountant
            .as_ref()
            .expect("live native source accountant");
        accountant.release(bytes);
        PROCESS_WRITER_BUDGET_DOMAIN
            .write()
            .release_in(accountant, lease.identity, lease.epoch);
    }
    fn copy_row(
        self: Arc<Self>,
        row: &contextdb_core::VersionedRow,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceRow> {
        let bytes = row.values.iter().try_fold(
            source_row_map_bytes(row.values.len())?,
            |bytes, (name, value)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_value_copy_bytes(value)?,
                )
            },
        )?;
        let funder = self.paired_receiver()?;
        // Use the actual original control-first reservation kernel before copying.
        let writer = self.writer.as_ref().expect("live native source writer");
        let grant = OwnedMemoryReservation::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.source_invoice_for(&funder)?;
        let mut values = std::collections::HashMap::with_capacity(row.values.len());
        for (name, value) in &row.values {
            values.insert(name.as_str().to_owned(), source_copy_value(value));
        }
        let copied = contextdb_core::VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        if let Err(error) = source_check_row_capacity(&copied) {
            drop(copied);
            return Err(error);
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceRow::new(
            copied,
            contextdb_tx::staged_source::OriginalRowInvoice::from_charge(charge),
        ))
    }
    fn copy_eager_row(
        self: Arc<Self>,
        row: &contextdb_core::VersionedRow,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedEagerSourceRow> {
        let bytes = row.values.iter().try_fold(
            source_row_map_bytes(row.values.len())?,
            |bytes, (name, value)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_value_copy_bytes(value)?,
                )
            },
        )?;
        // This is the same actual typed kernel used by the existing staged
        // posting provider: native reservation control precedes its debit.
        let funder = self.paired_receiver()?;
        let writer = self.writer.as_ref().expect("live native source writer");
        let grant = OwnedMemoryReservation::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        // The sole typed grant is private. Election yields its metadata only
        // AFTER the temporary reservation ArcInner has been deallocated.
        let mut original = Arc::into_inner(grant).expect("private eager grant has one owner");
        original.finish_freed_enclosing_control();
        let invoice = original.into_source_row_invoice_for(&funder)?;
        let bridge =
            contextdb_tx::staged_source::OriginalCopyBridge::prepare_funded(original, funder)?;
        let mut values = std::collections::HashMap::with_capacity(row.values.len());
        for (name, value) in &row.values {
            values.insert(name.as_str().to_owned(), source_copy_value(value));
        }
        let copied = contextdb_core::VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        if let Err(error) = source_check_row_capacity(&copied) {
            drop(copied);
            return Err(error);
        }
        Ok(contextdb_tx::staged_source::AdmittedEagerSourceRow::new(
            copied, invoice, bridge,
        ))
    }
    fn copy_vector(
        self: Arc<Self>,
        entry: &contextdb_core::VectorEntry,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceVector> {
        let bytes = source_add(
            source_add(entry.index.table.len(), entry.index.column.len())?,
            source_array::<f32>(entry.vector.len())?,
        )?;
        let funder = self.paired_receiver()?;
        // Use the actual original control-first reservation kernel before copying.
        let writer = self.writer.as_ref().expect("live native source writer");
        let grant = OwnedMemoryReservation::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.source_invoice_for(&funder)?;
        let copied = contextdb_core::VectorEntry {
            index: contextdb_core::VectorIndexRef {
                table: entry.index.table.as_str().to_owned(),
                column: entry.index.column.as_str().to_owned(),
            },
            row_id: entry.row_id,
            vector: entry.vector.as_slice().to_vec(),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        };
        if copied.vector.capacity() != copied.vector.len()
            || copied.index.table.capacity() != copied.index.table.len()
            || copied.index.column.capacity() != copied.index.column.len()
        {
            drop(copied);
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceVector::new(
            copied,
            contextdb_tx::staged_source::OriginalVectorInvoice::from_charge(charge),
        ))
    }
    fn copy_eager_vector(
        self: Arc<Self>,
        entry: &contextdb_core::VectorEntry,
    ) -> contextdb_core::Result<contextdb_tx::AdmittedEagerSourceVector> {
        let bytes = source_add(
            source_add(entry.index.table.len(), entry.index.column.len())?,
            source_array::<f32>(entry.vector.len())?,
        )?;
        // Validate the paired receiver before genuine original admission or
        // any fallible key/root preparation. This pins the original epoch.
        let funder = self.paired_receiver()?;
        let writer = self.writer.as_ref().expect("live native source writer");
        let grant = OwnedMemoryReservation::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original =
            Arc::into_inner(grant).expect("private eager vector grant has one owner");
        original.finish_freed_enclosing_control();
        let invoice = original.into_source_vector_invoice_for(&funder)?;
        let bridge = contextdb_tx::OriginalCopyBridge::prepare_funded(original, funder)?;
        // The original body debit, invoice and bridge all precede allocation.
        let copied = source_copy_vector(entry);
        source_check_vector_capacity(&copied)?;
        Ok(contextdb_tx::AdmittedEagerSourceVector::new(
            copied, invoice, bridge,
        ))
    }
    fn copy_key(
        self: Arc<Self>,
        key: &contextdb_core::IndexKey,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceKey> {
        let bytes = key.iter().try_fold(
            source_array::<contextdb_core::DirectedValue>(key.len())?,
            |bytes, value| {
                let value = match value {
                    contextdb_core::DirectedValue::Asc(value) => &value.0,
                    contextdb_core::DirectedValue::Desc(value) => &value.0,
                };
                source_add(bytes, source_value_copy_bytes(value)?)
            },
        )?;
        let funder = self.paired_receiver()?;
        // Use the actual original control-first reservation kernel before copying.
        let writer = self.writer.as_ref().expect("live native source writer");
        let grant = OwnedMemoryReservation::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.source_invoice_for(&funder)?;
        let mut copied = Vec::with_capacity(key.len());
        for value in key {
            copied.push(match value {
                contextdb_core::DirectedValue::Asc(value) => contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(source_copy_value(&value.0)),
                ),
                contextdb_core::DirectedValue::Desc(value) => contextdb_core::DirectedValue::Desc(
                    contextdb_core::TotalOrdDesc(source_copy_value(&value.0)),
                ),
            });
        }
        for value in &copied {
            let value = match value {
                contextdb_core::DirectedValue::Asc(v) => &v.0,
                contextdb_core::DirectedValue::Desc(v) => &v.0,
            };
            if let Err(error) = source_check_value_capacity(value) {
                drop(copied);
                return Err(error);
            }
        }
        if copied.capacity() != copied.len() {
            drop(copied);
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceKey::new(
            copied, charge,
        ))
    }
}

impl contextdb_tx::staged_source::SourceFunding for MemoryAccountant {
    fn copy_aux(
        self: Arc<Self>,
        bodies: contextdb_tx::staged_source::AuxBorrow<'_>,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedAuxBatch> {
        source_copy_aux(bodies, contextdb_tx::SourceFunder::from_host(self))
    }
    fn copy_sidecars(
        self: Arc<Self>,
        maps: contextdb_tx::staged_source::SidecarsBorrow<'_>,
        change: Option<(
            &str,
            contextdb_core::RowId,
            Option<(contextdb_core::Lsn, u8)>,
            Option<usize>,
        )>,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSidecars> {
        source_copy_sidecars(maps, change, contextdb_tx::SourceFunder::from_host(self))
    }

    fn matches_original_source_origin(
        &self,
        origin: contextdb_tx::NativeSourceOrigin,
        lease: contextdb_tx::SourceLease,
    ) -> Option<bool> {
        let actual =
            contextdb_tx::NativeSourceOrigin::new(std::ptr::from_ref(self) as usize, lease);
        Some(actual.same_origin(&origin))
    }
    fn admit_source(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::SourceLease> {
        self.try_allocate_for(
            bytes,
            "relational",
            "staged_source",
            "Release staged captures or raise MEMORY_LIMIT.",
        )?;
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let registered = self
            .writer_identity
            .get()
            .copied()
            .and_then(|identity| domain.lookup_in(identity).map(|r| (identity, r.epoch)));
        let (identity, epoch) = registered.unwrap_or((0, 0));
        if identity != 0 {
            domain.retain_in(identity, epoch);
        }
        Ok(contextdb_tx::staged_source::SourceLease { identity, epoch })
    }
    fn retain_source_lease(&self, lease: contextdb_tx::staged_source::SourceLease) {
        if lease.identity != 0 {
            PROCESS_WRITER_BUDGET_DOMAIN
                .write()
                .retain_in(lease.identity, lease.epoch);
        }
    }
    fn release_source(&self, bytes: usize, lease: contextdb_tx::staged_source::SourceLease) {
        self.release(bytes);
        if lease.identity != 0 {
            PROCESS_WRITER_BUDGET_DOMAIN
                .write()
                .release_in(self, lease.identity, lease.epoch);
        }
    }
    fn copy_row(
        self: Arc<Self>,
        row: &contextdb_core::VersionedRow,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceRow> {
        let bytes = row.values.iter().try_fold(
            source_row_map_bytes(row.values.len())?,
            |bytes, (name, value)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_value_copy_bytes(value)?,
                )
            },
        )?;
        let accountant = AccountantHandle::from_host(self.clone());
        let held_writer = WriterUse::retain_if_writer(&accountant);
        let grant = if let Some(writer) = &held_writer {
            OwnedMemoryReservation::try_new_arc_for_original(
                writer,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            OwnedMemoryReservation::try_new_arc_for_nonwriter(
                &accountant,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.into_source_charge()?;
        let mut values = std::collections::HashMap::with_capacity(row.values.len());
        for (name, value) in &row.values {
            values.insert(name.as_str().to_owned(), source_copy_value(value));
        }
        let copied = contextdb_core::VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        if let Err(error) = source_check_row_capacity(&copied) {
            drop(copied);
            return Err(error);
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceRow::new(
            copied,
            contextdb_tx::staged_source::OriginalRowInvoice::from_charge(charge),
        ))
    }
    fn copy_eager_row(
        self: Arc<Self>,
        row: &contextdb_core::VersionedRow,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedEagerSourceRow> {
        let bytes = row.values.iter().try_fold(
            source_row_map_bytes(row.values.len())?,
            |bytes, (name, value)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_value_copy_bytes(value)?,
                )
            },
        )?;
        // This is the same actual typed kernel used by the existing staged
        // posting provider: native reservation control precedes its debit.
        let accountant = AccountantHandle::from_host(self.clone());
        let held_writer = WriterUse::retain_if_writer(&accountant);
        let grant = if let Some(original) = &held_writer {
            OwnedMemoryReservation::try_new_arc_for_original(
                original,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            OwnedMemoryReservation::try_new_arc_for_nonwriter(
                &accountant,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        // The sole typed grant is private. Election yields its metadata only
        // AFTER the temporary reservation ArcInner has been deallocated.
        let mut original = Arc::into_inner(grant).expect("private eager grant has one owner");
        original.finish_freed_enclosing_control();
        let invoice = original.into_source_invoice()?;
        let funder = contextdb_tx::SourceFunder::from_host(self);
        let bridge =
            contextdb_tx::staged_source::OriginalCopyBridge::prepare_funded(original, funder)?;
        let mut values = std::collections::HashMap::with_capacity(row.values.len());
        for (name, value) in &row.values {
            values.insert(name.as_str().to_owned(), source_copy_value(value));
        }
        let copied = contextdb_core::VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        if let Err(error) = source_check_row_capacity(&copied) {
            drop(copied);
            return Err(error);
        }
        Ok(contextdb_tx::staged_source::AdmittedEagerSourceRow::new(
            copied, invoice, bridge,
        ))
    }
    fn copy_vector(
        self: Arc<Self>,
        entry: &contextdb_core::VectorEntry,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceVector> {
        let bytes = source_add(
            source_add(entry.index.table.len(), entry.index.column.len())?,
            source_array::<f32>(entry.vector.len())?,
        )?;
        let accountant = AccountantHandle::from_host(self.clone());
        let held_writer = WriterUse::retain_if_writer(&accountant);
        let grant = if let Some(writer) = &held_writer {
            OwnedMemoryReservation::try_new_arc_for_original(
                writer,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            OwnedMemoryReservation::try_new_arc_for_nonwriter(
                &accountant,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.into_source_charge()?;
        let copied = contextdb_core::VectorEntry {
            index: contextdb_core::VectorIndexRef {
                table: entry.index.table.as_str().to_owned(),
                column: entry.index.column.as_str().to_owned(),
            },
            row_id: entry.row_id,
            vector: entry.vector.as_slice().to_vec(),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        };
        if copied.vector.capacity() != copied.vector.len()
            || copied.index.table.capacity() != copied.index.table.len()
            || copied.index.column.capacity() != copied.index.column.len()
        {
            drop(copied);
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceVector::new(
            copied,
            contextdb_tx::staged_source::OriginalVectorInvoice::from_charge(charge),
        ))
    }
    fn copy_eager_vector(
        self: Arc<Self>,
        entry: &contextdb_core::VectorEntry,
    ) -> contextdb_core::Result<contextdb_tx::AdmittedEagerSourceVector> {
        let bytes = source_add(
            source_add(entry.index.table.len(), entry.index.column.len())?,
            source_array::<f32>(entry.vector.len())?,
        )?;
        let accountant = AccountantHandle::from_host(self.clone());
        let held_writer = WriterUse::retain_if_writer(&accountant);
        let grant = if let Some(original) = &held_writer {
            OwnedMemoryReservation::try_new_arc_for_original(
                original,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            OwnedMemoryReservation::try_new_arc_for_nonwriter(
                &accountant,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        let mut original =
            Arc::into_inner(grant).expect("private eager vector grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.into_source_charge()?;
        let invoice = contextdb_tx::OriginalVectorInvoice::from_charge(charge);
        let funder = contextdb_tx::SourceFunder::from_host(self);
        let bridge = contextdb_tx::OriginalCopyBridge::prepare_funded(original, funder)?;
        let copied = source_copy_vector(entry);
        source_check_vector_capacity(&copied)?;
        Ok(contextdb_tx::AdmittedEagerSourceVector::new(
            copied, invoice, bridge,
        ))
    }
    fn copy_key(
        self: Arc<Self>,
        key: &contextdb_core::IndexKey,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceKey> {
        let bytes = key.iter().try_fold(
            source_array::<contextdb_core::DirectedValue>(key.len())?,
            |bytes, value| {
                let value = match value {
                    contextdb_core::DirectedValue::Asc(value) => &value.0,
                    contextdb_core::DirectedValue::Desc(value) => &value.0,
                };
                source_add(bytes, source_value_copy_bytes(value)?)
            },
        )?;
        let accountant = AccountantHandle::from_host(self.clone());
        let held_writer = WriterUse::retain_if_writer(&accountant);
        let grant = if let Some(writer) = &held_writer {
            OwnedMemoryReservation::try_new_arc_for_original(
                writer,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        } else {
            OwnedMemoryReservation::try_new_arc_for_nonwriter(
                &accountant,
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?
        };
        let mut original = Arc::into_inner(grant).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let charge = original.into_source_charge()?;
        let mut copied = Vec::with_capacity(key.len());
        for value in key {
            copied.push(match value {
                contextdb_core::DirectedValue::Asc(value) => contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(source_copy_value(&value.0)),
                ),
                contextdb_core::DirectedValue::Desc(value) => contextdb_core::DirectedValue::Desc(
                    contextdb_core::TotalOrdDesc(source_copy_value(&value.0)),
                ),
            });
        }
        for value in &copied {
            let value = match value {
                contextdb_core::DirectedValue::Asc(v) => &v.0,
                contextdb_core::DirectedValue::Desc(v) => &v.0,
            };
            if let Err(error) = source_check_value_capacity(value) {
                drop(copied);
                return Err(error);
            }
        }
        if copied.capacity() != copied.len() {
            drop(copied);
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        Ok(contextdb_tx::staged_source::AdmittedSourceKey::new(
            copied, charge,
        ))
    }
}
impl OwnedMemoryReservation {
    /// Host transfer preserves the original standard receiver allocation.
    pub(crate) fn into_source_invoice(
        &mut self,
    ) -> contextdb_core::Result<contextdb_tx::OriginalRowInvoice> {
        self.into_source_charge()
            .map(contextdb_tx::OriginalRowInvoice::from_charge)
    }
    fn into_source_charge(&mut self) -> contextdb_core::Result<contextdb_tx::SourceCharge> {
        let receiver = self.accountant().standard_arc().ok_or_else(|| {
            contextdb_core::Error::Other("native source transfer requires an opaque funder".into())
        })?;
        let lease = self._writer_use.as_ref().map_or(
            contextdb_tx::SourceLease {
                identity: 0,
                epoch: 0,
            },
            |writer| contextdb_tx::SourceLease {
                identity: writer.stable_identity,
                epoch: writer.epoch,
            },
        );
        let charge = contextdb_tx::SourceCharge::adopt(receiver, self.bytes, lease)?;
        self.bytes = 0;
        Ok(charge)
    }
}
impl contextdb_tx::OriginalCopyChargeSource for OwnedMemoryReservation {
    fn copy_original_row_for(
        &self,
        row: &contextdb_core::VersionedRow,
        funder: &contextdb_tx::SourceFunder,
    ) -> Option<contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceRow>> {
        Some(self.copy_retained_original_row_for(row, funder))
    }
    fn copy_original_vector_for(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &contextdb_tx::SourceFunder,
    ) -> Option<contextdb_core::Result<contextdb_tx::AdmittedSourceVector>> {
        Some(self.copy_retained_original_vector_parts_for(
            &entry.index.table,
            &entry.index.column,
            &entry.vector,
            entry.row_id,
            entry.created_tx,
            entry.deleted_tx,
            entry.lsn,
            funder,
        ))
    }
    fn copy_original_vector_input_for(
        &self,
        input: contextdb_tx::staged_source::BorrowedOriginalVectorCopyInput<'_>,
        funder: &contextdb_tx::SourceFunder,
    ) -> Option<contextdb_core::Result<contextdb_tx::AdmittedSourceVector>> {
        Some(self.copy_retained_original_vector_parts_for(
            input.table(),
            input.column(),
            input.vector(),
            input.row_id(),
            input.created_tx(),
            input.deleted_tx(),
            input.lsn(),
            funder,
        ))
    }
    fn original_source_origin(&self) -> Option<contextdb_tx::NativeSourceOrigin> {
        let lease = if let Some(writer) = &self._writer_use {
            if writer.accountant.as_ptr() != self.accountant().as_ptr()
                || writer.stable_identity == 0
                || writer.epoch == 0
            {
                return None;
            }
            contextdb_tx::SourceLease {
                identity: writer.stable_identity,
                epoch: writer.epoch,
            }
        } else {
            // This is the genuine original nonwriter, not a fabricated native owner.
            if self.accountant().is_engine() {
                return None;
            }
            contextdb_tx::SourceLease {
                identity: 0,
                epoch: 0,
            }
        };
        Some(contextdb_tx::NativeSourceOrigin::new(
            self.accountant().as_ptr() as usize,
            lease,
        ))
    }
    fn retained_control_bytes(&self) -> Option<usize> {
        <Self as contextdb_tx::StagedPostingCharge>::retained_control_bytes(self)
    }
    fn visit_retained_controls<'a>(
        &'a self,
        visit: &mut dyn FnMut(contextdb_tx::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        self.visit_retained_source_controls(visit)
    }
    fn reserve_original_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        if self.accountant().is_engine() {
            // This standard output is terminal-only. Private native holders
            // dispatch the opaque grant companion below, never this shell.
            <Self as contextdb_tx::StagedPostingCharge>::reserve_terminal_copy(self, bytes)
        } else {
            <Self as contextdb_tx::StagedPostingCharge>::reserve_copy(self, bytes)
        }
    }
    fn reserve_private_original_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<contextdb_tx::OriginalCopyGrant> {
        self.reserve_original_grant(bytes)
    }
}

fn source_copy_vector(entry: &contextdb_core::VectorEntry) -> contextdb_core::VectorEntry {
    contextdb_core::VectorEntry {
        index: contextdb_core::VectorIndexRef {
            table: entry.index.table.as_str().to_owned(),
            column: entry.index.column.as_str().to_owned(),
        },
        row_id: entry.row_id,
        vector: entry.vector.as_slice().to_vec(),
        created_tx: entry.created_tx,
        deleted_tx: entry.deleted_tx,
        lsn: entry.lsn,
    }
}
fn source_check_vector_capacity(entry: &contextdb_core::VectorEntry) -> contextdb_core::Result<()> {
    if entry.vector.capacity() != entry.vector.len()
        || entry.index.table.capacity() != entry.index.table.len()
        || entry.index.column.capacity() != entry.index.column.len()
    {
        return Err(contextdb_core::Error::Other(
            "source native capacity mismatch".into(),
        ));
    }
    Ok(())
}

// Native clone builders use the effective standard-library allocation shape,
// rather than the row's historical admission estimate. JSON objects are rebuilt
// in lexical order so every B-tree split is known before construction. The
// independent allocator gate must witness these layouts on its actual compiler.
struct SourceJsonLeaf {
    _parent: Option<std::ptr::NonNull<SourceJsonInternal>>,
    _parent_index: std::mem::MaybeUninit<u16>,
    _len: u16,
    _keys: [std::mem::MaybeUninit<String>; 11],
    _values: [std::mem::MaybeUninit<serde_json::Value>; 11],
}
#[repr(C)]
struct SourceJsonInternal {
    _data: SourceJsonLeaf,
    _edges: [std::mem::MaybeUninit<std::ptr::NonNull<SourceJsonLeaf>>; 12],
}
fn source_size_overflow() -> contextdb_core::Error {
    contextdb_core::Error::Other("source allocation size overflow".into())
}
fn source_add(a: usize, b: usize) -> contextdb_core::Result<usize> {
    a.checked_add(b).ok_or_else(source_size_overflow)
}
// Private preparation copies use the same native layout and exact copy kernels
// as source funding. A caller must admit the returned plan BEFORE this copy.
pub(crate) fn preparation_row_copy_bytes(
    row: &contextdb_core::VersionedRow,
) -> contextdb_core::Result<usize> {
    row.values.iter().try_fold(
        source_row_map_bytes(row.values.len())?,
        |bytes, (name, value)| {
            source_add(
                source_add(bytes, name.len())?,
                source_value_copy_bytes(value)?,
            )
        },
    )
}
pub(crate) fn preparation_copy_row(
    row: &contextdb_core::VersionedRow,
) -> contextdb_core::Result<contextdb_core::VersionedRow> {
    let mut values = HashMap::with_capacity(row.values.len());
    for (name, value) in &row.values {
        values.insert(name.as_str().to_owned(), source_copy_value(value));
    }
    let copied = contextdb_core::VersionedRow {
        row_id: row.row_id,
        values,
        created_tx: row.created_tx,
        deleted_tx: row.deleted_tx,
        lsn: row.lsn,
        created_at: row.created_at,
    };
    source_check_row_capacity(&copied)?;
    Ok(copied)
}
pub(crate) fn preparation_value_copy_bytes(
    value: &contextdb_core::Value,
) -> contextdb_core::Result<usize> {
    source_value_copy_bytes(value)
}
pub(crate) fn preparation_copy_value(
    value: &contextdb_core::Value,
) -> contextdb_core::Result<contextdb_core::Value> {
    let copied = source_copy_value(value);
    source_check_value_capacity(&copied)?;
    Ok(copied)
}
pub(crate) fn preparation_row_map_bytes(members: usize) -> contextdb_core::Result<usize> {
    source_row_map_bytes(members)
}

pub(crate) fn preparation_check_row_capacity(
    row: &contextdb_core::VersionedRow,
) -> contextdb_core::Result<()> {
    source_check_row_capacity(row)
}

fn source_array<T>(len: usize) -> contextdb_core::Result<usize> {
    Layout::array::<T>(len)
        .map(|layout| layout.size())
        .map_err(|_| source_size_overflow())
}
fn source_json_map_bytes(members: usize) -> contextdb_core::Result<usize> {
    if members == 0 {
        return Ok(0);
    }
    // alloc::collections::btree::node::splitpoint(11) promotes index six;
    // its rightmost successor has five entries after the pending insertion.
    let mut rightmost = [0u8; usize::BITS as usize];
    let mut height = 0usize;
    let mut leaf_nodes = 1usize;
    let mut internal_nodes = 0usize;
    for _ in 0..members {
        let mut level = 0usize;
        loop {
            if rightmost[level] < 11 {
                rightmost[level] += 1;
                break;
            }
            rightmost[level] = 5;
            if level == 0 {
                leaf_nodes = source_add(leaf_nodes, 1)?
            } else {
                internal_nodes = source_add(internal_nodes, 1)?
            }
            level += 1;
            if level > height {
                height = level;
                if height == rightmost.len() {
                    return Err(source_size_overflow());
                }
                rightmost[height] = 1;
                internal_nodes = source_add(internal_nodes, 1)?;
                break;
            }
        }
    }
    source_add(
        source_array::<SourceJsonLeaf>(leaf_nodes)?,
        source_array::<SourceJsonInternal>(internal_nodes)?,
    )
}
fn source_json_copy_bytes(value: &serde_json::Value) -> contextdb_core::Result<usize> {
    match value {
        serde_json::Value::String(text) => Ok(text.len()),
        serde_json::Value::Array(items) => items.iter().try_fold(
            source_array::<serde_json::Value>(items.len())?,
            |bytes, child| source_add(bytes, source_json_copy_bytes(child)?),
        ),
        serde_json::Value::Object(members) => members.iter().try_fold(
            source_json_map_bytes(members.len())?,
            |bytes, (name, child)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_json_copy_bytes(child)?,
                )
            },
        ),
        _ => Ok(0),
    }
}
fn source_value_copy_bytes(value: &contextdb_core::Value) -> contextdb_core::Result<usize> {
    match value {
        contextdb_core::Value::Text(text) => Ok(text.len()),
        contextdb_core::Value::Vector(values) => source_array::<f32>(values.len()),
        contextdb_core::Value::Json(json) => source_json_copy_bytes(json),
        _ => Ok(0),
    }
}
fn source_copy_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(text.as_str().to_owned()),
        serde_json::Value::Array(items) => {
            let mut copied = Vec::with_capacity(items.len());
            for child in items {
                copied.push(source_copy_json(child));
            }
            serde_json::Value::Array(copied)
        }
        serde_json::Value::Object(members) => {
            let mut copied = serde_json::Map::new();
            // The pinned serde_json Map is a lexical BTreeMap; these insertions
            // follow the exact split sequence priced above, with no sort buffer.
            for (name, child) in members {
                copied.insert(name.as_str().to_owned(), source_copy_json(child));
            }
            serde_json::Value::Object(copied)
        }
        _ => value.clone(),
    }
}
fn source_copy_value(value: &contextdb_core::Value) -> contextdb_core::Value {
    match value {
        contextdb_core::Value::Text(text) => contextdb_core::Value::Text(text.as_str().to_owned()),
        contextdb_core::Value::Vector(values) => {
            contextdb_core::Value::Vector(values.as_slice().to_vec())
        }
        contextdb_core::Value::Json(json) => contextdb_core::Value::Json(source_copy_json(json)),
        _ => value.clone(),
    }
}
fn source_row_map_bytes(members: usize) -> contextdb_core::Result<usize> {
    if members == 0 {
        return Ok(0);
    }
    // std's pinned hashbrown TableLayout: the row entry is larger than the
    // small-item minimum. Preserve the target's control-group width/alignment.
    let group = if cfg!(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64")
    )) {
        16usize
    } else if cfg!(any(
        target_pointer_width = "64",
        target_arch = "aarch64",
        target_arch = "wasm32"
    )) {
        8usize
    } else {
        4usize
    };
    let buckets = if members < 4 {
        4
    } else if members < 8 {
        8
    } else if members < 15 {
        16
    } else {
        members
            .checked_mul(8)
            .ok_or_else(source_size_overflow)?
            .checked_div(7)
            .unwrap()
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
    };
    let item = Layout::new::<(String, contextdb_core::Value)>();
    let alignment = item.align().max(group);
    let data = source_array::<(String, contextdb_core::Value)>(buckets)?;
    let control_offset = source_add(data, alignment - 1)? & !(alignment - 1);
    let size = source_add(source_add(control_offset, buckets)?, group)?;
    Layout::from_size_align(size, alignment)
        .map(|layout| layout.size())
        .map_err(|_| source_size_overflow())
}

fn source_check_value_capacity(value: &contextdb_core::Value) -> contextdb_core::Result<()> {
    let valid = match value {
        contextdb_core::Value::Text(text) => text.capacity() == text.len(),
        contextdb_core::Value::Vector(values) => values.capacity() == values.len(),
        contextdb_core::Value::Json(json) => return source_check_json_capacity(json),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(contextdb_core::Error::Other(
            "source native capacity mismatch".into(),
        ))
    }
}
fn source_check_json_capacity(json: &serde_json::Value) -> contextdb_core::Result<()> {
    match json {
        serde_json::Value::String(text) if text.capacity() != text.len() => Err(
            contextdb_core::Error::Other("source native capacity mismatch".into()),
        ),
        serde_json::Value::Array(items) => {
            if items.capacity() != items.len() {
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            for value in items {
                source_check_json_capacity(value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(members) => {
            for (name, value) in members {
                if name.capacity() != name.len() {
                    return Err(contextdb_core::Error::Other(
                        "source native capacity mismatch".into(),
                    ));
                }
                source_check_json_capacity(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
fn source_check_row_capacity(row: &contextdb_core::VersionedRow) -> contextdb_core::Result<()> {
    let members = row.values.len();
    let expected = if members == 0 {
        0
    } else if members < 4 {
        3
    } else if members < 8 {
        7
    } else if members < 15 {
        14
    } else {
        (members * 8 / 7).next_power_of_two() / 8 * 7
    };
    if row.values.capacity() != expected {
        return Err(contextdb_core::Error::Other(
            "source native capacity mismatch".into(),
        ));
    }
    for (name, value) in &row.values {
        if name.capacity() != name.len() {
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        source_check_value_capacity(value)?;
    }
    Ok(())
}

// Copy only private auxiliary bodies. All native arrays/keys/recursive values
// are priced under the actual receiver before their respective allocation.
fn source_copy_aux(
    input: contextdb_tx::staged_source::AuxBorrow<'_>,
    funder: contextdb_tx::SourceFunder,
) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedAuxBatch> {
    use contextdb_tx::staged_source::{
        AdmittedAuxBatch, AuxBodies, AuxBorrow, OriginalAuxInvoice, SourceCharge,
    };
    match input {
        AuxBorrow::RelationalDeletes(source) => {
            let buffers = source_add(
                source_array::<(
                    contextdb_core::TableName,
                    contextdb_core::RowId,
                    contextdb_core::TxId,
                )>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = Ok(body.0.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((body.0.as_str().to_owned(), body.1, body.2));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    source_check_aux_string(&body.0)
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::RelationalDeletes(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::Predicates(source) => {
            let buffers = source_add(
                source_array::<contextdb_tx::RelationalDeletePredicate>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = (|| {
                    body.predicates.iter().try_fold(
                        source_add(
                            body.table.len(),
                            source_array::<(String, contextdb_core::Value)>(body.predicates.len())?,
                        )?,
                        |bytes, (name, value)| {
                            source_add(
                                source_add(bytes, name.len())?,
                                source_value_copy_bytes(value)?,
                            )
                        },
                    )
                })();
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push({
                    let mut predicates = Vec::with_capacity(body.predicates.len());
                    for (name, value) in &body.predicates {
                        predicates.push((name.as_str().to_owned(), source_copy_value(value)));
                    }
                    contextdb_tx::RelationalDeletePredicate {
                        table: body.table.as_str().to_owned(),
                        predicates,
                    }
                });
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    {
                        source_check_aux_string(&body.table)?;
                        if body.predicates.capacity() != body.predicates.len() {
                            return Err(source_native_capacity_error());
                        }
                        for (name, value) in &body.predicates {
                            source_check_aux_string(name)?;
                            source_check_value_capacity(value)?;
                        }
                        Ok(())
                    }
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::Predicates(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::AdjInserts(source) => {
            let buffers = source_add(
                source_array::<contextdb_core::AdjEntry>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = (|| {
                    body.properties.iter().try_fold(
                        source_add(
                            body.edge_type.len(),
                            source_row_map_bytes(body.properties.len())?,
                        )?,
                        |bytes, (name, value)| {
                            source_add(
                                source_add(bytes, name.len())?,
                                source_value_copy_bytes(value)?,
                            )
                        },
                    )
                })();
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push({
                    let mut properties =
                        std::collections::HashMap::with_capacity(body.properties.len());
                    for (name, value) in &body.properties {
                        properties.insert(name.as_str().to_owned(), source_copy_value(value));
                    }
                    contextdb_core::AdjEntry {
                        source: body.source,
                        target: body.target,
                        edge_type: body.edge_type.as_str().to_owned(),
                        properties,
                        created_tx: body.created_tx,
                        deleted_tx: body.deleted_tx,
                        lsn: body.lsn,
                    }
                });
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    {
                        source_check_aux_string(&body.edge_type)?;
                        source_check_aux_map(&body.properties)?;
                        for (name, value) in &body.properties {
                            source_check_aux_string(name)?;
                            source_check_value_capacity(value)?;
                        }
                        Ok(())
                    }
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::AdjInserts(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::AdjDeletes(source) => {
            let buffers = source_add(
                source_array::<(
                    contextdb_core::NodeId,
                    contextdb_core::EdgeType,
                    contextdb_core::NodeId,
                    contextdb_core::TxId,
                )>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = Ok(body.1.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((body.0, body.1.as_str().to_owned(), body.2, body.3));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    source_check_aux_string(&body.1)
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::AdjDeletes(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::VectorDeletes(source) => {
            let buffers = source_add(
                source_array::<(
                    contextdb_core::VectorIndexRef,
                    contextdb_core::RowId,
                    contextdb_core::TxId,
                )>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> =
                    source_add(body.0.table.len(), body.0.column.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((
                    contextdb_core::VectorIndexRef {
                        table: body.0.table.as_str().to_owned(),
                        column: body.0.column.as_str().to_owned(),
                    },
                    body.1,
                    body.2,
                ));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    {
                        source_check_aux_string(&body.0.table)?;
                        source_check_aux_string(&body.0.column)
                    }
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::VectorDeletes(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::VectorMoves(source) => {
            let buffers = source_add(
                source_array::<(
                    contextdb_core::VectorIndexRef,
                    contextdb_core::RowId,
                    contextdb_core::RowId,
                    contextdb_core::TxId,
                )>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> =
                    source_add(body.0.table.len(), body.0.column.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((
                    contextdb_core::VectorIndexRef {
                        table: body.0.table.as_str().to_owned(),
                        column: body.0.column.as_str().to_owned(),
                    },
                    body.1,
                    body.2,
                    body.3,
                ));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    {
                        source_check_aux_string(&body.0.table)?;
                        source_check_aux_string(&body.0.column)
                    }
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::VectorMoves(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::Provenance(source) => {
            let buffers = source_add(
                source_array::<(
                    contextdb_core::TableName,
                    contextdb_core::RowId,
                    contextdb_core::Lsn,
                    u8,
                )>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = Ok(body.0.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((body.0.as_str().to_owned(), body.1, body.2, body.3));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    source_check_aux_string(&body.0)
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::Provenance(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::ConfigWrites(source) => {
            let buffers = source_add(
                source_array::<(String, Vec<u8>)>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = source_add(body.0.len(), body.1.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push((body.0.as_str().to_owned(), body.1.as_slice().to_vec()));
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    {
                        source_check_aux_string(&body.0)?;
                        if body.1.capacity() != body.1.len() {
                            return Err(source_native_capacity_error());
                        }
                        Ok(())
                    }
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::ConfigWrites(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::ConfigDeletes(source) => {
            let buffers = source_add(
                source_array::<String>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = Ok(body.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push(body.as_str().to_owned());
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    source_check_aux_string(body)
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::ConfigDeletes(bodies),
                invoices,
                credit,
            ))
        }
        AuxBorrow::ConfigMaxU64Keys(source) => {
            let buffers = source_add(
                source_array::<String>(source.len())?,
                source_array::<OriginalAuxInvoice>(source.len())?,
            )?;
            let credit = SourceCharge::admit_funded(funder.clone(), buffers)?;
            let mut invoices = Vec::with_capacity(source.len());
            let mut bodies = Vec::with_capacity(source.len());
            for body in source {
                let bytes: contextdb_core::Result<usize> = Ok(body.len());
                let charge = SourceCharge::admit_funded(funder.clone(), bytes?)?;
                invoices.push(OriginalAuxInvoice::from_charge(charge));
                bodies.push(body.as_str().to_owned());
            }
            let valid = bodies
                .iter()
                .try_for_each(|body| -> contextdb_core::Result<()> {
                    source_check_aux_string(body)
                });
            if let Err(error) = valid {
                drop(bodies);
                drop(invoices);
                return Err(error);
            }
            if bodies.capacity() != bodies.len() || invoices.capacity() != invoices.len() {
                drop(bodies);
                drop(invoices);
                return Err(contextdb_core::Error::Other(
                    "source native capacity mismatch".into(),
                ));
            }
            Ok(AdmittedAuxBatch::from_parts(
                AuxBodies::ConfigMaxU64Keys(bodies),
                invoices,
                credit,
            ))
        }
    }
}
// The same pinned hashbrown layout as the existing row builder, parameterized
// by the actual sidecar bucket type. No reported admission value is a credit.
fn source_native_map_bytes<K, V>(members: usize) -> contextdb_core::Result<usize> {
    if members == 0 {
        return Ok(0);
    }
    let group = if cfg!(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64")
    )) {
        16usize
    } else if cfg!(any(
        target_pointer_width = "64",
        target_arch = "aarch64",
        target_arch = "wasm32"
    )) {
        8usize
    } else {
        4usize
    };
    let buckets = if members < 4 {
        4
    } else if members < 8 {
        8
    } else if members < 15 {
        16
    } else {
        members
            .checked_mul(8)
            .ok_or_else(source_size_overflow)?
            .checked_div(7)
            .unwrap()
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
    };
    let alignment = Layout::new::<(K, V)>().align().max(group);
    let data = source_array::<(K, V)>(buckets)?;
    let offset = source_add(data, alignment - 1)? & !(alignment - 1);
    let size = source_add(source_add(offset, buckets)?, group)?;
    Layout::from_size_align(size, alignment)
        .map(|l| l.size())
        .map_err(|_| source_size_overflow())
}
fn source_sidecar_count<V>(
    map: &std::collections::HashMap<contextdb_core::RowId, V>,
    change: Option<(contextdb_core::RowId, Option<V>)>,
) -> contextdb_core::Result<usize> {
    match change {
        None => Ok(map.len()),
        Some((row, Some(_))) => map
            .len()
            .checked_add(usize::from(!map.contains_key(&row)))
            .ok_or_else(source_size_overflow),
        Some((row, None)) => map
            .len()
            .checked_sub(usize::from(map.contains_key(&row)))
            .ok_or_else(source_size_overflow),
    }
}
fn source_sidecar_price<V: Copy>(
    maps: &std::collections::HashMap<String, std::collections::HashMap<contextdb_core::RowId, V>>,
    change: Option<(&str, contextdb_core::RowId, Option<V>)>,
) -> contextdb_core::Result<usize> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for (table, map) in maps {
        let selected = change
            .filter(|(name, _, _)| *name == table.as_str())
            .map(|(_, row, value)| (row, value));
        let len = source_sidecar_count(map, selected)?;
        if len != 0 || selected.is_none() {
            count = source_add(count, 1)?;
            bytes = source_add(
                source_add(bytes, table.len())?,
                source_native_map_bytes::<contextdb_core::RowId, V>(len)?,
            )?;
        }
    }
    if let Some((table, _, Some(_))) = change
        && !maps.contains_key(table)
    {
        count = source_add(count, 1)?;
        bytes = source_add(
            source_add(bytes, table.len())?,
            source_native_map_bytes::<contextdb_core::RowId, V>(1)?,
        )?;
    }
    source_add(
        bytes,
        source_native_map_bytes::<String, std::collections::HashMap<contextdb_core::RowId, V>>(
            count,
        )?,
    )
}
fn source_sidecar_copy<V: Copy>(
    maps: &std::collections::HashMap<String, std::collections::HashMap<contextdb_core::RowId, V>>,
    change: Option<(&str, contextdb_core::RowId, Option<V>)>,
) -> std::collections::HashMap<String, std::collections::HashMap<contextdb_core::RowId, V>> {
    let count = maps
        .iter()
        .filter(|(table, map)| {
            change.is_none_or(|(name, _, _)| name != table.as_str())
                || source_sidecar_count(
                    map,
                    change
                        .filter(|(name, _, _)| *name == table.as_str())
                        .map(|(_, row, value)| (row, value)),
                )
                .expect("priced sidecar count")
                    != 0
        })
        .count()
        .checked_add(usize::from(change.is_some_and(|(table, _, value)| {
            value.is_some() && !maps.contains_key(table)
        })))
        .expect("priced sidecar table count");
    let mut copied = std::collections::HashMap::with_capacity(count);
    for (table, map) in maps {
        let selected = change
            .filter(|(name, _, _)| *name == table.as_str())
            .map(|(_, row, value)| (row, value));
        let len = source_sidecar_count(map, selected).expect("priced sidecar count");
        if len == 0 && selected.is_some() {
            continue;
        }
        let mut inner = std::collections::HashMap::with_capacity(len);
        for (row, value) in map {
            if selected.is_none_or(|(id, _)| id != *row) {
                inner.insert(*row, *value);
            }
        }
        if let Some((row, Some(value))) = selected {
            inner.insert(row, value);
        }
        copied.insert(table.as_str().to_owned(), inner);
    }
    if let Some((table, row, Some(value))) = change
        && !maps.contains_key(table)
    {
        let mut inner = std::collections::HashMap::with_capacity(1);
        inner.insert(row, value);
        copied.insert(table.to_owned(), inner);
    }
    copied
}
fn source_copy_sidecars(
    maps: contextdb_tx::staged_source::SidecarsBorrow<'_>,
    change: Option<(
        &str,
        contextdb_core::RowId,
        Option<(contextdb_core::Lsn, u8)>,
        Option<usize>,
    )>,
    funder: contextdb_tx::SourceFunder,
) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSidecars> {
    let lsns = change.map(|(table, row, source, _)| (table, row, source.map(|v| v.0)));
    let kinds = change.map(|(table, row, source, _)| (table, row, source.map(|v| v.1)));
    let admitted = change.map(|(table, row, _, admitted)| (table, row, admitted));
    let bytes = source_add(
        source_add(
            source_sidecar_price(maps.lsns, lsns)?,
            source_sidecar_price(maps.kinds, kinds)?,
        )?,
        source_sidecar_price(maps.admitted, admitted)?,
    )?;
    let credit = contextdb_tx::SourceCharge::admit_funded(funder, bytes)?;
    let lsns = source_sidecar_copy(maps.lsns, lsns);
    let kinds = source_sidecar_copy(maps.kinds, kinds);
    let admitted = source_sidecar_copy(maps.admitted, admitted);
    if let Err(error) = source_check_sidecars(&lsns)
        .and_then(|()| source_check_sidecars(&kinds))
        .and_then(|()| source_check_sidecars(&admitted))
    {
        drop(lsns);
        drop(kinds);
        drop(admitted);
        return Err(error);
    }
    Ok(contextdb_tx::staged_source::AdmittedSidecars::from_parts(
        lsns, kinds, admitted, credit,
    ))
}

fn source_native_capacity_error() -> contextdb_core::Error {
    contextdb_core::Error::Other("source native capacity mismatch".into())
}
fn source_check_aux_string(text: &String) -> contextdb_core::Result<()> {
    if text.capacity() == text.len() {
        Ok(())
    } else {
        Err(source_native_capacity_error())
    }
}
fn source_check_aux_map<K, V>(map: &std::collections::HashMap<K, V>) -> contextdb_core::Result<()> {
    let members = map.len();
    let expected = if members == 0 {
        0
    } else if members < 4 {
        3
    } else if members < 8 {
        7
    } else if members < 15 {
        14
    } else {
        members
            .checked_mul(8)
            .ok_or_else(source_size_overflow)?
            .checked_div(7)
            .unwrap()
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
            / 8
            * 7
    };
    if map.capacity() == expected {
        Ok(())
    } else {
        Err(source_native_capacity_error())
    }
}
fn source_check_sidecars<V>(
    maps: &std::collections::HashMap<String, std::collections::HashMap<contextdb_core::RowId, V>>,
) -> contextdb_core::Result<()> {
    source_check_aux_map(maps)?;
    for (name, map) in maps {
        source_check_aux_string(name)?;
        source_check_aux_map(map)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn general_checkpoint_native_rows() {
    general_checkpoint_native_fixture::run();
}

#[cfg(test)]
mod general_checkpoint_native_fixture {
    use super::*;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    use contextdb_core::{
        AdjEntry, DirectedValue, Error, Lsn, NodeId, Result, RowId, TableMeta, TotalOrdAsc, TxId,
        Value, VectorEntry, VectorIndexRef, VersionedRow,
    };
    use contextdb_tx::manager::{EngineGuard, EngineRead, EngineTransactions};
    use contextdb_tx::staged_source::{AuxBodies, CanonicalStatementRows, SourceFunder};
    use contextdb_tx::{EngineCapturedWriteSet, EngineFlags, WriteSet, WriteSetApplicator};
    use std::collections::{BTreeSet, HashMap};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    // Real transaction manager, source trees and providers. The applicator is
    // never invoked: these are staging/rollback proofs, not Database activation.
    struct StagingOnly;
    impl WriteSetApplicator for StagingOnly {
        fn apply(
            &self,
            _: &WriteSet,
            _: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        ) -> Result<()> {
            panic!("checkpoint proof must not publish to a durable store")
        }
        fn new_row_id(&self) -> RowId {
            RowId(91)
        }
    }
    type Transactions = EngineTransactions<StagingOnly>;

    #[derive(Debug, PartialEq)]
    struct Image {
        base: Option<(contextdb_core::SnapshotId, Lsn, usize)>,
        rows: Vec<(String, VersionedRow)>,
        relational_deletes: Vec<(String, RowId, TxId)>,
        predicates: Vec<contextdb_tx::write_set::RelationalDeletePredicate>,
        adj_inserts: Vec<AdjEntry>,
        adj_deletes: Vec<(NodeId, String, NodeId, TxId)>,
        vectors: Vec<VectorEntry>,
        vector_deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        vector_moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsns: HashMap<String, HashMap<RowId, Lsn>>,
        kinds: HashMap<String, HashMap<RowId, u8>>,
        admitted: HashMap<String, HashMap<RowId, usize>>,
        provenance: Vec<(String, RowId, Lsn, u8)>,
        config_writes: Vec<(String, Vec<u8>)>,
        config_deletes: Vec<String>,
        config_max: Vec<String>,
        flags: (Option<Lsn>, Option<TxId>, bool, Option<TxId>, bool),
        capacities: [usize; 12],
        sidecar_capacities: [(usize, usize); 3],
    }
    fn image(r: EngineRead<'_>) -> Image {
        Image {
            base: r.base_read.as_ref().map(|b| {
                (
                    b.snapshot,
                    b.lsn,
                    b.handle
                        .as_ref()
                        .map_or(0, |h| Arc::as_ptr(h) as *const () as usize),
                )
            }),
            rows: r.relational_inserts.clone(),
            relational_deletes: r.relational_deletes.clone(),
            predicates: r.relational_delete_predicates.clone(),
            adj_inserts: r.adj_inserts.clone(),
            adj_deletes: r.adj_deletes.clone(),
            vectors: r.vector_inserts.clone(),
            vector_deletes: r.vector_deletes.clone(),
            vector_moves: r.vector_moves.clone(),
            lsns: r.relational_insert_source_lsns.clone(),
            kinds: r.relational_insert_source_kinds.clone(),
            admitted: r.relational_insert_admitted_bytes.clone(),
            provenance: r.sync_source_provenance_marks.clone(),
            config_writes: r.config_writes.clone(),
            config_deletes: r.config_deletes.clone(),
            config_max: r.config_max_u64_keys.clone(),
            flags: (
                *r.commit_lsn,
                *r.finalized_tx,
                *r.requires_commit_lsn,
                *r.visibility_floor,
                *r.propagation_in_progress,
            ),
            capacities: [
                r.relational_inserts.capacity(),
                r.relational_deletes.capacity(),
                r.relational_delete_predicates.capacity(),
                r.adj_inserts.capacity(),
                r.adj_deletes.capacity(),
                r.vector_inserts.capacity(),
                r.vector_deletes.capacity(),
                r.vector_moves.capacity(),
                r.sync_source_provenance_marks.capacity(),
                r.config_writes.capacity(),
                r.config_deletes.capacity(),
                r.config_max_u64_keys.capacity(),
            ],
            sidecar_capacities: [
                (
                    r.relational_insert_source_lsns.capacity(),
                    r.relational_insert_source_lsns
                        .values()
                        .map(HashMap::capacity)
                        .sum(),
                ),
                (
                    r.relational_insert_source_kinds.capacity(),
                    r.relational_insert_source_kinds
                        .values()
                        .map(HashMap::capacity)
                        .sum(),
                ),
                (
                    r.relational_insert_admitted_bytes.capacity(),
                    r.relational_insert_admitted_bytes
                        .values()
                        .map(HashMap::capacity)
                        .sum(),
                ),
            ],
        }
    }
    fn snapshot(t: &Transactions, tx: TxId) -> Image {
        t.with_read(tx, image).unwrap()
    }
    fn row(tx: TxId, id: u64, value: u64) -> VersionedRow {
        VersionedRow {
            row_id: RowId(id),
            values: [
                (
                    "text".into(),
                    Value::Text(format!("body-{value}-{}", "p".repeat(83))),
                ),
                ("rank".into(), Value::Int64(value as i64)),
            ]
            .into(),
            created_tx: tx,
            deleted_tx: Some(TxId(811)),
            lsn: Lsn(43),
            created_at: Some(contextdb_core::Wallclock::now()),
        }
    }
    fn index() -> VectorIndexRef {
        VectorIndexRef {
            table: "table".into(),
            column: "embedding".into(),
        }
    }
    fn original_row(
        writer: &WriterBudget,
        funder: &SourceFunder,
        template: &VersionedRow,
    ) -> Result<(VersionedRow, contextdb_tx::OriginalRowInvoice)> {
        let bytes = template.values.iter().try_fold(
            source_row_map_bytes(template.values.len())?,
            |bytes, (name, value)| {
                source_add(
                    source_add(bytes, name.len())?,
                    source_value_copy_bytes(value)?,
                )
            },
        )?;
        let reservation = OwnedMemoryReservation::try_new_arc_for_original(
            &writer.writer_use,
            bytes,
            "relational",
            "statement_overlay",
            "checkpoint proof original row",
        )?;
        let mut original = Arc::into_inner(reservation).expect("one genuine original owner");
        original.finish_freed_enclosing_control();
        let invoice = original.into_source_row_invoice_for(funder)?;
        let mut values = HashMap::with_capacity(template.values.len());
        for (name, value) in &template.values {
            values.insert(name.as_str().to_owned(), source_copy_value(value));
        }
        let body = VersionedRow {
            row_id: template.row_id,
            values,
            created_tx: template.created_tx,
            deleted_tx: template.deleted_tx,
            lsn: template.lsn,
            created_at: template.created_at,
        };
        source_check_row_capacity(&body)?;
        Ok((body, invoice))
    }
    fn original_vector(
        writer: &WriterBudget,
        funder: &SourceFunder,
        template: &VectorEntry,
    ) -> Result<(VectorEntry, contextdb_tx::OriginalVectorInvoice)> {
        let bytes = source_add(
            source_add(template.index.table.len(), template.index.column.len())?,
            source_array::<f32>(template.vector.len())?,
        )?;
        let reservation = OwnedMemoryReservation::try_new_arc_for_original(
            &writer.writer_use,
            bytes,
            "vector",
            "statement_overlay",
            "checkpoint proof original vector",
        )?;
        let mut original = Arc::into_inner(reservation).expect("one genuine original owner");
        original.finish_freed_enclosing_control();
        let invoice = original.into_source_vector_invoice_for(funder)?;
        let body = source_copy_vector(template);
        source_check_vector_capacity(&body)?;
        Ok((body, invoice))
    }
    fn duplicate_checkpoint_id(id: &contextdb_tx::TxCheckpointId) -> contextdb_tx::TxCheckpointId {
        // SAFETY: this opaque handle has only Copy scalar fields and no Drop.
        // This private replay constructor is authorized solely for invalid-id
        // tests; every source handle came from actual checkpoint operations.
        unsafe { std::ptr::read(id) }
    }
    fn meta() -> TableMeta {
        TableMeta {
            indexes: vec![contextdb_core::IndexDecl {
                name: "by_rank".into(),
                columns: vec![("rank".into(), contextdb_core::SortDirection::Asc)],
                kind: contextdb_core::IndexKind::UserDeclared,
            }],
            ..TableMeta::default()
        }
    }
    fn stage(
        g: &mut EngineGuard<'_>,
        f: &SourceFunder,
        writer: &WriterBudget,
        tx: TxId,
        id: u64,
        value: u64,
    ) -> Result<()> {
        let (body, invoice) = original_row(writer, f, &row(tx, id, value))?;
        let schema = g.prepare_schema("table", &meta())?;
        let keys = [(
            "by_rank".into(),
            vec![DirectedValue::Asc(TotalOrdAsc(Value::Int64(value as i64)))],
        )];
        let p = g.prepare_eager_row("table", body, &keys, schema, invoice)?;
        g.publish(p);
        let p = g.prepare_sidecars(
            "table",
            RowId(id),
            Some((Lsn(value + 10), 3)),
            Some(value as usize + 19),
        )?;
        g.publish_tx(p);
        Ok(())
    }
    fn populate_aux(
        g: &mut EngineGuard<'_>,
        tx: TxId,
        value: u64,
        replace: bool,
        count: usize,
    ) -> Result<()> {
        let a = NodeId::from_u128(11);
        let b = NodeId::from_u128(13);
        let batches = vec![
            AuxBodies::RelationalDeletes(vec![
                (format!("delete-{value}"), RowId(value), tx);
                count
            ]),
            AuxBodies::Predicates(vec![
                contextdb_tx::write_set::RelationalDeletePredicate {
                    table: format!("predicate-{value}"),
                    predicates: vec![(
                        "text".into(),
                        Value::Text(format!("predicate-body-{value}"))
                    )]
                };
                count
            ]),
            AuxBodies::AdjInserts(vec![
                AdjEntry {
                    source: a,
                    target: b,
                    edge_type: format!("edge-{value}"),
                    properties: [("property".into(), Value::Text(format!("edge-body-{value}")))]
                        .into(),
                    created_tx: tx,
                    deleted_tx: Some(TxId(812)),
                    lsn: Lsn(value)
                };
                count
            ]),
            AuxBodies::AdjDeletes(vec![(a, format!("removed-edge-{value}"), b, tx); count]),
            AuxBodies::VectorDeletes(vec![(index(), RowId(value), tx); count]),
            AuxBodies::VectorMoves(vec![(index(), RowId(value), RowId(value + 1), tx); count]),
            AuxBodies::Provenance(vec![
                (
                    format!("provenance-{value}"),
                    RowId(value),
                    Lsn(value),
                    5
                );
                count
            ]),
            AuxBodies::ConfigWrites(vec![
                (format!("config-{value}"), vec![value as u8; 73]);
                count
            ]),
            AuxBodies::ConfigDeletes(vec![format!("config-delete-{value}"); count]),
            AuxBodies::ConfigMaxU64Keys(vec![format!("frontier-{value}"); count]),
        ];
        for batch in batches {
            let copied = g.copy_aux(batch.borrow())?;
            let p = g.prepare_aux_splice(0, if replace { &[0] } else { &[] }, copied)?;
            g.publish_tx(p);
        }
        Ok(())
    }
    fn populate(
        g: &mut EngineGuard<'_>,
        f: &SourceFunder,
        writer: &WriterBudget,
        tx: TxId,
        value: u64,
    ) -> Result<()> {
        stage(g, f, writer, tx, 1, value)?;
        stage(g, f, writer, tx, 2, value + 1)?;
        stage(g, f, writer, tx, 3, value + 2)?;
        let v = VectorEntry {
            index: index(),
            row_id: RowId(1),
            vector: vec![value as f32; 17],
            created_tx: tx,
            deleted_tx: Some(TxId(813)),
            lsn: Lsn(value),
        };
        let (v, invoice) = original_vector(writer, f, &v)?;
        let p = g.prepare_eager_vector(v, None, invoice)?;
        g.publish(p);
        populate_aux(g, tx, value, false, 1)?;
        let p = g.prepare_flags(EngineFlags {
            commit_lsn: Some(Lsn(value)),
            finalized_tx: Some(TxId(value + 50)),
            requires_commit_lsn: true,
            visibility_floor: Some(TxId(value + 70)),
            propagation_in_progress: true,
        })?;
        g.publish_tx(p);
        Ok(())
    }
    fn mutate(
        g: &mut EngineGuard<'_>,
        f: &SourceFunder,
        writer: &WriterBudget,
        tx: TxId,
    ) -> Result<()> {
        // Same-length displaced bodies in ten auxiliary lanes, then growth.
        populate_aux(g, tx, 211, true, 1)?;
        populate_aux(g, tx, 311, false, 4)?;
        let p = g.prepare_remove("table", RowId(2))?;
        g.publish(p);
        let selected = [("table".into(), RowId(1)), ("table".into(), RowId(3))].into();
        let p = g.prepare_remove_rows(&selected)?;
        g.publish(p);
        for id in 4..=8 {
            stage(g, f, writer, tx, id, 411 + id)?;
        }
        let p = g.prepare_remove_vector(&index(), RowId(1))?;
        g.publish(p);
        for id in 4..=5 {
            let template = VectorEntry {
                index: index(),
                row_id: RowId(id),
                vector: vec![id as f32; 19],
                created_tx: tx,
                deleted_tx: Some(TxId(814)),
                lsn: Lsn(419),
            };
            let (body, invoice) = original_vector(writer, f, &template)?;
            let p = g.prepare_eager_vector(body, None, invoice)?;
            g.publish(p);
        }
        let p = g.prepare_flags(EngineFlags {
            commit_lsn: Some(Lsn(511)),
            finalized_tx: Some(TxId(512)),
            requires_commit_lsn: false,
            visibility_floor: Some(TxId(513)),
            propagation_in_progress: false,
        })?;
        g.publish_tx(p);
        let p = g.prepare_identity(tx, TxId(611), Lsn(612))?;
        g.publish_identity(p);
        let p = g.prepare_canonicalize()?;
        g.publish(p);
        Ok(())
    }
    fn no_admission<T>(
        o: &AccountantHandle,
        c: &AccountantHandle,
        operation: impl FnOnce() -> T,
    ) -> T {
        let before = (
            o.admitted_bytes_for_test(),
            c.admitted_bytes_for_test(),
            o.retained_charge_bytes_for_test(),
            c.retained_charge_bytes_for_test(),
        );
        // Fixed allocator storage counts every request even if more than 32;
        // assertions and snapshot creation are outside the observed interval.
        let (result, _, allocations) = oracle::native_requests_for_test(operation);
        assert_eq!(
            allocations, 0,
            "inverse rollback/abort must request no allocation"
        );
        assert_eq!(
            (
                o.admitted_bytes_for_test(),
                c.admitted_bytes_for_test(),
                o.retained_charge_bytes_for_test(),
                c.retained_charge_bytes_for_test()
            ),
            before,
            "inverse rollback/abort must neither admit nor unconditionally retain"
        );
        result
    }
    fn assert_canonical(source: &CanonicalStatementRows, tx: TxId, value: u64) {
        assert_eq!(source.len(), 3);
        assert!(source.has_index_for("by_rank", &meta()));
        let mut changed_schema = meta();
        changed_schema.indexes[0].columns[0].1 = contextdb_core::SortDirection::Desc;
        assert!(!source.has_index_for("by_rank", &changed_schema));
        let mut after_key = None;
        for id in 1..=3 {
            after_key = source
                .with_next_ordered(
                    "by_rank",
                    false,
                    after_key.as_ref(),
                    &std::ops::Bound::Unbounded,
                    &std::ops::Bound::Unbounded,
                    0,
                    |next| {
                        let (key, found) = next.unwrap();
                        assert_eq!(found, RowId(id));
                        assert_eq!(
                            key.0,
                            vec![DirectedValue::Asc(TotalOrdAsc(Value::Int64(
                                (value + id - 1) as i64
                            )))]
                        );
                        Ok(Some(key.clone()))
                    },
                )
                .unwrap();
        }
        let mut after = None;
        for (sequence, id) in [(0, 1), (1, 2), (2, 3)] {
            assert_eq!(source.next_unordered(after), Some((sequence, RowId(id))));
            source
                .with_row(RowId(id), |r| {
                    let r = r.unwrap();
                    assert_eq!(r.created_tx, tx);
                    assert_eq!(r.lsn, Lsn(43));
                    assert_eq!(r.values["rank"], Value::Int64((value + id - 1) as i64));
                    assert_eq!(r.deleted_tx, Some(TxId(811)));
                    Ok(())
                })
                .unwrap();
            after = Some(sequence);
        }
        assert_eq!(source.next_unordered(after), None);
    }
    fn assert_eager(capture: &EngineCapturedWriteSet, expected: &Image) {
        capture.with_legacy(|ws| {
            let rows = ws.ordered_staged_rows.statement_rows("table").unwrap();
            assert_eq!(rows.len(), expected.rows.len());
            let mut after = None;
            for (_, row) in &expected.rows {
                assert_eq!(rows.row(row.row_id), Some(row));
                let (key, ordered) = rows.next_ordered("by_rank", false, after.as_ref()).unwrap();
                assert_eq!(ordered, row);
                after = Some(key.clone());
            }
            assert!(
                rows.next_ordered("by_rank", false, after.as_ref())
                    .is_none()
            );
            let v = ws
                .ordered_staged_rows
                .vector_capability(&index(), RowId(1))
                .unwrap();
            assert_eq!(v.vector(), expected.vectors[0].vector.as_slice());
        });
    }
    fn typed_refusal(error: Error) {
        match error {
            Error::MemoryBudgetExceeded {
                requested_bytes,
                available_bytes,
                ..
            } => {
                assert!(requested_bytes > available_bytes);
            }
            other => panic!("expected actual typed original/caller refusal, got {other:?}"),
        }
    }
    fn removal_admission_cuts(
        t: &Transactions,
        tx: TxId,
        owner: &AccountantHandle,
        expected: &Image,
        retained: &CanonicalStatementRows,
        eager: &EngineCapturedWriteSet,
    ) {
        for bulk in [false, true] {
            let prepare = |g: &mut EngineGuard<'_>| {
                if bulk {
                    g.prepare_remove_rows(
                        &[("table".into(), RowId(1)), ("table".into(), RowId(3))].into(),
                    )
                } else {
                    g.prepare_remove("table", RowId(2))
                }
            };
            let base = owner.usage().used;
            watch_budget_ledger_for_test();
            let prepared = t.with_engine(tx, prepare).unwrap();
            let ledger = take_budget_ledger_for_test();
            drop(prepared);
            assert_eq!(owner.usage().used, base);
            let first_c = ledger
                .iter()
                .position(|entry| entry.what == "relational/staged_source")
                .expect("real caller source admission after original grants");
            let original = &ledger[..first_c];
            assert!(!original.is_empty());
            assert!(original.iter().filter(|entry| entry.what != "release").all(
                |entry| entry.what == "vector_maintenance/reservation_control"
                    || entry.what == "relational/statement_overlay"
            ));
            let original_credit = original
                .iter()
                .filter(|entry| entry.what != "release")
                .map(|entry| entry.bytes)
                .sum::<usize>();
            assert!(original_credit > 0);
            // Limit at the observed O boundary, never at a fabricated balance:
            // every actual original reservation fits, first actual C refuses.
            owner.set_budget(Some(base + original_credit)).unwrap();
            watch_budget_ledger_for_test();
            let outcome = t.with_engine(tx, prepare);
            let cut = take_budget_ledger_for_test();
            owner.set_budget(None).unwrap();
            typed_refusal(
                outcome
                    .err()
                    .expect("caller refusal after genuine original grant"),
            );
            let admitted: Vec<_> = cut.iter().filter(|entry| entry.what != "release").collect();
            assert_eq!(
                admitted.iter().map(|entry| entry.bytes).sum::<usize>(),
                original_credit
            );
            assert!(
                admitted
                    .iter()
                    .all(|entry| entry.what != "relational/staged_source")
            );
            assert_eq!(owner.usage().used, base);
            assert_eq!(snapshot(t, tx), *expected);
            assert_canonical(retained, tx, 101);
            assert_eager(eager, expected);
            // Original refuses with no caller source admission at all.
            owner.set_budget(Some(base)).unwrap();
            watch_budget_ledger_for_test();
            let outcome = t.with_engine(tx, prepare);
            let cut = take_budget_ledger_for_test();
            owner.set_budget(None).unwrap();
            typed_refusal(outcome.err().expect("original refuses first"));
            assert!(cut.iter().all(|entry| entry.what == "release"));
            assert_eq!(owner.usage().used, base);
            assert_eq!(snapshot(t, tx), *expected);
        }
    }
    fn run_frames(
        owner: &AccountantHandle,
        writer: &WriterBudget,
        funder: &SourceFunder,
        caller_owner: &AccountantHandle,
        caller_funder: &SourceFunder,
    ) {
        let t = Arc::new(Transactions::new_funded(StagingOnly, funder.clone()));
        let tx = t.try_begin().unwrap();
        t.with_engine(tx, |g| populate(g, funder, writer, tx, 101))
            .unwrap();
        let expected = snapshot(&t, tx);
        assert!(expected.base.is_some());
        assert!(expected.capacities.iter().all(|capacity| *capacity > 0));
        assert!(
            expected
                .sidecar_capacities
                .iter()
                .all(|(outer, inner)| *outer > 0 && *inner > 0)
        );
        let reader = Arc::new(MemoryAccountant::no_limit());
        let retained = t.capture(tx, "table", reader.clone()).unwrap();
        let eager = t.with_engine(tx, |g| g.capture_private()).unwrap();
        assert_canonical(&retained, tx, 101);
        assert_eager(&eager, &expected);
        removal_admission_cuts(&t, tx, owner, &expected, &retained, &eager);

        // The retained rows were funded by O. Reborrow the same live guard with
        // a separately registered native C only through the test-seam bridge.
        // The bridge itself proves O's reserve-copy runs before C's refusal,
        // restoration preserves this exact body, single/bulk publication, and
        // the isolated checkpoint-generation refusal path.
        t.with_engine(tx, |g| {
            g.__test_distinct_native_checkpoint_rows(
                caller_funder,
                "table",
                [RowId(1), RowId(3)],
                &|| owner.usage().used,
                &|| caller_owner.usage().used,
                &|| owner.admitted_bytes_for_test(),
                &|| caller_owner.admitted_bytes_for_test(),
                &|limit| owner.set_budget(limit).unwrap(),
                &|limit| caller_owner.set_budget(limit).unwrap(),
            )
        })
        .unwrap();
        assert_eq!(snapshot(&t, tx), expected);
        assert_canonical(&retained, tx, 101);
        assert_eager(&eager, &expected);

        // Both inner scopes settle; the outer scope must still own all inverses.
        let outer = t.checkpoint(tx).unwrap();
        t.with_engine(tx, |g| {
            g.with_tx_checkpoint(|g| {
                mutate(g, funder, writer, tx)?;
                g.with_tx_checkpoint(|g| {
                    let p = g.prepare_general_adoption(|candidate| {
                        populate(candidate, funder, writer, tx, 701)
                    })?;
                    g.publish_general_adoption(p);
                    Ok(())
                })
            })
        })
        .unwrap();
        assert_ne!(snapshot(&t, tx), expected);
        assert_canonical(&retained, tx, 101);
        assert_eager(&eager, &expected);
        no_admission(owner, owner, || outer.restore());
        assert_eq!(snapshot(&t, tx), expected);
        let current_eager = t.with_engine(tx, |g| g.capture_private()).unwrap();
        assert_eager(&current_eager, &expected);
        drop(current_eager);
        let current = t.capture(tx, "table", reader.clone()).unwrap();
        assert_canonical(&current, tx, 101);
        drop(current);

        // Err and panic restore after every mutation has completed. Allocation
        // assertions observe only restore/abort, never the user's panic machinery.
        let outer = t.checkpoint(tx).unwrap();
        let inner = t.checkpoint(tx).unwrap();
        t.with_engine(tx, |g| mutate(g, funder, writer, tx))
            .unwrap();
        no_admission(owner, owner, || drop(inner));
        assert_eq!(snapshot(&t, tx), expected);
        no_admission(owner, owner, || outer.settle());
        let refusal = Error::Other("inner refusal".into());
        let admissions = owner.admitted_bytes_for_test();
        let admission_at_abort = std::cell::Cell::new(admissions);
        let outcome: Result<()> = t.with_engine(tx, |g| {
            g.with_tx_checkpoint(|g| {
                mutate(g, funder, writer, tx)?;
                admission_at_abort.set(owner.admitted_bytes_for_test());
                oracle::arm_inverse_window();
                Err(refusal)
            })
        });
        let allocations = oracle::disarm_inverse_window();
        assert!(outcome.is_err());
        assert_eq!(allocations, 0);
        assert_eq!(owner.admitted_bytes_for_test(), admission_at_abort.get());
        assert_eq!(snapshot(&t, tx), expected);
        let panic_payload: Box<dyn std::any::Any + Send> = Box::new("inner callback unwind");
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                t.with_engine(tx, |g| {
                    g.with_tx_checkpoint::<()>(|g| {
                        mutate(g, funder, writer, tx)?;
                        admission_at_abort.set(owner.admitted_bytes_for_test());
                        oracle::arm_inverse_window();
                        std::panic::resume_unwind(panic_payload)
                    })
                })
                .unwrap();
            }))
            .is_err()
        );
        let allocations = oracle::disarm_inverse_window();
        assert_eq!(allocations, 0);
        assert_eq!(owner.admitted_bytes_for_test(), admission_at_abort.get());
        assert_eq!(snapshot(&t, tx), expected);

        // Retired nonce and non-LIFO attempts use real handles first, then the
        // authorized private replay constructor, never invented field layouts.
        t.with_engine(tx, |g| {
            let prepared = g.prepare_checkpoint()?;
            let outer = g.begin_checkpoint(prepared);
            let stale_outer = duplicate_checkpoint_id(&outer);
            let prepared = g.prepare_checkpoint()?;
            let inner = g.begin_checkpoint(prepared);
            assert!(catch_unwind(AssertUnwindSafe(|| g.restore_checkpoint(stale_outer))).is_err());
            let retired_inner = duplicate_checkpoint_id(&inner);
            g.settle_checkpoint(inner);
            let prepared = g.prepare_checkpoint()?;
            let new_inner = g.begin_checkpoint(prepared);
            assert!(
                catch_unwind(AssertUnwindSafe(|| g.restore_checkpoint(retired_inner))).is_err()
            );
            no_admission(owner, owner, || g.restore_checkpoint(new_inner));
            no_admission(owner, owner, || g.restore_checkpoint(outer));
            Ok(())
        })
        .unwrap();
        assert_eq!(snapshot(&t, tx), expected);

        // Prepared token stale in the same guard after a real rollback, avoiding
        // an accidental proof only of preparation-scope mismatch.
        t.with_engine(tx, |g| {
            let p = g.prepare_flags(EngineFlags {
                commit_lsn: None,
                finalized_tx: None,
                requires_commit_lsn: false,
                visibility_floor: None,
                propagation_in_progress: false,
            })?;
            let checkpoint = g.prepare_checkpoint()?;
            let id = g.begin_checkpoint(checkpoint);
            no_admission(owner, owner, || g.restore_checkpoint(id));
            assert!(catch_unwind(AssertUnwindSafe(|| g.publish_tx(p))).is_err());
            Ok(())
        })
        .unwrap();
        assert_eq!(snapshot(&t, tx), expected);

        // Refused checkpoint, adoption, single and real lower bulk preparation
        // preserve all installed ordinary values/capacities and retained views.
        for operation in 0..4 {
            let held = owner.usage().used;
            owner.set_budget(Some(held)).unwrap();
            let outcome = t.with_engine(tx, |g| match operation {
                0 => g.prepare_checkpoint().map(drop),
                1 => g
                    .prepare_general_adoption(|candidate| {
                        populate(candidate, funder, writer, tx, 901)
                    })
                    .map(drop),
                2 => g.prepare_remove("table", RowId(2)).map(drop),
                _ => g
                    .prepare_remove_rows(
                        &[("table".into(), RowId(1)), ("table".into(), RowId(3))].into(),
                    )
                    .map(drop),
            });
            owner.set_budget(None).unwrap();
            typed_refusal(outcome.unwrap_err());
            assert_eq!(owner.usage().used, held);
            assert_eq!(snapshot(&t, tx), expected);
            assert_canonical(&retained, tx, 101);
            assert_eager(&eager, &expected);
        }

        // Publication coherence at each actual single/bulk removal boundary.
        let frame = t.checkpoint(tx).unwrap();
        t.with_engine(tx, |g| {
            let p = g.prepare_remove("table", RowId(2))?;
            g.publish(p);
            Ok(())
        })
        .unwrap();
        let single = snapshot(&t, tx);
        assert_eq!(
            single
                .rows
                .iter()
                .map(|(_, r)| r.row_id)
                .collect::<Vec<_>>(),
            vec![RowId(1), RowId(3)]
        );
        let single_source = t.capture(tx, "table", reader.clone()).unwrap();
        assert_eq!(single_source.len(), 2);
        assert!(!single_source.contains(&RowId(2)));
        for maps_row in [
            single.lsns["table"].contains_key(&RowId(2)),
            single.kinds["table"].contains_key(&RowId(2)),
            single.admitted["table"].contains_key(&RowId(2)),
        ] {
            assert!(!maps_row);
        }
        t.with_engine(tx, |g| {
            let selected: BTreeSet<_> =
                [("table".into(), RowId(1)), ("table".into(), RowId(3))].into();
            let p = g.prepare_remove_rows(&selected)?;
            g.publish(p);
            Ok(())
        })
        .unwrap();
        let bulk = snapshot(&t, tx);
        assert!(bulk.rows.is_empty());
        assert!(bulk.lsns.is_empty() && bulk.kinds.is_empty() && bulk.admitted.is_empty());
        let bulk_source = t.capture(tx, "table", reader.clone()).unwrap();
        assert!(bulk_source.is_empty());
        let bulk_eager = t.with_engine(tx, |g| g.capture_private()).unwrap();
        bulk_eager.with_legacy(|ws| {
            assert!(
                ws.ordered_staged_rows
                    .statement_rows("table")
                    .is_none_or(|rows| rows.is_empty())
            )
        });
        assert_eq!(single_source.len(), 2);
        assert_canonical(&retained, tx, 101);
        no_admission(owner, owner, || frame.restore());
        assert_eq!(snapshot(&t, tx), expected);
        drop(bulk_eager);
        drop(bulk_source);
        drop(single_source);

        // Real cross-transaction handles must refuse before touching either host.
        let other = t.try_begin().unwrap();
        let id = t
            .with_engine(tx, |g| {
                let p = g.prepare_checkpoint()?;
                Ok(g.begin_checkpoint(p))
            })
            .unwrap();
        let valid_id = duplicate_checkpoint_id(&id);
        let other_frame = t.checkpoint(other).unwrap();
        let other_image = snapshot(&t, other);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                t.with_engine(other, |g| {
                    g.restore_checkpoint(id);
                    Ok(())
                })
                .unwrap();
            }))
            .is_err()
        );
        assert_eq!(snapshot(&t, tx), expected);
        assert_eq!(snapshot(&t, other), other_image);
        other_frame.restore();
        t.with_engine(tx, |g| {
            no_admission(owner, owner, || g.restore_checkpoint(valid_id));
            Ok(())
        })
        .unwrap();
        assert_eq!(snapshot(&t, tx), expected);
        drop(t.rollback_owned(tx).unwrap());
        drop(t.rollback_owned(other).unwrap());
        drop(eager);
        drop(retained);
        drop(t);
        assert_eq!(reader.usage().used, 0);
    }
    fn physical_originals(owner: &AccountantHandle, writer: &WriterBudget, funder: &SourceFunder) {
        let counter = owner.used.as_ptr() as usize;
        let template = row(TxId(17), 19, 23);
        let before = owner.usage().used;
        let (body, requests, count) =
            oracle::source_requests_for_test(counter, || funder.copy_row(&template).unwrap());
        assert!(count > 1 && count <= 32);
        let held = owner.usage().used;
        assert_eq!(
            held - before,
            requests[..count].iter().map(|r| r.1).sum::<usize>()
        );
        assert!(
            oracle::source_request_credit_for_test()[..count]
                .iter()
                .all(|credit| *credit == held)
        );
        oracle::watch_source_free_for_test(&requests[..count], counter);
        drop(body);
        let frees = oracle::source_free_for_test(count);
        // Disarm live raw addresses before any assertion or accountant cleanup.
        oracle::disarm_source_free_watch();
        assert!(frees[..count].iter().all(|free| *free == (held, held, 1)));
        assert_eq!(owner.usage().used, before);

        let payload: Box<dyn std::any::Any + Send> = Box::new("positive native cleanup unwind");
        let before = owner.usage().used;
        let (body, requests, count) =
            oracle::source_requests_for_test(counter, || funder.copy_row(&template).unwrap());
        assert!(count > 1 && count <= 32);
        let held = owner.usage().used;
        oracle::watch_source_free_for_test(&requests[..count], counter);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _original_body = body;
            std::panic::resume_unwind(payload)
        }));
        let frees = oracle::source_free_for_test(count);
        oracle::disarm_source_free_watch();
        assert!(outcome.is_err());
        assert!(frees[..count].iter().all(|free| *free == (held, held, 1)));
        assert_eq!(owner.usage().used, before);
        macro_rules! physical_copy {
            ($operation:expr) => {{
                let before = owner.usage().used;
                let (copy, requests, count) =
                    oracle::source_requests_for_test(counter, || $operation.unwrap());
                assert!(count > 1 && count <= 32);
                let held = owner.usage().used;
                assert_eq!(
                    held - before,
                    requests[..count].iter().map(|r| r.1).sum::<usize>()
                );
                assert!(
                    oracle::source_request_credit_for_test()[..count]
                        .iter()
                        .all(|credit| *credit == held)
                );
                oracle::watch_source_free_for_test(&requests[..count], counter);
                drop(copy);
                let frees = oracle::source_free_for_test(count);
                oracle::disarm_source_free_watch();
                assert!(frees[..count].iter().all(|free| *free == (held, held, 1)));
                assert_eq!(owner.usage().used, before);
            }};
        }
        let key = vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(
            "positive-key".repeat(7),
        )))];
        physical_copy!(funder.copy_key(&key));
        let vector = VectorEntry {
            index: index(),
            row_id: RowId(19),
            vector: vec![0.5; 17],
            created_tx: TxId(17),
            deleted_tx: None,
            lsn: Lsn(23),
        };
        physical_copy!(funder.copy_vector(&vector));

        let reservation = OwnedMemoryReservation::try_new_arc_for_original(
            &writer.writer_use,
            17,
            "relational",
            "statement_overlay",
            "actual original copy proof",
        )
        .unwrap();
        let mut reservation = Arc::into_inner(reservation).unwrap();
        reservation.finish_freed_enclosing_control();
        let before = owner.usage().used;
        let (grant, requests, count) = oracle::source_requests_for_test(counter, || {
            <OwnedMemoryReservation as contextdb_tx::OriginalCopyChargeSource>::reserve_private_original_copy(&reservation, 148).unwrap()
        });
        assert_eq!(count, 1);
        assert_eq!(grant.retained_bytes(), 148);
        let held = owner.usage().used;
        assert_eq!(
            held - before,
            148 + writer_adapter_type_layout::<OwnedMemoryReservation>().size()
        );
        oracle::watch_source_free_for_test(&requests[..count], counter);
        let retained = grant.clone();
        drop(grant);
        assert_eq!(oracle::source_free_for_test(1)[0].2, 0);
        drop(retained);
        let frees = oracle::source_free_for_test(count);
        oracle::disarm_source_free_watch();
        assert_eq!(frees[0], (held, held, 1));
        assert_eq!(owner.usage().used, before);
        drop(reservation);
    }
    pub(super) fn run() {
        let aggregate = process_aggregate_snapshot().0;
        let owner = AccountantHandle::engine(None).unwrap();
        let writer = WriterBudget::from_handle(owner.clone()).unwrap();
        let caller_owner = AccountantHandle::engine(None).unwrap();
        let caller_writer = WriterBudget::from_handle(caller_owner.clone()).unwrap();
        let counter = owner.used.as_ptr() as usize;
        let (funder, provider_requests, provider_count) =
            oracle::source_requests_for_test(counter, || writer.prepare_source_funder().unwrap());
        assert_eq!(provider_count, 1);
        assert_eq!(
            (provider_requests[0].1, provider_requests[0].2),
            (
                writer_adapter_type_layout::<NativeSourceProvider>().size(),
                writer_adapter_type_layout::<NativeSourceProvider>().align()
            )
        );
        let epoch = owner.writer_registration_observation_for_test().2;
        assert_ne!(epoch, 0);
        let caller_funder = caller_writer.prepare_source_funder().unwrap();
        physical_originals(&owner, &writer, &funder);
        run_frames(&owner, &writer, &funder, &caller_owner, &caller_funder);
        assert_eq!(owner.writer_registration_observation_for_test().2, epoch);
        drop(writer);
        assert_eq!(owner.writer_registration_observation_for_test().2, epoch);
        let held = owner.usage().used;
        oracle::watch_source_free_for_test(&provider_requests[..provider_count], counter);
        drop(funder);
        let frees = oracle::source_free_for_test(provider_count);
        oracle::disarm_source_free_watch();
        assert_eq!(
            frees[0],
            (held, held, 1),
            "actual original provider remains funded through its physical free"
        );
        assert_eq!(owner.writer_registration_observation_for_test().2, 0);
        assert_eq!(owner.usage().used, owner.backing_bytes());
        drop(caller_funder);
        drop(caller_writer);
        assert_eq!(caller_owner.writer_registration_observation_for_test().2, 0);
        assert_eq!(caller_owner.usage().used, caller_owner.backing_bytes());
        drop(caller_owner);
        drop(owner);
        assert_eq!(process_aggregate_snapshot().0, aggregate);
    }
}

pub(crate) fn preparation_copy_values(
    values: &HashMap<String, contextdb_core::Value>,
) -> contextdb_core::Result<HashMap<String, contextdb_core::Value>> {
    let mut copied = HashMap::with_capacity(values.len());
    for (name, value) in values {
        copied.insert(name.as_str().to_owned(), source_copy_value(value));
    }
    let members = copied.len();
    let expected = if members == 0 {
        0
    } else if members < 4 {
        3
    } else if members < 8 {
        7
    } else if members < 15 {
        14
    } else {
        members
            .checked_mul(8)
            .ok_or_else(source_size_overflow)?
            .checked_div(7)
            .ok_or_else(source_size_overflow)?
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
            / 8
            * 7
    };
    if copied.capacity() != expected {
        return Err(contextdb_core::Error::Other(
            "source native capacity mismatch".into(),
        ));
    }
    for (name, value) in &copied {
        if name.capacity() != name.len() {
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        source_check_value_capacity(value)?;
    }
    Ok(copied)
}

impl OwnedMemoryReservation {
    fn copy_retained_original_row_for(
        &self,
        row: &contextdb_core::VersionedRow,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::staged_source::AdmittedSourceRow> {
        if self._writer_use.is_none() {
            return self.copy_retained_original_nonwriter_row_for(row, funder);
        }
        let writer = self._writer_use.as_ref().ok_or_else(|| {
            contextdb_core::Error::Other(
                "native source transfer requires its held writer use".into(),
            )
        })?;
        let lease = contextdb_tx::SourceLease {
            identity: writer.stable_identity,
            epoch: writer.epoch,
        };
        if !self.accountant().is_engine()
            || writer.accountant.as_ptr() != self.accountant().as_ptr()
            || lease.identity == 0
            || lease.epoch == 0
            || !funder.matches_native_origin(self.accountant().as_ptr() as usize, lease)
        {
            return Err(contextdb_core::Error::Other(
                "native source transfer origin mismatch".into(),
            ));
        }
        let bytes = contextdb_tx::OriginalRowInvoice::original_copy_allocation_bytes(
            preparation_row_copy_bytes(row)?,
        )?;
        // The retained O admits both payload and invoice header before copying.
        // The current producer is only the validated recipient of that debit.
        let allocation = Self::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original = Arc::into_inner(allocation).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let origin =
            contextdb_tx::NativeSourceOrigin::new(self.accountant().as_ptr() as usize, lease);
        // SAFETY: the genuine O kernel holds the full measured allocation; the
        // invoice's control is included, and success disarms its sole debit.
        let invoice = unsafe {
            contextdb_tx::OriginalRowInvoice::from_original_copy_allocation(
                funder,
                original.bytes,
                origin,
                lease,
            )?
        };
        original.bytes = 0;
        // Refusal/unwind frees an incomplete body inside this kernel before
        // the invoice on this stack can refund the original allocation.
        let copied = preparation_copy_row(row)?;
        Ok(contextdb_tx::staged_source::AdmittedSourceRow::new(
            copied, invoice,
        ))
    }
}

impl OwnedMemoryReservation {
    #[allow(clippy::too_many_arguments)]
    fn copy_retained_original_vector_parts_for(
        &self,
        table: &str,
        column: &str,
        vector: &[f32],
        row_id: contextdb_core::RowId,
        created_tx: contextdb_core::TxId,
        deleted_tx: Option<contextdb_core::TxId>,
        lsn: contextdb_core::Lsn,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::AdmittedSourceVector> {
        if self._writer_use.is_none() {
            return self.copy_retained_original_nonwriter_vector_parts_for(
                table, column, vector, row_id, created_tx, deleted_tx, lsn, funder,
            );
        }
        let writer = self._writer_use.as_ref().ok_or_else(|| {
            contextdb_core::Error::Other(
                "native source transfer requires its held writer use".into(),
            )
        })?;
        let lease = contextdb_tx::SourceLease {
            identity: writer.stable_identity,
            epoch: writer.epoch,
        };
        if !self.accountant().is_engine()
            || writer.accountant.as_ptr() != self.accountant().as_ptr()
            || lease.identity == 0
            || lease.epoch == 0
            || !funder.matches_native_origin(self.accountant().as_ptr() as usize, lease)
        {
            return Err(contextdb_core::Error::Other(
                "native source transfer origin mismatch".into(),
            ));
        }
        let payload = source_add(
            source_add(table.len(), column.len())?,
            source_array::<f32>(vector.len())?,
        )?;
        let bytes = contextdb_tx::OriginalVectorInvoice::original_copy_allocation_bytes(payload)?;
        let allocation = Self::try_new_arc_for_original(
            writer,
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original = Arc::into_inner(allocation).expect("private copy grant has one owner");
        original.finish_freed_enclosing_control();
        let origin =
            contextdb_tx::NativeSourceOrigin::new(self.accountant().as_ptr() as usize, lease);
        // SAFETY: O holds the complete measured allocation; success transfers
        // its sole debit into the invoice and the stack reservation is disarmed.
        let invoice = unsafe {
            contextdb_tx::OriginalVectorInvoice::from_original_copy_allocation(
                funder,
                original.bytes,
                origin,
                lease,
            )?
        };
        original.bytes = 0;
        // The partially constructed entry is dropped before the invoice during
        // refusal or unwind. No unadmitted owned input precedes this reservation.
        let copied = contextdb_core::VectorEntry {
            index: contextdb_core::VectorIndexRef {
                table: table.to_owned(),
                column: column.to_owned(),
            },
            row_id,
            vector: vector.to_vec(),
            created_tx,
            deleted_tx,
            lsn,
        };
        if copied.index.table.capacity() != table.len()
            || copied.index.column.capacity() != column.len()
            || copied.vector.capacity() != vector.len()
        {
            drop(copied);
            return Err(contextdb_core::Error::Other(
                "source native capacity mismatch".into(),
            ));
        }
        Ok(contextdb_tx::AdmittedSourceVector::new(copied, invoice))
    }
}

// Count borrowed debug projections without building a String. Each field,
// element and escaped payload contributes to the conservative clone envelope.
pub(crate) fn preparation_debug_len<T: std::fmt::Debug + ?Sized>(
    value: &T,
) -> contextdb_core::Result<usize> {
    struct Count {
        bytes: usize,
    }
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.bytes = self.bytes.checked_add(text.len()).ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    use std::fmt::Write;
    let mut count = Count { bytes: 0 };
    write!(&mut count, "{value:?}").map_err(|_| source_size_overflow())?;
    Ok(count.bytes)
}

pub(crate) fn preparation_debug_native_bound<T: std::fmt::Debug + ?Sized>(
    value: &T,
) -> contextdb_core::Result<usize> {
    // One printed element includes at least one byte; the largest constituent
    // slot accounts for nested AST/metadata containers and allocation overlap.
    let slot = std::mem::size_of::<contextdb_parser::ast::Expr>()
        .max(std::mem::size_of::<contextdb_core::TableMeta>())
        .max(std::mem::size_of::<contextdb_core::Value>());
    preparation_debug_len(value)?
        .checked_mul(slot)
        .and_then(|n| n.checked_mul(2))
        .ok_or_else(source_size_overflow)
}

pub(crate) fn preparation_values_copy_bytes(
    values: &HashMap<String, contextdb_core::Value>,
) -> contextdb_core::Result<usize> {
    values.iter().try_fold(
        source_row_map_bytes(values.len())?,
        |bytes, (name, value)| {
            source_add(
                source_add(bytes, name.len())?,
                source_value_copy_bytes(value)?,
            )
        },
    )
}

pub(crate) fn preparation_assignment_sizes(
    expr: &contextdb_parser::ast::Expr,
    values: &HashMap<String, contextdb_core::Value>,
    params: &HashMap<String, contextdb_core::Value>,
) -> contextdb_core::Result<(usize, usize, usize)> {
    use contextdb_parser::ast::{Expr, Literal};
    let slot = std::mem::size_of::<Expr>();
    match expr {
        Expr::Literal(literal) => {
            let bytes = match literal {
                Literal::Text(text) => text.len(),
                Literal::Vector(vector) => source_array::<f32>(vector.len())?,
                _ => 0,
            };
            Ok((bytes, bytes, bytes))
        }
        Expr::Parameter(name) => {
            let bytes = params.get(name).map_or(Ok(0), source_value_copy_bytes)?;
            Ok((name.len(), bytes, bytes))
        }
        Expr::Column(column) => {
            let plan = source_add(
                column.column.len(),
                column.table.as_ref().map_or(0, String::len),
            )?;
            let bytes = values
                .get(&column.column)
                .map_or(Ok(0), source_value_copy_bytes)?;
            Ok((plan, bytes, bytes))
        }
        Expr::BinaryOp { left, right, .. } => {
            let left = preparation_assignment_sizes(left, values, params)?;
            let right = preparation_assignment_sizes(right, values, params)?;
            Ok((
                source_add(source_add(left.0, right.0)?, source_array::<Expr>(2)?)?,
                source_add(left.1, right.1)?,
                0,
            ))
        }
        Expr::UnaryOp { operand, .. } => {
            let child = preparation_assignment_sizes(operand, values, params)?;
            Ok((source_add(child.0, slot)?, child.1, 0))
        }
        Expr::FunctionCall { name, args } => {
            let mut plan = source_add(name.len(), source_array::<Expr>(args.len())?)?;
            let capacity = if args.is_empty() {
                0
            } else {
                args.len()
                    .max(4)
                    .checked_next_power_of_two()
                    .ok_or_else(source_size_overflow)?
            };
            let mut working = source_add(
                name.len(),
                source_array::<contextdb_core::Value>(
                    capacity.checked_mul(2).ok_or_else(source_size_overflow)?,
                )?,
            )?;
            let mut output = 0;
            for argument in args {
                let child = preparation_assignment_sizes(argument, values, params)?;
                plan = source_add(plan, child.0)?;
                working = source_add(working, child.1)?;
                output = output.max(child.2);
            }
            // COALESCE clones at most one evaluated argument. Arithmetic and
            // NOW produce scalar values and fit this same upper envelope.
            Ok((plan, source_add(working, output)?, output))
        }
        _ => {
            // Preserve the original evaluator's unsupported-expression error;
            // count its copied AST before constructing the original plan.
            Ok((
                preparation_debug_len(expr)?
                    .checked_mul(slot)
                    .and_then(|n| n.checked_mul(2))
                    .ok_or_else(source_size_overflow)?,
                0,
                0,
            ))
        }
    }
}

impl OwnedMemoryReservation {
    fn prepare_original_nonwriter_copy(
        &self,
        bytes: usize,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<(
        Self,
        contextdb_tx::NativeSourceOrigin,
        contextdb_tx::SourceLease,
    )> {
        if self._writer_use.is_some() || self.accountant().is_engine() {
            return Err(contextdb_core::Error::Other(
                "original nonwriter copy origin mismatch".into(),
            ));
        }
        let lease = contextdb_tx::SourceLease {
            identity: 0,
            epoch: 0,
        };
        let origin =
            contextdb_tx::NativeSourceOrigin::new(self.accountant().as_ptr() as usize, lease);
        if !funder.accepts_retained_original_copy(origin, lease) {
            return Err(contextdb_core::Error::Other(
                "original nonwriter copy recipient mismatch".into(),
            ));
        }
        // Use this exact untagged original, even if a writer was registered
        // later. No WriterUse lookup or current provider supplies its debit.
        let allocation = Self::try_new_arc_for_nonwriter(
            self.accountant(),
            bytes,
            "relational",
            "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?;
        let mut original =
            Arc::into_inner(allocation).expect("private original copy has one owner");
        original.finish_freed_enclosing_control();
        Ok((original, origin, lease))
    }

    fn copy_retained_original_nonwriter_row_for(
        &self,
        row: &contextdb_core::VersionedRow,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::AdmittedSourceRow> {
        let bytes = contextdb_tx::OriginalRowInvoice::original_copy_allocation_bytes(
            preparation_row_copy_bytes(row)?,
        )?;
        let (mut original, origin, lease) = self.prepare_original_nonwriter_copy(bytes, funder)?;
        // SAFETY: this genuine original holds payload plus invoice control.
        // Transfer retains its zero reader lease and disarms the sole debit.
        let invoice = unsafe {
            contextdb_tx::OriginalRowInvoice::from_original_copy_allocation(
                funder,
                original.bytes,
                origin,
                lease,
            )?
        };
        original.bytes = 0;
        let copied = preparation_copy_row(row)?;
        Ok(contextdb_tx::AdmittedSourceRow::new(copied, invoice))
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_retained_original_nonwriter_vector_parts_for(
        &self,
        table: &str,
        column: &str,
        vector: &[f32],
        row_id: contextdb_core::RowId,
        created_tx: contextdb_core::TxId,
        deleted_tx: Option<contextdb_core::TxId>,
        lsn: contextdb_core::Lsn,
        funder: &contextdb_tx::SourceFunder,
    ) -> contextdb_core::Result<contextdb_tx::AdmittedSourceVector> {
        let payload = source_add(
            source_add(table.len(), column.len())?,
            source_array::<f32>(vector.len())?,
        )?;
        let bytes = contextdb_tx::OriginalVectorInvoice::original_copy_allocation_bytes(payload)?;
        let (mut original, origin, lease) = self.prepare_original_nonwriter_copy(bytes, funder)?;
        // SAFETY: the original reader reserve owns the complete measured
        // allocation, and success transfers rather than refunds that debit.
        let invoice = unsafe {
            contextdb_tx::OriginalVectorInvoice::from_original_copy_allocation(
                funder,
                original.bytes,
                origin,
                lease,
            )?
        };
        original.bytes = 0;
        let copied = contextdb_core::VectorEntry {
            index: contextdb_core::VectorIndexRef {
                table: table.to_owned(),
                column: column.to_owned(),
            },
            row_id,
            vector: vector.to_vec(),
            created_tx,
            deleted_tx,
            lsn,
        };
        source_check_vector_capacity(&copied)?;
        Ok(contextdb_tx::AdmittedSourceVector::new(copied, invoice))
    }
}

// Private original-funded preparation. These helpers never select a current
// writer: the caller lends the use already held by terminal finalization.
fn validate_preparation_writer(
    original: &WriterUse,
    expected: &AccountantHandle,
) -> contextdb_core::Result<()> {
    if !expected.is_engine()
        || !original.accountant.is_engine()
        || original.accountant.as_ptr() != expected.as_ptr()
        || original.stable_identity == 0
        || original.epoch == 0
        || expected.writer_identity.get().copied() != Some(original.stable_identity)
    {
        return Err(contextdb_core::Error::Other(
            "preparation requires its pinned native original writer".into(),
        ));
    }
    let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
    let valid = domain
        .lookup_in(original.stable_identity)
        .is_some_and(|record| {
            record.epoch == original.epoch
                && record.users != 0
                && Arc::ptr_eq(&record.limit, &expected.limit)
                && record.used.as_ptr() == expected.used.as_ptr()
        });
    if !valid {
        return Err(contextdb_core::Error::Other(
            "preparation original writer registration mismatch".into(),
        ));
    }
    Ok(())
}

fn preparation_original_writer(
    original: &OwnedMemoryReservation,
) -> contextdb_core::Result<&WriterUse> {
    let accountant = original.accountant.as_ref().ok_or_else(|| {
        contextdb_core::Error::Other("preparation original has no accountant".into())
    })?;
    let writer = original._writer_use.as_ref().ok_or_else(|| {
        contextdb_core::Error::Other("preparation original has no held writer use".into())
    })?;
    validate_preparation_writer(writer, accountant)?;
    if original.own_control.as_ref().is_some_and(|control| {
        control.registration != Some((writer.stable_identity, writer.epoch))
            || control
                .accountant
                .as_ref()
                .is_none_or(|owner| !owner.is_engine() || owner.as_ptr() != accountant.as_ptr())
    }) {
        return Err(contextdb_core::Error::Other(
            "preparation original control origin mismatch".into(),
        ));
    }
    Ok(writer)
}

fn validate_preparation_funder(
    writer: &WriterUse,
    funder: &contextdb_tx::SourceFunder,
) -> contextdb_core::Result<()> {
    let lease = contextdb_tx::SourceLease {
        identity: writer.stable_identity,
        epoch: writer.epoch,
    };
    if !funder.matches_native_origin(writer.accountant.as_ptr() as usize, lease) {
        return Err(contextdb_core::Error::Other(
            "preparation funder does not match its pinned native original".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_owned_original(
    original: &OwnedMemoryReservation,
    expected: &AccountantHandle,
    funder: &contextdb_tx::SourceFunder,
) -> contextdb_core::Result<()> {
    let writer = preparation_original_writer(original)?;
    validate_preparation_writer(writer, expected)?;
    validate_preparation_funder(writer, funder)
}

fn reserve_preparation_for_use(
    original: &WriterUse,
    bytes: usize,
    subsystem: &str,
    operation: &str,
    hint: &str,
) -> contextdb_core::Result<OwnedMemoryReservation> {
    validate_preparation_writer(original, &original.accountant)?;
    let writer = original.clone();
    let projection = StoreAccountantHandle::from_writer_use(&writer);
    let accountant = projection.as_handle();
    projection.try_allocate_for_original(bytes, subsystem, operation, hint)?;
    Ok(OwnedMemoryReservation {
        accountant: Some(accountant),
        bytes,
        _writer_use: Some(writer),
        own_control: None,
    })
}

pub(crate) fn reserve_preparation_from_writer_use(
    original: &WriterUse,
    expected: &AccountantHandle,
    funder: &contextdb_tx::SourceFunder,
    bytes: usize,
    subsystem: &str,
    operation: &str,
    hint: &str,
) -> contextdb_core::Result<OwnedMemoryReservation> {
    validate_preparation_writer(original, expected)?;
    validate_preparation_funder(original, funder)?;
    reserve_preparation_for_use(original, bytes, subsystem, operation, hint)
}

/// Called only after the owning scope has validated the genuine original and
/// projected this tagged handle from that same held use. This private companion
/// never rediscovers a current registration or constructs a typed heap error.
/// Keeps the existing give-back-then-excess admission policy through scalar kernels.
#[allow(dead_code)] // Staged until the funded typed error can retain its backing credit.
pub(crate) fn reserve_native_storage_from_writer_use(
    original: &WriterUse,
    projection: &StoreAccountantHandle,
    bytes: usize,
) -> Result<OwnedMemoryReservation, contextdb_core::read_memory::NativeRefusal> {
    assert!(
        projection.is_writer() && projection.same_accountant(&original.accountant),
        "native storage projection belongs to its held original writer"
    );
    // from_writer_use created this projection while the original pins its epoch;
    // neither clone retags or selects a different writer registration.
    let writer = original.clone();
    let accountant = projection.as_handle();
    projection.try_allocate_for_provenance_native(
        bytes,
        "redb",
        "exact_byte_read",
        "Reduce the selected read or raise its declared memory limit.",
        Some(projection),
    )?;
    Ok(OwnedMemoryReservation {
        accountant: Some(accountant),
        bytes,
        _writer_use: Some(writer),
        own_control: None,
    })
}

pub(crate) fn reserve_preparation_from_original(
    original: &OwnedMemoryReservation,
    bytes: usize,
    subsystem: &str,
    operation: &str,
    hint: &str,
) -> contextdb_core::Result<OwnedMemoryReservation> {
    let writer = preparation_original_writer(original)?;
    reserve_preparation_for_use(writer, bytes, subsystem, operation, hint)
}

// std in the pinned compiler uses hashbrown 0.16.1. Keep bucket count
// distinct from usable capacity and use the concrete consumer entry layout.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NativeMapPlan {
    pub(crate) members: usize,
    pub(crate) buckets: usize,
    pub(crate) capacity: usize,
    pub(crate) layout: Layout,
}

fn native_map_group_width() -> usize {
    if cfg!(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri),
    )) || cfg!(all(
        target_arch = "loongarch64",
        target_feature = "lsx",
        not(miri)
    )) {
        16
    } else if cfg!(any(
        target_pointer_width = "64",
        target_arch = "aarch64",
        target_arch = "x86_64",
        target_arch = "wasm32",
    )) {
        8
    } else {
        4
    }
}

pub(crate) fn native_map_plan<K, V>(members: usize) -> contextdb_core::Result<NativeMapPlan> {
    let item = Layout::new::<(K, V)>();
    let group = native_map_group_width();
    let alignment = item.align().max(group);
    if members == 0 {
        return Ok(NativeMapPlan {
            members,
            buckets: 0,
            capacity: 0,
            layout: Layout::from_size_align(0, alignment).map_err(|_| source_size_overflow())?,
        });
    }
    // Hashbrown raises small-table minima for tiny entries so the data
    // region covers the control alignment. Event tuples exceed these minima.
    let minimum = match (group, item.size()) {
        (16, 0..=1) => 14,
        (16, 2..=3) | (8, 0..=1) => 7,
        _ => 3,
    };
    let adjusted = members.max(minimum);
    let buckets = if adjusted < 4 {
        4
    } else if adjusted < 8 {
        8
    } else if adjusted < 15 {
        16
    } else {
        adjusted
            .checked_mul(8)
            .ok_or_else(source_size_overflow)?
            .checked_div(7)
            .ok_or_else(source_size_overflow)?
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
    };
    let capacity = if buckets <= 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    };
    let data = item
        .size()
        .checked_mul(buckets)
        .ok_or_else(source_size_overflow)?;
    let control_offset = source_add(data, alignment - 1)? & !(alignment - 1);
    let size = source_add(source_add(control_offset, buckets)?, group)?;
    let layout = Layout::from_size_align(size, alignment).map_err(|_| source_size_overflow())?;
    Ok(NativeMapPlan {
        members,
        buckets,
        capacity,
        layout,
    })
}

fn native_capacity_mismatch() -> contextdb_core::Error {
    contextdb_core::Error::Other("preparation native capacity mismatch".into())
}

pub(crate) fn validate_native_map_capacity<K, V>(
    map: &HashMap<K, V>,
    plan: &NativeMapPlan,
) -> contextdb_core::Result<()> {
    if *plan != native_map_plan::<K, V>(plan.members)? || map.capacity() != plan.capacity {
        return Err(native_capacity_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_native_set_capacity<K>(
    set: &std::collections::HashSet<K>,
    plan: &NativeMapPlan,
) -> contextdb_core::Result<()> {
    if *plan != native_map_plan::<K, ()>(plan.members)? || set.capacity() != plan.capacity {
        return Err(native_capacity_mismatch());
    }
    Ok(())
}

pub(crate) fn native_ring_plan<T>(required: usize) -> contextdb_core::Result<(usize, Layout)> {
    if std::mem::size_of::<T>() == 0 {
        return Err(contextdb_core::Error::Other(
            "preparation exact ring capacity requires a nonzero entry layout".into(),
        ));
    }
    let capacity = if required == 0 {
        0
    } else {
        required
            .checked_next_power_of_two()
            .ok_or_else(source_size_overflow)?
    };
    let layout = Layout::array::<T>(capacity).map_err(|_| source_size_overflow())?;
    Ok((capacity, layout))
}

pub(crate) fn validate_native_ring_capacity<T>(
    ring: &std::collections::VecDeque<T>,
    capacity: usize,
) -> contextdb_core::Result<()> {
    let (expected, _) = native_ring_plan::<T>(capacity)?;
    if capacity != expected || ring.capacity() != capacity {
        return Err(native_capacity_mismatch());
    }
    Ok(())
}

fn reserve_native_container(
    original: &OwnedMemoryReservation,
    bytes: usize,
) -> contextdb_core::Result<OwnedMemoryReservation> {
    reserve_preparation_from_original(
        original,
        bytes,
        "relational",
        "statement_overlay",
        "Reduce prepared bodies or raise MEMORY_LIMIT.",
    )
}

pub(crate) fn new_owned_native_map<K, V>(
    original: &OwnedMemoryReservation,
    members: usize,
) -> contextdb_core::Result<(HashMap<K, V>, OwnedMemoryReservation)> {
    let plan = native_map_plan::<K, V>(members)?;
    let memory = reserve_native_container(original, plan.layout.size())?;
    let map = HashMap::with_capacity(members);
    if let Err(error) = validate_native_map_capacity(&map, &plan) {
        drop(map);
        return Err(error);
    }
    Ok((map, memory))
}

pub(crate) fn new_owned_native_set<K>(
    original: &OwnedMemoryReservation,
    members: usize,
) -> contextdb_core::Result<(std::collections::HashSet<K>, OwnedMemoryReservation)> {
    let plan = native_map_plan::<K, ()>(members)?;
    let memory = reserve_native_container(original, plan.layout.size())?;
    let set = std::collections::HashSet::with_capacity(members);
    if let Err(error) = validate_native_set_capacity(&set, &plan) {
        drop(set);
        return Err(error);
    }
    Ok((set, memory))
}

pub(crate) fn new_owned_native_ring<T>(
    original: &OwnedMemoryReservation,
    required: usize,
) -> contextdb_core::Result<(std::collections::VecDeque<T>, OwnedMemoryReservation)> {
    let (capacity, layout) = native_ring_plan::<T>(required)?;
    let memory = reserve_native_container(original, layout.size())?;
    let ring = std::collections::VecDeque::with_capacity(capacity);
    if let Err(error) = validate_native_ring_capacity(&ring, capacity) {
        drop(ring);
        return Err(error);
    }
    Ok((ring, memory))
}

fn validate_native_vec_capacity<T>(values: &Vec<T>, capacity: usize) -> contextdb_core::Result<()> {
    let expected = if std::mem::size_of::<T>() == 0 {
        usize::MAX
    } else {
        capacity
    };
    if values.capacity() != expected {
        return Err(native_capacity_mismatch());
    }
    Ok(())
}

pub(crate) fn new_owned_native_vec<T>(
    original: &OwnedMemoryReservation,
    capacity: usize,
) -> contextdb_core::Result<(Vec<T>, OwnedMemoryReservation)> {
    let layout = Layout::array::<T>(capacity).map_err(|_| source_size_overflow())?;
    let memory = reserve_native_container(original, layout.size())?;
    let values = Vec::with_capacity(capacity);
    if let Err(error) = validate_native_vec_capacity(&values, capacity) {
        drop(values);
        return Err(error);
    }
    Ok((values, memory))
}

pub(crate) fn copy_owned_native_slice<T: Copy>(
    original: &OwnedMemoryReservation,
    source: &[T],
) -> contextdb_core::Result<(Vec<T>, OwnedMemoryReservation)> {
    let (mut values, memory) = new_owned_native_vec(original, source.len())?;
    values.extend_from_slice(source);
    if let Err(error) = validate_native_vec_capacity(&values, source.len()) {
        drop(values);
        return Err(error);
    }
    Ok((values, memory))
}

fn copy_prepaid_native_string(source: &str) -> contextdb_core::Result<String> {
    let value = source.to_owned();
    if value.capacity() != source.len() {
        return Err(native_capacity_mismatch());
    }
    Ok(value)
}

pub(crate) fn copy_owned_native_string(
    original: &OwnedMemoryReservation,
    source: &str,
) -> contextdb_core::Result<(String, OwnedMemoryReservation)> {
    let layout = Layout::array::<u8>(source.len()).map_err(|_| source_size_overflow())?;
    let memory = reserve_native_container(original, layout.size())?;
    let value = copy_prepaid_native_string(source)?;
    Ok((value, memory))
}

// This recursion is entered only after a genuine original grant admitted the
// entire nested plan. Verify each container before inserting its children.
fn copy_prepaid_native_json(
    value: &serde_json::Value,
) -> contextdb_core::Result<serde_json::Value> {
    Ok(match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(copy_prepaid_native_string(text)?)
        }
        serde_json::Value::Array(items) => {
            let mut copied = Vec::with_capacity(items.len());
            validate_native_vec_capacity(&copied, items.len())?;
            for child in items {
                copied.push(copy_prepaid_native_json(child)?);
            }
            validate_native_vec_capacity(&copied, items.len())?;
            serde_json::Value::Array(copied)
        }
        serde_json::Value::Object(members) => {
            let mut copied = serde_json::Map::new();
            // Lexical BTree insertion is the existing source_json_map_bytes
            // split plan. The enclosing original grant covers all nodes/keys.
            for (name, child) in members {
                copied.insert(
                    copy_prepaid_native_string(name)?,
                    copy_prepaid_native_json(child)?,
                );
            }
            serde_json::Value::Object(copied)
        }
        _ => value.clone(),
    })
}

fn copy_prepaid_native_value(
    value: &contextdb_core::Value,
) -> contextdb_core::Result<contextdb_core::Value> {
    Ok(match value {
        contextdb_core::Value::Text(text) => {
            contextdb_core::Value::Text(copy_prepaid_native_string(text)?)
        }
        contextdb_core::Value::Vector(values) => {
            let mut copied = Vec::with_capacity(values.len());
            validate_native_vec_capacity(&copied, values.len())?;
            copied.extend_from_slice(values);
            validate_native_vec_capacity(&copied, values.len())?;
            contextdb_core::Value::Vector(copied)
        }
        contextdb_core::Value::Json(json) => {
            contextdb_core::Value::Json(copy_prepaid_native_json(json)?)
        }
        _ => value.clone(),
    })
}

fn reserve_preparation_original_copy(
    original: &OwnedMemoryReservation,
    bytes: usize,
) -> contextdb_core::Result<contextdb_tx::OriginalCopyGrant> {
    preparation_original_writer(original)?;
    // Validation excludes the host fallback of this existing typed receiver.
    <OwnedMemoryReservation as contextdb_tx::StagedPostingCharge>::reserve_private_original_copy(
        original, bytes,
    )
}

pub(crate) fn copy_owned_event_row_values(
    original: &OwnedMemoryReservation,
    row: &contextdb_core::VersionedRow,
) -> contextdb_core::Result<(
    HashMap<String, contextdb_core::Value>,
    contextdb_tx::OriginalCopyGrant,
)> {
    let plan = native_map_plan::<String, contextdb_core::Value>(row.values.len())?;
    let bytes = row
        .values
        .iter()
        .try_fold(plan.layout.size(), |bytes, (name, value)| {
            source_add(
                source_add(bytes, name.len())?,
                source_value_copy_bytes(value)?,
            )
        })?;
    let credit = reserve_preparation_original_copy(original, bytes)?;
    let mut values = HashMap::with_capacity(plan.members);
    validate_native_map_capacity(&values, &plan)?;
    for (name, value) in &row.values {
        values.insert(
            copy_prepaid_native_string(name)?,
            copy_prepaid_native_value(value)?,
        );
    }
    validate_native_map_capacity(&values, &plan)?;
    for value in values.values() {
        source_check_value_capacity(value)?;
    }
    Ok((values, credit))
}

pub(crate) fn copy_owned_native_value(
    original: &OwnedMemoryReservation,
    value: &contextdb_core::Value,
) -> contextdb_core::Result<(contextdb_core::Value, contextdb_tx::OriginalCopyGrant)> {
    let credit = reserve_preparation_original_copy(original, source_value_copy_bytes(value)?)?;
    let copied = copy_prepaid_native_value(value)?;
    source_check_value_capacity(&copied)?;
    Ok((copied, credit))
}

/// Exact original projection. This never discovers a writer registration.
pub(crate) fn persistence_original_projection(
    original: &OwnedMemoryReservation,
) -> contextdb_core::Result<StoreAccountantHandle> {
    Ok(StoreAccountantHandle::from_writer_use(
        preparation_original_writer(original)?,
    ))
}

pub(crate) fn grow_preparation_from_original(
    original: &mut OwnedMemoryReservation,
    bytes: usize,
    subsystem: &str,
    operation: &str,
    hint: &str,
) -> contextdb_core::Result<()> {
    let next = original
        .bytes
        .checked_add(bytes)
        .ok_or_else(source_size_overflow)?;
    let projection = persistence_original_projection(original)?;
    projection.try_allocate_for_original(bytes, subsystem, operation, hint)?;
    original.bytes = next;
    Ok(())
}

pub(crate) fn release_preparation_original(original: &mut OwnedMemoryReservation, bytes: usize) {
    let bytes = bytes.min(original.bytes);
    original
        .accountant
        .as_ref()
        .expect("validated original")
        .release(bytes);
    original.bytes -= bytes;
}

pub(crate) fn detach_preparation_original(
    original: &mut OwnedMemoryReservation,
    bytes: usize,
) -> contextdb_core::Result<OwnedMemoryReservation> {
    if bytes > original.bytes {
        return Err(source_size_overflow());
    }
    // Clone precisely the held use before transferring any debit. Failure
    // therefore leaves the caller's charge and lifetime completely intact.
    let mut detached = reserve_preparation_from_original(
        original,
        0,
        "content_identity",
        "maintain_committed_proof",
        "Raise MEMORY_LIMIT to admit the proof this transaction maintains.",
    )?;
    original.bytes -= bytes;
    detached.bytes = bytes;
    Ok(detached)
}

pub(crate) fn merge_preparation_original(
    original: &mut OwnedMemoryReservation,
    credit: &mut OwnedMemoryReservation,
) -> contextdb_core::Result<()> {
    let left = preparation_original_writer(original)?;
    let right = preparation_original_writer(credit)?;
    if left.stable_identity != right.stable_identity
        || left.epoch != right.epoch
        || left.accountant.as_ptr() != right.accountant.as_ptr()
    {
        return Err(contextdb_core::Error::Other(
            "original persistence credit origin mismatch".into(),
        ));
    }
    let next = original
        .bytes
        .checked_add(credit.bytes)
        .ok_or_else(source_size_overflow)?;
    original.bytes = next;
    credit.bytes = 0;
    Ok(())
}

pub(crate) fn clone_persistence_original_writer(
    original: &OwnedMemoryReservation,
) -> contextdb_core::Result<WriterUse> {
    Ok(preparation_original_writer(original)?.clone())
}

// Native siblings preserve the original counter, gates and reclamation order.
// Legacy kernels above remain byte-identical. Only the refusal representation
// is static; no owned Error is constructed and subsequently mapped.
impl MemoryAccountant {
    pub(crate) fn try_allocate_native(
        &self,
        bytes: usize,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.try_allocate_with_provenance_native(bytes, None)
    }
    pub(crate) fn try_allocate_for_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.try_allocate_for_provenance_native(bytes, subsystem, operation, hint, None)
    }
    fn admit_to_process_aggregate_native(
        &self,
        admission: &WriterAdmission,
        bytes: usize,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        if !self.used.is_member() {
            return Ok(());
        }
        let Some(ceiling) = admission.ceiling else {
            PROCESS_WRITER_AGGREGATE.fetch_add(bytes, Ordering::SeqCst);
            return Ok(());
        };
        if let Err(available) = aggregate_try_reserve(bytes, ceiling) {
            self.used.rollback_local_only(bytes);
            return Err(
                contextdb_core::read_memory::NativeRefusal::ProcessMemoryBudgetExceeded {
                    operation,
                    requested_bytes: bytes,
                    available_bytes: available,
                    budget_limit_bytes: ceiling,
                    hint,
                },
            );
        }
        Ok(())
    }
    fn try_allocate_with_provenance_native(
        &self,
        bytes: usize,
        provenance: Option<&StoreAccountantHandle>,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        if bytes == 0 {
            return Ok(());
        }

        let admission = self.writer_admission_for_provenance(provenance);
        #[cfg(test)]
        let mut admission = admission;
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let published = self.limit.load(Ordering::SeqCst);
            let limit = if published == 0 {
                0
            } else {
                published.saturating_add(self.private_transition_headroom())
            };
            if limit != 0 {
                let available = writer_headroom(limit, used, 0, 0)
                    .available
                    .unwrap_or(usize::MAX - used);
                if bytes > available {
                    // TOCTOU window: `limit` was loaded above; a concurrent
                    // `set_budget(None)` may have cleared it since. The re-check
                    // below closes that window. In test builds a hook lets a
                    // unit test land exactly that interleaving deterministically.
                    #[cfg(test)]
                    if self.fire_accounting_hook_without_gate_for_provenance(
                        &mut admission,
                        provenance,
                        false,
                    ) {
                        continue;
                    }
                    if self.limit.load(Ordering::SeqCst) != published {
                        continue;
                    }
                    return Err(
                        contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                            subsystem: "memory",
                            operation: "allocate",
                            requested_bytes: bytes,
                            available_bytes: available,
                            budget_limit_bytes: limit,
                            hint: "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT.",
                        },
                    );
                }
            }

            let Some(next) = used.checked_add(bytes) else {
                return Err(
                    contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                        subsystem: "memory",
                        operation: "allocate",
                        requested_bytes: bytes,
                        available_bytes: usize::MAX - used,
                        budget_limit_bytes: usize::MAX,
                        hint: "Reduce retained data or working-set size.",
                    },
                );
            };
            // The interleaving a narrowing limit has to survive: this
            // allocator has already read `published` and decided the request
            // fits it; a concurrent narrowing publishes a smaller limit right
            // here, and the CAS below still runs. In test builds a hook lands
            // exactly that order deterministically.
            #[cfg(test)]
            if self.fire_accounting_hook_without_gate_for_provenance(
                &mut admission,
                provenance,
                true,
            ) {
                continue;
            }
            if self.used.compare_exchange_local(used, next) {
                // Admission and publication share ONE linearization boundary,
                // and it is this load. An admission is not SUCCESSFUL until it
                // has been validated against a published limit read that
                // happens-after its own CAS. A narrowing that linearizes before
                // this load is therefore always seen: the CAS is undone and the
                // loop produces the ordinary typed refusal under the new limit.
                // A narrowing that linearizes after it is a publication this
                // admission genuinely preceded. Either way no request returns
                // success having been admitted under a limit already superseded,
                // and `used` is never left above the published ceiling by an
                // allocator that read a stale one.
                let current = self.limit.load(Ordering::SeqCst);
                let admissible = if current == 0 {
                    0
                } else {
                    current.saturating_add(self.private_transition_headroom())
                };
                if writer_headroom(admissible, used, 0, 0)
                    .available
                    .is_some_and(|available| bytes > available)
                {
                    // Still inside local admission: the aggregate step below
                    // has not run, so these bytes never entered it.
                    self.used.rollback_local_only(bytes);
                    continue;
                }
                self.admit_to_process_aggregate_native(
                    &admission,
                    bytes,
                    "allocate",
                    "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT.",
                )?;
                #[cfg(any(test, feature = "test-seams"))]
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                #[cfg(any(test, feature = "test-seams"))]
                self.admitted_bytes_for_test
                    .fetch_add(bytes, Ordering::SeqCst);
                return Ok(());
            }
        }
    }
    fn try_allocate_for_provenance_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
        provenance: Option<&StoreAccountantHandle>,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        let native_reclaim_meters = contextdb_core::find_attribution::native_reclaim_meter_scope();
        let allocation = (|| {
            #[cfg(any(test, feature = "test-seams"))]
            if operation == "current_identity_delta"
                && self
                    .tail_admission_failure
                    .compare_exchange(3, 4, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(
                    contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                        subsystem,
                        operation,
                        requested_bytes: bytes,
                        available_bytes: 0,
                        budget_limit_bytes: self.used.load(Ordering::SeqCst),
                        hint: "Injected current vector identity publication admission exhaustion.",
                    },
                );
            }
            #[cfg(any(test, feature = "test-seams"))]
            if operation == "prepare_hnsw_tail"
                && self
                    .tail_admission_failure
                    .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(
                    contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                        subsystem,
                        operation,
                        requested_bytes: bytes,
                        available_bytes: 0,
                        budget_limit_bytes: self.used.load(Ordering::SeqCst),
                        hint: "Injected fresh-tail admission exhaustion.",
                    },
                );
            }
            let mut allocation = self.try_allocate_with_provenance_native(bytes, provenance);
            #[cfg(any(test, feature = "test-seams"))]
            if allocation.is_ok() {
                note_budget_admission(
                    subsystem,
                    operation,
                    bytes,
                    self.used.load(Ordering::SeqCst),
                );
            }
            // A saved graph whose topology does not fit is admitted once the
            // serving pages already resident give way to it: they are re-readable,
            // the topology is what makes its partition searchable at all.
            // Idle graphs of partitions the statement is not searching go first;
            // serving pages give way only after them.
            if allocation.is_err() && operation == "load_retained_vector_generation" {
                let vector = self
                    .idle_graph_reclaimer
                    .lock()
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                if let Some(vector) = vector {
                    let short = || {
                        bytes.saturating_sub(
                            self.available_for_provenance(provenance)
                                .unwrap_or(usize::MAX),
                        )
                    };
                    if vector.make_room_for_graph_load_native(short()) != 0 {
                        allocation = self.try_allocate_with_provenance_native(bytes, provenance);
                    }
                    if allocation.is_err() && vector.give_back_serving_pages_native(short()) != 0 {
                        allocation = self.try_allocate_with_provenance_native(bytes, provenance);
                    }
                }
            }
            // Serving pages are a cache a read can always refill, so nothing is
            // refused while they hold the memory it needs: they give way to any
            // allocation exactly as they give way to a graph load. The give-back
            // only tries its locks, so it is safe from inside any caller.
            if allocation.is_err() && operation != "load_retained_vector_generation" {
                let vector = self
                    .idle_graph_reclaimer
                    .lock()
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                let short = bytes.saturating_sub(
                    self.available_for_provenance(provenance)
                        .unwrap_or(usize::MAX),
                );
                if vector.is_some_and(|vector| vector.give_back_serving_pages_native(short) != 0) {
                    allocation = self.try_allocate_with_provenance_native(bytes, provenance);
                }
            }
            if allocation.is_err() && matches!(operation, "read/table_scan" | "read/index_range") {
                let vector = self
                    .idle_graph_reclaimer
                    .lock()
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                let graphs = vector
                    .as_ref()
                    .map_or(0, |vector| vector.evict_idle_graphs());
                if graphs != 0 {
                    allocation = self.try_allocate_with_provenance_native(bytes, provenance);
                }
            }
            // A received schema unit that was already ADMITTED AS A WHOLE is
            // applying against THIS accountant, and an allocation it did not model
            // does not fit. It draws on the reservation the preflight is holding
            // for it, and past that on a BOUNDED excess.
            //
            // The unit was priced and admitted before its first item mutated
            // anything, so a limit genuinely too small for it was already refused
            // there, leaving the receiver's old complete state. Refusing at the
            // first byte the model got slightly wrong would instead leave a proper
            // subset of an arriving schema installed and durable -- the one outcome
            // the design's "Atomic schema installation" row forbids outright -- and
            // there is no way back: a received DROP TABLE cannot be taken back at
            // all, and compensating DDL written to undo it lands in the REPLICATED
            // log, so one node's memory shortfall would propagate a schema revert
            // to every peer.
            //
            // But the model's error is driven by what a PEER sent, so absorbing it
            // without a ceiling would hand the peer the operator's declared limit.
            // Past `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES` the ordinary typed refusal
            // fires: the limit the operator declared bounds the process, and the
            // most a unit can take past what it was priced at is that constant.
            if allocation.is_err() && self.charge_admitted_unit_excess(bytes) {
                return Ok(());
            }
            allocation.map_err(|err| match err {
                contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                    requested_bytes,
                    budget_limit_bytes,
                    available_bytes,
                    ..
                }
                | contextdb_core::read_memory::NativeRefusal::ProcessMemoryBudgetExceeded {
                    requested_bytes,
                    budget_limit_bytes,
                    available_bytes,
                    ..
                } => contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                    subsystem,
                    operation,
                    requested_bytes,
                    budget_limit_bytes,
                    available_bytes,
                    hint,
                },
                other => other,
            })
        })();
        // The fixed scope binds observations to the currently open record on
        // completion and on unwind, or drops them when no record is open.
        // Its bookkeeping cannot change the original allocation result.
        let _meters = native_reclaim_meters.finish();
        allocation
    }
}

impl StoreAccountantHandle {
    pub(crate) fn try_allocate_for_original_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.try_allocate_for_provenance_native(bytes, subsystem, operation, hint, Some(self))
    }
}

impl OwnedMemoryReservation {
    /// A genuine new grant from the originating reservation, retaining its
    /// existing writer epoch rather than discovering a later registration.
    pub(crate) fn reserve_copy_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, contextdb_core::read_memory::NativeRefusal> {
        let mut copy = Self {
            accountant: Some(self.accountant().clone()),
            bytes: 0,
            _writer_use: self._writer_use.clone(),
            own_control: None,
        };
        copy.try_grow_for_native(bytes, subsystem, operation, hint)?;
        Ok(copy)
    }

    pub(crate) fn try_new_for_native(
        accountant: AccountantHandle,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, contextdb_core::read_memory::NativeRefusal> {
        // Capture the epoch before admission, as a zero-byte guard. It remains
        // pinned through success, refusal, and later body destruction.
        let mut reservation = Self::new(accountant);
        reservation.try_grow_for_native(bytes, subsystem, operation, hint)?;
        Ok(reservation)
    }

    pub(crate) fn try_grow_for_native(
        &mut self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        let next = self.bytes.checked_add(bytes).ok_or(
            contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: usize::MAX - self.bytes,
                budget_limit_bytes: usize::MAX,
                hint,
            },
        )?;
        self.accountant()
            .try_allocate_for_native(bytes, subsystem, operation, hint)?;
        self.bytes = next;
        Ok(())
    }
}
