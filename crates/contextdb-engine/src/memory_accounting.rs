use std::cell::RefCell;
use std::ptr::NonNull;
use std::alloc::Layout;
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
    aggregate_try_reserve(bytes, ceiling).map_err(|available| contextdb_core::Error::MemoryBudgetExceeded {
        subsystem: "process_native_ceiling".to_string(),
        operation: operation.to_string(),
        requested_bytes: bytes,
        available_bytes: available,
        budget_limit_bytes: ceiling,
        hint: "The inherited process ceiling funds fixed writer open residue.".to_string(),
    })
}

pub(crate) fn refund_open_residue(bytes: usize) {
    aggregate_refund(bytes);
}

fn aggregate_refund(bytes: usize) {
    if bytes != 0 {
        PROCESS_WRITER_AGGREGATE
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| held.checked_sub(bytes))
            .map_or_else(|_| refund_invariant_broken("process aggregate refund is owned"), |_| ());
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
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| used.checked_sub(bytes))
            .map_or_else(|_| refund_invariant_broken("local rollback is owned"), |_| ());
    }
    /// Unconditional charge (retained startup floors); a member's bytes join
    /// the aggregate without a ceiling check, as the local charge has none.
    fn charge(&self, bytes: usize) {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| used.checked_add(bytes))
            .expect("retained ownership cannot overflow its counter");
        if self.is_member() {
            PROCESS_WRITER_AGGREGATE.fetch_add(bytes, Ordering::SeqCst);
        }
    }
    /// Return owned bytes; a member's bytes leave the aggregate once, here.
    pub(crate) fn release_owned(&self, bytes: usize) -> Result<(), usize> {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| used.checked_sub(bytes))?;
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
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| Some(used.saturating_sub(bytes)))
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
        if let Some(total) = std::fs::read_to_string("/proc/meminfo").ok().and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("MemTotal:"))
                .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse::<usize>().ok())
                .and_then(|kib| kib.checked_mul(1024))
        }) {
            return total;
        }
    }
    #[cfg(unix)]
    {
        // SAFETY: sysconf has no preconditions and reads process-global values.
        let (pages, page) = unsafe { (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE)) };
        if pages > 0 && page > 0 {
            return (pages as usize).saturating_mul(page as usize);
        }
    }
    usize::MAX
}

#[cfg(target_os = "linux")]
fn cgroup_memory_max() -> Option<usize> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = membership.lines().find_map(|line| line.strip_prefix("0::"))?.trim();
    let mut current = std::path::PathBuf::from("/sys/fs/cgroup");
    let mut limit: Option<usize> = None;
    let mut visit = |dir: &std::path::Path| {
        if let Ok(text) = std::fs::read_to_string(dir.join("memory.max"))
            && let Ok(bytes) = text.trim().parse::<usize>()
        {
            limit = Some(limit.map_or(bytes, |held| held.min(bytes)));
        }
    };
    visit(&current);
    for component in std::path::Path::new(path).components() {
        if let std::path::Component::Normal(name) = component {
            current.push(name);
            visit(&current);
        }
    }
    limit
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

/// (aggregate bytes, recorded process ceiling) for receipts and rows.
#[allow(dead_code)] // Keep the aggregate receipt seam available to memory-accounting tests.
pub(crate) fn process_aggregate_snapshot() -> (usize, Option<usize>) {
    let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
    (PROCESS_WRITER_AGGREGATE.load(Ordering::SeqCst), domain.ceiling)
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
    used: Arc<WriterCounter>,
    // The engine's backing for this accountant, refunded at last use.
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
            if record.stable_identity == stable_identity { return Some(record); }
            next = record.next.as_deref();
        }
        None
    }
    fn lookup_mut_in(&mut self, stable_identity: usize) -> Option<&mut WriterRegistration> {
        let mut next = self.writers.as_deref_mut();
        while let Some(record) = next {
            if record.stable_identity == stable_identity { return Some(record); }
            next = record.next.as_deref_mut();
        }
        None
    }
    fn retain_in(&mut self, stable_identity: usize, epoch: usize) {
        let record = self.lookup_mut_in(stable_identity).expect("live use has its registration");
        assert_eq!(record.epoch, epoch, "live use cannot change epoch");
        record.users = record.users.checked_add(1).expect("writer use count overflow");
    }
    fn release_in(&mut self, accountant: &MemoryAccountant, stable_identity: usize, epoch: usize) {
        let mut slot = &mut self.writers;
        while slot.is_some() {
            if slot.as_ref().is_some_and(|record| record.stable_identity == stable_identity) {
                let record = slot.as_mut().expect("live registration");
                assert_eq!(record.epoch, epoch, "live use cannot change epoch");
                record.users = record.users.checked_sub(1).expect("writer use underflow");
                if record.users != 0 { return; }
                let mut record = slot.take().expect("last-use registration");
                *slot = record.next.take();
                // The engine's custody of this accountant ends with its last
                // use: the registration and the engine's backing for the
                // accountant both leave the books here. A holder that keeps
                // the accountant afterwards holds it outside the engine.
                let backing = record.engine_backing_bytes.load(Ordering::SeqCst);
                // Only counter handles and inert receipt state remain in the
                // detached record. No payload destructor runs under this gate.
                drop(record);
                accountant.release_under_exclusive_gate(
                    Layout::new::<WriterRegistration>().size() + backing,
                );
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
    accountant: Arc<MemoryAccountant>,
    stable_identity: usize,
    epoch: usize,
}
impl WriterUse {
    pub(crate) fn retain_if_writer(accountant: &Arc<MemoryAccountant>) -> Option<Self> {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let stable_identity = accountant.writer_identity.get().copied()?;
        let epoch = domain.lookup_in(stable_identity)?.epoch;
        domain.retain_in(stable_identity, epoch);
        Some(Self { accountant: accountant.clone(), stable_identity, epoch })
    }
}
impl Clone for WriterUse {
    fn clone(&self) -> Self {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        domain.retain_in(self.stable_identity, self.epoch);
        Self { accountant: self.accountant.clone(), stable_identity: self.stable_identity, epoch: self.epoch }
    }
}
impl Drop for WriterUse {
    fn drop(&mut self) {
        PROCESS_WRITER_BUDGET_DOMAIN.write().release_in(&self.accountant, self.stable_identity, self.epoch);
    }
}

// A provenance-preserving one-word strong Arc projection. A tagged handle
// contributes exactly one active engine use; an untagged handle never acquires
// one on clone, even if a writer subsequently registers the same accountant.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct StoreAccountantHandle(NonNull<MemoryAccountant>);
const _: () = assert!(std::mem::align_of::<MemoryAccountant>() >= 2);
impl StoreAccountantHandle {
    fn decoded(&self) -> NonNull<MemoryAccountant> {
        self.0.map_addr(|address| std::num::NonZeroUsize::new(address.get() & !1).expect("Arc pointer is non-null"))
    }
    pub(crate) fn is_writer(&self) -> bool { self.0.addr().get() & 1 != 0 }
    pub(crate) fn retain_if_writer(accountant: &Arc<MemoryAccountant>) -> Option<Self> {
        // Construct and check the tag from the same exclusive-gate snapshot.
        let handle = Self::new(Arc::clone(accountant));
        handle.is_writer().then_some(handle)
    }
    pub(crate) fn same_allocation(&self, other: &Self) -> bool {
        self.decoded() == other.decoded()
    }
    pub(crate) fn new(accountant: Arc<MemoryAccountant>) -> Self {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let active = accountant.writer_identity.get().copied()
            .and_then(|key| domain.lookup_in(key).map(|record| (key, record.epoch)));
        if let Some((key, epoch)) = active { domain.retain_in(key, epoch); }
        let raw = NonNull::new(Arc::into_raw(accountant).cast_mut()).expect("Arc pointer is non-null");
        Self(if active.is_some() {
            raw.map_addr(|address| std::num::NonZeroUsize::new(address.get() | 1).expect("tagged Arc pointer is non-null"))
        } else { raw })
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
    pub(crate) fn try_allocate_for_original(&self, bytes: usize, subsystem: &str,
        operation: &str, hint: &str) -> contextdb_core::Result<()> {
        self.try_allocate_for_provenance(bytes, subsystem, operation, hint, Some(self))
    }
    pub(crate) fn same_accountant(&self, accountant: &Arc<MemoryAccountant>) -> bool {
        std::ptr::eq(self.decoded().as_ptr(), Arc::as_ptr(accountant))
    }
    pub(crate) fn as_arc(&self) -> Arc<MemoryAccountant> {
        let raw = self.decoded().as_ptr();
        // SAFETY: this handle owns a live strong Arc count on the decoded
        // allocation. Increment before reconstructing the additional owner.
        unsafe { Arc::increment_strong_count(raw); Arc::from_raw(raw) }
    }
    pub(crate) fn vector_budget(
        &self,
    ) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        let accountant = self.as_arc();
        if !self.is_writer() {
            // An untagged derived owner stays a reader even after a writer
            // later registers this same accountant.
            return Ok(accountant);
        }
        let writer_use = {
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
        };
        Ok(accountant.budget_adapter(writer_use, false)?)
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
        let accountant = self.as_arc();
        if self.is_writer() {
            let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
            let key = *accountant.writer_identity.get().expect("tagged handle has a stable key");
            let epoch = domain.lookup_in(key).expect("tagged handle pins its record").epoch;
            domain.retain_in(key, epoch);
        }
        let raw = NonNull::new(Arc::into_raw(accountant).cast_mut()).expect("Arc pointer is non-null");
        Self(if self.is_writer() {
            raw.map_addr(|address| std::num::NonZeroUsize::new(address.get() | 1).expect("tagged Arc pointer is non-null"))
        } else { raw })
    }
}
impl Drop for StoreAccountantHandle {
    fn drop(&mut self) {
        // SAFETY: exactly one into_raw ownership is consumed per handle and
        // only the decoded pointer is passed to Arc's ownership operations.
        let accountant = unsafe { Arc::from_raw(self.decoded().as_ptr()) };
        if self.is_writer() {
            let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
            let key = *accountant.writer_identity.get().expect("tagged handle has a stable key");
            let epoch = domain.lookup_in(key).expect("tagged handle pins its record").epoch;
            domain.release_in(&accountant, key, epoch);
        }
        drop(accountant);
    }
}
// SAFETY: the pointee has Arc<MemoryAccountant>'s Send/Sync bounds. The tag is
// immutable per handle; every mutable registration operation uses the one
// exclusive process-domain gate. No raw mutable pointee access is provided.
unsafe impl Send for StoreAccountantHandle {}
unsafe impl Sync for StoreAccountantHandle {}

#[derive(Debug, Clone)]
pub(crate) struct WriterBudget {
    accountant: Arc<MemoryAccountant>,
    writer_use: WriterUse,
    vector_accountant: Arc<dyn contextdb_vector::MemoryBudget>,
}
impl WriterBudget {
    pub(crate) fn from_accountant(accountant: Arc<MemoryAccountant>) -> contextdb_core::Result<Self> {
        Self::from_accountant_with_backing(accountant, 0)
    }
    fn from_accountant_with_backing(accountant: Arc<MemoryAccountant>, engine_backing_bytes: usize) -> contextdb_core::Result<Self> {
        let mut domain = PROCESS_WRITER_BUDGET_DOMAIN.write();
        let stable_identity = if let Some(key) = accountant.writer_identity.get() { *key } else {
            let key = domain.next_identity;
            domain.next_identity = key.checked_add(1)
                .ok_or_else(|| contextdb_core::Error::Other("writer identity overflow".into()))?;
            accountant.writer_identity.set(key).expect("association published under the exclusive gate");
            key
        };
        let epoch = if let Some(record) = domain.lookup_in(stable_identity) {
            let epoch = record.epoch;
            assert_eq!(engine_backing_bytes, 0, "existing writer backing has one origin");
            domain.retain_in(stable_identity, epoch);
            epoch
        } else {
            let epoch = domain.next_identity;
            let next_identity = epoch.checked_add(1)
                .ok_or_else(|| contextdb_core::Error::Other("writer identity overflow".into()))?;
            let registration_bytes = Layout::new::<WriterRegistration>().size();
            accountant.used.load(Ordering::SeqCst).checked_add(registration_bytes)
                .ok_or_else(|| contextdb_core::Error::Other("writer registration size overflow".into()))?;
            accountant.charge_retained_under_admission(registration_bytes);
            accountant.used.join_aggregate();
            let registration = Box::new(WriterRegistration {
                stable_identity, epoch, users: 1,
                limit: accountant.limit.clone(), used: accountant.used.clone(),
                engine_backing_bytes: AtomicUsize::new(engine_backing_bytes),
                adapter_control_bytes: AtomicUsize::new(0), next: domain.writers.take(),
            });
            domain.writers = Some(registration);
            domain.next_identity = next_identity;
            epoch
        };
        drop(domain);
        let writer_use = WriterUse { accountant: accountant.clone(), stable_identity, epoch };
        let vector_accountant = accountant.vector_budget_for_startup()?;
        Ok(Self { accountant, writer_use, vector_accountant })
    }
    pub(crate) fn accountant(&self) -> Arc<MemoryAccountant> { self.accountant.clone() }
    pub(crate) fn into_parts(self) -> (Arc<MemoryAccountant>, WriterUse, Arc<dyn contextdb_vector::MemoryBudget>) { (self.accountant, self.writer_use, self.vector_accountant) }
    pub(crate) fn vector_accountant(&self) -> Arc<dyn contextdb_vector::MemoryBudget> { self.vector_accountant.clone() }
}

// Exact pinned Rust 1.94.1 alloc::sync ArcInner repr(C) layout, sync.rs
// sync.rs:377-400: #[repr(C, align(2))] ArcInner has two Atomic<usize>
// header fields and data: T; arcinner_layout_for_value_layout extends and pads. Existing GlobalAlloc witness verifies the request.
pub(crate) fn writer_adapter_layout<T>(adapter: &T) -> Layout {
    Layout::new::<[AtomicUsize; 2]>().extend(Layout::for_value(adapter))
        .expect("adapter Arc layout overflow").0.pad_to_align()
}
pub(crate) fn writer_adapter_type_layout<T>() -> Layout {
    Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<T>())
        .expect("adapter Arc layout overflow").0.pad_to_align()
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
fn writer_headroom(
    limit: usize,
    used: usize,
    releasing: usize,
    private: usize,
) -> WriterHeadroom {
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
    accountant: &Arc<MemoryAccountant>,
    transfer: VectorLoadReservationTransfer,
    operation: impl FnOnce() -> T,
) -> T {
    VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
        transfers
            .borrow_mut()
            .push(ActiveVectorLoadReservationTransfer {
                accountant: Arc::as_ptr(accountant) as usize,
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
pub struct MemoryAccountant {
    limit: Arc<AtomicUsize>,
    used: Arc<WriterCounter>,
    writer_identity: OnceLock<usize>,
    #[cfg(feature = "test-seams")]
    high_water_for_test: AtomicUsize,
    #[cfg(feature = "test-seams")]
    admitted_bytes_for_test: AtomicUsize,
    /// Bytes taken through `charge_retained` -- the writer's own retention
    /// door, which never refuses. A read session must never pay at it.
    #[cfg(feature = "test-seams")]
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
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetLedgerEntry {
    /// The `subsystem/operation` that charged, or `"release"`.
    pub what: String,
    pub bytes: usize,
    pub used_after: usize,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static BUDGET_LEDGER: std::cell::RefCell<Option<Vec<BudgetLedgerEntry>>> =
        const { std::cell::RefCell::new(None) };
}

/// Start recording this thread's budget traffic. Recording is off until this is
/// called, so nothing pays for the ledger on an ordinary run.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn watch_budget_ledger_for_test() {
    BUDGET_LEDGER.with(|ledger| *ledger.borrow_mut() = Some(Vec::new()));
}

/// Stop recording and take what was recorded.
#[cfg(feature = "test-seams")]
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
    accountant: Arc<MemoryAccountant>,
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

#[cfg(feature = "test-seams")]
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
    #[cfg(feature = "test-seams")]
    pub reader_file_high_water: u64,
    pub limit: Option<usize>,
    pub used: usize,
    pub available: Option<usize>,
    pub startup_ceiling: Option<usize>,
}

// The allocator oracle must not count a disabled ledger's formatting as a
// control allocation. Construct the identical observation only for a live
// explicitly scoped ledger; this seam is absent without test-seams.
#[cfg(feature = "test-seams")]
fn note_budget_admission(subsystem: &str, operation: &str, bytes: usize, used_after: usize) {
    BUDGET_LEDGER.with(|ledger| {
        if let Some(entries) = ledger.borrow_mut().as_mut() {
            entries.push(BudgetLedgerEntry {
                what: format!("{subsystem}/{operation}"), bytes, used_after,
            });
        }
    });
}

// One complete fixed-control liability, separate from payload credit.
// Its final owner extracts the bounded refund capability before dropping
// payload/use fields; PendingControlRefund is then the last local destroyed.
#[derive(Debug)]
pub(crate) struct ControlCredit {
    accountant: Option<Arc<MemoryAccountant>>,
    bytes: usize,
    registration: Option<(usize, usize)>,
}
impl ControlCredit {
    // CDB127 private static-map extension: registry controls inherit the
    // creating handle's provenance and the existing family's refusal door.
    // An untagged reader never acquires a later writer's receipt/epoch.
    pub(crate) fn admit_from_handle(
        accountant: &StoreAccountantHandle, bytes: usize,
        subsystem: &str, operation: &str, hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for_provenance(bytes, subsystem, operation, hint, Some(accountant))?;
        let registration = if accountant.is_writer() {
            let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
            let key = *accountant.writer_identity.get().expect("tagged handle has its original key");
            let record = domain.lookup_in(key).expect("tagged handle pins its original epoch");
            record.adapter_control_bytes.fetch_update(Ordering::SeqCst, Ordering::SeqCst,
                |current| current.checked_add(bytes)).expect("control receipt arithmetic must be owned");
            Some((key, record.epoch))
        } else { None };
        Ok(Self { accountant: Some(accountant.as_arc()), bytes, registration })
    }
    pub(crate) fn admit_open_residue(accountant: &StoreAccountantHandle, bytes: usize, operation: &str) -> contextdb_core::Result<Self> {
        admit_open_residue(bytes, operation)?;
        Ok(Self { accountant: Some(accountant.as_arc()), bytes: bytes | OPEN_RESIDUE_TAG, registration: None })
    }
    // Only constant per-assembly controls call this door. Growing write
    // payloads, including staged posting charges, retain ordinary admission.
    pub(crate) fn admit_assembly(accountant: &Arc<MemoryAccountant>, bytes: usize, operation: &str) -> contextdb_core::Result<Self> {
        let handle = StoreAccountantHandle::new(accountant.clone());
        if handle.is_writer() {
            Self::admit_open_residue(&handle, bytes, operation)
        } else {
            Self::admit(accountant, bytes, operation)
        }
    }
    pub(crate) fn admit(accountant: &Arc<MemoryAccountant>, bytes: usize, operation: &str) -> contextdb_core::Result<Self> {
        accountant.admit_box_control(bytes, operation, "Raise MEMORY_LIMIT for this fixed writer control.")?;
        Ok(Self { accountant: Some(accountant.clone()), bytes, registration: accountant.prepare_control_refund().1 })
    }
    pub(crate) fn from_admitted(accountant: &Arc<MemoryAccountant>, bytes: usize) -> Self {
        // Transfer a named portion of an existing control reservation. No
        // admission, refund or payer substitution happens at this handoff.
        accountant.change_adapter_control_receipt(bytes, true);
        Self { accountant: Some(Arc::clone(accountant)), bytes, registration: accountant.prepare_control_refund().1 }
    }
    pub(crate) fn retained_startup(accountant: &Arc<MemoryAccountant>, bytes: usize) -> Self {
        // Preserve the existing captured-schema startup floor: accounted
        // before allocation, but never refused solely by a persisted limit.
        accountant.charge_retained(bytes);
        accountant.change_adapter_control_receipt(bytes, true);
        Self { accountant: Some(Arc::clone(accountant)), bytes, registration: accountant.prepare_control_refund().1 }
    }
    pub(crate) fn prepare(mut self) -> PendingControlRefund {
        let accountant = self.accountant.take().expect("live control credit has its original funder");
        let bytes = std::mem::take(&mut self.bytes);
        let counter = accountant.original_control_counter();
        let registration = self.registration.take();
        drop(accountant);
        PendingControlRefund { counter, bytes, registration }
    }
}
impl Drop for ControlCredit {
    fn drop(&mut self) {
        if let Some(accountant) = self.accountant.take() {
            let counter = accountant.original_control_counter();
            let registration = self.registration.take();
            drop(accountant);
            drop(PendingControlRefund { counter, bytes: std::mem::take(&mut self.bytes), registration });
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
    counter: Arc<WriterCounter>,
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
            let receipt = record.adapter_control_bytes.fetch_update(Ordering::SeqCst, Ordering::SeqCst,
                |used| used.checked_sub(self.bytes));
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
    accountant: Option<Arc<MemoryAccountant>>,
    writer_use: Option<WriterUse>,
    payload: usize,
    refund: Option<PendingControlRefund>,
    next: u8,
}
impl ReservationCleanup {
    pub(crate) fn new(accountant: Option<Arc<MemoryAccountant>>, writer_use: Option<WriterUse>,
        payload: usize, refund: Option<PendingControlRefund>) -> Self {
        Self { accountant, writer_use, payload, refund, next: 0 }
    }
    pub(crate) fn finish(&mut self) {
        while self.next < 4 {
            let step = self.next; self.next += 1;
            match step {
                0 => if let Some(accountant) = &self.accountant { accountant.release(std::mem::take(&mut self.payload)); },
                1 => drop(self.writer_use.take()),
                2 => drop(self.accountant.take()),
                3 => drop(self.refund.take()),
                _ => unreachable!(),
            }
        }
    }
}
impl Drop for ReservationCleanup { fn drop(&mut self) { self.finish(); } }

/// An owned database-accountant charge used by suspendable read operations.
///
/// Unlike the short-lived call-site pairs used by the eager executor, this
/// reservation can grow as a pull source retains continuation state and can
/// outlive the stack frame that opened a cursor. Dropping it returns the
/// complete charge exactly once.
#[derive(Debug)]
pub(crate) struct OwnedMemoryReservation {
    accountant: Option<Arc<MemoryAccountant>>,
    bytes: usize,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
    own_control: Option<ControlCredit>,
}

/// Pre2-shaped credit for values whose existing enclosing owner retains the
/// writer use through payload destruction. Conversion transfers admitted
/// bytes exactly once; it never releases and readmits them.
pub(crate) struct ContainedMemoryReservation {
    accountant: Arc<MemoryAccountant>,
    bytes: usize,
}
impl ContainedMemoryReservation {
    pub(crate) fn from_owned(mut reservation: OwnedMemoryReservation) -> Self {
        let bytes = std::mem::take(&mut reservation.bytes);
        Self { accountant: reservation.accountant().clone(), bytes }
    }
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn bytes(&self) -> usize { self.bytes }
}
impl Drop for ContainedMemoryReservation {
    fn drop(&mut self) { self.accountant.release(self.bytes); }
}

/// Admission transferred from a displaced ordinary write into mutation undo.
#[derive(Debug)]
pub(crate) struct TransferredRowReservation {
    accountant: Option<Arc<MemoryAccountant>>,
    bytes: AtomicUsize,
    _writer_use: Option<crate::memory_accounting::WriterUse>,
    own_control: Option<ControlCredit>,
}
impl TransferredRowReservation {
    pub(crate) fn adopt(&self, bytes: usize) {
        self.bytes.store(bytes, Ordering::Relaxed);
    }
    pub(crate) fn prepare_control(accountant: &Arc<MemoryAccountant>) -> contextdb_core::Result<ControlCredit> {
        ControlCredit::admit(accountant, writer_adapter_type_layout::<Self>().size(), "transferred_row_control")
    }
    pub(crate) fn from_admitted_with_control(
        accountant: Arc<MemoryAccountant>, bytes: usize, control: ControlCredit,
    ) -> Arc<Self> {
        Arc::new(Self {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&accountant),
            accountant: Some(accountant), bytes: AtomicUsize::new(bytes),
            own_control: Some(control),
        })
    }
    pub(crate) fn try_from_admitted(accountant: Arc<MemoryAccountant>, bytes: usize) -> contextdb_core::Result<Arc<Self>> {
        // Admission failure leaves the original payload's credit untouched.
        let control = Self::prepare_control(&accountant)?;
        Ok(Self::from_admitted_with_control(accountant, bytes, control))
    }
}
impl contextdb_tx::StagedPostingCharge for TransferredRowReservation {
    fn retained_bytes(&self) -> usize { self.bytes.load(Ordering::Relaxed) }
    fn transfer_back(&self) -> usize { self.bytes.swap(0, Ordering::Relaxed) }
}
impl Drop for TransferredRowReservation {
    fn drop(&mut self) {
        let refund = self.own_control.take().map(ControlCredit::prepare);
        let mut cleanup = ReservationCleanup::new(
            self.accountant.take(), self._writer_use.take(), self.bytes.swap(0, Ordering::Relaxed), refund,
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
impl contextdb_tx::StagedPostingCharge for OwnedMemoryReservation {
    fn retained_bytes(&self) -> usize {
        self.bytes()
    }
    fn reserve_copy(
        &self,
        bytes: usize,
    ) -> contextdb_core::Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        Self::try_new_for(
            self.accountant().clone(), bytes, "relational", "statement_overlay",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?.into_admitted_arc().map(|owner| owner as Arc<dyn contextdb_tx::StagedPostingCharge>)
    }
}

impl OwnedMemoryReservation {
    fn accountant(&self) -> &Arc<MemoryAccountant> {
        self.accountant.as_ref().expect("live reservation has its accountant")
    }
    pub(crate) fn with_enclosing_arc_control<T>(mut self, retained_startup: bool) -> contextdb_core::Result<Self> {
        assert!(self.own_control.is_none(), "one reservation owns one enclosing control");
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
        self.own_control = Some(ControlCredit::admit(self.accountant(), layout.size(), "reservation_control")?);
        Ok(Arc::new(self))
    }
    pub(crate) fn into_admitted_mutex_arc(self) -> contextdb_core::Result<Arc<parking_lot::Mutex<Self>>> {
        let mut value = parking_lot::Mutex::new(self);
        let layout = writer_adapter_layout(&value);
        let reservation = value.get_mut();
        reservation.own_control = Some(ControlCredit::admit(reservation.accountant(), layout.size(), "reservation_mutex_control")?);
        Ok(Arc::new(value))
    }
    pub(crate) fn from_admitted(accountant: Arc<MemoryAccountant>, bytes: usize) -> Self {
        Self { _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
accountant: Some(accountant), bytes, own_control: None }
    }
    pub(crate) fn transfer_to_staged(mut self) {
        self.bytes = 0;
    }

    pub(crate) fn new(accountant: Arc<MemoryAccountant>) -> Self {
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
        accountant: Arc<MemoryAccountant>,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(Self { _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(accountant)),
accountant: Some(accountant), bytes, own_control: None })
    }
}

impl Drop for OwnedMemoryReservation {
    fn drop(&mut self) {
        let refund = self.own_control.take().map(ControlCredit::prepare);
        let mut cleanup = ReservationCleanup::new(
            self.accountant.take(), self._writer_use.take(), std::mem::take(&mut self.bytes), refund,
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
        Self {
            limit: Arc::new(AtomicUsize::new(0)),
            used: Arc::new(WriterCounter::default()),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
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

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            limit: Arc::new(AtomicUsize::new(bytes)),
            used: Arc::new(WriterCounter::default()),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(bytes),
            has_ceiling: AtomicBool::new(true),
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

    /// A persisted live setting, not a process-start ceiling. Reopening a
    /// database must restore enforcement without turning later durable SETs
    /// into forbidden attempts to cross a ceiling the operator never passed.
    pub(crate) fn with_runtime_budget(bytes: usize) -> Self {
        Self {
            limit: Arc::new(AtomicUsize::new(bytes)),
            used: Arc::new(WriterCounter::default()),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
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
        Self::new_engine_writer(Self::with_budget(bytes))
    }
    pub(crate) fn writer_no_limit() -> contextdb_core::Result<WriterBudget> {
        Self::new_engine_writer(Self::no_limit())
    }
    fn new_engine_writer(accountant: Self) -> contextdb_core::Result<WriterBudget> {
        let bytes = writer_adapter_layout(&accountant).size()
            + 2 * writer_adapter_layout(&AtomicUsize::new(0)).size();
        accountant.charge_retained_under_admission(bytes);
        WriterBudget::from_accountant_with_backing(Arc::new(accountant), bytes)
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn writer_fixed_ownership(&self) -> (usize, usize, usize) {
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        self.writer_identity.get().and_then(|key| domain.lookup_in(*key)).map_or((0, 0, 0), |record| (
            Layout::new::<WriterRegistration>().size(),
            record.engine_backing_bytes.load(Ordering::Relaxed),
            record.adapter_control_bytes.load(Ordering::Relaxed),
        ))
    }
    fn change_adapter_control_receipt(&self, bytes: usize, adding: bool) {
        assert!(self.exclusive_activation().is_none(), "control allocation is outside borrowed activation");
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if let Some(record) = self.writer_identity.get().and_then(|key| domain.lookup_in(*key)) {
            record.adapter_control_bytes.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if adding { current.checked_add(bytes) } else { current.checked_sub(bytes) }
            }).expect("adapter control receipt arithmetic must be owned");
        }
    }
    // A ControlCredit already captured its original registration. Refund
    // custody needs only that accountant's source counter, never a new epoch.
    fn original_control_counter(&self) -> Arc<WriterCounter> {
        assert!(self.exclusive_activation().is_none(), "control cleanup is outside borrowed activation");
        self.used.clone()
    }
    fn prepare_control_refund(&self) -> (Arc<WriterCounter>, Option<(usize, usize)>) {
        assert!(self.exclusive_activation().is_none(), "control cleanup is outside borrowed activation");
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        let registration = self.writer_identity.get().and_then(|key|
            domain.lookup_in(*key).map(|record| (*key, record.epoch)));
        (self.used.clone(), registration)
    }
    pub(crate) fn retain_box_control_refund(&self, bytes: usize) -> Arc<WriterCounter> {
        self.change_adapter_control_receipt(bytes, false);
        self.used.clone()
    }
    pub(crate) fn admit_box_control(&self, bytes: usize, operation: &str, hint: &str) -> contextdb_core::Result<()> {
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
        ACTIVATING_WRITER.with(|slot| slot.get().filter(|(id, _)| *id == address).map(|(_, bytes)| bytes))
    }
    // Private admissions derived from counted custody must use its original
    // tag. The retained tagged handle prevents epoch replacement; a reader
    // handle intentionally ignores any later association of this accountant.
    fn writer_admission_for_provenance(&self, provenance: Option<&StoreAccountantHandle>) -> WriterAdmission {
        let Some(handle) = provenance else { return self.writer_admission(); };
        assert!(std::ptr::eq(self, &**handle), "admission retains its original accountant");
        assert!(self.exclusive_activation().is_none(), "registry admission is outside borrowed activation");
        let gate = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if handle.is_writer() {
            let key = *self.writer_identity.get().expect("tagged handle has its original key");
            gate.lookup_in(key).expect("tagged handle pins its original epoch");
        }
        let ceiling = gate.ceiling.filter(|_| self.used.is_member());
        WriterAdmission { _gate: Some(gate), ceiling }
    }
    fn writer_admission(&self) -> WriterAdmission {
        if self.exclusive_activation().is_some() {
            return WriterAdmission { _gate: None, ceiling: None };
        }
        let gate = PROCESS_WRITER_BUDGET_DOMAIN.read();
        let ceiling = gate.ceiling.filter(|_| self.used.is_member());
        WriterAdmission { _gate: Some(gate), ceiling }
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
        let identity = *self.writer_identity.get().expect("resident insertion requires a stable association");
        domain.lookup_in(identity).expect("resident insertion requires a retained writer use");
        if domain.native.is_some() { return Ok(false); }
        let mut next = domain.writers.as_deref();
        while let Some(record) = next {
            let limit = record.limit.load(Ordering::SeqCst);
            let used = record.used.load(Ordering::SeqCst);
            // Activation has no private transition allowance on other writers.
            let releasing = if record.stable_identity == identity { old_workspace } else { 0 };
            let requested = if record.stable_identity == identity { complete_peak } else { 0 };
            let local = used.checked_sub(releasing)
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
            .ok_or_else(|| contextdb_core::Error::Other("runtime activation combined size overflow".into()))?;
        if after > ceiling {
            return Ok(false);
        }
        let previous = ACTIVATING_WRITER.with(|slot| slot.replace(Some((self as *const Self as usize, native_bytes))));
        let scope = ActivationScope(previous);
        admit_workspace()?;
        drop(scope);
        PROCESS_WRITER_AGGREGATE.fetch_add(native_bytes, Ordering::SeqCst);
        domain.ceiling = Some(ceiling);
        domain.native = Some(ResidentInsertionRuntime {
            width, funder_identity: Arc::as_ptr(&self.used) as usize,
            reservation: ProcessNativeReservation { bytes: native_bytes },
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
        let process = admission.process_available().map(|room| room.saturating_add(bytes));
        min_available(local, process)
    }

    /// Step 2 of admission, run only for bytes this writer's own limit has
    /// already admitted (step 1). A member's bytes join the process aggregate;
    /// once a ceiling is recorded, they must fit it. A failed aggregate CAS
    /// rolls back ONLY the local counter: those bytes never entered the
    /// aggregate, the one member decrement that does not refund it.
    fn admit_to_process_aggregate(
        &self, admission: &WriterAdmission, bytes: usize, operation: &str, hint: &str,
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
    fn try_allocate_with_provenance(&self, bytes: usize, provenance: Option<&StoreAccountantHandle>) -> contextdb_core::Result<()> {
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
                published
                    .saturating_add(self.private_transition_headroom())
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
                    if self.fire_accounting_hook_without_gate_for_provenance(&mut admission, provenance, false) { continue; }
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
            if self.fire_accounting_hook_without_gate_for_provenance(&mut admission, provenance, true) { continue; }
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
                    current
                        .saturating_add(self.private_transition_headroom())
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
                self.admit_to_process_aggregate(&admission, bytes, "allocate",
                    "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT.")?;
                #[cfg(feature = "test-seams")]
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                #[cfg(feature = "test-seams")]
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
                published
                    .saturating_add(self.private_transition_headroom())
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
                current
                    .saturating_add(self.private_transition_headroom())
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
                self.admit_to_process_aggregate(&admission, new_bytes - old_bytes, operation, hint)?;
            } else if self.used.is_member() {
                aggregate_refund(old_bytes - new_bytes);
            }
            #[cfg(feature = "test-seams")]
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
        #[cfg(feature = "test-seams")]
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

    /// Hold what a whole arriving schema unit was priced at until that unit is
    /// installed or refused.
    ///
    /// The bytes are admitted the ordinary way, so a unit that does not fit is
    /// refused here and nothing of it is applied. What the reservation adds is
    /// that the room it found is still there when the unit's items take it: an
    /// allocation that lands between this call and the unit's first item can no
    /// longer spend the headroom the unit was just promised.
    pub(crate) fn reserve_schema_unit(
        self: &Arc<Self>,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<SchemaUnitReservation> {
        self.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(SchemaUnitReservation {
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&(Arc::clone(self))),
accountant: Arc::clone(self),
            remaining: std::cell::Cell::new(bytes),
        })
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
        #[cfg(feature = "test-seams")]
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
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn reset_high_water_for_test(&self) {
        self.high_water_for_test
            .store(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Peak successfully admitted bytes, including allocations already released.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn high_water_for_test(&self) -> usize {
        self.high_water_for_test.load(Ordering::SeqCst)
    }

    /// Successful byte admissions at this accountant, on every calling thread.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn admitted_bytes_for_test(&self) -> usize {
        self.admitted_bytes_for_test.load(Ordering::SeqCst)
    }

    /// Bytes this accountant took through `charge_retained`, the writer's
    /// unconditional retention door. A read session's accountant must report
    /// zero here: a committed image never appends to a change log, and every
    /// byte of the image it does hold was already admitted when it was
    /// decoded.
    #[cfg(feature = "test-seams")]
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
            #[cfg(feature = "test-seams")]
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
    fn available_for_provenance(&self, provenance: Option<&StoreAccountantHandle>) -> Option<usize> {
        let admission = self.writer_admission_for_provenance(provenance);
        min_available(
            writer_headroom(self.limit.load(Ordering::SeqCst), self.used.load(Ordering::SeqCst), 0, 0)
                .available,
            admission.process_available(),
        )
    }
    fn try_allocate_for_provenance(
        &self, bytes: usize, subsystem: &str, operation: &str, hint: &str,
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
                hint: "Injected current vector identity publication admission exhaustion.".to_owned(),
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
        #[cfg(feature = "test-seams")]
        if allocation.is_ok() {
            note_budget_admission(subsystem, operation, bytes, self.used.load(Ordering::SeqCst));
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
                let short = || bytes.saturating_sub(self.available_for_provenance(provenance).unwrap_or(usize::MAX));
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
            let short = bytes.saturating_sub(self.available_for_provenance(provenance).unwrap_or(usize::MAX));
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
    accountant: Option<Arc<MemoryAccountant>>,
    layout: Layout,
    _writer_use: Option<WriterUse>,
}
impl WriterVectorBudget {
    fn accountant(&self) -> &Arc<MemoryAccountant> {
        self.accountant.as_ref().expect("live adapter has its accountant")
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
        // Only std's AtomicUsize Arc destruction and ArcInner deallocation
        // follow; all payload fields are empty or Copy.
    }
}
impl contextdb_core::read_memory::ReadMemoryBudget for WriterVectorBudget {
    fn try_reserve(&self, bytes: usize, subsystem: &'static str, operation: &'static str, hint: &'static str) -> contextdb_core::Result<()> {
        self.accountant().try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) { self.accountant().release(bytes); }
}
impl contextdb_vector::MemoryBudget for WriterVectorBudget {
    fn stable_funder_identity(&self) -> usize {
        Arc::as_ptr(self.accountant()) as usize
    }
    fn transition_reservation_for(&self, old: usize, new: usize, subsystem: &str, operation: &str, hint: &str) -> contextdb_core::Result<()> {
        contextdb_vector::MemoryBudget::transition_reservation_for(&**self.accountant(), old, new, subsystem, operation, hint)
    }
    fn try_allocate_for(&self, bytes: usize, subsystem: &str, operation: &str, hint: &str) -> contextdb_core::Result<()> {
        self.accountant().try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) { self.accountant().release(bytes); }
    fn available_bytes(&self) -> Option<usize> { self.accountant().usage().available }
    fn adopt_caller_reservation(&self, bytes: usize) -> contextdb_core::Result<bool> { adopt_vector_load_reservation(self.accountant(), bytes) }
}
impl MemoryAccountant {
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn writer_registration_observation_for_test(&self) -> (usize, usize, usize, usize, usize) {
        let domain = PROCESS_WRITER_BUDGET_DOMAIN.read();
        if let Some(record) = self.writer_identity.get().and_then(|key| domain.lookup_in(*key)) {
            return (record as *const WriterRegistration as usize, Layout::new::<WriterRegistration>().size(), record.epoch, record.users, Arc::as_ptr(&self.used) as usize);
        }
        (0, 0, 0, 0, Arc::as_ptr(&self.used) as usize)
    }
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn writer_vector_adapter_type_layout_for_test() -> Layout {
        writer_adapter_type_layout::<WriterVectorBudget>()
    }
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn writer_vector_adapter_layout_for_test(self: &Arc<Self>) -> Layout {
        crate::persistence::returned_loader_rows_for_test();
        crate::persistence::content_identity::proof_allocation_oracle_tests::r1_mapped_control_rows();
        let adapter = WriterVectorBudget { accountant: Some(self.clone()), layout: Layout::new::<()>(), _writer_use: WriterUse::retain_if_writer(self) };
        writer_adapter_layout(&adapter)
    }
    fn budget_adapter(self: &Arc<Self>, writer_use: WriterUse, open_residue: bool) -> contextdb_core::Result<Arc<WriterVectorBudget>> {
        let mut adapter = WriterVectorBudget {
            accountant: Some(self.clone()), layout: Layout::new::<()>(),
            _writer_use: Some(writer_use),
        };
        let layout = writer_adapter_layout(&adapter);
        if open_residue {
            assert!(layout.align() > 1);
            admit_open_residue(layout.size(), "assembly_budget_adapter_open_residue")?;
            adapter.layout = Layout::from_size_align(layout.size(), 1).expect("receipt size is already valid");
        } else {
            self.try_allocate_for(layout.size(), "writer", "adapter_control", "Raise MEMORY_LIMIT for the writer adapter.")?;
            adapter.layout = layout;
            self.change_adapter_control_receipt(layout.size(), true);
        }
        Ok(Arc::new(adapter))
    }
    fn vector_budget_for_startup(self: &Arc<Self>) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        let writer_use = WriterUse::retain_if_writer(self).expect("construction root retains the writer epoch");
        Ok(self.budget_adapter(writer_use, true)?)
    }
    pub(crate) fn vector_budget(self: &Arc<Self>) -> contextdb_core::Result<Arc<dyn contextdb_vector::MemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(writer_use, false)?),
            None => Ok(self.clone()),
        }
    }
    pub(crate) fn assembly_read_budget(self: &Arc<Self>) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(writer_use, true)?),
            None => Ok(self.clone()),
        }
    }
    pub(crate) fn read_budget(self: &Arc<Self>) -> contextdb_core::Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        match WriterUse::retain_if_writer(self) {
            Some(writer_use) => Ok(self.budget_adapter(writer_use, false)?),
            None => Ok(self.clone()),
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
    fn fire_accounting_hook_without_gate(&self, admission: &mut WriterAdmission, success: bool) -> bool {
        self.fire_accounting_hook_without_gate_for_provenance(admission, None, success)
    }
    #[cfg(test)]
    fn fire_accounting_hook_without_gate_for_provenance(
        &self, admission: &mut WriterAdmission, provenance: Option<&StoreAccountantHandle>, success: bool,
    ) -> bool {
        if self.exclusive_activation().is_some() { return false; }
        let ceiling_before = admission.ceiling;
        drop(admission._gate.take());
        if success { self.fire_admission_race_hook(); } else { self.fire_alloc_race_hook(); }
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
        #[cfg(feature = "test-seams")]
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
        #[cfg(feature = "test-seams")]
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
        #[cfg(feature = "test-seams")]
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

        let first = accountant
            .reserve_schema_unit(100_000, "sync", "received_schema_unit", "")
            .expect("the first unit's price fits");
        let second = accountant
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
    pub(crate) fn new(accountant: Arc<MemoryAccountant>) -> Self {
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
        let mut owner = RedbAllocatorStateCharge::new(Arc::clone(&accountant));
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
