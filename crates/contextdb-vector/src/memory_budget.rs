use contextdb_core::Result;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Opaque accounting capability consumed by vector storage. The database
/// engine owns the implementation and never returns that implementation to
/// callers; this crate can reserve and release bytes without exposing a
/// durable-limit mutation handle.
#[doc(hidden)]
pub trait MemoryBudget: Debug + Send + Sync {
    /// Retained consumers keep this original fixed invoice outside the budget Arc.
    #[doc(hidden)]
    fn control_invoice(&self) -> Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>> { None }

    /// Lifetime-bounded identity of the funder retained by this capability.
    /// The default preserves the existing trait-object data-address identity.
    #[doc(hidden)]
    fn stable_funder_identity(&self) -> usize {
        self as *const Self as *const () as usize
    }

    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()>;

    fn release(&self, bytes: usize);

    /// Atomically replace the caller's held charge. Delta-based budgets can
    /// use the default: it never exposes the old charge or adds old + new.
    /// A shared budget must validate growth against its published limit.
    fn transition_reservation_for(
        &self,
        old_bytes: usize,
        new_bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if new_bytes > old_bytes {
            self.try_allocate_for(new_bytes - old_bytes, subsystem, operation, hint)?;
        } else if new_bytes < old_bytes {
            self.release(old_bytes - new_bytes);
        }
        Ok(())
    }

    /// Bytes currently available under a finite shared budget. Unlimited or
    /// request-local budgets return `None`, so residency is retained.
    fn available_bytes(&self) -> Option<usize> {
        None
    }

    /// Move bytes already charged to the active caller into a retained store
    /// owner without touching the shared total. Budgets with no caller-bound
    /// transfer return `false`, and loaders reserve those bytes themselves.
    fn adopt_caller_reservation(&self, _bytes: usize) -> Result<bool> {
        Ok(false)
    }
}

pub(crate) fn same_funder(left: &dyn MemoryBudget, right: &dyn MemoryBudget) -> bool {
    left.stable_funder_identity() == right.stable_funder_identity()
}

/// An owned, already admitted charge supplied by the executing caller.
/// Ordinary drop releases its exact retained bytes. `transfer` disarms that
/// release only after another owner has adopted the complete reservation.
#[doc(hidden)]
pub trait PrepaidVectorCopyCharge: Send + Sync {
    fn accountant(&self) -> Arc<dyn MemoryBudget>;
    fn retained_bytes(&self) -> usize;
    fn transfer(self: Box<Self>);
}

/// Explicit caller-owned admission for materialized vector copies.
/// Construct this with the executing transaction or read working-set accountant.
/// Keep it alive until every output charged to it has been destroyed; returning
/// an output transfers that lifetime obligation to the caller, never to a store.
#[doc(hidden)]
pub struct VectorCopyAdmission {
    accountant: Arc<dyn MemoryBudget>,
    prepaid: Option<Box<dyn PrepaidVectorCopyCharge>>,
    prepaid_bytes: usize,
    prepaid_control_bytes: usize,
    additional_bytes: usize,
    used_bytes: usize,
    historical_durable_reads: bool,
    raw_only_sources: bool,
    control_invoice: Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}

impl Debug for VectorCopyAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorCopyAdmission")
            .field("accountant", &self.accountant)
            .field("prepaid_bytes", &self.prepaid_bytes)
            .field("additional_bytes", &self.additional_bytes)
            .field("used_bytes", &self.used_bytes)
            .finish_non_exhaustive()
    }
}

impl VectorCopyAdmission {
    pub fn new(accountant: Arc<dyn MemoryBudget>) -> Self {
        let control_invoice = accountant.control_invoice();
        Self {
            accountant,
            control_invoice,
            prepaid: None,
            prepaid_bytes: 0,
            prepaid_control_bytes: 0,
            additional_bytes: 0,
            used_bytes: 0,
            historical_durable_reads: false,
            raw_only_sources: false,
        }
    }

    /// Consume the caller's real prepaid owner without admitting it again.
    /// A mismatched accountant refuses and drops the supplied charge normally.
    pub fn from_prepaid<C: PrepaidVectorCopyCharge + 'static>(
        accountant: Arc<dyn MemoryBudget>,
        charge: C,
    ) -> Result<Self> {
        if !same_funder(accountant.as_ref(), charge.accountant().as_ref()) {
            return Err(contextdb_core::Error::Other(
                "vector copy prepaid charge has a different accountant".into()));
        }
        let prepaid_bytes = charge.retained_bytes();
        let prepaid_control_bytes = std::mem::size_of::<C>();
        accountant.try_allocate_for(prepaid_control_bytes, "vector_copy", "prepaid_control",
            "Raise reader memory to fund the retained copy credit.")?;
        let control_invoice = accountant.control_invoice();
        Ok(Self {
            accountant,
            control_invoice,
            prepaid: Some(Box::new(charge)),
            prepaid_bytes,
            prepaid_control_bytes,
            additional_bytes: 0,
            used_bytes: 0,
            historical_durable_reads: false,
            raw_only_sources: false,
        })
    }

    /// Transferable copy reservation, including unused prepaid capacity.
    /// The token's own control allocation is separate and never transferred.
    pub fn retained_bytes(&self) -> usize {
        self.prepaid_bytes + self.additional_bytes
    }

    /// Report one retained credit and whether its actual funder is the caller.
    /// Compare retained funders independently of their capability allocations.
    /// The callback receives scalar observations and cannot obtain the funder.
    #[doc(hidden)]
    pub fn visit_retained_copy_credit(
        credit: &Arc<Self>,
        caller: &dyn MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        let funded_by_caller = same_funder(credit.accountant.as_ref(), caller);
        visit(
            Arc::as_ptr(credit) as usize,
            credit.retained_bytes(),
            funded_by_caller,
        );
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<()> {
        self.reserve_for(
            bytes,
            "vector_copy",
            "materialize",
            "Reduce the selected copy or raise the executing statement's memory limit.",
        )
    }

    #[doc(hidden)]
    pub fn reserve_for(
        &mut self,
        bytes: usize,
        component: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        let used_bytes = self
            .used_bytes
            .checked_add(bytes)
            .ok_or_else(|| contextdb_core::Error::Other("vector copy admission overflow".into()))?;
        let additional_bytes = used_bytes.saturating_sub(self.prepaid_bytes);
        if additional_bytes > self.additional_bytes {
            self.accountant.try_allocate_for(
                additional_bytes - self.additional_bytes,
                component,
                operation,
                hint,
            )?;
        }
        self.additional_bytes = additional_bytes;
        self.used_bytes = used_bytes;
        Ok(())
    }

    /// Consume this token after the destination has adopted `retained_bytes()`.
    /// No allocation or release occurs here; the destination now releases it.
    pub fn transfer(mut self) {
        if let Some(charge) = self.prepaid.take() {
            charge.transfer();
        }
        self.prepaid_bytes = 0;
        self.additional_bytes = 0;
    }

    pub(crate) fn accountant(&self) -> &Arc<dyn MemoryBudget> {
        &self.accountant
    }

    /// A prepaid owner already holds its copy capacity until transfer/drop.
    #[doc(hidden)]
    pub fn is_prepaid(&self) -> bool {
        self.prepaid.is_some()
    }

    /// Local copies reuse prepaid headroom; other owners get a scoped charge.
    #[doc(hidden)]
    pub fn local_scratch(&self) -> Option<Self> {
        (!self.is_prepaid()).then(|| self.scratch())
    }

    /// Borrow the admission's actual funder for deterministic ownership proofs.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn accountant_for_test(&self) -> &Arc<dyn MemoryBudget> {
        &self.accountant
    }

    /// The executing operation's actual funder. An operation-owned loader
    /// keeps it so the residency it returns is charged to, and released by,
    /// the same reader that admitted its load, never a store-wide parent.
    #[doc(hidden)]
    pub fn funder(&self) -> Arc<dyn MemoryBudget> {
        Arc::clone(&self.accountant)
    }

    #[doc(hidden)]
    pub fn scratch(&self) -> Self {
        let mut scratch = Self::new(self.accountant.clone());
        scratch.historical_durable_reads = self.historical_durable_reads;
        scratch.raw_only_sources = self.raw_only_sources;
        scratch
    }

    /// Internal execution intent for a route already resolved to exact. Raw
    /// membership/root capture remains required; graph suppliers do not exist
    /// on this route and must not be constructed and immediately discarded.
    #[doc(hidden)]
    pub fn with_raw_only_sources<T>(&mut self, raw_only: bool,
        operation: impl FnOnce(&mut Self) -> T) -> T {
        struct Restore<'a> { owner: &'a mut VectorCopyAdmission, previous: bool }
        impl Drop for Restore<'_> {
            fn drop(&mut self) { self.owner.raw_only_sources = self.previous; }
        }
        let previous = self.raw_only_sources;
        self.raw_only_sources = raw_only;
        let restore = Restore { owner: self, previous };
        operation(restore.owner)
    }
    pub(crate) fn uses_only_raw_sources(&self) -> bool { self.raw_only_sources }

    /// Route durable reads through the operation's captured authority. This
    /// routing preference does not itself authenticate a capture or a body.
    #[doc(hidden)]
    pub fn with_historical_durable_reads<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        struct Restore<'a> {
            owner: &'a mut VectorCopyAdmission,
            prior: bool,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.owner.historical_durable_reads = self.prior;
            }
        }
        let prior = self.historical_durable_reads;
        self.historical_durable_reads = true;
        let restore = Restore { owner: self, prior };
        operation(restore.owner)
    }

    pub(crate) fn historical_durable_reads(&self) -> bool {
        self.historical_durable_reads
    }
}

/// A local copy and only its own charge. Field order frees the value first.
/// Returned and retained outputs keep their existing caller-owned admissions.
#[doc(hidden)]
pub struct ScopedVectorCopy<T> {
    value: T,
    _admission: Option<VectorCopyAdmission>,
}

impl<T> ScopedVectorCopy<T> {
    /// A native value and its per-value charge. Declaration order ensures
    /// the complete decoded value drops before the admission is released.
    #[doc(hidden)]
    pub fn from_admitted(value: T, admission: Option<VectorCopyAdmission>) -> Self {
        Self {
            value,
            _admission: admission,
        }
    }

    /// Transform while source and output coexist under the same admitted
    /// window; on every exit the source buffer drops before its token.
    #[doc(hidden)]
    pub fn try_map<U>(
        self,
        materialize: impl FnOnce(&T) -> Result<U>,
    ) -> Result<ScopedVectorCopy<U>> {
        let value = materialize(&self.value)?;
        drop(self.value);
        Ok(ScopedVectorCopy {
            value,
            _admission: self._admission,
        })
    }

    /// Only for provider paths whose allocation is already funded elsewhere.
    #[doc(hidden)]
    pub fn into_already_funded(self) -> Result<T> {
        if self._admission.is_some() {
            return Err(contextdb_core::Error::Other(
                "admitted value cannot detach its charge".into()));
        }
        Ok(self.value)
    }

    #[doc(hidden)]
    pub fn new(
        owner: &mut VectorCopyAdmission,
        materialize: impl FnOnce(&mut VectorCopyAdmission) -> Result<T>,
    ) -> Result<Self> {
        let mut admission = owner.local_scratch();
        let value = materialize(admission.as_mut().unwrap_or(owner))?;
        Ok(Self {
            value,
            _admission: admission,
        })
    }
}

impl<T> std::ops::Deref for ScopedVectorCopy<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for ScopedVectorCopy<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Workspace-internal layout copy; public layout fields remain unchanged.
#[doc(hidden)]
pub fn scoped_index_layout(
    store: &crate::VectorStore,
    index: &contextdb_core::VectorIndexRef,
    owner: &mut VectorCopyAdmission,
) -> Result<ScopedVectorCopy<crate::store::VectorIndexLayout>> {
    ScopedVectorCopy::new(owner, |scratch| store.index_layout(index, scratch))
}

impl<T> ScopedVectorCopy<Vec<T>> {
    #[allow(clippy::should_implement_trait)] // Conversion must retain this scoped copy's admission token.
    pub fn into_iter(self) -> ScopedVectorCopy<std::vec::IntoIter<T>> {
        ScopedVectorCopy {
            value: self.value.into_iter(),
            _admission: self._admission,
        }
    }
}

impl<I: Iterator> Iterator for ScopedVectorCopy<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.value.next()
    }
}

impl<'a, T> IntoIterator for &'a ScopedVectorCopy<Vec<T>> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.value.iter()
    }
}

impl Drop for VectorCopyAdmission {
    fn drop(&mut self) {
        // Its original payload invoice refunds itself, but the Box holding
        // that token also needs admission through its own physical free.
        drop(self.prepaid.take());
        if self.additional_bytes != 0 {
            self.accountant.release(self.additional_bytes);
        }
        if self.prepaid_control_bytes != 0 {
            self.accountant.release(self.prepaid_control_bytes);
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)] // Preserve the no-op budget implementation for explicit fallback routes.
struct UnlimitedMemoryBudget;

impl MemoryBudget for UnlimitedMemoryBudget {
    fn try_allocate_for(
        &self,
        _bytes: usize,
        _subsystem: &str,
        _operation: &str,
        _hint: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn release(&self, _bytes: usize) {}
}

#[allow(dead_code)] // Preserve the no-op budget constructor for explicit fallback routes.
pub(crate) fn unlimited_memory_budget() -> Arc<dyn MemoryBudget> {
    Arc::new(UnlimitedMemoryBudget)
}

/// One admitted vector workspace.  The counter and accountant reservation
/// move together and are both returned by this value's single `Drop`.
#[doc(hidden)]
pub struct VectorWorkspaceReservation {
    accountant: Arc<dyn MemoryBudget>,
    live_bytes: Arc<AtomicUsize>,
    bytes: usize,
    _control_invoice: Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}

impl VectorWorkspaceReservation {
    pub fn accountant(&self) -> &Arc<dyn MemoryBudget> {
        &self.accountant
    }

    /// Complete capacity held by this workspace's actual funder.
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }

    /// The destination has adopted this funding. End workspace residency
    /// without returning the same funding a second time.
    pub fn transfer(mut self) {
        let previous = self.live_bytes.fetch_sub(self.bytes, Ordering::SeqCst);
        debug_assert!(previous >= self.bytes);
        self.bytes = 0;
    }

    /// Validate a materialization against this already admitted workspace.
    /// The caller retains this reservation until all copied output is dropped.
    pub fn require_capacity(&self, bytes: usize) -> Result<()> {
        require_copy_capacity(self.bytes, bytes)
    }

    pub(crate) fn try_new(
        accountant: Arc<dyn MemoryBudget>,
        live_bytes: Arc<AtomicUsize>,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> Result<Self> {
        accountant.try_allocate_for(bytes, "vector_index", operation, hint)?;
        live_bytes.fetch_add(bytes, Ordering::SeqCst);
        let control_invoice = accountant.control_invoice();
        Ok(Self {
            _control_invoice: control_invoice,
            accountant,
            live_bytes,
            bytes,
        })
    }

    /// Atomically replace this phase's charge after dead owners are dropped
    /// and before new owners allocate. Refusal preserves the old token and
    /// charge; success changes the token and the live workspace counter once.
    #[doc(hidden)]
    pub fn resize(&mut self, bytes: usize, operation: &str, hint: &str) -> Result<()> {
        self.accountant.transition_reservation_for(
            self.bytes,
            bytes,
            "vector_index",
            operation,
            hint,
        )?;
        if bytes > self.bytes {
            let additional = bytes - self.bytes;
            self.live_bytes.fetch_add(additional, Ordering::SeqCst);
        } else if bytes < self.bytes {
            let released = self.bytes - bytes;
            let previous = self.live_bytes.fetch_sub(released, Ordering::SeqCst);
            debug_assert!(previous >= released);
        }
        self.bytes = bytes;
        Ok(())
    }
}

impl PrepaidVectorCopyCharge for VectorWorkspaceReservation {
    fn accountant(&self) -> Arc<dyn MemoryBudget> {
        self.accountant.clone()
    }

    fn retained_bytes(&self) -> usize {
        self.bytes
    }

    fn transfer(self: Box<Self>) {
        VectorWorkspaceReservation::transfer(*self);
    }
}

impl Drop for VectorWorkspaceReservation {
    fn drop(&mut self) {
        let previous = self.live_bytes.fetch_sub(self.bytes, Ordering::SeqCst);
        debug_assert!(previous >= self.bytes);
        if self.bytes != 0 {
            self.accountant.release(self.bytes);
        }
        self.bytes = 0;
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)] // Keep related test helpers and runtime seams together.
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct CountingBudget(AtomicUsize);

    impl MemoryBudget for CountingBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            _subsystem: &str,
            _operation: &str,
            _hint: &str,
        ) -> Result<()> {
            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                    used.checked_add(bytes).filter(|next| *next <= 1_000)
                })
                .map_err(|used| contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: "vector_index".into(),
                    operation: "resize".into(),
                    requested_bytes: bytes,
                    available_bytes: 1_000 - used,
                    budget_limit_bytes: 1_000,
                    hint: "test".into(),
                })?;
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.0.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    #[test]
    fn resizing_workspace_moves_one_charge_between_ownership_phases() {
        // Additive historical-route ownership rows of the existing workspace host.
        // The flag is crate-private; this module reaches it without a forwarding seam.
        let routing_budget = Arc::new(CountingBudget::default());
        let initial = routing_budget.0.load(Ordering::SeqCst);
        let bytes = b"historical routing".len();
        let mut owner = VectorCopyAdmission::new(routing_budget.clone());
        assert!(!owner.historical_durable_reads());
        owner.reserve(bytes).unwrap();
        let retained = routing_budget.0.load(Ordering::SeqCst);
        assert_eq!(retained, initial + bytes);
        owner.with_historical_durable_reads(|marked| {
            assert!(marked.historical_durable_reads());
            let mut scratch = marked.scratch();
            assert!(scratch.historical_durable_reads());
            assert!(Arc::ptr_eq(scratch.accountant(), marked.accountant()));
            scratch.reserve(bytes).unwrap();
            assert_eq!(routing_budget.0.load(Ordering::SeqCst), retained + bytes);
            drop(scratch);
            assert_eq!(routing_budget.0.load(Ordering::SeqCst), retained);
            let local = marked.local_scratch().unwrap();
            assert!(local.historical_durable_reads());
            assert!(Arc::ptr_eq(local.accountant(), marked.accountant()));
            drop(local);
            marked.with_historical_durable_reads(|nested| {
                assert!(nested.historical_durable_reads());
            });
            assert!(marked.historical_durable_reads());
            let failure: Result<()> = marked.with_historical_durable_reads(|nested| {
                assert!(nested.historical_durable_reads());
                Err(contextdb_core::Error::Other("routing control error".into()))
            });
            assert!(failure.is_err());
            assert!(marked.historical_durable_reads());
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                marked.with_historical_durable_reads(|nested| {
                    assert!(nested.historical_durable_reads());
                    panic!("routing control unwind");
                });
            }));
            assert!(panic.is_err());
            assert!(marked.historical_durable_reads());
            assert_eq!(routing_budget.0.load(Ordering::SeqCst), retained);
        });
        assert!(!owner.historical_durable_reads());
        drop(owner);
        assert_eq!(routing_budget.0.load(Ordering::SeqCst), initial);
        let prepaid_live = Arc::new(AtomicUsize::new(0));
        let charge = VectorWorkspaceReservation::try_new(
            routing_budget.clone(),
            prepaid_live.clone(),
            bytes,
            "routing",
            "control",
        )
        .unwrap();
        let mut prepaid =
            VectorCopyAdmission::from_prepaid(routing_budget.clone(), charge).unwrap();
        assert!(!prepaid.historical_durable_reads());
        assert_eq!(routing_budget.0.load(Ordering::SeqCst), initial + bytes);
        prepaid.with_historical_durable_reads(|marked| {
            assert!(marked.local_scratch().is_none());
            marked.reserve(bytes).unwrap();
            assert!(marked.historical_durable_reads());
            assert_eq!(routing_budget.0.load(Ordering::SeqCst), initial + bytes);
        });
        assert!(!prepaid.historical_durable_reads());
        drop(prepaid);
        assert_eq!(routing_budget.0.load(Ordering::SeqCst), initial);
        assert_eq!(prepaid_live.load(Ordering::SeqCst), 0);
        let budget = Arc::new(CountingBudget::default());
        let live = Arc::new(AtomicUsize::new(0));
        let mut reservation =
            VectorWorkspaceReservation::try_new(budget.clone(), live.clone(), 900, "build", "test")
                .unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 900);
        assert_eq!(live.load(Ordering::SeqCst), 900);
        assert!(reservation.resize(1_001, "encode", "test").is_err());
        assert_eq!(
            reservation.bytes, 900,
            "refusal preserves the ownership token"
        );
        assert_eq!(budget.0.load(Ordering::SeqCst), 900);
        assert_eq!(live.load(Ordering::SeqCst), 900);
        reservation.resize(900, "encode", "test").unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 900);

        reservation.resize(300, "persist", "test").unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 300);
        assert_eq!(live.load(Ordering::SeqCst), 300);

        reservation.resize(450, "persist", "test").unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 450);
        assert_eq!(live.load(Ordering::SeqCst), 450);

        drop(reservation);
        assert_eq!(budget.0.load(Ordering::SeqCst), 0);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}

pub(crate) fn require_copy_capacity(capacity: usize, bytes: usize) -> Result<()> {
    if bytes > capacity {
        return Err(contextdb_core::Error::MemoryBudgetExceeded {
            subsystem: "vector_copy".into(),
            operation: "materialize_in_workspace".into(),
            requested_bytes: bytes,
            available_bytes: capacity,
            budget_limit_bytes: capacity,
            hint: "Admit the generation copy before materialization.".into(),
        });
    }
    Ok(())
}
