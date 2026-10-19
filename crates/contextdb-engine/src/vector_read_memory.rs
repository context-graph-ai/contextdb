//! Vector copies use the requesting reader's existing budget capability.
use contextdb_core::{
    Result,
    read_memory::{NativeRefusal, ReadCredit, ReadMemoryBudget, ReadMemoryOwner},
};
use contextdb_vector::{MemoryBudget, VectorCopyAdmission};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ReaderVectorBudget(
    Arc<dyn ReadMemoryBudget>,
    Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
    Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
);

impl MemoryBudget for ReaderVectorBudget {
    fn control_invoice(&self) -> Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>> {
        self.2.clone()
    }
    /// A refusal names the admission that asked for the bytes, so a reader
    /// can tell which one its declared memory could not hold.
    fn try_allocate_for(
        &self,
        bytes: usize,
        component: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        let result = self.0
            .try_reserve(
                bytes,
                "vector_copy",
                "materialize",
                "Reduce the selected copy or raise the executing reader's memory limit.",
            )
            .map_err(|error| match error {
                contextdb_core::Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    ..
                } => contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: (component.to_owned()).into(),
                    operation: (operation.to_owned()).into(),
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint: (hint.to_owned()).into(),
                },
                other => other,
            });
        #[cfg(any(test, feature = "test-seams"))]
        if result.is_ok() {
            let identity = Arc::as_ptr(&self.0) as *const () as usize;
            READER_BUDGET_LEDGER.with(|ledger| {
                if let Some(entries) = ledger.borrow_mut().as_mut() {
                    if let Some(event) = entries.last_mut().filter(|event|
                        event.owner_identity == identity && event.what == "vector_copy/materialize") {
                        event.what = format!("{component}/{operation}");
                    }
                }
            });
        }
        result
    }

    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}

/// Adapt the executing READ once, so copy admission and search share its capability.
pub(crate) fn budget(read_memory: Arc<dyn ReadMemoryBudget>) -> Arc<dyn MemoryBudget> {
    Arc::new(ReaderVectorBudget(read_memory, None, None))
}

pub fn admission(read_memory: Arc<dyn ReadMemoryBudget>) -> VectorCopyAdmission {
    VectorCopyAdmission::new(budget(read_memory))
}

/// The actual cyclic request has already admitted both adapter layouts.
/// The read adapter's original keeper is outside its raw Arc field; the vector
/// adapter's keeper travels outside its Arc in every retained copy admission.
pub(crate) fn admission_prepaid(
    read_memory: Arc<dyn ReadMemoryBudget>,
    read_invoice: contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>,
    vector_invoice: contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>,
) -> VectorCopyAdmission {
    VectorCopyAdmission::new(Arc::new(ReaderVectorBudget(
        read_memory, Some(read_invoice), Some(vector_invoice))))
}

pub trait ReadRequestBudgetProvider: std::fmt::Debug + Send + Sync {
    fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>>;

    /// Existing host providers acquire their exact legacy capability once.
    #[doc(hidden)]
    fn request_budget_owner(&self) -> Result<RequestReadOwner> {
        self.request_budget().map(RequestReadOwner::legacy)
    }

    /// The acquisition a native route makes. A provider that selects the
    /// Native route overrides this so its refusal is a `NativeRefusal` with no
    /// owned diagnostic; the default keeps today's acquisition, so a Legacy
    /// provider still selects Legacy and refuses on its Legacy terms.
    #[doc(hidden)]
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<RequestReadOwner, RequestOwnerFailure> {
        self.request_budget_owner()
            .map_err(RequestOwnerFailure::Legacy)
    }
}

/// Why a native route could not acquire its request owner: a native
/// provider's allocation-free refusal, or a Legacy provider's own error.
#[derive(Debug)]
#[doc(hidden)]
pub enum RequestOwnerFailure {
    Native(NativeRefusal),
    Legacy(contextdb_core::Error),
}

/// An acquired request capability whose native provenance is sealed here.
#[derive(Debug)]
#[doc(hidden)]
pub struct RequestReadOwner(RequestReadRoute);

#[derive(Debug)]
#[allow(dead_code)] // Consumed by the separately owned native dispatch integration.
pub(crate) enum RequestReadRoute {
    Legacy(Arc<dyn ReadMemoryBudget>),
    Native(NativeReadChildReceipt),
}

#[derive(Debug)]
pub(crate) struct NativeReadChildReceipt {
    #[allow(dead_code)] // Retained now; native dispatch will consume this owner.
    owner: ReadMemoryOwner,
}
impl NativeReadChildReceipt {
    pub(crate) fn control_bytes(&self) -> usize {
        ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size()
    }
    pub(crate) fn into_owner(self) -> ReadMemoryOwner {
        self.owner
    }
}

impl RequestReadOwner {
    /// Alias the acquired capability without allocating or acquiring a child.
    /// The native alias increments the same physical block's strong count;
    /// a legacy alias retains the host's exact Arc identity.
    pub(crate) fn share(&self) -> Self {
        match &self.0 {
            RequestReadRoute::Legacy(reader) => Self::legacy(reader.clone()),
            RequestReadRoute::Native(receipt) => Self::native(NativeReadChildReceipt {
                owner: receipt.owner.clone(),
            }),
        }
    }
    pub(crate) fn legacy(reader: Arc<dyn ReadMemoryBudget>) -> Self {
        Self(RequestReadRoute::Legacy(reader))
    }
    pub(crate) fn native(receipt: NativeReadChildReceipt) -> Self {
        Self(RequestReadRoute::Native(receipt))
    }
    #[allow(dead_code)] // Additive companion ahead of native dispatch integration.
    pub(crate) fn into_route(self) -> RequestReadRoute {
        self.0
    }
}
#[derive(Debug)]
pub struct DeclaredReadMemory {
    limit: u64,
    file_child_high_water: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
    live_child_control_bytes: std::sync::atomic::AtomicU64,
    live_acl_cache_bytes: std::sync::atomic::AtomicU64,
}
impl DeclaredReadMemory {
    // Both doors use the same real parent counter and the same admission boundary.
    fn try_count(&self, bytes: usize) -> std::result::Result<(), u64> {
        use std::sync::atomic::Ordering;
        self.retained
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes as u64)
                    .filter(|after| *after <= self.limit)
            })
            .map(|_| ())
    }
    /// A client query is the declared reader's entire lifetime. Admit this
    /// root on its stack counter before allocating it. The external keeper
    /// retires that original fixed reservation only after the final Arc frees.
    pub(crate) fn query_root(limit: u64) -> Result<QueryReadRoot> {
        let root = Self {
            limit,
            file_child_high_water: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            live_child_control_bytes: std::sync::atomic::AtomicU64::new(0),
            live_acl_cache_bytes: std::sync::atomic::AtomicU64::new(0),
        };
        let bytes = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        root.try_reserve(bytes, "reader", "query_root",
            "Reduce simultaneous query memory or raise the declared reader limit.")?;
        let invoice = QueryRootInvoice { bytes };
        let root = Arc::new(root);
        #[cfg(any(test, feature = "test-seams"))]
        READER_PARENTS_CREATED.with(|created| created.set(created.get() + 1));
        Ok(QueryReadRoot { root, _invoice: invoice })
    }

    pub fn from_reader_limit(limit: u64) -> Arc<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        READER_PARENTS_CREATED.with(|created| created.set(created.get() + 1));
        Arc::new(Self {
            limit,
            file_child_high_water: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            live_child_control_bytes: std::sync::atomic::AtomicU64::new(0),
            live_acl_cache_bytes: std::sync::atomic::AtomicU64::new(0),
        })
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn retained_bytes_for_test(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Quiescent observation of actual admitted live child controls and parent bytes.
    pub(crate) fn standing_control_bytes(&self) -> (u64, u64) {
        (
            self.live_child_control_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// Bytes every reader child currently holds against this parent.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn acl_cache_bytes(&self) -> u64 {
        self.live_acl_cache_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn acl_cache_budget(self: &Arc<Self>) -> Result<Arc<dyn ReadMemoryBudget>> {
        let control_bytes =
            std::mem::size_of::<ReaderAclCacheBudget>() + 2 * std::mem::size_of::<usize>();
        self.try_reserve(
            control_bytes,
            "reader_acl_cache",
            "budget_owner",
            "Increase reader memory to cache ACL grants.",
        )?;
        self.live_acl_cache_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(Arc::new(ReaderAclCacheBudget {
            parent: self.clone(),
            control_bytes,
        }))
    }

    /// Largest admitted live-byte figure recorded by an individual file
    /// reader child. Children update this before the native allocation and
    /// the session keeps the receipt after an operation child is dropped.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn file_child_high_water(&self) -> u64 {
        self.file_child_high_water
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Admit the actual native cell against this original declared parent.
    pub(crate) fn native_child(
        self: &Arc<Self>,
    ) -> std::result::Result<NativeReadChildReceipt, NativeRefusal> {
        let control_bytes =
            ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size();
        self.try_reserve_native(
            control_bytes,
            "reader",
            "request_owner",
            "Reduce simultaneous requests or raise the declared reader memory limit.",
        )?;
        self.live_child_control_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let invoice = NativeChildInvoice {
            parent: self.clone(),
            bytes: control_bytes,
        };
        let payload = NativeDeclaredChild {
            parent: self.clone(),
            operation_limit: std::sync::atomic::AtomicU64::new(self.limit),
            retained: std::sync::atomic::AtomicU64::new(0),
        };
        ReadMemoryOwner::try_native_reserving(payload, invoice)
            .map(|owner| NativeReadChildReceipt { owner })
    }

    /// Acquire one native child together with its mandatory failure controls.
    /// Admission happens on the actual child payload before that payload is
    /// moved into its native cell. No acquired capability escapes without the
    /// controls, and no existing connection allocation changes payer.
    pub(crate) fn native_child_with_controls(
        self: &Arc<Self>, bytes: usize,
    ) -> std::result::Result<(NativeReadChildReceipt, ReadCredit), NativeRefusal> {
        let control_bytes =
            ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size();
        self.try_reserve_native(control_bytes, "reader", "request_owner",
            "Reduce simultaneous requests or raise the declared reader memory limit.")?;
        self.live_child_control_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let invoice = NativeChildInvoice { parent: self.clone(), bytes: control_bytes };
        let payload = NativeDeclaredChild {
            parent: self.clone(),
            operation_limit: std::sync::atomic::AtomicU64::new(self.limit), retained: std::sync::atomic::AtomicU64::new(0),
        };
        payload.try_reserve_native(bytes, "reader", "query_refusal_controls",
            "Reduce simultaneous requests or raise the declared reader memory limit.")?;
        // Before the cell exists this rollback guard owns the child's parent
        // debit. Its success handoff keeps that same debit and the payload's
        // retained counter intact; it does not refund or reserve again.
        let rollback = ReadCredit::from_prepaid(self.clone(), bytes);
        let owner = ReadMemoryOwner::try_native_reserving(payload, invoice)?;
        let controls = ReadCredit::from_prepaid_owned(owner.clone(), bytes);
        rollback.transfer();
        Ok((NativeReadChildReceipt { owner }, controls))
    }

    /// Existing public provider result remains unqualified at its Error terminal.
    fn native_child_unqualified(self: &Arc<Self>) -> Result<NativeReadChildReceipt> {
        let control_bytes =
            ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size();
        self.try_reserve(
            control_bytes,
            "reader",
            "request_owner",
            "Reduce simultaneous requests or raise the declared reader memory limit.",
        )?;
        self.live_child_control_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let invoice = NativeChildInvoice {
            parent: self.clone(),
            bytes: control_bytes,
        };
        let payload = NativeDeclaredChild {
            parent: self.clone(),
            operation_limit: std::sync::atomic::AtomicU64::new(self.limit),
            retained: std::sync::atomic::AtomicU64::new(0),
        };
        ReadMemoryOwner::try_native(payload, invoice).map(|owner| NativeReadChildReceipt { owner })
    }

    pub fn child(self: &Arc<Self>) -> Result<Arc<dyn ReadMemoryBudget>> {
        let control_bytes =
            std::mem::size_of::<DeclaredReadChild>() + 2 * std::mem::size_of::<usize>();
        self.try_reserve(
            control_bytes,
            "reader",
            "request_owner",
            "Reduce simultaneous requests or raise the declared reader memory limit.",
        )?;
        // The existing nonzero amount is the counted marker; no child exists
        // on refusal, and the native child layout and admission stay unchanged.
        debug_assert_ne!(control_bytes, 0);
        self.live_child_control_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let child = Arc::new(DeclaredReadChild {
            parent: self.clone(),
            retained: std::sync::atomic::AtomicU64::new(0),
            control_bytes,
        });
        #[cfg(any(test, feature = "test-seams"))]
        register_child_observer(&child);
        Ok(child)
    }
}
/// This keeper is never put inside the root allocation. Its root and every
/// acquired child must be destroyed before the original root invoice retires.
#[derive(Debug)]
pub(crate) struct QueryReadRoot {
    pub(crate) root: Arc<DeclaredReadMemory>,
    _invoice: QueryRootInvoice,
}
#[derive(Debug)]
struct QueryRootInvoice {
    // This is a terminal reservation: no live counter exists after root free.
    // It is killed here, not refunded into another query or reconstructed.
    bytes: usize,
}
impl Drop for QueryRootInvoice {
    fn drop(&mut self) {
        // QueryReadRoot's preceding Arc field has physically freed the root;
        // the original terminal reservation now retires, with no live funder
        // into which these bytes could be refunded or readmitted.
        self.bytes = 0;
    }
}

#[derive(Debug)]
struct DeclaredReadChild {
    control_bytes: usize,
    parent: Arc<DeclaredReadMemory>,
    retained: std::sync::atomic::AtomicU64,
}
/// The invoice, not the payload, owns the native block's parent debit.
#[derive(Debug)]
struct NativeChildInvoice {
    parent: Arc<DeclaredReadMemory>,
    bytes: usize,
}
impl Drop for NativeChildInvoice {
    fn drop(&mut self) {
        self.parent
            .live_child_control_bytes
            .fetch_sub(self.bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(self.bytes);
    }
}
#[derive(Debug)]
struct NativeDeclaredChild {
    parent: Arc<DeclaredReadMemory>,
    operation_limit: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
}
impl NativeDeclaredChild {
    fn try_count(&self, bytes: usize) -> std::result::Result<u64, u64> {
        use std::sync::atomic::Ordering;
        let limit = self.operation_limit.load(Ordering::SeqCst);
        let control = ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
        self.retained.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
            held.checked_add(bytes as u64).filter(|next|
                next.checked_add(control).is_some_and(|total| total <= limit))
        })
    }
}
impl ReadMemoryBudget for NativeDeclaredChild {
    fn constrain_operation_memory(&self, limit: usize) {
        self.operation_limit.fetch_min(limit as u64, std::sync::atomic::Ordering::SeqCst);
    }

    fn native_reserve_supported(&self) -> bool {
        true
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        let previous = self.try_count(bytes).map_err(|held| {
            let control = ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
            let limit = self.operation_limit.load(std::sync::atomic::Ordering::SeqCst);
            NativeRefusal::MemoryBudgetExceeded { subsystem, operation, requested_bytes: bytes,
                available_bytes: limit.saturating_sub(held.saturating_add(control)) as usize,
                budget_limit_bytes: limit as usize, hint }
        })?;
        if let Err(error) = self.parent.try_reserve_native(bytes, subsystem, operation, hint) {
            self.retained.fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
            return Err(error);
        }
        let held = previous + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let previous = self.try_count(bytes).map_err(|held| {
            let limit = self.operation_limit.load(std::sync::atomic::Ordering::SeqCst);
            let control = ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
            reader_memory_refusal(limit, held.saturating_add(control), bytes, subsystem, operation, hint)
        })?;
        if let Err(error) = self.parent.try_reserve(bytes, subsystem, operation, hint) {
            self.retained.fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
            return Err(error);
        }
        let held = previous + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(bytes);
    }
}
fn reader_memory_refusal(
    limit: u64,
    held: u64,
    requested: usize,
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
) -> contextdb_core::Error {
    contextdb_core::Error::MemoryBudgetExceeded {
        subsystem: subsystem.into(),
        operation: operation.into(),
        requested_bytes: requested,
        available_bytes: usize::try_from(limit.saturating_sub(held)).unwrap_or(usize::MAX),
        budget_limit_bytes: usize::try_from(limit).unwrap_or(usize::MAX),
        hint: hint.into(),
    }
}
#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static READER_PARENTS_CREATED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static UNOWNED_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_unowned_decode_for_test() {
    UNOWNED_DECODES.with(|count| count.set(count.get() + 1));
}

/// Decodes on this thread so far that found no operation owner and were
/// charged to the private default decode accountant instead.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn unowned_decodes_for_test() -> u64 {
    UNOWNED_DECODES.with(std::cell::Cell::get)
}

/// Declared reader parents constructed on this thread so far. A caller that
/// should be funded by an existing owner proves it created none by comparing
/// this count before and after.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn reader_parents_created_for_test() -> u64 {
    READER_PARENTS_CREATED.with(std::cell::Cell::get)
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static READER_BUDGET_LEDGER: std::cell::RefCell<Option<Vec<crate::memory_accounting::BudgetLedgerEntry>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn watch_reader_budget_ledger_for_test() {
    READER_BUDGET_LEDGER.with(|ledger| *ledger.borrow_mut() = Some(Vec::new()));
}

#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn take_reader_budget_ledger_for_test() -> Vec<crate::memory_accounting::BudgetLedgerEntry> {
    READER_BUDGET_LEDGER.with(|ledger| ledger.borrow_mut().take().unwrap_or_default())
}

// Refine the real admitted event at the storage producer, rather than
// infer body identity from an incidental numerical request size.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_reader_body_admission_for_test(bytes: usize, row: contextdb_core::RowId,
    created: contextdb_core::TxId, lsn: contextdb_core::Lsn) {
    READER_BUDGET_LEDGER.with(|ledger| {
        if let Some(events) = ledger.borrow_mut().as_mut() {
            if let Some(event) = events.last_mut().filter(|event|
                event.owner_identity != event.parent_identity && event.bytes == bytes && event.what != "release") {
                event.what = "vector_store/load_raw_vector_candidate".into();
                event.row_identity = Some((row.0, created.0, lsn.0));
            }
        }
    });
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_reader_container_allocation_for_test(bytes: usize, pointer: usize,
    capacity: usize, element_bytes: usize) {
    READER_BUDGET_LEDGER.with(|ledger| {
        if let Some(events) = ledger.borrow_mut().as_mut() {
            if let Some(event) = events.last_mut().filter(|event|
                event.owner_identity != event.parent_identity && event.bytes == bytes) {
                event.allocation = Some((pointer, capacity, element_bytes));
            }
        }
    });
}

/// One native reservation against a declared reader parent, as tests observe
/// it: which site admitted (or was refused) how many bytes, and what the
/// parent retained afterwards.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAdmissionWitness {
    pub parent: usize,
    pub subsystem: &'static str,
    pub operation: &'static str,
    pub bytes: usize,
    pub admitted: bool,
    pub retained_after: u64,
}

#[cfg(any(test, feature = "test-seams"))]
static NATIVE_ADMISSIONS: std::sync::Mutex<Option<Vec<NativeAdmissionWitness>>> =
    std::sync::Mutex::new(None);

/// Start recording every native reservation on any declared reader parent.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn watch_native_admissions_for_test() {
    *NATIVE_ADMISSIONS
        .lock()
        .unwrap_or_else(|err| err.into_inner()) = Some(Vec::new());
}

/// Stop recording and return the native reservations seen, in order.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn take_native_admissions_for_test() -> Vec<NativeAdmissionWitness> {
    NATIVE_ADMISSIONS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .take()
        .unwrap_or_default()
}

impl DeclaredReadMemory {
    /// This parent's identity in native admission witnesses.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn witness_identity_for_test(&self) -> usize {
        self as *const Self as usize
    }
}

impl ReadMemoryBudget for DeclaredReadMemory {
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        let result = self
            .try_count(bytes)
            .map_err(|held| NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: usize::try_from(self.limit.saturating_sub(held))
                    .unwrap_or(usize::MAX),
                budget_limit_bytes: usize::try_from(self.limit).unwrap_or(usize::MAX),
                hint,
            });
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(witnesses) = NATIVE_ADMISSIONS
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_mut()
        {
            witnesses.push(NativeAdmissionWitness {
                parent: self.witness_identity_for_test(),
                subsystem,
                operation,
                bytes,
                admitted: result.is_ok(),
                retained_after: self.retained.load(std::sync::atomic::Ordering::SeqCst),
            });
        }
        result
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {

        self.try_count(bytes).map_err(|held| {
            reader_memory_refusal(self.limit, held, bytes, subsystem, operation, hint)
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        READER_BUDGET_LEDGER.with(|ledger| {
            if let Some(entries) = ledger.borrow_mut().as_mut() {
                entries.push(crate::memory_accounting::BudgetLedgerEntry {
                    what: format!("{subsystem}/{operation}"),
                    bytes,
                    used_after: usize::try_from(self.retained.load(std::sync::atomic::Ordering::SeqCst))
                        .unwrap_or(usize::MAX),
                    owner_identity: self as *const Self as usize,
                    parent_identity: self as *const Self as usize,
                    row_identity: None,
                    allocation: None,
                });
            }
        });
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl DeclaredReadChild {
    fn note_reader_budget_event(&self, subsystem: &str, operation: &str, bytes: usize, held: u64) {
        READER_BUDGET_LEDGER.with(|ledger| {
            if let Some(events) = ledger.borrow_mut().as_mut() {
                events.push(crate::memory_accounting::BudgetLedgerEntry {
                    what: if subsystem.is_empty() { operation.to_owned() } else { format!("{subsystem}/{operation}") },
                    bytes, used_after: usize::try_from(held).unwrap_or(usize::MAX),
                    owner_identity: self as *const Self as usize,
                    parent_identity: Arc::as_ptr(&self.parent) as usize,
                    row_identity: None,
                    allocation: None,
                });
            }
        });
    }
}
impl ReadMemoryBudget for DeclaredReadChild {
    fn native_reserve_supported(&self) -> bool {
        true
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        self.parent
            .try_reserve_native(bytes, subsystem, operation, hint)?;
        let held = self
            .retained
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst)
            + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_reader_budget_event(subsystem, operation, bytes, held);
        Ok(())
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.parent.try_reserve(bytes, subsystem, operation, hint)?;
        let held = self
            .retained
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst)
            + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_reader_budget_event(subsystem, operation, bytes, held);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        let before = self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(bytes);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_reader_budget_event("", "release", bytes, before - bytes as u64);
    }
}
impl Drop for DeclaredReadChild {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-seams"))]
        child_observers()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&(self as *const Self as usize));
        if self.control_bytes != 0 {
            self.parent.live_child_control_bytes.fetch_sub(
                self.control_bytes as u64,
                std::sync::atomic::Ordering::SeqCst,
            );
            self.parent.release(self.control_bytes);
        }
    }
}
impl ReadRequestBudgetProvider for Arc<DeclaredReadMemory> {
    fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>> {
        self.child()
    }
    fn request_budget_owner(&self) -> Result<RequestReadOwner> {
        self.native_child_unqualified()
            .map(RequestReadOwner::native)
    }
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<RequestReadOwner, RequestOwnerFailure> {
        self.native_child()
            .map(RequestReadOwner::native)
            .map_err(RequestOwnerFailure::Native)
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl DeclaredReadMemory {
    /// Passive parent-byte observation for independent source-unit lifetimes.
    pub(crate) fn retained_bytes_for_source_unit_test(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct ReaderAclCacheBudget {
    parent: Arc<DeclaredReadMemory>,
    control_bytes: usize,
}
impl ReadMemoryBudget for ReaderAclCacheBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.parent.try_reserve(bytes, subsystem, operation, hint)?;
        self.parent
            .live_acl_cache_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.parent
            .live_acl_cache_bytes
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(bytes);
    }
}
impl Drop for ReaderAclCacheBudget {
    fn drop(&mut self) {
        self.release(self.control_bytes);
    }
}

// Legacy-only Weak observation; it does not certify native physical free.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone)]
pub(crate) struct DeclaredReadChildObserver {
    child: std::sync::Weak<DeclaredReadChild>,
    parent: std::sync::Weak<DeclaredReadMemory>,
    owner_identity: usize,
    parent_identity: usize,
}
#[cfg(any(test, feature = "test-seams"))]
impl DeclaredReadChildObserver {
    // Caller retains the actual child throughout the allocator observation.
    #[cfg(test)]
    pub(crate) fn physical_funding_counter_for_test(&self) -> usize {
        let child = self.child.upgrade().expect("physical observation needs a live child");
        &child.retained as *const std::sync::atomic::AtomicU64 as usize
    }

    pub(crate) fn sample(&self) -> crate::read_work_probe::DeclaredReadChildFunding {
        use std::sync::atomic::Ordering;
        let child = self.child.upgrade();
        let parent = self.parent.upgrade();
        crate::read_work_probe::DeclaredReadChildFunding {
            owner_identity: self.owner_identity,
            alive: child.is_some(),
            charged_bytes: child
                .as_ref()
                .map_or(0, |child| child.retained.load(Ordering::SeqCst)),
            parent_identity: self.parent_identity,
            parent_alive: parent.is_some(),
            parent_charged_bytes: parent
                .as_ref()
                .map_or(0, |parent| parent.retained.load(Ordering::SeqCst)),
        }
    }
}
#[cfg(any(test, feature = "test-seams"))]
fn child_observers()
-> &'static std::sync::Mutex<std::collections::HashMap<usize, DeclaredReadChildObserver>> {
    static OBSERVERS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<usize, DeclaredReadChildObserver>>,
    > = std::sync::OnceLock::new();
    OBSERVERS.get_or_init(Default::default)
}
#[cfg(any(test, feature = "test-seams"))]
fn register_child_observer(child: &Arc<DeclaredReadChild>) {
    let owner_identity = Arc::as_ptr(child) as usize;
    let observer = DeclaredReadChildObserver {
        child: Arc::downgrade(child),
        parent: Arc::downgrade(&child.parent),
        owner_identity,
        parent_identity: Arc::as_ptr(&child.parent) as usize,
    };
    child_observers()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(owner_identity, observer);
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn declared_child_observer(
    read_memory: &Arc<dyn ReadMemoryBudget>,
) -> Option<DeclaredReadChildObserver> {
    let identity = Arc::as_ptr(read_memory) as *const () as usize;
    child_observers()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&identity)
        .cloned()
}

// Evidence-only rows called by the existing cross-thread allocation oracle test.
#[cfg(test)]
pub(crate) mod native_ownership_proof {
    use super::*;
    use contextdb_core::read_memory::ReadCredit;
    use std::alloc::Layout;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex, Weak};

    thread_local! {
        static WATCH_REQUEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        // The allocator oracle can refuse one selected live cache Layout.  It
        // is test-only and consumed before System is called, exactly like the
        // native-owner null row below.
        static REFUSE_ONE_LAYOUT: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    }
    static POINTER: AtomicUsize = AtomicUsize::new(0);
    static OBSERVATION: Mutex<Option<Observation>> = Mutex::new(None);

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct Sample {
        alive: bool,
        controls: u64,
        retained: u64,
    }
    #[derive(Debug)]
    struct Observation {
        parent: Weak<DeclaredReadMemory>,
        layout: Layout,
        requests: usize,
        allocations: usize,
        frees: usize,
        pointer: usize,
        window: usize,
        free_window: usize,
        free_layout: Option<Layout>,
        before_free: Sample,
        after_free: Sample,
        null_once: bool,
    }
    fn sample(parent: &Weak<DeclaredReadMemory>) -> Sample {
        parent.upgrade().map_or(Sample::default(), |parent| {
            let (controls, retained) = parent.standing_control_bytes();
            Sample {
                alive: true,
                controls,
                retained,
            }
        })
    }
    fn layout() -> Layout {
        ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>()
    }
    fn begin(parent: &Arc<DeclaredReadMemory>, null_once: bool) {
        assert_eq!(POINTER.load(Ordering::SeqCst), 0);
        let mut observation = OBSERVATION.lock().unwrap();
        assert!(observation.is_none());
        *observation = Some(Observation {
            parent: Arc::downgrade(parent),
            layout: layout(),
            requests: 0,
            allocations: 0,
            frees: 0,
            pointer: 0,
            window: 0,
            free_window: 0,
            free_layout: None,
            before_free: Sample::default(),
            after_free: Sample::default(),
            null_once,
        });
        WATCH_REQUEST.with(|watch| watch.set(true));
    }
    fn stop_requests() {
        WATCH_REQUEST.with(|watch| watch.set(false));
    }
    fn finish() -> Observation {
        stop_requests();
        POINTER.store(0, Ordering::SeqCst);
        OBSERVATION.lock().unwrap().take().unwrap()
    }
    pub(crate) fn arm_refuse_one_live_layout(layout: Layout) {
        assert_ne!(layout.size(), 0);
        REFUSE_ONE_LAYOUT.with(|armed| armed.set((layout.size(), layout.align())));
    }
    pub(crate) fn refusal_armed_for_test() -> bool {
        REFUSE_ONE_LAYOUT.with(|armed| armed.get().0 != 0)
    }
    pub(crate) fn refuse_selected_live_layout(request: Layout) -> bool {
        REFUSE_ONE_LAYOUT
            .try_with(|armed| {
                let selected = armed.get();
                if selected == (request.size(), request.align()) {
                    armed.set((0, 0));
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }

    // Called by the real System-backed global allocator, before requesting storage.
    pub(crate) fn refuse_allocation(request: Layout) -> bool {
        if !WATCH_REQUEST
            .try_with(std::cell::Cell::get)
            .unwrap_or(false)
        {
            return false;
        }
        let mut slot = OBSERVATION.lock().unwrap();
        let Some(observation) = slot.as_mut() else {
            return false;
        };
        if request != observation.layout {
            return false;
        }
        observation.requests += 1;
        std::mem::take(&mut observation.null_once)
    }
    pub(crate) fn allocated(pointer: *mut u8, request: Layout, window: usize) {
        if !WATCH_REQUEST
            .try_with(std::cell::Cell::get)
            .unwrap_or(false)
        {
            return;
        }
        let mut slot = OBSERVATION.lock().unwrap();
        let Some(observation) = slot.as_mut() else {
            return;
        };
        if request != observation.layout {
            return;
        }
        observation.allocations += 1;
        observation.pointer = pointer as usize;
        observation.window = window;
        POINTER.store(pointer as usize, Ordering::SeqCst);
    }
    pub(crate) fn before_free(pointer: *mut u8, request: Layout, window: usize) {
        if POINTER.load(Ordering::SeqCst) != pointer as usize {
            return;
        }
        let mut slot = OBSERVATION.lock().unwrap();
        let observation = slot.as_mut().unwrap();
        observation.frees += 1;
        observation.free_layout = Some(request);
        observation.free_window = window;
        observation.before_free = sample(&observation.parent);
    }
    pub(crate) fn after_free(pointer: *mut u8) {
        if POINTER.load(Ordering::SeqCst) != pointer as usize {
            return;
        }
        let mut slot = OBSERVATION.lock().unwrap();
        let observation = slot.as_mut().unwrap();
        observation.after_free = sample(&observation.parent);
        POINTER.store(0, Ordering::SeqCst);
    }
    fn acquire(parent: &Arc<DeclaredReadMemory>) -> ReadMemoryOwner {
        let (request, requests, count) = crate::persistence::content_identity::proof_allocation_oracle_tests::native_requests_for_test(|| parent.request_budget_owner().unwrap());
        assert_eq!(
            count, 1,
            "provider acquisition must make exactly one allocation request"
        );
        assert_eq!(
            (requests[0].1, requests[0].2),
            (layout().size(), layout().align())
        );
        stop_requests();
        match request.into_route() {
            RequestReadRoute::Native(receipt) => receipt.owner,
            RequestReadRoute::Legacy(_) => {
                panic!("declared provider must acquire native ownership")
            }
        }
    }
    fn assert_freed(observation: &Observation) {
        let bytes = observation.layout.size() as u64;
        assert_eq!(
            (
                observation.requests,
                observation.allocations,
                observation.frees
            ),
            (1, 1, 1)
        );
        assert_ne!(observation.pointer, 0);
        assert_eq!(observation.pointer % observation.layout.align(), 0);
        assert_ne!(observation.window, 0);
        assert_eq!(observation.free_window, observation.window);
        assert_eq!(observation.free_layout, Some(observation.layout));
        let funded = Sample {
            alive: true,
            controls: bytes,
            retained: bytes,
        };
        assert_eq!(
            observation.before_free, funded,
            "parent invoice must survive until physical free begins"
        );
        assert_eq!(
            observation.after_free, funded,
            "refund must follow System.dealloc, not payload destruction"
        );
    }

    pub(crate) fn run_native_rows() {
        let bytes = layout().size() as u64;
        // Exact-limit activation, untouched clone identity, split/merge and refusal rows.
        for external_drop in [false, true] {
            let parent = DeclaredReadMemory::from_reader_limit(bytes + 17);
            let weak = Arc::downgrade(&parent);
            begin(&parent, false);
            let owner = acquire(&parent);
            assert_eq!(parent.standing_control_bytes(), (bytes, bytes));
            let clone = owner.clone();
            assert!(owner.same_owner(&clone));
            let mut credit =
                ReadCredit::try_new_owned(clone, 17, "proof", "activation", "proof").unwrap();
            assert_eq!(parent.standing_control_bytes(), (bytes, bytes + 17));
            assert!(credit.try_grow_for(1, "proof", "refusal", "proof").is_err());
            assert_eq!(credit.bytes(), 17);
            assert!(credit.try_split_off(18).is_err());
            assert_eq!(credit.bytes(), 17);
            let split = credit.try_split_off(7).unwrap();
            assert_eq!(credit.bytes(), 10);
            credit.try_merge_prepaid(split).unwrap();
            assert_eq!(credit.bytes(), 17);
            let other_parent = DeclaredReadMemory::from_reader_limit(bytes + 3);
            let other = acquire(&other_parent);
            assert!(!owner.same_owner(&other));
            let other_credit =
                ReadCredit::try_new_owned(other, 3, "proof", "other", "proof").unwrap();
            assert!(credit.try_merge_prepaid(other_credit).is_err());
            assert_eq!(credit.bytes(), 17);
            assert_eq!(other_parent.standing_control_bytes(), (0, 0));
            drop(owner);
            assert_eq!(parent.standing_control_bytes(), (bytes, bytes + 17));
            if external_drop {
                drop(parent);
                assert!(weak.upgrade().is_some());
                drop(credit);
                assert!(weak.upgrade().is_none());
            } else {
                drop(credit);
                assert_eq!(parent.standing_control_bytes(), (0, 0));
                drop(parent);
            }
            assert_freed(&finish());
        }
        // Different native requests remain different capabilities even at one parent.
        let parent = DeclaredReadMemory::from_reader_limit(2 * bytes + 5);
        begin(&parent, false);
        let owner = acquire(&parent);
        let other = acquire(&parent);
        assert!(!owner.same_owner(&other));
        let mut credit =
            ReadCredit::try_new_owned(owner, 3, "proof", "same-parent", "proof").unwrap();
        let other_credit =
            ReadCredit::try_new_owned(other, 2, "proof", "same-parent-other", "proof").unwrap();
        assert!(credit.try_merge_prepaid(other_credit).is_err());
        assert_eq!(credit.bytes(), 3);
        assert_eq!(parent.standing_control_bytes(), (bytes, bytes + 3));
        drop(credit);
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, 0));

        // A real host provider returning a production legacy child keeps its exact capability.
        #[derive(Debug)]
        struct HostProvider(Arc<dyn ReadMemoryBudget>);
        impl ReadRequestBudgetProvider for HostProvider {
            fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>> {
                Ok(self.0.clone())
            }
        }
        let parent = DeclaredReadMemory::from_reader_limit(u64::MAX);
        let child = parent.child().unwrap();
        let provider = HostProvider(child.clone());
        let request = provider.request_budget_owner().unwrap();
        match request.into_route() {
            RequestReadRoute::Legacy(actual) => assert!(Arc::ptr_eq(&actual, &child)),
            RequestReadRoute::Native(_) => {
                panic!("host provider's legacy capability must stay legacy")
            }
        }
        let owner = ReadMemoryOwner::from_legacy(child.clone());
        assert!(owner.same_owner(&ReadMemoryOwner::from_legacy(child.clone())));
        let native = acquire(&parent);
        assert!(!native.same_owner(&owner));
        drop(native);
        drop(owner);
        drop(provider);
        drop(child);
        assert_eq!(parent.standing_control_bytes(), (0, 0));

        // Original parent remains funded even when all external parent handles close.
        // Barriers make every contender own its credit before any contender releases it.
        for contenders in [2, 8] {
            let parent = DeclaredReadMemory::from_reader_limit(bytes + contenders as u64);
            let weak = Arc::downgrade(&parent);
            begin(&parent, false);
            let owner = acquire(&parent);
            let ready = Arc::new(Barrier::new(contenders + 1));
            let release = Arc::new(Barrier::new(contenders + 1));
            let mut threads = Vec::new();
            for _ in 0..contenders {
                let credit =
                    ReadCredit::try_new_owned(owner.clone(), 1, "proof", "concurrent", "proof")
                        .unwrap();
                let ready = ready.clone();
                let release = release.clone();
                threads.push(std::thread::spawn(move || {
                    ready.wait();
                    release.wait();
                    drop(credit);
                }));
            }
            ready.wait();
            assert_eq!(
                parent.standing_control_bytes(),
                (bytes, bytes + contenders as u64)
            );
            drop(owner);
            drop(parent);
            assert!(weak.upgrade().is_some());
            release.wait();
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(weak.upgrade().is_none());
            assert_freed(&finish());
        }
        // Declared refusal must precede any allocator request.
        let parent = DeclaredReadMemory::from_reader_limit(bytes - 1);
        begin(&parent, false);
        assert!(parent.request_budget_owner().is_err());
        let refused = finish();
        assert_eq!(
            (refused.requests, refused.allocations, refused.frees),
            (0, 0, 0)
        );
        assert_eq!(parent.standing_control_bytes(), (0, 0));

        // Null from the same real allocator drops actual payload/invoice without a free.
        let parent = DeclaredReadMemory::from_reader_limit(bytes);
        begin(&parent, true);
        assert!(parent.request_budget_owner().is_err());
        let refused = finish();
        assert_eq!(
            (refused.requests, refused.allocations, refused.frees),
            (1, 0, 0)
        );
        assert_eq!(parent.standing_control_bytes(), (0, 0));

        // Request cancellation before activation is ordinary owner destruction.
        let parent = DeclaredReadMemory::from_reader_limit(bytes);
        begin(&parent, false);
        let owner = acquire(&parent);
        drop(owner);
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, 0));

        // Unwind after activation must drop credits and the actual native request.
        let parent = DeclaredReadMemory::from_reader_limit(bytes + 17);
        begin(&parent, false);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = acquire(&parent);
            let _credit =
                ReadCredit::try_new_owned(owner.clone(), 17, "proof", "unwind", "proof").unwrap();
            panic!("deterministic request unwind");
        }));
        assert!(result.is_err());
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, 0));
    }
}
