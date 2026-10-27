//! Vector copies use the requesting reader's existing budget capability.
use contextdb_core::{
    Result,
    read_memory::{NativeReadMemory, NativeRefusal, ReadCredit, ReadMemoryBudget, ReadMemoryOwner},
};
use contextdb_vector::{MemoryBudget, VectorCopyAdmission};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ReaderVectorBudget {
    reader: Arc<dyn ReadMemoryBudget>,
    _read_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
    vector_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}

impl MemoryBudget for ReaderVectorBudget {
    fn control_invoice(
        &self,
    ) -> Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>
    {
        self.vector_invoice.clone()
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
        let result = self
            .reader
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
            let identity = Arc::as_ptr(&self.reader) as *const () as usize;
            READER_BUDGET_LEDGER.with(|ledger| {
                if let Some(entries) = ledger.borrow_mut().as_mut() {
                    if let Some(event) = entries.last_mut().filter(|event| {
                        event.owner_identity == identity && event.what == "vector_copy/materialize"
                    }) {
                        event.what = format!("{component}/{operation}");
                    }
                }
            });
        }
        result
    }

    fn release(&self, bytes: usize) {
        self.reader.release(bytes);
    }
}

/// Adapt the executing READ once, so copy admission and search share its capability.
pub(crate) fn budget(
    read_memory: impl Into<ReadMemoryOwner>,
) -> contextdb_vector::MemoryBudgetOwner {
    let budget = contextdb_vector::MemoryBudgetOwner::from(read_memory.into());
    #[cfg(any(test, feature = "test-seams"))]
    let budget = budget.with_read_copy_observer(note_copy_admission);
    budget
}

#[cfg(any(test, feature = "test-seams"))]
fn note_copy_admission(identity: usize, component: &str, operation: &str) {
    READER_BUDGET_LEDGER.with(|ledger| {
        if let Some(entries) = ledger.borrow_mut().as_mut() {
            if let Some(event) = entries.last_mut().filter(|event| {
                event.owner_identity == identity && event.what == "vector_copy/materialize"
            }) {
                event.what = format!("{component}/{operation}");
            }
        }
    });
}

pub fn admission(read_memory: impl Into<ReadMemoryOwner>) -> VectorCopyAdmission {
    VectorCopyAdmission::new(budget(read_memory))
}

/// The actual cyclic request has already admitted both adapter layouts.
/// The read adapter's original keeper is outside its raw Arc field; the vector
/// adapter's keeper travels outside its Arc in every retained copy admission.
pub(crate) fn admission_prepaid(
    read_memory: Arc<dyn ReadMemoryBudget>,
    read_invoice: contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>,
    vector_invoice: contextdb_core::read_memory::ReadShared<
        contextdb_core::read_memory::ReadCredit,
    >,
) -> VectorCopyAdmission {
    VectorCopyAdmission::new(Arc::new(ReaderVectorBudget {
        reader: read_memory,
        _read_invoice: Some(read_invoice),
        vector_invoice: Some(vector_invoice),
    }))
}

pub trait ReadRequestBudgetProvider: std::fmt::Debug + Send + Sync {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>>;

    /// Existing host providers acquire their exact legacy capability once.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn request_budget_owner(&self) -> Result<RequestReadOwner> {
        self.request_budget().map(RequestReadOwner::legacy)
    }

    /// The acquisition a native route makes. A provider that selects the
    /// Native route overrides this so its refusal is a `NativeRefusal` with no
    /// owned diagnostic; the default keeps today's acquisition, so a Legacy
    /// provider still selects Legacy and refuses on its Legacy terms.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<RequestReadOwner, RequestOwnerFailure> {
        self.request_budget_owner()
            .map_err(RequestOwnerFailure::Legacy)
    }
}

/// An application-created dynamic supplier of an already acquired native
/// capability. The returned route carries its original invoice and typed refusal;
/// the engine neither wraps it in an Arc nor acquires a second request.
#[doc(hidden)]
pub trait NativeReadRequestBudgetProvider: std::fmt::Debug + Send + Sync {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<RequestReadOwner, RequestOwnerFailure>;
}

/// An engine supplier aliases its existing parent directly. Host suppliers
/// retain their original application-created Arc, including its Weak boundary.
/// Constructing or cloning this handle never allocates a wrapper control.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct ReadRequestProvider(ReadRequestProviderKind);
#[derive(Debug, Clone)]
enum ReadRequestProviderKind {
    Declared(DeclaredReadRoot),
    External(Arc<dyn ReadRequestBudgetProvider>),
    Native(Arc<dyn NativeReadRequestBudgetProvider>),
}
impl ReadRequestProvider {
    pub(crate) fn declared(parent: DeclaredReadRoot) -> Self {
        Self(ReadRequestProviderKind::Declared(parent))
    }
    pub(crate) fn external(provider: Arc<dyn ReadRequestBudgetProvider>) -> Self {
        Self(ReadRequestProviderKind::External(provider))
    }
    /// Retain host-created supplier storage without an engine allocation.
    pub fn native(provider: Arc<dyn NativeReadRequestBudgetProvider>) -> Self {
        Self(ReadRequestProviderKind::Native(provider))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn request_budget(&self) -> Result<ReadMemoryOwner> {
        match &self.0 {
            ReadRequestProviderKind::Declared(parent) => parent.child(),
            ReadRequestProviderKind::External(provider) => {
                provider.request_budget().map(ReadMemoryOwner::from_legacy)
            }
            ReadRequestProviderKind::Native(provider) => provider
                .request_budget_owner_native()
                .map(|request| match request.into_route() {
                    RequestReadRoute::Legacy(owner) => ReadMemoryOwner::from_legacy(owner),
                    RequestReadRoute::Native(receipt) => receipt.into_owner(),
                })
                .map_err(RequestOwnerFailure::into_error),
        }
    }
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn request_budget_owner(&self) -> Result<RequestReadOwner> {
        match &self.0 {
            ReadRequestProviderKind::Declared(parent) => parent.request_budget_owner(),
            ReadRequestProviderKind::External(provider) => provider.request_budget_owner(),
            ReadRequestProviderKind::Native(provider) => provider
                .request_budget_owner_native()
                .map_err(RequestOwnerFailure::into_error),
        }
    }
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn request_budget_owner_native(
        &self,
    ) -> std::result::Result<RequestReadOwner, RequestOwnerFailure> {
        match &self.0 {
            ReadRequestProviderKind::Declared(parent) => parent.request_budget_owner_native(),
            ReadRequestProviderKind::External(provider) => provider.request_budget_owner_native(),
            ReadRequestProviderKind::Native(provider) => provider.request_budget_owner_native(),
        }
    }
}
impl From<DeclaredReadRoot> for ReadRequestProvider {
    fn from(parent: DeclaredReadRoot) -> Self {
        Self::declared(parent)
    }
}
impl From<Arc<dyn ReadRequestBudgetProvider>> for ReadRequestProvider {
    fn from(provider: Arc<dyn ReadRequestBudgetProvider>) -> Self {
        Self::external(provider)
    }
}
impl<T: ReadRequestBudgetProvider + 'static> From<Arc<T>> for ReadRequestProvider {
    fn from(provider: Arc<T>) -> Self {
        Self::external(provider)
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

impl RequestOwnerFailure {
    fn into_error(self) -> contextdb_core::Error {
        match self {
            Self::Native(refusal) => contextdb_core::Error::NativeEngineOther(refusal),
            Self::Legacy(error) => error,
        }
    }
}

/// An acquired request capability whose native provenance is sealed here.
#[derive(Debug)]
#[doc(hidden)]
pub struct RequestReadOwner(RequestReadRoute);

#[derive(Debug)]
pub(crate) enum RequestReadRoute {
    Legacy(Arc<dyn ReadMemoryBudget>),
    Native(NativeReadChildReceipt),
}

#[derive(Debug)]
pub(crate) struct NativeReadChildReceipt {
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
    pub(crate) fn into_route(self) -> RequestReadRoute {
        self.0
    }
}
/// Actual parent charge over a scoped observation; not an admission count.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderMemoryHighWaterForTest {
    pub before: u64,
    pub peak: u64,
    pub after: u64,
}
#[cfg(any(test, feature = "test-seams"))]
struct ReaderPeakObservation {
    registration: u64,
    parent: usize,
    peak: u64,
}
#[cfg(any(test, feature = "test-seams"))]
static READER_PEAK_OBSERVATIONS: parking_lot::Mutex<Vec<ReaderPeakObservation>> =
    parking_lot::Mutex::new(Vec::new());
#[cfg(any(test, feature = "test-seams"))]
static READER_PEAK_REGISTRATION: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "test-seams"))]
static READER_PEAK_ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug)]
pub struct DeclaredReadMemory {
    limit: u64,
    terminal_bytes: usize,
    file_child_high_water: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
    live_child_control_bytes: std::sync::atomic::AtomicU64,
    live_child_payload_bytes: std::sync::atomic::AtomicU64,
    live_acl_cache_bytes: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "test-seams"))]
    observation: Option<root_observations::Token>,
}
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn native_control_layouts_for_test() -> (std::alloc::Layout, std::alloc::Layout) {
    (
        ReadMemoryOwner::native_layout::<DeclaredReadMemory, RootTerminalInvoice>(),
        ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>(),
    )
}
impl DeclaredReadMemory {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn observe_high_water_for_test<R>(
        &self,
        operation: impl FnOnce() -> R,
    ) -> (R, ReaderMemoryHighWaterForTest) {
        use std::sync::atomic::Ordering;
        struct Remove(u64);
        impl Drop for Remove {
            fn drop(&mut self) {
                READER_PEAK_OBSERVATIONS
                    .lock()
                    .retain(|entry| entry.registration != self.0);
                READER_PEAK_ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let registration = READER_PEAK_REGISTRATION
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
            .expect("reader observation identity overflow");
        let mut observations = READER_PEAK_OBSERVATIONS.lock();
        let before = self.retained.load(Ordering::SeqCst);
        observations.push(ReaderPeakObservation {
            registration,
            parent: self as *const Self as usize,
            peak: before,
        });
        READER_PEAK_ACTIVE.fetch_add(1, Ordering::SeqCst);
        drop(observations);
        let _remove = Remove(registration);
        let answer = operation();
        let observations = READER_PEAK_OBSERVATIONS.lock();
        let peak = observations
            .iter()
            .find(|entry| entry.registration == registration)
            .expect("live reader observation")
            .peak;
        let after = self.retained.load(Ordering::SeqCst);
        drop(observations);
        (
            answer,
            ReaderMemoryHighWaterForTest {
                before,
                peak,
                after,
            },
        )
    }

    fn effective_limit(&self) -> u64 {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(limit) = statement_child_proof::limit(self) {
            return self.limit.min(limit);
        }
        self.limit
    }
    // Both doors use the same real parent counter and the same admission boundary.
    fn try_count(&self, bytes: usize) -> std::result::Result<(), u64> {
        use std::sync::atomic::Ordering;
        self.retained
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes as u64)
                    .filter(|after| *after <= self.effective_limit())
            })
            .map(|before| {
                // This is the only increasing write to the original parent's
                // retained counter. Failed CAS attempts never reach this hook.
                #[cfg(any(test, feature = "test-seams"))]
                statement_child_proof::parent_admitted(self, before + bytes as u64);
                #[cfg(any(test, feature = "test-seams"))]
                if READER_PEAK_ACTIVE.load(Ordering::SeqCst) != 0 {
                    for observation in READER_PEAK_OBSERVATIONS.lock().iter_mut() {
                        if observation.parent == self as *const Self as usize {
                            observation.peak = observation.peak.max(before + bytes as u64);
                        }
                    }
                }
                #[cfg(not(any(test, feature = "test-seams")))]
                let _ = before;
            })
    }
    /// Admit every original root, including a standing parent, against its
    /// declared TOTAL limit before requesting the exact native allocation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn query_root(limit: u64) -> Result<QueryReadRoot> {
        Self::from_reader_limit(limit).map(|root| QueryReadRoot { root })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn from_reader_limit(limit: u64) -> Result<DeclaredReadRoot> {
        Self::try_from_reader_limit(limit).map_err(contextdb_core::Error::NativeEngineOther)
    }

    #[doc(hidden)]
    pub fn try_from_reader_limit(
        limit: u64,
    ) -> std::result::Result<DeclaredReadRoot, NativeRefusal> {
        let mut root = Self {
            limit,
            terminal_bytes: 0,
            file_child_high_water: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            live_child_control_bytes: std::sync::atomic::AtomicU64::new(0),
            live_child_payload_bytes: std::sync::atomic::AtomicU64::new(0),
            live_acl_cache_bytes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            observation: None,
        };
        let bytes = ReadMemoryOwner::native_layout::<Self, RootTerminalInvoice>().size();
        // The admitted AtomicU64 itself moves into the block, retaining this
        // exact debit; there is no second bank or standing-root allowance.
        root.try_count(bytes)
            .map_err(|held| NativeRefusal::MemoryBudgetExceeded {
                subsystem: "reader",
                operation: "query_root",
                requested_bytes: bytes,
                available_bytes: usize::try_from(limit.saturating_sub(held)).unwrap_or(usize::MAX),
                budget_limit_bytes: usize::try_from(limit).unwrap_or(usize::MAX),
                hint: "Reduce simultaneous reads or raise the declared reader memory limit.",
            })?;
        root.terminal_bytes = bytes;
        #[cfg(any(test, feature = "test-seams"))]
        let observation = root_observations::begin(bytes);
        #[cfg(any(test, feature = "test-seams"))]
        {
            root.observation = Some(observation);
        }
        let invoice = RootTerminalInvoice {
            bytes,
            #[cfg(any(test, feature = "test-seams"))]
            observation,
        };
        let root = DeclaredReadRoot(ReadMemoryOwner::try_native_typed(root, invoice)?);
        #[cfg(any(test, feature = "test-seams"))]
        {
            root_observations::bind(observation, &root);
            READER_PARENTS_CREATED.with(|created| created.set(created.get() + 1));
        }
        Ok(root)
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn retained_bytes_for_test(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Read the genuine effective ceiling without changing the parent declaration.
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn effective_limit_for_physical_test(&self) -> u64 {
        self.effective_limit()
    }
    /// Quiescent observation of actual admitted live child controls and parent bytes.
    pub(crate) fn standing_control_bytes(&self) -> (u64, u64) {
        (
            self.live_child_control_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// Total admitted live reader storage, including this original root block.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The actual original root allocation remains charged until physical free.
    pub(crate) fn root_control_bytes(&self) -> usize {
        self.terminal_bytes
    }

    /// Successful live child payload debits, excluding their native controls.
    pub(crate) fn child_payload_bytes(&self) -> u64 {
        self.live_child_payload_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn acl_cache_bytes(&self) -> u64 {
        self.live_acl_cache_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl DeclaredReadRoot {
    #[cfg(test)]
    pub(crate) fn assert_final_root_free_for_test(self) {
        let token = self.observation.expect("original root observation");
        drop(self);
        root_observations::assert_terminal_free(token);
        assert_eq!(root_observations::sample(token), (false, 0, 0));
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn acl_cache_budget(&self) -> Result<ReadMemoryOwner> {
        let bytes =
            ReadMemoryOwner::native_layout::<ReaderAclCacheBudget, ReaderAclCacheInvoice>().size();
        self.try_reserve_native(
            bytes,
            "reader_acl_cache",
            "budget_owner",
            "Increase reader memory to cache ACL grants.",
        )
        .map_err(contextdb_core::Error::NativeEngineOther)?;
        self.live_acl_cache_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let invoice = ReaderAclCacheInvoice {
            parent: self.clone(),
            bytes,
        };
        ReadMemoryOwner::try_native_result(
            ReaderAclCacheBudget {
                parent: self.clone(),
            },
            invoice,
        )
        .map_err(contextdb_core::Error::NativeEngineOther)
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
        &self,
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
        #[cfg(any(test, feature = "test-seams"))]
        let request_before = statement_child_proof::constructor_ordinal();
        #[cfg(any(test, feature = "test-seams"))]
        let initial_held = payload.retained.load(std::sync::atomic::Ordering::SeqCst);
        ReadMemoryOwner::try_native_typed(payload, invoice).map(|child| {
            let owner = child.owner();
            #[cfg(all(test, feature = "test-seams"))]
            crate::database::ordinary_native_owner_tests::child_allocated(
                self,
                &self.retained,
                owner.accounting_identity(),
                ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>(),
                &child.retained,
            );
            #[cfg(any(test, feature = "test-seams"))]
            register_native_child_observer(self, &owner, &child, request_before, initial_held);
            NativeReadChildReceipt { owner }
        })
    }

    /// Acquire one native child together with its mandatory failure controls.
    /// Admission happens on the actual child payload before that payload is
    /// moved into its native cell. No acquired capability escapes without the
    /// controls, and no existing connection allocation changes payer.
    pub(crate) fn native_child_with_controls(
        &self,
        bytes: usize,
    ) -> std::result::Result<(NativeReadChildReceipt, ReadCredit), NativeRefusal> {
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
        payload.try_reserve_native(
            bytes,
            "reader",
            "query_refusal_controls",
            "Reduce simultaneous requests or raise the declared reader memory limit.",
        )?;
        // Before the cell exists this rollback guard owns the child's parent
        // debit. Its success handoff keeps that same debit and the payload's
        // retained counter intact; it does not refund or reserve again.
        let rollback = ReadCredit::from_prepaid_owned(self.original_owner(), bytes);
        #[cfg(any(test, feature = "test-seams"))]
        let request_before = statement_child_proof::constructor_ordinal();
        #[cfg(any(test, feature = "test-seams"))]
        let initial_held = payload.retained.load(std::sync::atomic::Ordering::SeqCst);
        let child = ReadMemoryOwner::try_native_typed(payload, invoice)?;
        let owner = child.owner();
        #[cfg(any(test, feature = "test-seams"))]
        register_native_child_observer(self, &owner, &child, request_before, initial_held);
        let controls = ReadCredit::from_prepaid_owned(owner.clone(), bytes);
        rollback.transfer();
        Ok((NativeReadChildReceipt { owner }, controls))
    }

    /// Existing public provider result remains unqualified at its Error terminal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn native_child_unqualified(&self) -> Result<NativeReadChildReceipt> {
        self.native_child_unqualified_observed(|_, _| {})
    }
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn child_with_physical_counter_for_test(
        &self,
    ) -> Result<(ReadMemoryOwner, (usize, usize))> {
        let mut observed = (0, 0);
        let receipt = self.native_child_unqualified_observed(|child, owner| {
            observed = (
                owner.accounting_identity(),
                &child.retained as *const _ as usize,
            );
        })?;
        Ok((receipt.into_owner(), observed))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn native_child_unqualified_observed(
        &self,
        observe: impl FnOnce(&NativeDeclaredChild, &ReadMemoryOwner),
    ) -> Result<NativeReadChildReceipt> {
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
        #[cfg(any(test, feature = "test-seams"))]
        let request_before = statement_child_proof::constructor_ordinal();
        #[cfg(any(test, feature = "test-seams"))]
        let initial_held = payload.retained.load(std::sync::atomic::Ordering::SeqCst);
        ReadMemoryOwner::try_native_typed_unqualified(payload, invoice).map(|child| {
            let owner = child.owner();
            #[cfg(all(test, feature = "test-seams"))]
            crate::database::ordinary_native_owner_tests::child_allocated(
                self,
                &self.retained,
                owner.accounting_identity(),
                ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>(),
                &child.retained,
            );
            #[cfg(any(test, feature = "test-seams"))]
            register_native_child_observer(self, &owner, &child, request_before, initial_held);
            observe(&child, &owner);
            NativeReadChildReceipt { owner }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn child(&self) -> Result<ReadMemoryOwner> {
        self.native_child_unqualified()
            .map(NativeReadChildReceipt::into_owner)
    }
}
/// Strong aliases retain the same original native block. No Arc wrapper,
/// Weak header, external bank or self-credit cycle is created by this handle.
#[derive(Debug, Clone)]
pub struct DeclaredReadRoot(NativeReadMemory<DeclaredReadMemory>);
impl std::ops::Deref for DeclaredReadRoot {
    type Target = DeclaredReadMemory;
    fn deref(&self) -> &DeclaredReadMemory {
        &self.0
    }
}
impl AsRef<DeclaredReadMemory> for DeclaredReadRoot {
    fn as_ref(&self) -> &DeclaredReadMemory {
        self
    }
}
impl DeclaredReadRoot {
    pub(crate) fn original_owner(&self) -> ReadMemoryOwner {
        self.0.owner()
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn original_owner_for_test(&self) -> ReadMemoryOwner {
        self.original_owner()
    }
}
#[derive(Debug)]
pub(crate) struct QueryReadRoot {
    pub(crate) root: DeclaredReadRoot,
}
/// Terminal reservation metadata resides in the native block's invoice slot.
/// The last real strong owner moves it to NativeFreeGuard before payload Drop;
/// that guard physically frees the block before this invoice is destroyed.
#[derive(Debug)]
struct RootTerminalInvoice {
    bytes: usize,
    #[cfg(any(test, feature = "test-seams"))]
    observation: root_observations::Token,
}
impl Drop for RootTerminalInvoice {
    fn drop(&mut self) {
        // No root counter exists after physical free. Retire only this fixed
        // terminal debit; never dereference that counter or credit another bank.
        let _bytes = std::mem::replace(&mut self.bytes, 0);
        #[cfg(any(test, feature = "test-seams"))]
        root_observations::retire(self.observation, _bytes);
    }
}
impl Drop for DeclaredReadMemory {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(token) = self.observation {
            root_observations::payload_dropped(token, self);
        }
        debug_assert_eq!(
            self.live_child_control_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        debug_assert_eq!(
            self.live_child_payload_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        debug_assert_eq!(
            self.live_acl_cache_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        debug_assert_eq!(
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
            self.terminal_bytes as u64
        );
    }
}

/// The invoice, not the payload, owns the native block's parent debit.
#[derive(Debug)]
struct NativeChildInvoice {
    parent: DeclaredReadRoot,
    bytes: usize,
}
impl Drop for NativeChildInvoice {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-seams"))]
        let before = self.parent.standing_control_bytes();
        self.parent
            .live_child_control_bytes
            .fetch_sub(self.bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(self.bytes);
        #[cfg(any(test, feature = "test-seams"))]
        statement_child_proof::native_control_refunded(&self.parent, self.bytes, before);
    }
}
#[derive(Debug)]
struct NativeDeclaredChild {
    parent: DeclaredReadRoot,
    operation_limit: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
}
impl NativeDeclaredChild {
    #[cfg(any(test, feature = "test-seams"))]
    fn note_reader_budget_event(&self, subsystem: &str, operation: &str, bytes: usize, held: u64) {
        let identity =
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize);
        READER_BUDGET_LEDGER.with(|ledger| {
            if let Some(events) = ledger.borrow_mut().as_mut() {
                events.push(crate::memory_accounting::BudgetLedgerEntry {
                    what: if subsystem.is_empty() {
                        operation.to_owned()
                    } else {
                        format!("{subsystem}/{operation}")
                    },
                    bytes,
                    used_after: usize::try_from(held).unwrap_or(usize::MAX),
                    owner_identity: identity,
                    parent_identity: self.parent.as_ref() as *const DeclaredReadMemory as usize,
                    row_identity: None,
                    allocation: None,
                });
            }
        });
    }
    fn try_count(&self, bytes: usize) -> std::result::Result<u64, u64> {
        use std::sync::atomic::Ordering;
        let limit = self.operation_limit.load(Ordering::SeqCst);
        let control = ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
        self.retained
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes as u64).filter(|next| {
                    next.checked_add(control)
                        .is_some_and(|total| total <= limit)
                })
            })
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for NativeDeclaredChild {
    fn drop(&mut self) {
        native_child_identity(&self.parent, &self.retained, self as *const Self as usize);
        let identity = unregister_native_child_observer(&self.retained);
        if let Some(identity) = identity {
            statement_child_proof::native_dropped(self, identity);
        }
    }
}
impl ReadMemoryBudget for NativeDeclaredChild {
    #[cfg(feature = "test-seams")]
    fn coordination_counter_for_test(
        &self,
    ) -> Option<contextdb_core::coordination_test_observation::FundingCounter<'_>> {
        Some(contextdb_core::coordination_test_observation::FundingCounter::U64(&self.retained))
    }

    #[cfg(feature = "test-seams")]
    fn native_allocation_for_test(&self, _layout: std::alloc::Layout) {
        // The child's cell is paid by the original parent, not its own payload.
        crate::read_work_probe::allocator_events::funding_next(
            &self.parent.retained as *const _ as usize,
        );
    }

    fn constrain_operation_memory(&self, limit: usize) {
        self.operation_limit
            .fetch_min(limit as u64, std::sync::atomic::Ordering::SeqCst);
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
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::ordinary_native_owner_tests::child_counter(
            &self.parent,
            self as *const Self as usize,
            &self.retained,
            self.operation_limit
                .load(std::sync::atomic::Ordering::SeqCst),
        );
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ChildCall::new(
            &self.parent,
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize),
            &self.retained,
            ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size(),
            bytes,
            subsystem,
            operation,
            "NativeDeclaredChild::try_reserve_native",
            false,
        );
        let previous = self.try_count(bytes).map_err(|held| {
            #[cfg(any(test, feature = "test-seams"))]
            _capture.refused();
            let control =
                ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
            let limit = self
                .operation_limit
                .load(std::sync::atomic::Ordering::SeqCst);
            NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: limit.saturating_sub(held.saturating_add(control)) as usize,
                budget_limit_bytes: limit as usize,
                hint,
            }
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        let _forward = _capture.forwarding();
        if let Err(error) = self
            .parent
            .try_reserve_native(bytes, subsystem, operation, hint)
        {
            self.retained
                .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
            #[cfg(all(test, feature = "test-seams"))]
            crate::database::retained_trace_ownership_tests::ordered_cut(
                "child-reserve-refusal",
                [
                    native_child_identity(
                        &self.parent,
                        &self.retained,
                        self as *const Self as usize,
                    ),
                    bytes,
                    self.parent
                        .retained
                        .load(std::sync::atomic::Ordering::SeqCst) as usize,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
            );
            #[cfg(any(test, feature = "test-seams"))]
            _capture.refused();
            return Err(error);
        }
        // Both real owner admissions succeeded. Observe that exact debit;
        // a refusal rolls back above and never enters this component.
        self.parent
            .live_child_payload_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let held = previous + bytes as u64;
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
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::ordinary_native_owner_tests::child_counter(
            &self.parent,
            self as *const Self as usize,
            &self.retained,
            self.operation_limit
                .load(std::sync::atomic::Ordering::SeqCst),
        );
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ChildCall::new(
            &self.parent,
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize),
            &self.retained,
            ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size(),
            bytes,
            subsystem,
            operation,
            "NativeDeclaredChild::try_reserve",
            false,
        );
        let previous = self.try_count(bytes).map_err(|held| {
            #[cfg(any(test, feature = "test-seams"))]
            _capture.refused();
            let limit = self
                .operation_limit
                .load(std::sync::atomic::Ordering::SeqCst);
            let control =
                ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size() as u64;
            reader_memory_refusal(
                limit,
                held.saturating_add(control),
                bytes,
                subsystem,
                operation,
                hint,
            )
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        let _forward = _capture.forwarding();
        if let Err(error) = self.parent.try_reserve(bytes, subsystem, operation, hint) {
            self.retained
                .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
            #[cfg(all(test, feature = "test-seams"))]
            crate::database::retained_trace_ownership_tests::ordered_cut(
                "child-reserve-refusal",
                [
                    native_child_identity(
                        &self.parent,
                        &self.retained,
                        self as *const Self as usize,
                    ),
                    bytes,
                    self.parent
                        .retained
                        .load(std::sync::atomic::Ordering::SeqCst) as usize,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
            );
            #[cfg(any(test, feature = "test-seams"))]
            _capture.refused();
            return Err(error);
        }
        // Both real owner admissions succeeded. Observe that exact debit;
        // a refusal rolls back above and never enters this component.
        self.parent
            .live_child_payload_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let held = previous + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_reader_budget_event(subsystem, operation, bytes, held);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ChildCall::new(
            &self.parent,
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize),
            &self.retained,
            ReadMemoryOwner::native_layout::<Self, NativeChildInvoice>().size(),
            bytes,
            "",
            "release",
            "NativeDeclaredChild::release",
            false,
        );
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::retained_trace_ownership_tests::ordered_child_release(
            false,
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize),
            bytes,
        );
        let _before = self
            .retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let payload_before = self
            .parent
            .live_child_payload_bytes
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        assert!(
            payload_before >= bytes as u64,
            "reader payload observation underflow"
        );
        #[cfg(any(test, feature = "test-seams"))]
        self.note_reader_budget_event("", "release", bytes, _before - bytes as u64);
        #[cfg(any(test, feature = "test-seams"))]
        let _forward = _capture.forwarding();
        self.parent.release(bytes);
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::retained_trace_ownership_tests::ordered_child_release(
            true,
            native_child_identity(&self.parent, &self.retained, self as *const Self as usize),
            bytes,
        );
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
pub(crate) fn note_reader_body_admission_for_test(
    bytes: usize,
    row: contextdb_core::RowId,
    created: contextdb_core::TxId,
    lsn: contextdb_core::Lsn,
) {
    READER_BUDGET_LEDGER.with(|ledger| {
        if let Some(events) = ledger.borrow_mut().as_mut() {
            if let Some(event) = events.last_mut().filter(|event| {
                event.owner_identity != event.parent_identity
                    && event.bytes == bytes
                    && event.what != "release"
            }) {
                event.what = "vector_store/load_raw_vector_candidate".into();
                event.row_identity = Some((row.0, created.0, lsn.0));
            }
        }
    });
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn note_reader_container_allocation_for_test(
    bytes: usize,
    pointer: usize,
    capacity: usize,
    element_bytes: usize,
) {
    READER_BUDGET_LEDGER.with(|ledger| {
        if let Some(events) = ledger.borrow_mut().as_mut() {
            if let Some(event) = events.last_mut().filter(|event| {
                event.owner_identity != event.parent_identity && event.bytes == bytes
            }) {
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
    #[cfg(feature = "test-seams")]
    fn coordination_counter_for_test(
        &self,
    ) -> Option<contextdb_core::coordination_test_observation::FundingCounter<'_>> {
        Some(contextdb_core::coordination_test_observation::FundingCounter::U64(&self.retained))
    }

    #[cfg(feature = "test-seams")]
    fn native_allocation_for_test(&self, layout: std::alloc::Layout) {
        if let Some(token) = self.observation {
            root_observations::before_allocation(token, self, layout);
        }
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
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ParentCall::new(
            self,
            bytes,
            subsystem,
            operation,
            "DeclaredReadMemory::try_reserve_native",
        );
        let result = self
            .try_count(bytes)
            .map_err(|held| NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: usize::try_from(self.effective_limit().saturating_sub(held))
                    .unwrap_or(usize::MAX),
                budget_limit_bytes: usize::try_from(self.effective_limit()).unwrap_or(usize::MAX),
                hint,
            });
        #[cfg(any(test, feature = "test-seams"))]
        if result.is_err() {
            _capture.refused();
        }
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
        #[cfg(test)]
        if result.is_ok() {
            trace_reader_credit(
                self as *const Self as usize,
                "reserve_native",
                bytes,
                subsystem,
                operation,
                self.retained.load(std::sync::atomic::Ordering::SeqCst),
            );
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
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ParentCall::new(
            self,
            bytes,
            subsystem,
            operation,
            "DeclaredReadMemory::try_reserve",
        );

        self.try_count(bytes).map_err(|held| {
            #[cfg(any(test, feature = "test-seams"))]
            _capture.refused();
            reader_memory_refusal(
                self.effective_limit(),
                held,
                bytes,
                subsystem,
                operation,
                hint,
            )
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        READER_BUDGET_LEDGER.with(|ledger| {
            if let Some(entries) = ledger.borrow_mut().as_mut() {
                entries.push(crate::memory_accounting::BudgetLedgerEntry {
                    what: format!("{subsystem}/{operation}"),
                    bytes,
                    used_after: usize::try_from(
                        self.retained.load(std::sync::atomic::Ordering::SeqCst),
                    )
                    .unwrap_or(usize::MAX),
                    owner_identity: self as *const Self as usize,
                    parent_identity: self as *const Self as usize,
                    row_identity: None,
                    allocation: None,
                });
            }
        });
        #[cfg(test)]
        trace_reader_credit(
            self as *const Self as usize,
            "reserve",
            bytes,
            subsystem,
            operation,
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
        );
        Ok(())
    }
    fn release(&self, bytes: usize) {
        #[cfg(any(test, feature = "test-seams"))]
        let _capture = statement_child_proof::ParentCall::new(
            self,
            bytes,
            "",
            "release",
            "DeclaredReadMemory::release",
        );
        self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        #[cfg(test)]
        trace_reader_credit(
            self as *const Self as usize,
            "release",
            bytes,
            "",
            "",
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
        );
    }
}
impl DeclaredReadRoot {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn request_budget_owner(&self) -> Result<RequestReadOwner> {
        self.native_child_unqualified()
            .map(RequestReadOwner::native)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn request_budget_owner_native(
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
    parent: DeclaredReadRoot,
}
impl ReadMemoryBudget for ReaderAclCacheBudget {
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
        self.parent
            .live_acl_cache_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
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
// The original root pin and control invoice live outside the payload during
// destruction. NativeFreeGuard frees the actual block before this refund.
#[derive(Debug)]
struct ReaderAclCacheInvoice {
    parent: DeclaredReadRoot,
    bytes: usize,
}
impl Drop for ReaderAclCacheInvoice {
    fn drop(&mut self) {
        self.parent
            .live_acl_cache_bytes
            .fetch_sub(self.bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(self.bytes);
    }
}

// Observers borrow counters only while the fixed registry lock excludes
// payload retirement. Their scalar generations never extend allocation life.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Clone)]
pub(crate) struct DeclaredReadChildObserver {
    native_generation: u64,
    parent: root_observations::Token,
    owner_identity: usize,
    parent_identity: usize,
}
#[cfg(any(test, feature = "test-seams"))]
impl DeclaredReadChildObserver {
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from retained_trace_ownership_tests.rs"
        )
    )]
    #[allow(
        dead_code,
        reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
    )]
    pub(crate) fn retain_original_for_physical_test(&self) -> Option<ReadMemoryOwner> {
        let source = {
            let registry = native_child_observers()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            registry
                .get_generation(self.owner_identity, self.native_generation)?
                .forwarder
                .as_ref()?
                .upgrade()?
        };
        Some(source.original_native_owner())
    }

    // Raw child counter is valid only while the caller pins the original
    // native owner. It is INVALID at its own block free. For that final free,
    // use NativeOwnerControls.parent_counter while Database or the original
    // NativeFreeGuard invoice retains that parent; sample() never reads a
    // retired child. The pointer itself carries no lifetime or ownership.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn physical_funding_counter_for_test(&self) -> usize {
        let registry = native_child_observers()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let record = registry
            .get_generation(self.owner_identity, self.native_generation)
            .expect("physical observation generation invalidated");
        assert!(
            record.alive && record.counter != 0,
            "physical observation requires a pinned live original child"
        );
        record.counter
    }
    pub(crate) fn sample(&self) -> crate::read_work_probe::DeclaredReadChildFunding {
        use std::sync::atomic::Ordering;
        let mut registry = native_child_observers()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let record = registry
            .get_generation_mut(self.owner_identity, self.native_generation)
            .expect("child observation generation retired; proof invalidated");
        if record.alive {
            assert_ne!(record.counter, 0, "real child counter not bound");
            // SAFETY: child Drop invalidates under this same mutex before free.
            record.last_charged =
                unsafe { &*(record.counter as *const std::sync::atomic::AtomicU64) }
                    .load(Ordering::SeqCst);
        }
        let (parent_alive, _, parent_charged_bytes) = root_observations::sample(self.parent);
        crate::read_work_probe::DeclaredReadChildFunding {
            owner_identity: self.owner_identity,
            alive: record.alive,
            charged_bytes: record.last_charged,
            parent_identity: self.parent_identity,
            parent_alive,
            parent_charged_bytes,
        }
    }
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn declared_child_observer(
    read_memory: impl Into<ReadMemoryOwner>,
) -> Option<DeclaredReadChildObserver> {
    native_observer(observed_reader_identity(read_memory))
}
#[cfg(all(test, feature = "test-seams"))]
pub(crate) fn original_child_observer_for_test(
    identity: usize,
) -> Option<DeclaredReadChildObserver> {
    native_observer(identity)
}

/// Genuine native owner and a separately retained original parent for physical
/// observations. Neither handle allocates a replacement owner or pays credits.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct NativePhysicalPin {
    _owner: ReadMemoryOwner,
    observer: DeclaredReadChildObserver,
}
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub struct ParentPhysicalPin(DeclaredReadRoot);
#[cfg(any(test, feature = "test-seams"))]
impl NativePhysicalPin {
    pub fn sample(&self) -> crate::read_work_probe::DeclaredReadChildFunding {
        self.observer.sample()
    }
    pub fn counter(&self) -> crate::read_work_probe::allocator_events::FundingCounter<'_> {
        // SAFETY: clone the genuine in-block owner that contains this counter.
        // A forgotten watch must retain the owner even after this borrow ends.
        unsafe {
            crate::read_work_probe::allocator_events::FundingCounter::from_native_pinned(
                self.observer.physical_funding_counter_for_test(),
                self._owner.clone(),
            )
        }
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl ParentPhysicalPin {
    pub fn counter(&self) -> crate::read_work_probe::allocator_events::FundingCounter<'_> {
        // SAFETY: the cloned original parent contains this retained counter.
        unsafe {
            crate::read_work_probe::allocator_events::FundingCounter::from_parent_pinned(
                &self.0.retained as *const _ as usize,
                self.0.clone(),
            )
        }
    }
    pub fn charged_bytes(&self) -> u64 {
        self.0.retained_bytes_for_test()
    }
}
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn pin_original_native_for_test(
    window: &crate::read_work_probe::allocator_events::Window,
    native: statement_child_proof::NativeOwnerControls,
    parent: &DeclaredReadRoot,
    owner: &ReadMemoryOwner,
) -> Option<(NativePhysicalPin, ParentPhysicalPin)> {
    use crate::read_work_probe::allocator_events as events;
    let Some(actual) = native.block.actual_request else {
        events::fail(events::MISSING);
        return None;
    };
    let request = window.live_request(native.payload).ok()?;
    if (request.pointer, request.bytes, request.align) != actual
        || request.ordinal != native.block.ordinal
        || request.pointer != native.owner
    {
        events::fail(events::STALE);
        return None;
    }
    let Some(observer) = native_observer(native.owner) else {
        events::fail(events::MISSING);
        return None;
    };
    if !owner.is_native() || owner.accounting_identity() != native.owner {
        events::fail(events::STALE);
        return None;
    }
    // A scalar registry token cannot promote a retired native root. The
    // fixture supplies its genuine original strong capability; validate it
    // against the constructor receipt before retaining that same allocation.
    if observer.physical_funding_counter_for_test() != native.counter
        || parent.as_ref() as *const _ as usize != native.parent
        || observer.parent_identity != native.parent
        || &parent.retained as *const _ as usize != native.parent_counter
    {
        events::fail(events::STALE);
        return None;
    }
    Some((
        NativePhysicalPin {
            _owner: owner.clone(),
            observer,
        },
        ParentPhysicalPin(parent.clone()),
    ))
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) trait NativeOriginalPinSource: Send + Sync {
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from control_memory.rs"
        )
    )]
    #[allow(
        dead_code,
        reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
    )]
    fn original_native_owner(&self) -> ReadMemoryOwner;
}
#[cfg(any(test, feature = "test-seams"))]
struct NativeChildObservation {
    identity: usize,
    generation: u64,
    parent: root_observations::Token,
    parent_identity: usize,
    bytes: usize,
    counter: usize,
    payload_identity: usize,
    forwarder_identity: usize,
    forwarder: Option<std::sync::Weak<dyn NativeOriginalPinSource>>,
    alive: bool,
    last_charged: u64,
    native: bool,
}
// Observation capacity only, never a product limit. Saturation fails the proof
// explicitly. These slots live in static storage, not on a TLS or caller stack.
#[cfg(any(test, feature = "test-seams"))]
struct NativeChildObservers {
    slots: [Option<NativeChildObservation>; 64],
    generation: u64,
}
#[cfg(any(test, feature = "test-seams"))]
impl NativeChildObservers {
    fn get(&self, identity: &usize) -> Option<&NativeChildObservation> {
        self.slots
            .iter()
            .flatten()
            .find(|r| r.alive && r.identity == *identity)
    }
    fn get_mut(&mut self, identity: &usize) -> Option<&mut NativeChildObservation> {
        self.slots
            .iter_mut()
            .flatten()
            .find(|r| r.alive && r.identity == *identity)
    }
    fn iter(&self) -> impl Iterator<Item = (&usize, &NativeChildObservation)> {
        self.slots
            .iter()
            .flatten()
            .filter(|r| r.alive)
            .map(|r| (&r.identity, r))
    }
    fn get_generation(&self, identity: usize, generation: u64) -> Option<&NativeChildObservation> {
        self.slots
            .iter()
            .flatten()
            .find(|r| r.identity == identity && r.generation == generation)
    }
    fn get_generation_mut(
        &mut self,
        identity: usize,
        generation: u64,
    ) -> Option<&mut NativeChildObservation> {
        self.slots
            .iter_mut()
            .flatten()
            .find(|r| r.identity == identity && r.generation == generation)
    }
    fn retire(&mut self, identity: usize, held: u64) {
        let record = self
            .get_mut(&identity)
            .expect("child retirement lost its original record");
        assert!(
            record.forwarder.is_none() && record.forwarder_identity == 0,
            "child payload cannot retire before its forwarding weak tail"
        );
        record.last_charged = held;
        record.counter = 0;
        record.alive = false;
    }
    fn insert(&mut self, mut record: NativeChildObservation) {
        assert!(
            self.get(&record.identity).is_none(),
            "duplicate native observer invalidates proof"
        );
        let index = self
            .slots
            .iter()
            .position(Option::is_none)
            .or_else(|| {
                self.slots
                    .iter()
                    .enumerate()
                    .filter_map(|(i, r)| r.as_ref().filter(|r| !r.alive).map(|r| (i, r.generation)))
                    .min_by_key(|(_, generation)| *generation)
                    .map(|(i, _)| i)
            })
            .expect("native observer static capacity exhausted; proof invalid");
        self.generation = self
            .generation
            .checked_add(1)
            .expect("native observer generation exhausted");
        record.generation = self.generation;
        self.slots[index] = Some(record);
    }
}
#[cfg(any(test, feature = "test-seams"))]
fn native_child_observers() -> &'static std::sync::Mutex<NativeChildObservers> {
    static REGISTRY: std::sync::Mutex<NativeChildObservers> =
        std::sync::Mutex::new(NativeChildObservers {
            slots: [const { None }; 64],
            generation: 0,
        });
    &REGISTRY
}
#[cfg(any(test, feature = "test-seams"))]
fn register_native_child_observer(
    parent: &DeclaredReadRoot,
    owner: &ReadMemoryOwner,
    child: &NativeDeclaredChild,
    request_before: usize,
    initial_held: u64,
) {
    // Active and inactive observation both allocate nothing. No strong owner
    // or invoice enters this registry, including during first registration.
    if statement_child_proof::limit(parent).is_none()
        && !READER_BUDGET_LEDGER.with(|ledger| ledger.borrow().is_some())
    {
        return;
    }
    // Charge-only ledgers remain useful without an observing allocator, but
    // their roots carry no physical evidence for a child consumer to sample.
    if parent
        .observation
        .is_none_or(|token| !root_observations::has_evidence(token))
    {
        return;
    }
    let identity = owner.accounting_identity();
    let bytes = ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size();
    native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(NativeChildObservation {
            identity,
            generation: 0,
            parent: parent.observation.expect("original root observation"),
            parent_identity: parent.as_ref() as *const _ as usize,
            bytes,
            counter: &child.retained as *const _ as usize,
            payload_identity: child as *const _ as usize,
            forwarder_identity: 0,
            forwarder: None,
            alive: true,
            last_charged: initial_held,
            native: true,
        });
    statement_child_proof::native_created(
        parent,
        identity,
        bytes,
        request_before,
        initial_held,
        child,
    );
}
#[cfg(any(test, feature = "test-seams"))]
fn native_child_identity(
    parent: &DeclaredReadMemory,
    counter: &std::sync::atomic::AtomicU64,
    fallback: usize,
) -> usize {
    #[cfg(feature = "test-seams")]
    crate::read_work_probe::note_native_child_identity();
    let address = counter as *const _ as usize;
    let mut registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    for record in registry
        .slots
        .iter_mut()
        .flatten()
        .filter(|r| r.alive && r.native)
    {
        let identity = record.identity;
        // Bind the actual counter supplied by NativeDeclaredChild, using the
        // genuine allocator block bounds, never an assumed payload offset.
        if address >= identity
            && address
                .checked_add(std::mem::size_of_val(counter))
                .is_some_and(|end| end <= identity + record.bytes)
        {
            assert_eq!(
                record.parent_identity, parent as *const _ as usize,
                "foreign parent in native observer invalidates proof"
            );
            assert!(record.counter == 0 || record.counter == address);
            assert!(fallback >= identity && fallback < identity + record.bytes);
            record.counter = address;
            record.payload_identity = fallback;
            statement_child_proof::native_forwarder(
                parent,
                identity,
                fallback,
                address,
                record.forwarder_identity,
            );
            return identity;
        }
    }
    assert!(
        !statement_child_proof::expects_native_address(parent, address),
        "selected native counter missing original registry entry"
    );
    fallback
}
#[cfg(any(test, feature = "test-seams"))]
fn unregister_native_child_observer(counter: &std::sync::atomic::AtomicU64) -> Option<usize> {
    let address = counter as *const _ as usize;
    let mut registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let identity = registry.iter().find_map(|(&identity, record)| {
        (address >= identity && address < identity + record.bytes).then_some(identity)
    });
    if let Some(identity) = identity {
        registry.retire(identity, counter.load(std::sync::atomic::Ordering::SeqCst));
    }
    identity
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn register_native_forwarder<T: NativeOriginalPinSource + 'static>(
    identity: usize,
    forwarder: &Arc<T>,
) {
    let mut registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if statement_child_proof::selected_native(identity) {
        assert!(
            registry.get(&identity).is_some(),
            "selected native forwarder missing original record"
        );
    }
    if let Some(record) = registry.get_mut(&identity) {
        assert_eq!(
            record.forwarder_identity, 0,
            "duplicate native forwarder invalidates proof"
        );
        assert_ne!(
            record.counter, 0,
            "real adapter admissions must bind the native counter"
        );
        record.forwarder_identity = Arc::as_ptr(forwarder) as usize;
        let erased: Arc<dyn NativeOriginalPinSource> = forwarder.clone();
        record.forwarder = Some(Arc::downgrade(&erased));
        root_observations::with_live(record.parent, |parent| {
            statement_child_proof::native_forwarder(
                parent,
                identity,
                record.payload_identity,
                record.counter,
                record.forwarder_identity,
            )
        });
    }
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn unregister_native_forwarder(identity: usize) {
    let mut registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if statement_child_proof::selected_native(identity) {
        assert!(
            registry.get(&identity).is_some(),
            "selected native retirement missing original record"
        );
    }
    if let Some(record) = registry.get_mut(&identity) {
        record.forwarder = None;
        // Retire the alias before its address can be reused by another Arc.
        // Existing observers and pins already name the original native block.
        record.forwarder_identity = 0;
        statement_child_proof::native_forwarder_retired(identity);
    }
}
#[cfg(any(test, feature = "test-seams"))]
fn native_observer(identity: usize) -> Option<DeclaredReadChildObserver> {
    let registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let record = registry.get(&identity)?;
    Some(DeclaredReadChildObserver {
        native_generation: record.generation,
        parent: record.parent,
        owner_identity: identity,
        parent_identity: record.parent_identity,
    })
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn observed_identity(identity: usize) -> usize {
    let registry = native_child_observers()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    registry
        .iter()
        .find_map(|(&native, record)| {
            (record.forwarder_identity != 0 && record.forwarder_identity == identity)
                .then_some(native)
        })
        .unwrap_or(identity)
}
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn observed_reader_identity(reader: impl Into<ReadMemoryOwner>) -> usize {
    observed_identity(reader.into().accounting_identity())
}

// Evidence-only rows called by the existing cross-thread allocation oracle test.
#[cfg(test)]
pub(crate) mod native_ownership_proof {
    use super::*;
    use contextdb_core::read_memory::ReadCredit;
    use std::alloc::Layout;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};

    thread_local! {
        static WATCH_REQUEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        // The allocator oracle can refuse one selected live cache Layout.  It
        // is test-only and consumed before System is called, exactly like the
        // native-owner null row below.
        static REFUSE_ONE_LAYOUT: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
        static REFUSE_LAYOUT_SKIP: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
        parent: root_observations::Token,
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
        baseline: u64,
    }
    fn sample(parent: &root_observations::Token) -> Sample {
        let (alive, controls, retained) = root_observations::sample(*parent);
        Sample {
            alive,
            controls,
            retained,
        }
    }
    fn layout() -> Layout {
        ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>()
    }
    fn begin(parent: &DeclaredReadRoot, null_once: bool) {
        assert_eq!(POINTER.load(Ordering::SeqCst), 0);
        let mut observation = OBSERVATION.lock().unwrap();
        assert!(observation.is_none());
        *observation = Some(Observation {
            parent: parent.observation.expect("original root observation"),
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
            baseline: parent.charged_bytes(),
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
        REFUSE_LAYOUT_SKIP.with(|skip| skip.set(0));
    }
    pub(crate) fn arm_refuse_nth_live_layout(layout: Layout, preceding_matches: usize) {
        arm_refuse_one_live_layout(layout);
        REFUSE_LAYOUT_SKIP.with(|skip| skip.set(preceding_matches));
    }
    pub(crate) fn refusal_armed_for_test() -> bool {
        REFUSE_ONE_LAYOUT.with(|armed| armed.get().0 != 0)
    }
    pub(crate) fn refuse_selected_live_layout(request: Layout) -> bool {
        REFUSE_ONE_LAYOUT
            .try_with(|armed| {
                let selected = armed.get();
                if selected == (request.size(), request.align()) {
                    if REFUSE_LAYOUT_SKIP.with(|skip| {
                        let remaining = skip.get();
                        if remaining == 0 {
                            false
                        } else {
                            skip.set(remaining - 1);
                            true
                        }
                    }) {
                        return false;
                    }
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
    fn acquire(parent: &DeclaredReadRoot) -> ReadMemoryOwner {
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
            retained: observation.baseline + bytes,
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

    fn root_rows() {
        use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
        let layout = native_control_layouts_for_test().0;
        let bytes = layout.size() as u64;
        for limit in [0, bytes - 1] {
            let (refused, _, count) = oracle::native_requests_for_test(|| {
                DeclaredReadMemory::try_from_reader_limit(limit)
            });
            assert_eq!(count, 0, "root refusal must precede all allocator requests");
            assert!(matches!(refused, Err(NativeRefusal::MemoryBudgetExceeded {
                subsystem: "reader", operation: "query_root", requested_bytes,
                available_bytes, budget_limit_bytes, ..
            }) if requested_bytes == layout.size() && available_bytes == limit as usize
                && budget_limit_bytes == limit as usize));
        }
        arm_refuse_one_live_layout(layout);
        let (refused, _, count) =
            oracle::native_requests_for_test(|| DeclaredReadMemory::try_from_reader_limit(bytes));
        assert!(
            !refusal_armed_for_test(),
            "root null control must reach its actual allocator"
        );
        assert_eq!(count, 0, "null attempt creates no physical allocation");
        assert!(
            matches!(refused, Err(NativeRefusal::AllocatorFailed { layout: actual, .. })
            if actual == layout)
        );
        root_observations::assert_null_terminal_retired();
        for unwind in [false, true] {
            let (root, requests, count) = oracle::native_requests_for_test(|| {
                DeclaredReadMemory::try_from_reader_limit(bytes).unwrap()
            });
            assert_eq!(
                count, 1,
                "root plus terminal invoice is exactly one actual request"
            );
            let token = root.observation.unwrap();
            let request = requests[0];
            assert_eq!(request.0, root.original_owner().accounting_identity());
            assert_eq!((request.1, request.2), (layout.size(), layout.align()));
            assert_eq!(root.standing_control_bytes(), (0, bytes));
            let alias = root.clone();
            drop(root);
            assert!(
                root_observations::sample(token).0,
                "same original alias keeps root alive"
            );
            if unwind {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _last = alias;
                    panic!("root alias unwind");
                }));
                assert!(result.is_err());
            } else {
                drop(alias);
            }
            root_observations::assert_terminal_free(token);
        }
    }

    pub(crate) fn run_native_rows() {
        let bytes = layout().size() as u64;
        let root_bytes = native_control_layouts_for_test().0.size() as u64;
        // These synthetic constructor boundaries include the actual root Layout;
        // no workload ceiling or operation budget is increased.
        root_rows();
        // Exact-limit activation, untouched clone identity, split/merge and refusal rows.
        for external_drop in [false, true] {
            let parent = DeclaredReadMemory::from_reader_limit(root_bytes + bytes + 17).unwrap();
            let lifetime = parent.observation.expect("original root generation");
            begin(&parent, false);
            let owner = acquire(&parent);
            assert_eq!(parent.standing_control_bytes(), (bytes, root_bytes + bytes));
            let clone = owner.clone();
            assert!(owner.same_owner(&clone));
            let mut credit =
                ReadCredit::try_new_owned(clone, 17, "proof", "activation", "proof").unwrap();
            assert_eq!(
                parent.standing_control_bytes(),
                (bytes, root_bytes + bytes + 17)
            );
            assert!(credit.try_grow_for(1, "proof", "refusal", "proof").is_err());
            assert_eq!(credit.bytes(), 17);
            assert!(credit.try_split_off(18).is_err());
            assert_eq!(credit.bytes(), 17);
            let split = credit.try_split_off(7).unwrap();
            assert_eq!(credit.bytes(), 10);
            credit.try_merge_prepaid(split).unwrap();
            assert_eq!(credit.bytes(), 17);
            let other_parent =
                DeclaredReadMemory::from_reader_limit(root_bytes + bytes + 3).unwrap();
            let other = acquire(&other_parent);
            assert!(!owner.same_owner(&other));
            let other_credit =
                ReadCredit::try_new_owned(other, 3, "proof", "other", "proof").unwrap();
            assert!(credit.try_merge_prepaid(other_credit).is_err());
            assert_eq!(credit.bytes(), 17);
            assert_eq!(other_parent.standing_control_bytes(), (0, root_bytes));
            drop(owner);
            assert_eq!(
                parent.standing_control_bytes(),
                (bytes, root_bytes + bytes + 17)
            );
            if external_drop {
                drop(parent);
                assert!(root_observations::sample(lifetime).0);
                drop(credit);
                assert!(!root_observations::sample(lifetime).0);
                root_observations::assert_terminal_free(lifetime);
            } else {
                drop(credit);
                assert_eq!(parent.standing_control_bytes(), (0, root_bytes));
                drop(parent);
            }
            assert_freed(&finish());
        }
        // Different native requests remain different capabilities even at one parent.
        let parent = DeclaredReadMemory::from_reader_limit(root_bytes + 2 * bytes + 5).unwrap();
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
        assert_eq!(
            parent.standing_control_bytes(),
            (bytes, root_bytes + bytes + 3)
        );
        drop(credit);
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));

        // A real application Legacy budget keeps the exact capability supplied by its host.
        #[derive(Debug)]
        struct HostProvider(Arc<dyn ReadMemoryBudget>);
        impl ReadRequestBudgetProvider for HostProvider {
            fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>> {
                Ok(self.0.clone())
            }
        }
        #[derive(Debug, Default)]
        struct HostBudget(AtomicUsize);
        impl ReadMemoryBudget for HostBudget {
            fn try_reserve(
                &self,
                bytes: usize,
                _: &'static str,
                _: &'static str,
                _: &'static str,
            ) -> Result<()> {
                self.0.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            fn release(&self, bytes: usize) {
                self.0.fetch_sub(bytes, Ordering::SeqCst);
            }
        }
        let parent = DeclaredReadMemory::from_reader_limit(u64::MAX).unwrap();
        let child: Arc<dyn ReadMemoryBudget> = Arc::new(HostBudget::default());
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
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));

        // Original parent remains funded even when all external parent handles close.
        // Barriers make every contender own its credit before any contender releases it.
        for contenders in [2, 8] {
            let parent =
                DeclaredReadMemory::from_reader_limit(root_bytes + bytes + contenders as u64)
                    .unwrap();
            let lifetime = parent.observation.expect("original root generation");
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
                (bytes, root_bytes + bytes + contenders as u64)
            );
            drop(owner);
            drop(parent);
            assert!(root_observations::sample(lifetime).0);
            release.wait();
            for thread in threads {
                thread.join().unwrap();
            }
            assert!(!root_observations::sample(lifetime).0);
            root_observations::assert_terminal_free(lifetime);
            assert_freed(&finish());
        }
        // Declared refusal must precede any allocator request.
        let parent = DeclaredReadMemory::from_reader_limit(root_bytes + bytes - 1).unwrap();
        begin(&parent, false);
        assert!(parent.request_budget_owner().is_err());
        let refused = finish();
        assert_eq!(
            (refused.requests, refused.allocations, refused.frees),
            (0, 0, 0)
        );
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));

        // Null from the same real allocator drops actual payload/invoice without a free.
        let parent = DeclaredReadMemory::from_reader_limit(root_bytes + bytes).unwrap();
        begin(&parent, true);
        assert!(parent.request_budget_owner().is_err());
        let refused = finish();
        assert_eq!(
            (refused.requests, refused.allocations, refused.frees),
            (1, 0, 0)
        );
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));

        // Request cancellation before activation is ordinary owner destruction.
        let parent = DeclaredReadMemory::from_reader_limit(root_bytes + bytes).unwrap();
        begin(&parent, false);
        let owner = acquire(&parent);
        drop(owner);
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));

        // Unwind after activation must drop credits and the actual native request.
        let parent = DeclaredReadMemory::from_reader_limit(root_bytes + bytes + 17).unwrap();
        begin(&parent, false);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let owner = acquire(&parent);
            let _credit =
                ReadCredit::try_new_owned(owner.clone(), 17, "proof", "unwind", "proof").unwrap();
            panic!("deterministic request unwind");
        }));
        assert!(result.is_err());
        assert_freed(&finish());
        assert_eq!(parent.standing_control_bytes(), (0, root_bytes));
    }
}

#[cfg(test)]
// Diagnostic only: the unchanged public-answer test can opt in without editing
// its local observer or any assertion. Observe the real declared parent after
// successful admission/refund; a stack identifies producers despite label adapters.
fn trace_reader_credit(
    parent: usize,
    event: &str,
    bytes: usize,
    subsystem: &str,
    operation: &str,
    retained_after: u64,
) {
    use std::fmt::Write as _;
    use std::io::Write as _;

    const SELECTOR: &str = "database::content_size_contract_tests::public_execute_releases_answer_and_copy_credit_before_return";
    const EVENT_LIMIT: usize = 2048;
    const TEXT_LIMIT: usize = 8192;
    struct Trace {
        file: std::fs::File,
        events: usize,
    }
    struct Text {
        bytes: [u8; TEXT_LIMIT],
        used: usize,
        truncated: bool,
    }
    impl std::fmt::Write for Text {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let room = self.bytes.len() - self.used;
            let mut end = text.len().min(room);
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.bytes[self.used..self.used + end].copy_from_slice(&text.as_bytes()[..end]);
            self.used += end;
            self.truncated |= end != text.len();
            if self.truncated {
                Err(std::fmt::Error)
            } else {
                Ok(())
            }
        }
    }
    thread_local! {
        static TRACE: std::cell::RefCell<Option<Trace>> = const { std::cell::RefCell::new(None) };
    }
    if std::thread::current().name() != Some(SELECTOR) {
        return;
    }
    TRACE.with(|slot| {
        // Backtrace formatting and file I/O never run inside an allocator hook.
        // Also prevent a nested diagnostic from borrowing the observer recursively.
        let Ok(mut state) = slot.try_borrow_mut() else { return };
        if state.is_none() {
            let Some(path) = std::env::var_os("CONTEXTDB_TEST_READ_CREDIT_TRACE") else { return };
            let file = std::fs::File::create(path).expect("create opted-in reader credit trace");
            *state = Some(Trace { file, events: 0 });
        }
        let trace = state.as_mut().unwrap();
        if trace.events == EVENT_LIMIT {
            return;
        }
        trace.events += 1;
        let mut text = Text { bytes: [0; TEXT_LIMIT], used: 0, truncated: false };
        let _ = writeln!(text,
            "sequence={} parent={parent:#x} event={event} bytes={bytes} subsystem={subsystem} operation={operation} retained_after={retained_after}",
            trace.events,
        );
        let _ = writeln!(text, "{}", std::backtrace::Backtrace::force_capture());
        trace.file.write_all(&text.bytes[..text.used]).expect("write reader credit trace");
        if text.truncated {
            trace.file.write_all(b"\nBACKTRACE_TRUNCATED\n").expect("write trace truncation marker");
        }
        if trace.events == EVENT_LIMIT {
            trace.file.write_all(b"EVENT_LIMIT_REACHED; later events omitted\n").expect("write trace limit marker");
        }
    });
}

/// Scalar-only observations of the genuine child acquired by the public SQL door.
/// No witness owner, retained Arc, heap log, credit, or counter reset is created.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub mod statement_child_proof {
    use super::*;
    use std::cell::Cell;
    use std::sync::atomic::Ordering;

    /// Payload addresses identify two separate physical allocator requests by
    /// containment. Layouts are expectations to compare with the raw oracle;
    /// they do not substitute for measured requests. The caller must pin the
    /// original child before sampling `counter` at either side of native free.
    #[cfg(any(test, feature = "test-seams"))]
    #[derive(Debug, Default, Clone, Copy)]
    pub struct TraceOwnerControls {
        pub payer: usize,
        pub parent: usize,
        pub parent_counter: usize,
        pub native_forwarder: usize,
        pub native_keeper: usize,
        pub native_adapter_invoice_bytes: usize,
        pub counter: usize,
        pub before_admission: u64,
        pub after_admission: u64,
        pub provider_payload: usize,
        pub provider_layout: (usize, usize),
        pub keeper_payload: usize,
        pub keeper_layout: (usize, usize),
    }
    /// Expected Layout and actual allocator request are deliberately separate.
    /// Actual comes from the existing unit allocator or the integration allocator
    /// bridge. An inactive/missing/unqualified observer leaves it None.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct ControlAllocation {
        pub payload: usize,
        pub layout: (usize, usize),
        pub actual_request: Option<(usize, usize, usize)>,
        pub ordinal: usize,
    }
    #[derive(Debug, Default, Clone, Copy)]
    pub struct ControlFunding {
        pub child_counter: usize,
        pub child_bytes: u64,
        pub parent_counter: usize,
        pub parent_bytes: u64,
        pub live_child_controls: u64,
        pub acl_bytes: u64,
    }
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NativeOwnerControls {
        pub owner: usize,
        pub parent: usize,
        pub parent_counter: usize,
        pub parent_before_block: Option<u64>,
        pub parent_after_block: u64,
        pub initial_child_bytes: u64,
        pub block: ControlAllocation,
        pub payload: usize,
        pub counter: usize,
        pub keeper_before: Option<ControlFunding>,
        pub keeper_after: Option<ControlFunding>,
        pub adapter_before: Option<ControlFunding>,
        pub adapter_after: Option<ControlFunding>,
        pub keeper: Option<ControlAllocation>,
        pub adapter: Option<ControlAllocation>,
        pub keeper_payer: Option<usize>,
        pub adapter_payer: Option<usize>,
        pub keeper_credit_bytes: usize,
        pub keeper_refused: bool,
        pub adapter_refused: bool,
        pub adapter_invoice_bytes: usize,
        pub adapter_payload_layout: (usize, usize),
        pub outer_keeper: usize,
        pub inner_keeper: usize,
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn funding_next(owner: &ReadMemoryOwner) {
        if let Some(original) = owner.original_native_owner_for_test()
            && let Some(observer) = native_observer(original.accounting_identity())
        {
            // Caller borrows the actual retained producer/invoice through the
            // immediately following allocator request. No observational alias.
            crate::read_work_probe::allocator_events::funding_next(
                observer.physical_funding_counter_for_test(),
            );
        }
    }
    pub(crate) fn constructor_ordinal() -> usize {
        #[cfg(all(test, feature = "test-seams"))]
        {
            crate::persistence::content_identity::proof_allocation_oracle_tests::last_request_for_test(false).2
        }
        #[cfg(not(all(test, feature = "test-seams")))]
        {
            crate::read_work_probe::allocator_events::constructor_ordinal()
        }
    }
    pub(crate) fn control_allocation(
        payload: usize,
        layout: std::alloc::Layout,
        _before: usize,
    ) -> ControlAllocation {
        let mut capture = ControlAllocation {
            payload,
            layout: (layout.size(), layout.align()),
            ..ControlAllocation::default()
        };
        #[cfg(all(test, feature = "test-seams"))]
        {
            use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
            let (bytes, align, ordinal) = oracle::last_request_for_test(false);
            let pointer = oracle::last_request_pointer_for_test();
            // No active allocator watch means UNKNOWN, not a fabricated request.
            if ordinal != _before {
                assert_eq!(
                    ordinal,
                    _before.checked_add(1).expect("allocator ordinal overflow"),
                    "constructor must add exactly one actual allocator request"
                );
                assert!(
                    pointer != 0
                        && pointer <= payload
                        && pointer.checked_add(bytes).is_some_and(|end| payload < end),
                    "constructor payload must belong to the actual last request"
                );
                assert_eq!(
                    (bytes, align),
                    capture.layout,
                    "constructor request Layout mismatch"
                );
                capture.actual_request = Some((pointer, bytes, align));
                capture.ordinal = ordinal;
            }
        }
        #[cfg(not(all(test, feature = "test-seams")))]
        if let Some(request) =
            crate::read_work_probe::allocator_events::constructor_request(_before, payload, layout)
        {
            capture.actual_request = Some((request.pointer, request.bytes, request.align));
            capture.ordinal = request.ordinal;
        }
        capture
    }
    /// Every term comes from an original counter or the existing parent-call
    /// ledger. The target obligation is its inline counter, not a residual
    /// calculated from the parent. Sibling and direct changes include refunds
    /// of controls that were already live at observation entry.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct ParentEquation {
        pub baseline: u64,
        pub target_initial: u64,
        pub target_current: u64,
        pub target_forwarded_net: i128,
        pub target_control_net: i128,
        pub sibling_net: i128,
        pub direct_net: i128,
        pub expected: i128,
        pub actual: u64,
    }
    fn reconcile_parent(sample: &mut Observation, actual: u64) {
        let identity = &sample.identity;
        let expected = sample.parent_before as i128 + identity.target_last_held as i128
            - identity.target_initial_held as i128
            + identity.sibling_parent_net
            + identity.direct_parent_net
            + identity.target_control_parent_net;
        sample.parent_equation = ParentEquation {
            baseline: sample.parent_before,
            target_initial: identity.target_initial_held,
            target_current: identity.target_last_held,
            target_forwarded_net: identity.target_parent_net,
            target_control_net: identity.target_control_parent_net,
            sibling_net: identity.sibling_parent_net,
            direct_net: identity.direct_parent_net,
            expected,
            actual,
        };
        sample.complete_parent_expected = expected;
        sample.complete_parent_actual = actual;
        if expected != actual as i128 {
            sample.parent_funding_mismatches += 1;
            sample.complete_parent_mismatches += 1;
            if sample.first_complete_parent_mismatch.is_none() {
                sample.first_complete_parent_mismatch = sample.identity.last_completed;
            }
        }
    }
    #[derive(Debug, Default, Clone, Copy)]
    pub struct Observation {
        pub parent_identity: usize,
        pub owner_identity: usize,
        pub native_payload_identity: usize,
        pub native_counter: usize,
        /// Live alias only; zero as soon as its forwarding payload retires.
        pub native_forwarder_identity: usize,
        /// Historical constructor provenance, NOT a live alias or an owner pin.
        /// payload is the original Arc payload identity; actual_request and
        /// ordinal are captured by that constructor, never reconstructed later.
        pub native_forwarder_history_not_live: Option<ControlAllocation>,
        pub native_controls: Option<NativeOwnerControls>,
        pub complete_parent_expected: i128,
        pub complete_parent_actual: u64,
        pub complete_parent_mismatches: usize,
        pub first_complete_parent_mismatch: Option<Event>,
        pub limit: u64,
        pub parent_before: u64,
        pub parent_after: u64,
        /// Maximum actual original-parent charge over successful admissions,
        /// including target, sibling, direct and native-control reservations.
        pub parent_high_water: u64,
        pub parent_successful_admissions: usize,
        pub children: usize,
        pub control_bytes: usize,
        pub peak: u64,
        /// Original parent counter sampled at the same completed child peak.
        pub peak_completed: Option<Event>,
        /// First actual pre-existing child release during this observation.
        pub first_sibling_release: Option<Event>,
        /// First actual native control refund during this observation.
        pub first_control_refund: Option<Event>,
        pub final_bytes: u64,
        pub drops: usize,
        pub refusals: usize,
        pub workspace_samples: usize,
        pub foreign_workspace_owners: usize,
        pub underfunded_workspace_samples: usize,
        pub parent_funding_mismatches: usize,
        /// Historical comparison that assumes no sibling or direct-parent change.
        pub single_child_parent_mismatches: usize,
        pub parent_equation: ParentEquation,
        pub identity: IdentityObservation,
        #[cfg(feature = "test-seams")]
        pub trace_produced: Option<contextdb_vector::mem::TraceInvoiceForTest>,
        #[cfg(any(test, feature = "test-seams"))]
        pub trace_owner_controls: Option<TraceOwnerControls>,
        #[cfg(feature = "test-seams")]
        pub trace_old: Option<contextdb_vector::mem::TraceInvoiceForTest>,
        #[cfg(feature = "test-seams")]
        pub trace_cleared: Option<contextdb_vector::mem::TraceInvoiceForTest>,
        #[cfg(feature = "test-seams")]
        pub trace_new: Option<contextdb_vector::mem::TraceInvoiceForTest>,
    }
    /// Fixed scalar capture only: no event ring, heap ledger, pointer dereference,
    /// owner clone, formatter, or I/O in the physical observation window.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct IdentityObservation {
        pub sequence: u64,
        pub target_identity: usize,
        pub target_control_bytes: usize,
        pub target_created: Option<Event>,
        pub first_legacy_mismatch: Option<Event>,
        pub first_completed_mismatch: Option<Event>,
        pub last_final_writer: Option<Event>,
        pub last_completed: Option<Event>,
        /// The actual refused request, retained independently of later frees.
        pub last_target_refusal: Option<Event>,
        pub last_parent: Option<Event>,
        pub target_drop_seen: bool,
        pub target_drop: Option<Event>,
        pub sibling_drops: usize,
        pub last_sibling_drop: Option<Event>,
        pub target_calls: usize,
        pub target_completed_mismatches: usize,
        pub sibling_completed_mismatches: usize,
        pub sibling_calls: usize,
        pub direct_parent_calls: usize,
        pub direct_parent_reserves: u64,
        pub direct_parent_releases: u64,
        pub direct_parent_refusals: usize,
        pub target_last_held: u64,
        pub target_initial_held: u64,
        pub sibling_last_held: u64,
        pub forwarded_parent_samples: usize,
        pub target_reserves: u64,
        pub target_releases: u64,
        pub target_refusals: usize,
        pub sibling_reserves: u64,
        pub sibling_releases: u64,
        pub sibling_refusals: usize,
        pub target_parent_net: i128,
        pub target_control_parent_net: i128,
        pub sibling_parent_net: i128,
        pub direct_parent_net: i128,
    }
    #[derive(Debug, Default, Clone, Copy)]
    pub struct Event {
        pub sequence: u64,
        pub kind: &'static str,
        pub phase: &'static str,
        pub relation: &'static str,
        pub site: &'static str,
        pub subsystem: &'static str,
        pub operation: &'static str,
        pub bytes: usize,
        pub child_identity: usize,
        pub parent_identity: usize,
        pub target_identity: usize,
        pub child_before: u64,
        pub child_after: u64,
        pub parent_before: u64,
        pub parent_after: u64,
        pub child_control_bytes: usize,
        pub live_controls_before: u64,
        pub live_controls_after: u64,
        pub acl_before: u64,
        pub acl_after: u64,
        pub target_parent_net: i128,
        pub target_control_parent_net: i128,
        pub sibling_parent_net: i128,
        pub direct_parent_net: i128,
    }
    #[derive(Clone, Copy)]
    struct CallContext {
        child: usize,
        parent: usize,
        held: u64,
        parent_held: u64,
        controls: u64,
        acl: u64,
        control_bytes: usize,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        site: &'static str,
    }
    #[derive(Clone, Copy)]
    struct ForwardContext {
        call: CallContext,
        held: u64,
    }
    pub(super) struct ForwardGuard(Option<ForwardContext>);
    impl Drop for ForwardGuard {
        fn drop(&mut self) {
            FORWARD.with(|slot| slot.set(self.0));
        }
    }
    pub(super) struct ChildCall<'a> {
        parent: &'a DeclaredReadMemory,
        counter: &'a std::sync::atomic::AtomicU64,
        context: CallContext,
        previous: Option<CallContext>,
        refused: Cell<bool>,
        legacy: bool,
    }
    impl<'a> ChildCall<'a> {
        pub(super) fn new(
            parent: &'a DeclaredReadMemory,
            child: usize,
            counter: &'a std::sync::atomic::AtomicU64,
            control_bytes: usize,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            site: &'static str,
            legacy: bool,
        ) -> Self {
            let context = CallContext {
                child,
                parent: parent as *const _ as usize,
                held: counter.load(Ordering::SeqCst),
                parent_held: parent.retained.load(Ordering::SeqCst),
                controls: parent.live_child_control_bytes.load(Ordering::SeqCst),
                acl: parent.live_acl_cache_bytes.load(Ordering::SeqCst),
                control_bytes,
                bytes,
                subsystem,
                operation,
                site,
            };
            Self {
                parent,
                counter,
                context,
                previous: CALL.with(|slot| slot.replace(Some(context))),
                refused: Cell::new(false),
                legacy,
            }
        }
        pub(super) fn forwarding(&self) -> ForwardGuard {
            ForwardGuard(FORWARD.with(|slot| {
                slot.replace(Some(ForwardContext {
                    call: self.context,
                    held: self.counter.load(Ordering::SeqCst),
                }))
            }))
        }
        pub(super) fn refused(&self) {
            self.refused.set(true);
        }
    }
    impl Drop for ChildCall<'_> {
        fn drop(&mut self) {
            #[cfg(all(test, feature = "test-seams"))]
            crate::database::ordinary_native_owner_tests::named_image_child_call(
                self.context.child,
                self.counter as *const _ as usize,
                self.context.subsystem,
                self.context.operation,
                self.context.site,
                self.context.bytes,
                self.context.held,
                self.counter.load(Ordering::SeqCst),
                self.refused.get(),
            );
            update(self.parent, |sample| {
                let held = self.counter.load(Ordering::SeqCst);
                let record = event(
                    self.parent,
                    sample,
                    Some(self.context.child),
                    held,
                    if self.refused.get() {
                        "ChildRefused"
                    } else {
                        "ChildCompleted"
                    },
                    "completed",
                );
                let target = record.child_identity == sample.identity.target_identity;
                let delta = held as i128 - self.context.held as i128;
                if !self.legacy && target {
                    let completed_peak = held + self.context.control_bytes as u64;
                    if completed_peak > sample.peak {
                        PEAK_COMPLETED.with(|slot| slot.set(Some(record)));
                        sample.peak_completed = Some(record);
                        sample.peak = completed_peak;
                    }
                    sample.final_bytes = held;
                    sample.refusals += usize::from(self.refused.get());
                    sample.single_child_parent_mismatches += usize::from(
                        self.parent.retained.load(Ordering::SeqCst)
                            != sample.parent_before + self.context.control_bytes as u64 + held,
                    );
                    sample.identity.last_final_writer = Some(record);
                }
                let identity = &mut sample.identity;
                if target {
                    identity.target_calls += 1;
                    identity.target_last_held = held;
                    identity.target_refusals += usize::from(self.refused.get());
                    if self.refused.get() {
                        identity.last_target_refusal = Some(record);
                    }
                    if delta >= 0 {
                        identity.target_reserves += delta as u64;
                    } else {
                        identity.target_releases += (-delta) as u64;
                    }
                } else {
                    identity.sibling_calls += 1;
                    identity.sibling_last_held = held;
                    identity.sibling_refusals += usize::from(self.refused.get());
                    if delta >= 0 {
                        identity.sibling_reserves += delta as u64;
                    } else {
                        identity.sibling_releases += (-delta) as u64;
                    }
                }
                identity.last_completed = Some(record);
                if let Some(controls) = sample.native_controls.as_mut()
                    && controls.owner == self.context.child
                {
                    let before = ControlFunding {
                        child_counter: self.counter as *const _ as usize,
                        child_bytes: self.context.held,
                        parent_counter: &self.parent.retained as *const _ as usize,
                        parent_bytes: self.context.parent_held,
                        live_child_controls: self.context.controls,
                        acl_bytes: self.context.acl,
                    };
                    let after = ControlFunding {
                        child_bytes: held,
                        parent_bytes: record.parent_after,
                        live_child_controls: record.live_controls_after,
                        acl_bytes: record.acl_after,
                        ..before
                    };
                    match self.context.operation {
                        "native_adapter_keeper" => {
                            controls.keeper_before = Some(before);
                            controls.keeper_after = Some(after);
                            controls.keeper_refused = self.refused.get();
                        }
                        "native_adapter" => {
                            controls.adapter_before = Some(before);
                            controls.adapter_after = Some(after);
                            controls.adapter_refused = self.refused.get();
                        }
                        _ => {}
                    }
                }
                // This retains the old comparison as a diagnostic, independently
                // of the measured direct/sibling obligations. No residual is asserted.
                if self.legacy
                    && self.context.operation != "control_refund"
                    && self.parent.retained.load(Ordering::SeqCst)
                        != sample.parent_before + self.context.control_bytes as u64 + held
                {
                    if target {
                        identity.target_completed_mismatches += 1;
                    } else {
                        identity.sibling_completed_mismatches += 1;
                    }
                    if identity.first_completed_mismatch.is_none() {
                        identity.first_completed_mismatch = Some(record);
                    }
                }
                if sample.owner_identity != 0 {
                    reconcile_parent(sample, record.parent_after);
                }
            });
            CALL.with(|slot| slot.set(self.previous));
        }
    }
    pub(super) struct ParentCall<'a> {
        parent: &'a DeclaredReadMemory,
        before: u64,
        controls: u64,
        acl: u64,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        site: &'static str,
        forward: Option<ForwardContext>,
        refused: Cell<bool>,
    }
    impl<'a> ParentCall<'a> {
        pub(super) fn new(
            parent: &'a DeclaredReadMemory,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            site: &'static str,
        ) -> Self {
            Self {
                parent,
                before: parent.retained.load(Ordering::SeqCst),
                controls: parent.live_child_control_bytes.load(Ordering::SeqCst),
                acl: parent.live_acl_cache_bytes.load(Ordering::SeqCst),
                bytes,
                subsystem,
                operation,
                site,
                forward: FORWARD.with(|slot| {
                    slot.get()
                        .filter(|forward| forward.call.parent == parent as *const _ as usize)
                }),
                refused: Cell::new(false),
            }
        }
        pub(super) fn refused(&self) {
            self.refused.set(true);
        }
    }
    impl Drop for ParentCall<'_> {
        fn drop(&mut self) {
            update(self.parent, |sample| {
                let after = self.parent.retained.load(Ordering::SeqCst);
                let delta = after as i128 - self.before as i128;
                let child = self.forward.map_or(0, |forward| forward.call.child);
                if child == 0 {
                    sample.identity.direct_parent_calls += 1;
                    sample.identity.direct_parent_net += delta;
                    sample.identity.direct_parent_refusals += usize::from(self.refused.get());
                    if !self.refused.get() {
                        if self.operation == "release" {
                            sample.identity.direct_parent_releases += self.bytes as u64;
                        } else {
                            sample.identity.direct_parent_reserves += self.bytes as u64;
                        }
                    }
                } else {
                    sample.identity.forwarded_parent_samples += 1;
                    if child == sample.identity.target_identity {
                        sample.identity.target_parent_net += delta;
                        if self
                            .forward
                            .is_some_and(|f| f.call.operation == "control_refund")
                        {
                            // Legacy control bytes live on the parent, outside
                            // its child's retained counter, just like a native
                            // block invoice. Preserve that separate obligation.
                            sample.identity.target_control_parent_net += delta;
                        }
                    } else {
                        sample.identity.sibling_parent_net += delta;
                    }
                }
                let mut record = event(
                    self.parent,
                    sample,
                    self.forward.map(|forward| forward.call.child),
                    self.forward.map_or(0, |forward| forward.held),
                    if self.operation == "release" {
                        "ParentRelease"
                    } else if self.refused.get() {
                        "ParentRefused"
                    } else {
                        "ParentReserved"
                    },
                    if self.forward.is_some() {
                        "parent-before-child-completion"
                    } else {
                        "direct-parent-completed"
                    },
                );
                record.site = self.site;
                record.subsystem = self.subsystem;
                record.operation = self.operation;
                if delta < 0 && child != 0 && child != sample.identity.target_identity {
                    sample.first_sibling_release.get_or_insert(record);
                }
                record.bytes = self.bytes;
                record.child_before = self.forward.map_or(0, |forward| forward.held);
                record.child_after = record.child_before;
                record.parent_before = self.before;
                record.live_controls_before = self.controls;
                record.acl_before = self.acl;
                sample.identity.last_parent = Some(record);
            });
        }
    }
    fn event(
        parent: &DeclaredReadMemory,
        sample: &mut Observation,
        child: Option<usize>,
        held: u64,
        kind: &'static str,
        phase: &'static str,
    ) -> Event {
        sample.identity.sequence += 1;
        let context = CALL.with(|slot| {
            slot.get().filter(|call| {
                Some(call.child) == child && call.parent == parent as *const _ as usize
            })
        });
        Event {
            sequence: sample.identity.sequence,
            kind,
            phase,
            relation: match child {
                None => "direct-parent",
                Some(child) if child == sample.identity.target_identity => "target",
                Some(_) => "sibling",
            },
            site: context.map_or("DeclaredReadMemory::child", |call| call.site),
            subsystem: context.map_or("reader", |call| call.subsystem),
            operation: context.map_or("request_owner", |call| call.operation),
            bytes: context.map_or(0, |call| call.bytes),
            child_identity: child.unwrap_or(0),
            parent_identity: parent as *const _ as usize,
            target_identity: sample.identity.target_identity,
            child_before: context.map_or(held, |call| call.held),
            child_after: held,
            parent_before: context.map_or_else(
                || parent.retained.load(Ordering::SeqCst),
                |call| call.parent_held,
            ),
            parent_after: parent.retained.load(Ordering::SeqCst),
            child_control_bytes: context.map_or(0, |call| call.control_bytes),
            live_controls_before: context.map_or_else(
                || parent.live_child_control_bytes.load(Ordering::SeqCst),
                |call| call.controls,
            ),
            live_controls_after: parent.live_child_control_bytes.load(Ordering::SeqCst),
            acl_before: context.map_or_else(
                || parent.live_acl_cache_bytes.load(Ordering::SeqCst),
                |call| call.acl,
            ),
            acl_after: parent.live_acl_cache_bytes.load(Ordering::SeqCst),
            target_parent_net: sample.identity.target_parent_net,
            target_control_parent_net: sample.identity.target_control_parent_net,
            sibling_parent_net: sample.identity.sibling_parent_net,
            direct_parent_net: sample.identity.direct_parent_net,
        }
    }
    thread_local! {
        static PEAK_COMPLETED: Cell<Option<Event>> = const { Cell::new(None) };
        static CALL: Cell<Option<CallContext>> = const { Cell::new(None) };
        static FORWARD: Cell<Option<ForwardContext>> = const { Cell::new(None) };
        static ACTIVE_PARENT: Cell<usize> = const { Cell::new(0) };
        static ACTIVE: Cell<Option<Observation>> = const { Cell::new(None) };
    }
    thread_local! {
        static COPY_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
    }
    // Count at the actual full Cell<Option<Observation>> read, independently
    // of ACTIVE. Measuring an inactive statement must not enable observation.
    #[inline(never)]
    fn copy_observation(slot: &Cell<Option<Observation>>) -> Option<Observation> {
        COPY_COUNT.with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        slot.get()
    }
    #[doc(hidden)]
    pub fn count_observation_copies<T>(operation: impl FnOnce() -> T) -> (T, usize) {
        struct Restore(Option<usize>);
        impl Drop for Restore {
            fn drop(&mut self) {
                COPY_COUNT.with(|slot| slot.set(self.0));
            }
        }
        let restore = Restore(COPY_COUNT.with(|slot| slot.replace(Some(0))));
        let result = operation();
        let copies = COPY_COUNT.with(|slot| slot.get().unwrap());
        drop(restore);
        (result, copies)
    }
    #[inline(always)]
    fn observing(parent: &DeclaredReadMemory) -> bool {
        ACTIVE_PARENT.with(|key| key.get() == parent as *const _ as usize)
    }
    #[inline(always)]
    fn observing_any() -> bool {
        ACTIVE_PARENT.with(|key| key.get() != 0)
    }
    pub(super) fn parent_admitted(parent: &DeclaredReadMemory, charged: u64) {
        update(parent, |sample| {
            sample.parent_high_water = sample.parent_high_water.max(charged);
            sample.parent_successful_admissions += 1;
        });
    }
    #[inline(always)]
    fn update(parent: &DeclaredReadMemory, f: impl FnOnce(&mut Observation)) {
        if !observing(parent) {
            return;
        }
        update_cold(parent, f)
    }
    #[cold]
    #[inline(never)]
    fn update_cold(parent: &DeclaredReadMemory, f: impl FnOnce(&mut Observation)) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot)
                && sample.parent_identity == parent as *const DeclaredReadMemory as usize
            {
                f(&mut sample);
                slot.set(Some(sample));
            }
        });
    }
    #[inline(always)]
    pub(super) fn expects_native_address(parent: &DeclaredReadMemory, address: usize) -> bool {
        if !observing(parent) {
            return false;
        }
        expects_native_address_cold(parent, address)
    }
    #[cold]
    #[inline(never)]
    fn expects_native_address_cold(parent: &DeclaredReadMemory, address: usize) -> bool {
        ACTIVE.with(|slot| {
            copy_observation(slot).is_some_and(|s| {
                s.parent_identity == parent as *const _ as usize
                    && s.native_controls.is_some_and(|n| {
                        address >= n.block.payload
                            && n.block
                                .payload
                                .checked_add(n.block.layout.0)
                                .is_some_and(|end| address < end)
                    })
            })
        })
    }
    #[inline(always)]
    pub(super) fn selected_native(identity: usize) -> bool {
        if !observing_any() {
            return false;
        }
        selected_native_cold(identity)
    }
    #[cold]
    #[inline(never)]
    fn selected_native_cold(identity: usize) -> bool {
        ACTIVE.with(|slot| {
            copy_observation(slot)
                .is_some_and(|s| s.owner_identity == identity && s.native_controls.is_some())
        })
    }
    #[inline(always)]
    pub(super) fn limit(parent: &DeclaredReadMemory) -> Option<u64> {
        if !observing(parent) {
            return None;
        }
        limit_cold(parent)
    }
    #[cold]
    #[inline(never)]
    fn limit_cold(parent: &DeclaredReadMemory) -> Option<u64> {
        ACTIVE.with(|slot| {
            copy_observation(slot)
                .filter(|sample| {
                    sample.parent_identity == parent as *const DeclaredReadMemory as usize
                })
                .map(|sample| sample.limit)
        })
    }
    /// Temporarily lower the ORIGINAL parent's admission ceiling. Existing
    /// child/control/bookkeeping invoices remain counted and are not refunded.
    /// The callback still calls Database::execute_at_snapshot itself.
    pub fn observe<T>(
        parent: &DeclaredReadMemory,
        limit: u64,
        operation: impl FnOnce() -> T,
    ) -> (T, Observation) {
        struct Restore(Option<Observation>, usize);
        impl Drop for Restore {
            fn drop(&mut self) {
                ACTIVE.with(|slot| slot.set(self.0));
                ACTIVE_PARENT.with(|key| key.set(self.1));
            }
        }
        // The allocator observes this door too. getenv only borrows its static
        // result; copying an enabled value via var_os would add an allocation
        // to the exact zero-allocation refusal control.
        // SAFETY: the process environment is read only during these test runs;
        // the pointer is checked for presence and never retained or dereferenced.
        let diagnostic =
            unsafe { !libc::getenv(c"CONTEXTDB_NATIVE_PEAK_PHASES".as_ptr()).is_null() };
        PEAK_COMPLETED.with(|slot| slot.set(None));
        CALL.with(|_| ());
        FORWARD.with(|_| ());
        let before = parent.retained.load(Ordering::SeqCst);
        assert!(limit <= parent.effective_limit());
        let previous_parent = ACTIVE_PARENT.with(Cell::get);
        let restore = Restore(
            ACTIVE.with(|slot| {
                slot.replace(Some(Observation {
                    parent_identity: parent as *const DeclaredReadMemory as usize,
                    parent_before: before,
                    parent_high_water: before,
                    limit,
                    ..Observation::default()
                }))
            }),
            previous_parent,
        );
        ACTIVE_PARENT.with(|key| key.set(parent as *const _ as usize));
        let result = operation();
        let mut sample = ACTIVE.with(|slot| copy_observation(slot).unwrap());
        sample.parent_after = parent.retained.load(Ordering::SeqCst);
        if sample.owner_identity != 0 {
            let after = sample.parent_after;
            reconcile_parent(&mut sample, after);
        }
        drop(restore);
        if diagnostic {
            eprintln!(
                "native_completed_peak pid={} limit={} parent_before={} parent_after={} control={} peak={} completed={:?}",
                std::process::id(),
                sample.limit,
                sample.parent_before,
                sample.parent_after,
                sample.control_bytes,
                sample.peak,
                PEAK_COMPLETED.with(Cell::get)
            );
        }
        (result, sample)
    }
    pub(super) fn native_created(
        parent: &DeclaredReadMemory,
        identity: usize,
        control: usize,
        request_before: usize,
        initial_held: u64,
        child: &NativeDeclaredChild,
    ) {
        let block = control_allocation(
            identity,
            ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>(),
            request_before,
        );
        update(parent, |sample| {
            sample.children += 1;
            // The constructor borrows the real in-block payload; no forwarder
            // is needed to observe its original identity and inline counter.
            sample.native_payload_identity = child as *const _ as usize;
            sample.native_counter = &child.retained as *const _ as usize;
            sample.native_forwarder_identity = 0;
            sample.native_forwarder_history_not_live = None;
            sample.native_controls = Some(NativeOwnerControls {
                owner: identity,
                parent: parent as *const _ as usize,
                parent_counter: &parent.retained as *const _ as usize,
                parent_before_block: sample
                    .identity
                    .last_parent
                    .filter(|e| e.operation == "request_owner" && e.kind == "ParentReserved")
                    .map(|e| e.parent_before),
                parent_after_block: parent.retained.load(Ordering::SeqCst),
                initial_child_bytes: initial_held,
                block,
                payload: child as *const _ as usize,
                counter: &child.retained as *const _ as usize,
                ..NativeOwnerControls::default()
            });
            sample.owner_identity = identity;
            sample.control_bytes = control;
            sample.peak = sample.peak.max(control as u64);
            sample.identity.target_identity = identity;
            sample.identity.target_initial_held = initial_held;
            sample.identity.target_last_held = initial_held;
            sample.identity.target_control_bytes = control;
            let mut record = event(
                parent,
                sample,
                Some(identity),
                0,
                "ChildCreated",
                "completed",
            );
            record.site = "DeclaredReadMemory::native_child";
            record.child_control_bytes = control;
            sample.identity.target_created = Some(record);
        });
    }
    pub(super) fn native_forwarder(
        parent: &DeclaredReadMemory,
        identity: usize,
        payload: usize,
        counter: usize,
        forwarder: usize,
    ) {
        update(parent, |sample| {
            if sample.owner_identity == identity {
                sample.native_payload_identity = payload;
                sample.native_counter = counter;
                sample.native_forwarder_identity = forwarder;
                if let Some(controls) = sample.native_controls.as_mut() {
                    controls.payload = payload;
                    controls.counter = counter;
                }
            }
        });
    }
    #[inline(always)]
    pub(super) fn native_forwarder_retired(identity: usize) {
        if !observing_any() {
            return;
        }
        native_forwarder_retired_cold(identity)
    }
    #[cold]
    #[inline(never)]
    fn native_forwarder_retired_cold(identity: usize) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot)
                && sample.owner_identity == identity
            {
                sample.native_forwarder_identity = 0;
                slot.set(Some(sample));
            }
        });
    }
    #[inline(always)]
    pub(crate) fn native_control_credits(
        identity: usize,
        keeper_bytes: usize,
        adapter_bytes: usize,
        keeper_payer: Option<usize>,
        adapter_payer: Option<usize>,
        adapter_payload_layout: std::alloc::Layout,
    ) {
        if !observing_any() {
            return;
        }
        native_control_credits_cold(
            identity,
            keeper_bytes,
            adapter_bytes,
            keeper_payer,
            adapter_payer,
            adapter_payload_layout,
        )
    }
    #[cold]
    #[inline(never)]
    fn native_control_credits_cold(
        identity: usize,
        keeper_bytes: usize,
        adapter_bytes: usize,
        keeper_payer: Option<usize>,
        adapter_payer: Option<usize>,
        adapter_payload_layout: std::alloc::Layout,
    ) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot)
                && sample.owner_identity == identity
            {
                let controls = sample
                    .native_controls
                    .as_mut()
                    .expect("selected native credits missing original child receipt");
                controls.keeper_credit_bytes = keeper_bytes;
                controls.adapter_invoice_bytes = adapter_bytes;
                controls.keeper_payer = keeper_payer;
                controls.adapter_payer = adapter_payer;
                controls.adapter_payload_layout = (
                    adapter_payload_layout.size(),
                    adapter_payload_layout.align(),
                );
                slot.set(Some(sample));
            }
        });
    }
    #[inline(always)]
    pub(crate) fn native_control_allocated(
        identity: usize,
        keeper: bool,
        allocation: ControlAllocation,
        payer: usize,
        invoice_bytes: usize,
        outer_keeper: usize,
        inner_keeper: usize,
    ) {
        if !observing_any() {
            return;
        }
        native_control_allocated_cold(
            identity,
            keeper,
            allocation,
            payer,
            invoice_bytes,
            outer_keeper,
            inner_keeper,
        )
    }
    #[cold]
    #[inline(never)]
    fn native_control_allocated_cold(
        identity: usize,
        keeper: bool,
        allocation: ControlAllocation,
        payer: usize,
        invoice_bytes: usize,
        outer_keeper: usize,
        inner_keeper: usize,
    ) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot)
                && sample.owner_identity == identity
            {
                let controls = sample
                    .native_controls
                    .as_mut()
                    .expect("selected native constructor has no original child receipt");
                assert_eq!(
                    payer, identity,
                    "native controls must use the original child"
                );
                if keeper {
                    assert!(controls.keeper.is_none(), "duplicate native keeper receipt");
                    controls.keeper = Some(allocation);
                    controls.adapter_invoice_bytes = invoice_bytes;
                } else {
                    assert!(
                        controls.adapter.is_none(),
                        "duplicate native adapter receipt"
                    );
                    controls.adapter = Some(allocation);
                    controls.outer_keeper = outer_keeper;
                    controls.inner_keeper = inner_keeper;
                    // This hook is adjacent to the actual forwarder constructor.
                    // Freeze its original identity/request even after alias retirement.
                    sample.native_forwarder_history_not_live = Some(allocation);
                }
                slot.set(Some(sample));
            }
        });
    }
    pub(super) fn native_control_refunded(
        parent: &DeclaredReadMemory,
        bytes: usize,
        (controls_before, parent_before): (u64, u64),
    ) {
        update(parent, |sample| {
            let mut record = event(parent, sample, None, 0, "ControlRefund", "physical-free");
            record.site = "NativeChildInvoice::drop";
            record.subsystem = "reader";
            record.operation = "native_control_refund";
            record.bytes = bytes;
            record.child_control_bytes = bytes;
            record.live_controls_before = controls_before;
            record.parent_before = parent_before;
            sample.first_control_refund.get_or_insert(record);
        });
    }
    pub(super) fn native_dropped(child: &NativeDeclaredChild, identity: usize) {
        update(&child.parent, |sample| {
            sample.drops += 1;
            let held = child.retained.load(Ordering::SeqCst);
            let mut record = event(
                &child.parent,
                sample,
                Some(identity),
                held,
                "ChildDropBeforeControlRefund",
                "payload-drop",
            );
            record.site = "NativeDeclaredChild::drop";
            record.child_control_bytes =
                ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>().size();
            if identity == sample.identity.target_identity {
                sample.final_bytes = held;
                sample.identity.target_drop_seen = true;
                sample.identity.target_drop = Some(record);
            } else {
                sample.identity.sibling_drops += 1;
                sample.identity.last_sibling_drop = Some(record);
            }
        });
    }
    /// Borrow the existing scalar receipt while its observe closure is still
    /// open. This neither ends/resets observation nor retains a forwarding Arc.
    /// parent_after is the operation-exit sample and is final only on return.
    #[doc(hidden)]
    #[inline(always)]
    pub fn current() -> Option<Observation> {
        if !observing_any() {
            return None;
        }
        current_cold()
    }
    #[cold]
    #[inline(never)]
    fn current_cold() -> Option<Observation> {
        ACTIVE.with(copy_observation)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[inline(always)]
    pub(crate) fn native_controls(identity: usize) -> Option<NativeOwnerControls> {
        if !observing_any() {
            return None;
        }
        native_controls_cold(identity)
    }
    #[cold]
    #[inline(never)]
    fn native_controls_cold(identity: usize) -> Option<NativeOwnerControls> {
        ACTIVE.with(|slot| {
            copy_observation(slot)
                .filter(|s| s.owner_identity == identity)
                .and_then(|s| s.native_controls)
        })
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[inline(always)]
    pub(crate) fn trace_owner_controls(controls: TraceOwnerControls) {
        if !observing_any() {
            return;
        }
        trace_owner_controls_cold(controls)
    }
    #[cold]
    #[inline(never)]
    fn trace_owner_controls_cold(controls: TraceOwnerControls) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot) {
                sample.trace_owner_controls = Some(controls);
                slot.set(Some(sample));
            }
        });
    }
    #[cfg(feature = "test-seams")]
    #[inline(always)]
    pub(crate) fn trace_produced(
        receipt: contextdb_vector::mem::TraceInvoiceForTest,
        payer: usize,
        owner: usize,
        prepaid: std::alloc::Layout,
    ) {
        if !observing_any() {
            return;
        }
        trace_produced_cold(receipt, payer, owner, prepaid)
    }
    #[cfg(feature = "test-seams")]
    #[cold]
    #[inline(never)]
    fn trace_produced_cold(
        mut receipt: contextdb_vector::mem::TraceInvoiceForTest,
        payer: usize,
        owner: usize,
        prepaid: std::alloc::Layout,
    ) {
        receipt.original_payer = payer;
        receipt.owner_payload_pointer = owner;
        receipt.prepaid_control = (0, prepaid.size(), prepaid.align());
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot) {
                sample.trace_produced = Some(receipt);
                slot.set(Some(sample));
            }
        });
    }
    #[cfg(feature = "test-seams")]
    #[inline(always)]
    pub(crate) fn trace_cleared(old: Option<&contextdb_vector::OwnedVectorSearchTrace>) {
        if !observing_any() {
            return;
        }
        trace_cleared_cold(old)
    }
    #[cfg(feature = "test-seams")]
    #[cold]
    #[inline(never)]
    fn trace_cleared_cold(old: Option<&contextdb_vector::OwnedVectorSearchTrace>) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot) {
                if let Some(old) = old {
                    sample.trace_cleared = Some(old.complete_invoice_for_test(|_| {}));
                }
                slot.set(Some(sample));
            }
        });
    }
    #[cfg(feature = "test-seams")]
    #[inline(always)]
    pub(crate) fn trace_published(
        new: &contextdb_vector::OwnedVectorSearchTrace,
        old: Option<&contextdb_vector::OwnedVectorSearchTrace>,
    ) {
        if !observing_any() {
            return;
        }
        trace_published_cold(new, old)
    }
    #[cfg(feature = "test-seams")]
    #[cold]
    #[inline(never)]
    fn trace_published_cold(
        new: &contextdb_vector::OwnedVectorSearchTrace,
        old: Option<&contextdb_vector::OwnedVectorSearchTrace>,
    ) {
        ACTIVE.with(|slot| {
            if let Some(mut sample) = copy_observation(slot) {
                sample.trace_new = Some(new.complete_invoice_for_test(|_| {}));
                sample.trace_old = old.map(|trace| trace.complete_invoice_for_test(|_| {}));
                slot.set(Some(sample));
            }
        });
    }

    /// Sample the real owner while its physical membership capture is live.
    #[inline(always)]
    pub(crate) fn live_workspace(memory: impl Into<ReadMemoryOwner>, bytes: usize) {
        if !observing_any() {
            return;
        }
        live_workspace_cold(memory, bytes)
    }
    #[cold]
    #[inline(never)]
    fn live_workspace_cold(memory: impl Into<ReadMemoryOwner>, bytes: usize) {
        let memory: ReadMemoryOwner = memory.into();
        ACTIVE.with(|slot| {
            let Some(mut sample) = copy_observation(slot) else {
                return;
            };
            let identity = observed_reader_identity(&memory);
            sample.workspace_samples += 1;
            sample.foreign_workspace_owners += usize::from(identity != sample.owner_identity);
            let funding = declared_child_observer(&memory).map(|observer| observer.sample());
            sample.underfunded_workspace_samples += usize::from(funding.is_none_or(|funding| {
                !funding.alive
                    || funding.charged_bytes < bytes as u64
                    || funding.parent_identity != sample.parent_identity
            }));
            slot.set(Some(sample));
        });
    }

    #[cfg(test)]
    mod recovery_tests {
        use super::*;
        #[test]
        fn parent_high_water_records_successful_direct_admissions_but_not_refusals() {
            let parent = DeclaredReadMemory::from_reader_limit(8192).unwrap();
            let before = parent.retained.load(Ordering::SeqCst);
            let (_, sample) = observe(&parent, 4096, || {
                parent
                    .try_reserve_native(3000, "reader", "direct-peak", "test")
                    .unwrap();
                parent.release(3000);
                assert!(
                    parent
                        .try_reserve_native(4096, "reader", "refused", "test")
                        .is_err()
                );
                parent
                    .try_reserve(100, "reader", "fallible-door", "test")
                    .unwrap();
                parent.release(100);
            });
            assert_eq!(sample.parent_high_water, before + 3000);
            assert_eq!(sample.parent_successful_admissions, 2);
            assert_eq!(sample.parent_before, before);
            assert_eq!(sample.parent_after, before);
            assert_eq!(
                sample.peak, 0,
                "parent maximum is independent of child peak"
            );
        }
        #[test]
        fn observation_parent_key_restores_normal_error_panic_and_nested_scopes() {
            let parent = DeclaredReadMemory::from_reader_limit(8192).unwrap();
            let foreign = DeclaredReadMemory::from_reader_limit(8192).unwrap();
            assert_eq!(limit(&parent), None);
            let (_, outer) = observe(&parent, 4096, || {
                assert_eq!(limit(&parent), Some(4096));
                assert_eq!(limit(&foreign), None);
                let (_, inner) = observe(&foreign, 2048, || {
                    assert_eq!(limit(&parent), None);
                    assert_eq!(limit(&foreign), Some(2048));
                });
                assert_eq!(inner.parent_identity, &*foreign as *const _ as usize);
                assert_eq!(limit(&parent), Some(4096));
                let (error, _) = observe(&parent, 2048, || Err::<(), _>("expected"));
                assert!(error.is_err());
                assert_eq!(limit(&parent), Some(4096));
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    observe(&parent, 2048, || panic!("scope unwind"));
                }));
                assert!(panic.is_err());
                assert_eq!(limit(&parent), Some(4096));
            });
            assert_eq!(outer.parent_identity, &*parent as *const _ as usize);
            assert_eq!(limit(&parent), None);
            let (_, copies) = count_observation_copies(|| {
                assert_eq!(limit(&parent), None);
                update(&foreign, |_| panic!("inactive update must not execute"));
            });
            assert_eq!(copies, 0);
        }
    }
}

#[cfg(all(test, feature = "test-seams"))]
pub(crate) fn ordinary_native_child_layout_for_test() -> std::alloc::Layout {
    ReadMemoryOwner::native_layout::<NativeDeclaredChild, NativeChildInvoice>()
}

#[cfg(all(test, feature = "test-seams"))]
impl DeclaredReadMemory {
    pub(crate) fn ordinary_parent_counter_for_test(&self) -> usize {
        &self.retained as *const std::sync::atomic::AtomicU64 as usize
    }
}

// Fixed observation storage. Tokens do not own a root, child or allocation.
// Retired slots retain scalar evidence until reused; reuse invalidates their
// generation explicitly. A forgotten observer cannot pin or dereference storage.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) mod root_observations {
    use super::*;
    use std::sync::{Mutex, atomic::Ordering};
    thread_local! {
        static LAST_ROOT_ATTEMPT: std::cell::Cell<Option<Token>> = const { std::cell::Cell::new(None) };
    }
    // Set only by hooks called by the actual observing GlobalAlloc wrapper.
    static INSTALLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    pub(crate) fn install_physical_observer() {
        INSTALLED.store(true, Ordering::SeqCst);
    }
    fn installed() -> bool {
        INSTALLED.load(Ordering::SeqCst)
    }
    pub(super) fn has_evidence(token: Token) -> bool {
        token.0 != 0
    }
    static INVALID_FREE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Token(u64);
    #[derive(Clone, Copy)]
    struct Record {
        token: Token,
        block: usize,
        payload: usize,
        bytes: usize,
        alignment: usize,
        alive: bool,
        payload_dropped: bool,
        terminal_retired: bool,
        physical_free_returned: bool,
        last_charged: u64,
        last_controls: u64,
        // 0 root, 1 image, 2 native/standard accountant, 3 moved engine accountant. Existing fixed slots and generations
        // also carry producer copies; observers never promote these identities.
        producer: u8,
        counter: usize,
        payload_bytes: usize,
        #[cfg_attr(
            not(feature = "test-seams"),
            allow(
                dead_code,
                reason = "KEEP-SEAM: live only under the test-seams feature; used from allocator_events.rs"
            )
        )]
        allocation_attempts: usize,
        #[cfg_attr(
            not(feature = "test-seams"),
            allow(
                dead_code,
                reason = "KEEP-SEAM: live only under the test-seams feature"
            )
        )]
        before_allocation: u64,
        payer: Option<usize>,
        free_before: u64,
        free_after: u64,
        retired_at: u64,
    }
    struct Registry {
        slots: [Option<Record>; 64],
        generation: u64,
        event: u64,
    }
    static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
        slots: [None; 64],
        generation: 0,
        event: 0,
    });
    impl Registry {
        fn record(&mut self, token: Token) -> &mut Record {
            self.slots
                .iter_mut()
                .flatten()
                .find(|r| r.token.0 == token.0)
                .expect("root observation generation retired; proof invalidated")
        }
    }
    pub(super) fn begin(bytes: usize) -> Token {
        if !installed() {
            return Token(0);
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let slot = registry
            .slots
            .iter()
            .position(Option::is_none)
            .or_else(|| {
                registry
                    .slots
                    .iter()
                    .enumerate()
                    .filter_map(|(i, r)| r.filter(|r| r.terminal_retired).map(|r| (i, r.token.0)))
                    .min_by_key(|(_, generation)| *generation)
                    .map(|(i, _)| i)
            })
            .expect("root observation static capacity exhausted; proof invalid");
        registry.generation = registry
            .generation
            .checked_add(1)
            .expect("root observation generation exhausted");
        let token = Token(registry.generation);
        registry.slots[slot] = Some(Record {
            token,
            block: 0,
            payload: 0,
            bytes,
            alignment: ReadMemoryOwner::native_layout::<DeclaredReadMemory, RootTerminalInvoice>()
                .align(),
            alive: false,
            payload_dropped: false,
            terminal_retired: false,
            physical_free_returned: false,
            last_charged: bytes as u64,
            last_controls: 0,
            producer: 0,
            counter: 0,
            payload_bytes: 0,
            allocation_attempts: 0,
            before_allocation: 0,
            payer: None,
            free_before: 0,
            free_after: 0,
            retired_at: 0,
        });
        LAST_ROOT_ATTEMPT.with(|slot| slot.set(Some(token)));
        token
    }
    pub(super) fn bind(token: Token, root: &DeclaredReadRoot) {
        if token.0 == 0 {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let record = registry.record(token);
        assert!(!record.payload_dropped && record.payload == 0);
        record.block = root.original_owner().accounting_identity();
        record.payload = root.as_ref() as *const _ as usize;
        assert_ne!(record.block, 0);
        assert_eq!(record.block % record.alignment, 0);
        record.alive = true;
    }
    pub(super) fn with_live<T>(
        token: Token,
        f: impl FnOnce(&DeclaredReadMemory) -> T,
    ) -> Option<T> {
        if token.0 == 0 {
            return None;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let record = registry.record(token);
        assert!(
            record.alive && record.payload != 0,
            "original root is retired"
        );
        // SAFETY: bind records only the real projected payload. Root Drop
        // invalidates under this same mutex before its native block can free.
        // f cannot return a reference tied to this temporary root borrow.
        Some(f(unsafe {
            &*(record.payload as *const DeclaredReadMemory)
        }))
    }
    pub(super) fn sample(token: Token) -> (bool, u64, u64) {
        assert_ne!(token.0, 0, "physical observer not installed; no evidence");
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let record = registry.record(token);
        if record.alive {
            // SAFETY: this lock excludes payload retirement and physical free.
            let root = unsafe { &*(record.payload as *const DeclaredReadMemory) };
            record.last_charged = root.retained.load(Ordering::SeqCst);
            record.last_controls = root.live_child_control_bytes.load(Ordering::SeqCst);
        }
        // Zero follows the recorded terminal retirement, never a missing
        // registry entry. Keep last_charged separately as pre-free evidence.
        (
            record.alive,
            record.last_controls,
            if record.terminal_retired {
                0
            } else {
                record.last_charged
            },
        )
    }
    pub(super) fn payload_dropped(token: Token, root: &DeclaredReadMemory) {
        if token.0 == 0 {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let record = registry.record(token);
        record.last_charged = root.retained.load(Ordering::SeqCst);
        record.last_controls = root.live_child_control_bytes.load(Ordering::SeqCst);
        record.alive = false;
        record.payload_dropped = true;
        // Keep the scalar identity, but every dereference is now disabled.
    }
    pub(super) fn retire(token: Token, bytes: usize) {
        if token.0 == 0 {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        registry.event = registry
            .event
            .checked_add(1)
            .expect("terminal event ordinal overflow");
        let event = registry.event;
        let record = registry.record(token);
        assert!(record.payload_dropped && !record.terminal_retired);
        assert_eq!(record.bytes, bytes);
        // An initialized block's invoice reaches here only after the existing
        // NativeFreeGuard's dealloc returned. A null allocation never binds.
        // A bound root needs the independent allocator return event. A null
        // constructor has no block and cannot be a physical-free control pass.
        record.terminal_retired = true;
        record.retired_at = event;
    }
    #[cfg(feature = "test-seams")]
    pub(super) fn before_allocation(
        token: Token,
        root: &DeclaredReadMemory,
        layout: std::alloc::Layout,
    ) {
        if token.0 == 0 {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let r = registry.record(token);
        assert_eq!(r.producer, 0);
        assert_eq!((r.bytes, r.alignment), (layout.size(), layout.align()));
        r.before_allocation = root.retained.load(Ordering::SeqCst);
        r.allocation_attempts += 1;
        crate::read_work_probe::allocator_events::funding_next(&root.retained as *const _ as usize);
    }
    #[cfg(test)]
    pub(super) fn assert_null_terminal_retired() {
        let token = LAST_ROOT_ATTEMPT
            .with(|slot| slot.get())
            .expect("actual root attempt token");
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let r = registry.record(token);
        assert!(r.payload_dropped && r.terminal_retired && !r.physical_free_returned);
        assert_eq!(r.block, 0, "null produced no original allocation");
        assert_eq!(r.last_charged, r.bytes as u64);
        #[cfg(feature = "test-seams")]
        {
            assert_eq!(r.allocation_attempts, 1);
            assert_eq!(r.before_allocation, r.bytes as u64);
        }
        assert!(r.retired_at > 0);
    }
    #[cfg(test)]
    pub(super) fn assert_terminal_free(token: Token) {
        assert_ne!(token.0, 0, "physical observer not installed; no evidence");
        assert!(
            !INVALID_FREE.load(Ordering::SeqCst),
            "unqualified physical-free observation"
        );
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let r = registry.record(token);
        assert_eq!(r.producer, 0);
        #[cfg(feature = "test-seams")]
        {
            assert_eq!(r.allocation_attempts, 1);
            assert_eq!(
                r.before_allocation, r.bytes as u64,
                "real original root debit was present at the actual cell allocation boundary"
            );
        }
        assert!(
            r.payload_dropped && r.terminal_retired && r.physical_free_returned,
            "original root requires payload retirement, actual allocator free, then terminal retirement"
        );
        assert!(
            0 < r.free_before && r.free_before < r.free_after && r.free_after < r.retired_at,
            "actual allocator free must precede the original terminal invoice retirement"
        );
        assert_eq!(r.last_controls, 0);
        assert_eq!(
            r.last_charged, r.bytes as u64,
            "every child and payload invoice must retire before the root's own physical free"
        );
    }
    /// Extend the same fixed provenance slots. Registration borrows a real
    /// producer and copies its current counters; no pointer is read by sample.
    pub(crate) fn producer(
        payload: usize,
        kind: u8,
        counter: usize,
        held: usize,
        payload_bytes: usize,
        payer: Option<usize>,
    ) -> Token {
        if !installed() {
            return Token(0);
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(record) = registry
            .slots
            .iter_mut()
            .flatten()
            .find(|r| r.producer == kind && r.payload == payload && r.alive)
        {
            if record.payer != payer {
                // Panic cleanup can deallocate through the observer. Never hold
                // its registry while reporting rejected provenance.
                drop(registry);
                panic!("producer cannot change its original payer");
            }
            record.last_charged = held as u64;
            record.payload_bytes = payload_bytes;
            return record.token;
        }
        if registry.slots.iter().flatten().any(|r| {
            r.producer == kind && r.payload == payload && !r.alive && !r.physical_free_returned
        }) {
            drop(registry);
            panic!(
                "address reused without original physical-free evidence; observation unqualified"
            );
        }
        let slot = registry.slots.iter().position(Option::is_none).or_else(|| {
            registry
                .slots
                .iter()
                .enumerate()
                .filter_map(|(i, r)| r.filter(|r| r.terminal_retired).map(|r| (i, r.token.0)))
                .min_by_key(|(_, generation)| *generation)
                .map(|(i, _)| i)
        });
        let Some(slot) = slot else {
            drop(registry);
            panic!("producer observation static capacity exhausted; proof invalid");
        };
        registry.generation = registry
            .generation
            .checked_add(1)
            .expect("producer generation overflow");
        let token = Token(registry.generation);
        registry.slots[slot] = Some(Record {
            token,
            block: 0,
            payload,
            bytes: 0,
            alignment: 0,
            alive: true,
            payload_dropped: false,
            terminal_retired: false,
            physical_free_returned: false,
            last_charged: held as u64,
            last_controls: 0,
            producer: kind,
            counter,
            payload_bytes,
            allocation_attempts: 0,
            before_allocation: 0,
            payer,
            free_before: 0,
            free_after: 0,
            retired_at: 0,
        });
        token
    }
    pub(crate) fn producer_update(payload: usize, kind: u8, held: usize, payload_bytes: usize) {
        if !installed() {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        for r in registry
            .slots
            .iter_mut()
            .flatten()
            .filter(|r| r.producer == kind && r.payload == payload && r.alive)
        {
            r.last_charged = held as u64;
            r.payload_bytes = payload_bytes;
        }
    }
    pub(crate) fn producer_used(counter: usize, used: usize) {
        if !installed() {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        for r in registry
            .slots
            .iter_mut()
            .flatten()
            .filter(|r| (r.producer == 2 || r.producer == 3) && r.counter == counter && r.alive)
        {
            r.last_charged = used as u64;
        }
    }
    pub(crate) fn producer_dropped(payload: usize, kind: u8, held: usize, payload_bytes: usize) {
        if !installed() {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        for r in registry
            .slots
            .iter_mut()
            .flatten()
            .filter(|r| r.producer == kind && r.payload == payload && !r.physical_free_returned)
        {
            r.last_charged = held as u64;
            r.payload_bytes = payload_bytes;
            r.alive = false;
            r.payload_dropped = true;
        }
    }
    /// Actual allocator boundaries, including std Arc backing. Containment
    /// matches only registered genuine typed payload identities. No dead load.
    /// The returned generation is stable across System.dealloc/address reuse.
    pub(crate) fn before_physical_free(pointer: usize, layout: std::alloc::Layout) -> u64 {
        if !installed() {
            return 0;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let Some(next) = registry.event.checked_add(1) else {
            INVALID_FREE.store(true, Ordering::SeqCst);
            return 0;
        };
        registry.event = next;
        let event = registry.event;
        let mut generation = 0;
        for r in registry.slots.iter_mut().flatten().filter(|r| {
            !r.physical_free_returned
                && (r.payload_dropped || r.producer == 3)
                && pointer <= r.payload
                && pointer
                    .checked_add(layout.size())
                    .is_some_and(|end| r.payload < end)
        }) {
            if generation != 0 {
                INVALID_FREE.store(true, Ordering::SeqCst);
                return 0;
            }
            if (r.block != 0 && r.block != pointer)
                || (r.producer == 0 && (r.bytes, r.alignment) != (layout.size(), layout.align()))
            {
                INVALID_FREE.store(true, Ordering::SeqCst);
                return 0;
            }
            r.block = pointer;
            r.bytes = layout.size();
            r.alignment = layout.align();
            r.free_before = event;
            generation = r.token.0;
        }
        generation
    }
    pub(crate) fn after_physical_free(generation: u64) {
        if generation == 0 {
            return;
        }
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let Some(next) = registry.event.checked_add(1) else {
            INVALID_FREE.store(true, Ordering::SeqCst);
            return;
        };
        registry.event = next;
        let event = registry.event;
        let Some(r) = registry
            .slots
            .iter_mut()
            .flatten()
            .find(|r| r.token.0 == generation)
        else {
            INVALID_FREE.store(true, Ordering::SeqCst);
            return;
        };
        if !(r.payload_dropped || r.producer == 3)
            || r.physical_free_returned
            || r.free_before == 0
            || r.free_before >= event
        {
            INVALID_FREE.store(true, Ordering::SeqCst);
            return;
        }
        r.free_after = event;
        r.physical_free_returned = true;
        if r.producer != 0 {
            r.alive = false;
            r.terminal_retired = true;
        }
    }
    pub(crate) fn producer_sample(token: Token) -> (bool, usize, usize, bool) {
        assert_ne!(token.0, 0, "physical observer not installed; no evidence");
        assert!(
            !INVALID_FREE.load(Ordering::SeqCst),
            "unqualified physical-free observation"
        );
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let r = registry.record(token);
        assert_ne!(r.producer, 0);
        (
            r.alive,
            r.last_charged as usize,
            r.payload_bytes,
            r.physical_free_returned,
        )
    }

    #[cfg(test)]
    #[test]
    pub(crate) fn installed_provenance_rejects_reuse_and_payer_changes() {
        const CHILD: &str = "CONTEXTDB_INSTALLED_PROVENANCE_NEGATIVE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact",
                    "vector_read_memory::root_observations::installed_provenance_rejects_reuse_and_payer_changes",
                    "--nocapture"])
                .env(CHILD, "1").output().expect("run isolated provenance negatives");
            assert!(
                output.status.success(),
                "isolated provenance negatives failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .contains("PROVENANCE_NEGATIVES payer reuse duplicate_free capacity PASS"),
                "the child must execute every destructive negative branch"
            );
            return;
        }
        fn rejects(operation: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
            let error = std::panic::catch_unwind(operation).expect_err(expected);
            let message = error
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| error.downcast_ref::<String>().map(String::as_str));
            assert_eq!(message, Some(expected));
        }
        use crate::read_image_memory::{ImageMemory, ImageMemoryObservation};
        assert!(
            installed(),
            "unit GlobalAlloc must establish the installation fact"
        );
        let image = ImageMemory::new(None).unwrap();
        let _observation = ImageMemoryObservation::new(&image);
        let address = image.as_ref() as *const ImageMemory as usize;
        let actual = {
            let registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
            *registry
                .slots
                .iter()
                .flatten()
                .find(|record| record.payload == address && record.producer == 1 && record.alive)
                .unwrap()
        };
        let token = actual.token;
        assert!(producer_sample(token).0);
        let wrong_payer = if actual.payer.is_some() {
            None
        } else {
            Some(image.accountant.as_ptr() as usize)
        };
        rejects(
            std::panic::AssertUnwindSafe(|| {
                producer(
                    address,
                    1,
                    actual.counter,
                    actual.last_charged as usize,
                    actual.payload_bytes,
                    wrong_payer,
                );
            }),
            "producer cannot change its original payer",
        );
        // Withhold only the observer's deallocation boundary. The real image
        // remains pinned; a second registration cannot qualify address reuse.
        producer_dropped(
            address,
            1,
            actual.last_charged as usize,
            actual.payload_bytes,
        );
        rejects(
            || {
                producer(
                    address,
                    1,
                    actual.counter,
                    actual.last_charged as usize,
                    actual.payload_bytes,
                    actual.payer,
                );
            },
            "address reused without original physical-free evidence; observation unqualified",
        );
        assert!(
            !producer_sample(token).3,
            "a rejected reuse cannot invent a free"
        );
        drop(image);
        assert!(
            producer_sample(token).3,
            "only the GlobalAlloc return qualifies free"
        );
        // Invalidate this actual free before capacity reuse can retire its slot.
        after_physical_free(token.0);
        rejects(
            || {
                producer_sample(token);
            },
            "unqualified physical-free observation",
        );
        let images: Vec<_> = (0..65).map(|_| ImageMemory::new(None).unwrap()).collect();
        let mut rejected = 0;
        for image in &images {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ImageMemoryObservation::new(image)
            })) {
                Ok(_) => {}
                Err(error) => {
                    assert_eq!(
                        error.downcast_ref::<&str>().copied(),
                        Some("producer observation static capacity exhausted; proof invalid")
                    );
                    rejected += 1;
                }
            }
        }
        assert!(
            rejected > 0,
            "installed provenance capacity remains fail-closed"
        );
        drop(images);
        println!("PROVENANCE_NEGATIVES payer reuse duplicate_free capacity PASS");
    }
}
