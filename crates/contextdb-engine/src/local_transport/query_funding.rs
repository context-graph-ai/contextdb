//! Private query payloads retain their original invoice through physical
//! destruction. Ordinary public answers also retain the original query controls.
use crate::read_image_memory::ImageMemory;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget, ReadMemoryOwner};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Funded<T> {
    // Declaration order is destruction order: payload, then its invoice.
    pub(crate) value: T,
    pub(crate) credit: Option<ReadCredit>,
}

impl<T> Funded<T> {
    pub(crate) fn external(value: T) -> Self {
        Self {
            value,
            credit: None,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn allocate_owned(
        owner: Option<ReadMemoryOwner>,
        bytes: usize,
        make: impl FnOnce() -> T,
    ) -> contextdb_core::Result<Self> {
        let credit = owner
            .map(|owner| {
                ReadCredit::try_new_owned(
                    owner,
                    bytes,
                    "reader",
                    "query_buffer",
                    "Reduce simultaneous query working memory or raise the declared reader limit.",
                )
            })
            .transpose()?;
        let value = make();
        Ok(Self { value, credit })
    }

    /// Only call at an actual public consumer boundary. The value moves;
    /// its engine invoice returns as custody leaves the engine.
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from read_session.rs"
        )
    )]
    pub(crate) fn into_public(self) -> T {
        self.value
    }

    /// Move decoded payload into the SAME ImageMemory's retained answer.
    /// No reserve or release occurs: the original debit remains on that owner.
    pub(crate) fn into_retained(self) -> T {
        if let Some(credit) = self.credit {
            credit.transfer();
        }
        self.value
    }
}

impl AsMut<[u8]> for Funded<Vec<u8>> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.value.as_mut_slice()
    }
}

#[derive(Debug)]
pub(crate) struct FundedVec<T> {
    pub(crate) values: Vec<T>,
    credit: Option<ReadCredit>,
    owner: Option<ReadMemoryOwner>,
}

impl<T> Default for FundedVec<T> {
    fn default() -> Self {
        Self::new(None)
    }
}

impl<T> FundedVec<T> {
    pub(crate) fn new(owner: Option<contextdb_core::read_memory::ReadMemoryOwner>) -> Self {
        Self::new_owned(owner)
    }
    pub(crate) fn new_owned(owner: Option<ReadMemoryOwner>) -> Self {
        Self {
            values: Vec::new(),
            credit: None,
            owner,
        }
    }
    pub(crate) fn owner(&self) -> Option<ReadMemoryOwner> {
        self.owner.clone()
    }
    pub(crate) fn clear(&mut self) {
        // clear retains Vec capacity, so retain its invoice too.
        self.values.clear();
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn push(&mut self, value: T) -> contextdb_core::Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| contextdb_core::Error::Other("query container overflow".into()))?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    contextdb_core::Error::Other("query container size overflow".into())
                })?;
            // The complete replacement coexists with the old allocation.
            let mut replacement =
                Funded::allocate_owned(self.owner.clone(), bytes, || Vec::with_capacity(capacity))?;
            replacement.value.append(&mut self.values);
            let old = std::mem::replace(&mut self.values, replacement.value);
            drop(old);
            let old_credit = std::mem::replace(&mut self.credit, replacement.credit);
            drop(old_credit);
        }
        self.values.push(value);
        Ok(())
    }
}

impl FundedVec<Funded<super::LocalResponse>> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn into_public(mut self) -> contextdb_core::Result<Vec<super::LocalResponse>> {
        let bytes = self
            .values
            .len()
            .checked_mul(std::mem::size_of::<super::LocalResponse>())
            .ok_or_else(|| contextdb_core::Error::Other("query handoff size overflow".into()))?;
        let mut output = Funded::allocate_owned(self.owner.clone(), bytes, || {
            Vec::with_capacity(self.values.len())
        })?;
        for response in &mut self.values {
            let value = std::mem::replace(
                &mut response.value,
                super::LocalResponse::CancelApplied { request_ordinal: 0 },
            );
            output.value.push(value);
            if let Some(credit) = response.credit.take() {
                credit.transfer();
            }
        }
        // Return-place evaluation moves the output into the consumer before
        // local source invoices settle. The placeholders own no payload.
        Ok(output.into_retained())
    }
}

/// Admission is performed by the existing scoped decoder. This guard keeps
/// precisely those original reservations, including through unwind; it never
/// estimates an invoice from a returned DTO's length.
struct DecodeCredit {
    owner: crate::read_image_memory::ImageMemoryHandle,
    before: usize,
    armed: bool,
}
impl Drop for DecodeCredit {
    fn drop(&mut self) {
        if self.armed {
            self.owner.release(self.owner.held() - self.before);
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    owner: &crate::read_image_memory::ImageMemoryHandle,
) -> contextdb_core::Result<Funded<T>> {
    // Input is already funded by the carrier/assembly, never charged twice.
    let mut pending = DecodeCredit {
        owner: owner.clone(),
        before: owner.held(),
        armed: true,
    };
    let (value, consumed) = owner.scope(|| {
        crate::read_image_memory::decode(
            &crate::read_image_memory::ImageMemory::enter_decode_call()?,
            bytes,
        )
    })?;
    if consumed != bytes.len() {
        drop(value);
        return Err(contextdb_core::Error::Other(
            "query decode trailing bytes".into(),
        ));
    }
    let credit = ReadCredit::from_prepaid_owned(owner.owner(), owner.held() - pending.before);
    pending.armed = false;
    Ok(Funded {
        value,
        credit: Some(credit),
    })
}

/// One private envelope around the genuine acquired query child. Every debit
/// still reaches that exact child; the counter only enforces this request's
/// narrower limit across the executor AND its later transport allocations.
#[derive(Debug)]
pub(crate) struct QueryOperationBudget {
    parent: ReadMemoryOwner,
    limit: usize,
    held: std::sync::atomic::AtomicUsize,
    checkpoint: std::sync::Mutex<Option<QueryCheckpoint>>,
    terminal: std::sync::atomic::AtomicBool,
    pending_stop: std::sync::Mutex<Option<QueryStop>>,
}
#[derive(Clone)]
enum QueryCheckpoint {
    Owner(crate::executor::ReadPublicationBudget),
    Client {
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        deadline_ms: u64,
        cancellation: Option<contextdb_core::read_contract::OwnerReadCancellation>,
    },
}
impl std::fmt::Debug for QueryCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original query checkpoint")
    }
}
#[derive(Debug, Clone, Copy)]
enum QueryStop {
    Cancelled,
    OwnerTimeout,
    ActiveMs(u64),
}
impl QueryStop {
    fn into_error(self) -> contextdb_core::Error {
        use contextdb_core::read_contract::{
            OwnerLimitExceededDetail, ReadFailure, ReadFailureDetail, ReadFailureKind,
            ReadFailureLimit,
        };
        match self {
            Self::Cancelled => contextdb_core::Error::ReadCancelled,
            Self::OwnerTimeout => contextdb_core::Error::ReadFailure(
                ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
                    .expect("owner timeout has no detail"),
            ),
            Self::ActiveMs(value) => contextdb_core::Error::ReadFailure(
                ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::ActiveMs,
                    value,
                    required: None,
                    statement: None,
                }),
            ),
        }
    }
}
impl QueryCheckpoint {
    fn check(&self) -> Result<(), QueryStop> {
        match self {
            Self::Owner(budget) => {
                if budget.cancellation.is_cancelled() {
                    return Err(QueryStop::Cancelled);
                }
                if budget.elapsed() {
                    return Err(QueryStop::ActiveMs(budget.active_ms));
                }
            }
            Self::Client {
                clock,
                deadline_ms,
                cancellation,
            } => {
                if cancellation
                    .as_ref()
                    .is_some_and(|token| token.is_cancelled())
                {
                    return Err(QueryStop::Cancelled);
                }
                if clock.now_ms() >= *deadline_ms {
                    return Err(QueryStop::OwnerTimeout);
                }
            }
        }
        Ok(())
    }
}
impl ReadMemoryBudget for QueryOperationBudget {
    #[cfg(feature = "test-seams")]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        None
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        use std::sync::atomic::Ordering;
        if !self.terminal.load(Ordering::SeqCst) {
            let checkpoint = self
                .checkpoint
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if let Some(checkpoint) = checkpoint
                && let Err(stop) = checkpoint.check()
            {
                *self.pending_stop.lock().unwrap_or_else(|p| p.into_inner()) = Some(stop);
                // The request is already on the wire. A cancelled client's
                // response must reach its terminal before this channel can
                // serve another request; abandoning a frame after its prefix
                // would leave its body to be read as the next frame header.
                // Preserve the cancellation for the public terminal while
                // charging the drain to the same memory and response limits.
                // Owner execution still stops at its cancellation checkpoint.
                if !matches!(
                    (&checkpoint, stop),
                    (QueryCheckpoint::Client { .. }, QueryStop::Cancelled)
                ) {
                    return Err(stop.into_error());
                }
            }
        }
        if self
            .held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .is_err()
        {
            return Err(query_limit_error(self.limit));
        }
        if let Err(error) = self.parent.try_reserve(bytes, subsystem, operation, hint) {
            self.held.fetch_sub(bytes, Ordering::SeqCst);
            return Err(error);
        }
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.parent.release(bytes);
        self.held
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Every alias owns the same native limiter cell. Its original invoice stays
/// outside the payload destructor and settles only after actual physical free.
#[derive(Debug)]
pub(crate) struct QueryBudget {
    pub(crate) budget: ReadMemoryOwner,
    control: contextdb_core::read_memory::NativeReadMemory<QueryOperationBudget>,
}

impl QueryBudget {
    pub(crate) fn bind_owner(&self, budget: crate::executor::ReadPublicationBudget) {
        *self
            .control
            .checkpoint
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(QueryCheckpoint::Owner(budget));
    }
    /// A final refusal still owes its bytes after cancellation. Keep the
    /// original checkpoint/token, and fund only its terminal carrier under
    /// the SAME memory limit; this does not admit more execution work.
    pub(crate) fn terminal(&self) {
        self.control
            .terminal
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Private codecs cannot put a core error into their public error enum.
    /// Preserve the actual query stop by value until its real terminal maps it.
    pub(crate) fn take_stop(&self) -> Option<contextdb_core::Error> {
        self.control
            .pending_stop
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .map(QueryStop::into_error)
    }
}

/// The supplier's original fixed invoice is retained by its native owner.
/// Its byte count seeds the request limiter; it is NOT another reservation.
#[derive(Debug, Clone)]
pub(crate) struct AcquiredQueryOwner {
    owner: ReadMemoryOwner,
    control_bytes: usize,
}
impl AcquiredQueryOwner {
    pub(crate) fn funding_owner(&self) -> ReadMemoryOwner {
        self.owner.clone()
    }
    /// Prepay a surrounding keeper on the same genuine child. Its native
    /// invoice must not retain an Arc to the limiter stored inside that
    /// keeper. Seed the limiter with these already-debited bytes exactly once.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepay_control(&mut self, bytes: usize) -> contextdb_core::Result<ReadCredit> {
        let total = self
            .control_bytes
            .checked_add(bytes)
            .ok_or_else(size_error)?;
        let credit = ReadCredit::try_new_owned(
            self.owner.clone(),
            bytes,
            "reader",
            "query_ingress",
            "Reduce simultaneous query memory or raise the declared reader limit.",
        )?;
        self.control_bytes = total;
        Ok(credit)
    }
    pub(crate) fn native_with_controls(
        parent: &crate::vector_read_memory::DeclaredReadRoot,
        bytes: usize,
    ) -> Result<(Self, ReadCredit), contextdb_core::read_memory::NativeRefusal> {
        let (receipt, controls) = parent.native_child_with_controls(bytes)?;
        let control_bytes = receipt.control_bytes() + controls.bytes();
        Ok((
            Self {
                owner: receipt.into_owner(),
                control_bytes,
            },
            controls,
        ))
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn acquired_owner(
    provider: &crate::vector_read_memory::ReadRequestProvider,
) -> contextdb_core::Result<AcquiredQueryOwner> {
    use crate::vector_read_memory::RequestReadRoute;
    Ok(match provider.request_budget_owner()?.into_route() {
        RequestReadRoute::Legacy(owner) => AcquiredQueryOwner {
            owner: ReadMemoryOwner::from(owner),
            control_bytes: 0,
        },
        RequestReadRoute::Native(receipt) => {
            let control_bytes = receipt.control_bytes();
            AcquiredQueryOwner {
                owner: receipt.into_owner(),
                control_bytes,
            }
        }
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn query_operation(
    acquired: AcquiredQueryOwner,
    limit: u64,
) -> contextdb_core::Result<QueryBudget> {
    query_operation_retained(&acquired, limit)
}

/// The caller keeps the acquired capability outside this fallible constructor.
/// Failure to construct the limiter is not failure to acquire its child.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn query_operation_retained(
    acquired: &AcquiredQueryOwner,
    limit: u64,
) -> contextdb_core::Result<QueryBudget> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let bytes = ReadMemoryOwner::native_layout::<QueryOperationBudget, ReadCredit>().size();
    let held = bytes
        .checked_add(acquired.control_bytes)
        .ok_or_else(size_error)?;
    if held > limit {
        return Err(query_limit_error(limit));
    }
    let parent = acquired.owner.clone();
    // Listener invoices retain this exact child, avoiding a token/limiter cycle.
    // Bind the existing request ceiling at that source, including its fixed cell.
    parent.constrain_operation_memory(limit);
    let control = ReadCredit::try_new_owned(
        parent.clone(),
        bytes,
        "reader",
        "query_owner",
        "Raise the declared reader limit to admit a query owner.",
    )?;
    let payload = QueryOperationBudget {
        parent,
        limit,
        held: std::sync::atomic::AtomicUsize::new(held),
        checkpoint: std::sync::Mutex::new(None),
        terminal: std::sync::atomic::AtomicBool::new(false),
        pending_stop: std::sync::Mutex::new(None),
    };
    let control = ReadMemoryOwner::try_native_typed(payload, control)
        .map_err(crate::vector_read_memory::public_read_refusal)?;
    Ok(QueryBudget {
        budget: control.owner(),
        control,
    })
}

#[derive(Debug)]
pub(crate) struct QueryMemory {
    memory: crate::read_image_memory::ImageMemoryHandle,
}
impl QueryMemory {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn new(limit: u64) -> contextdb_core::Result<Self> {
        let root = crate::vector_read_memory::DeclaredReadMemory::query_root(limit)?;
        let provider = crate::vector_read_memory::ReadRequestProvider::from(root.root.clone());
        let acquired = acquired_owner(&provider)?;
        let operation = query_operation(acquired, limit)?;
        let budget = operation.budget.clone();
        let handle = crate::memory_accounting::AccountantHandle::reader(
            Some(usize::try_from(limit).unwrap_or(usize::MAX)),
            budget.clone(),
            false,
        )?;
        let control = Some(operation.control.clone());
        let memory = ImageMemory::native_reader_with_checkpoint(
            handle,
            budget,
            "reader",
            "materialize_answer",
            "Reduce simultaneous query memory or raise the declared reader limit.",
            (),
            control,
        )?;
        Ok(Self { memory })
    }
    /// A new request limiter forwards to the genuine supplied connection/read
    /// owner. It creates no root, independent budget, or replacement ceiling.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_original_owner(
        parent: ReadMemoryOwner,
        limit: u64,
    ) -> contextdb_core::Result<Self> {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let bytes = ReadMemoryOwner::native_layout::<QueryOperationBudget, ReadCredit>().size();
        if bytes > limit {
            return Err(query_limit_error(limit));
        }
        let invoice = ReadCredit::try_new_owned(
            parent.clone(),
            bytes,
            "reader",
            "query_owner",
            "Raise the declared reader limit to admit a query owner.",
        )?;
        let payload = QueryOperationBudget {
            parent,
            limit,
            held: std::sync::atomic::AtomicUsize::new(bytes),
            checkpoint: std::sync::Mutex::new(None),
            terminal: std::sync::atomic::AtomicBool::new(false),
            pending_stop: std::sync::Mutex::new(None),
        };
        let control = ReadMemoryOwner::try_native_typed(payload, invoice)
            .map_err(crate::vector_read_memory::public_read_refusal)?;
        let budget = control.owner();
        let handle =
            crate::memory_accounting::AccountantHandle::reader(Some(limit), budget.clone(), false)?;
        let memory = ImageMemory::native_reader_with_checkpoint(
            handle,
            budget,
            "reader",
            "materialize_answer",
            "Reduce simultaneous query memory or raise the declared reader limit.",
            (),
            Some(control),
        )?;
        Ok(Self { memory })
    }
    pub(crate) fn image(&self) -> crate::read_image_memory::ImageMemoryHandle {
        self.memory.clone()
    }
    pub(crate) fn bind_client(
        &self,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        deadline_ms: u64,
        cancellation: Option<contextdb_core::read_contract::OwnerReadCancellation>,
    ) {
        *self
            .memory
            .query_control
            .as_ref()
            .expect("original query checkpoint")
            .checkpoint
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(QueryCheckpoint::Client {
            clock,
            deadline_ms,
            cancellation,
        });
    }
    pub(crate) fn terminal(&self) {
        self.memory
            .query_control
            .as_ref()
            .expect("original query checkpoint")
            .terminal
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn take_stop(&self) -> Option<contextdb_core::Error> {
        self.memory
            .query_control
            .as_ref()
            .expect("original query checkpoint")
            .pending_stop
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .map(QueryStop::into_error)
    }
}

/// The result's native funding capability keeps the original receiver stack
/// outside every allocation that can retain an alias to it. Its invoice drops
/// only after this forwarding cell and the public result cell physically free.
#[derive(Debug)]
struct RetainedQueryBudget {
    budget: ReadMemoryOwner,
}
impl ReadMemoryBudget for RetainedQueryBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.budget.try_reserve(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.budget.release(bytes);
    }
}
#[derive(Debug)]
struct RetainedQueryInvoice {
    // On allocation refusal, the unpublished answer is physically destroyed
    // before the forwarding debit and the original query controls retire.
    unpublished: Option<crate::database::OwnedQueryResult>,
    _cell: ReadCredit,
    _memory: QueryMemory,
}
impl QueryMemory {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_result(
        self,
        result: crate::database::OwnedQueryResult,
    ) -> contextdb_core::Result<crate::QueryResult> {
        let budget = self
            .memory
            .reader_budget()
            .expect("query image retains its reader");
        let bytes =
            ReadMemoryOwner::native_layout::<RetainedQueryBudget, RetainedQueryInvoice>().size();
        let credit = match ReadCredit::try_new_owned(
            budget.clone(),
            bytes,
            "reader",
            "retain_query_owner",
            "Release retained answers or raise the declared reader memory limit.",
        ) {
            Ok(credit) => credit,
            Err(error) => {
                drop(result);
                drop(budget);
                drop(self);
                return Err(error);
            }
        };
        drop(budget);
        let invoice = RetainedQueryInvoice {
            unpublished: Some(result),
            _cell: credit,
            _memory: self,
        };
        let (owner, result) = ReadMemoryOwner::try_native_result_with(invoice, |invoice| {
            let budget = invoice
                ._memory
                .memory
                .reader_budget()
                .expect("query image retains its reader");
            let answer = invoice
                .unpublished
                .take()
                .expect("receiver retains its unpublished answer");
            (RetainedQueryBudget { budget }, answer)
        })
        .map_err(crate::vector_read_memory::public_read_refusal)?;
        result.into_public_with_owner(owner)
    }
}

/// A cursor reuses one genuine receiving operation across all of its pages.
/// Its forwarding owner retains the original stack once, outside every alias.
#[derive(Debug)]
struct RetainedCursorInvoice {
    _cell: ReadCredit,
    _memory: QueryMemory,
}
impl QueryMemory {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn into_cursor_owner(
        self,
    ) -> contextdb_core::Result<(ReadMemoryOwner, crate::read_image_memory::ImageMemoryHandle)>
    {
        let budget = self
            .memory
            .reader_budget()
            .expect("query image retains its reader");
        let bytes =
            ReadMemoryOwner::native_layout::<RetainedQueryBudget, RetainedCursorInvoice>().size();
        let credit = ReadCredit::try_new(
            budget.clone(),
            bytes,
            "reader",
            "retain_cursor_owner",
            "Release retained pages or raise the declared reader memory limit.",
        )?;
        let invoice = RetainedCursorInvoice {
            _cell: credit,
            _memory: self,
        };
        // Clone the image only AFTER nullable allocation succeeds. Otherwise
        // no external image alias can outlive the original invoice on null.
        ReadMemoryOwner::try_native_result_with(invoice, |invoice| {
            (RetainedQueryBudget { budget }, invoice._memory.image())
        })
        .map_err(crate::vector_read_memory::public_read_refusal)
    }
}
/// The decoder debited the operation behind this exact forwarding capability.
/// Move that prepaid debit (no refund or reservation) under its full keeper.
/// Callers use only the owner returned with this decoder by into_cursor_owner.
pub(crate) fn retain_cursor_credit(credit: ReadCredit, owner: ReadMemoryOwner) -> ReadCredit {
    let bytes = credit.bytes();
    credit.transfer();
    ReadCredit::from_prepaid_owned(owner, bytes)
}

fn query_limit_error(limit: usize) -> contextdb_core::Error {
    use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit};
    contextdb_core::Error::ReadFailure(ReadFailure::owner_limit_exceeded(
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::Memory,
            value: u64::try_from(limit).unwrap_or(u64::MAX),
            required: None,
            statement: None,
        },
    ))
}

fn size_error() -> contextdb_core::Error {
    contextdb_core::Error::Other("query allocation size overflow".into())
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn add(a: usize, b: usize) -> contextdb_core::Result<usize> {
    a.checked_add(b).ok_or_else(size_error)
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn mul(a: usize, b: usize) -> contextdb_core::Result<usize> {
    a.checked_mul(b).ok_or_else(size_error)
}

/// Rust's existing B-tree node envelope: at most eleven key/value slots,
/// twelve child links, the parent/index/length header and alignment padding.
/// The occupancy lower bound (five entries outside the root) bounds ALL live
/// nodes, including internal nodes. This does not change a consumer limit.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn tree_bytes<K, V>(len: usize) -> contextdb_core::Result<usize> {
    if len == 0 {
        return Ok(0);
    }
    let node = add(
        mul(11, add(std::mem::size_of::<K>(), std::mem::size_of::<V>())?)?,
        mul(16, std::mem::size_of::<usize>())?,
    )?;
    mul(1 + (len - 1) / 5, node)
}

/// The private client clones only after admitting every destination buffer.
/// Insert explicitly to avoid collect's hidden sorting Vec overlapping nodes.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn query_request(
    statement: &str,
    params: &std::collections::HashMap<String, contextdb_core::Value>,
    memory: crate::read_image_memory::ImageMemoryHandle,
) -> contextdb_core::Result<Funded<super::LocalRequest>> {
    let mut bytes = add(
        statement.len(),
        tree_bytes::<String, contextdb_core::Value>(params.len())?,
    )?;
    for (key, value) in params {
        bytes = add(bytes, key.len())?;
        let heap = match value {
            contextdb_core::Value::Text(text) => text.len(),
            contextdb_core::Value::Vector(vector) => mul(vector.len(), std::mem::size_of::<f32>())?,
            contextdb_core::Value::Json(value) => {
                crate::read_image_memory::json_clone_heap_bytes(value)
            }
            _ => 0,
        };
        bytes = add(bytes, heap)?;
    }
    Funded::allocate_owned(Some(memory.owner()), bytes, || {
        let statement = statement.to_owned();
        let mut copied = std::collections::BTreeMap::new();
        for (key, value) in params {
            copied.insert(key.clone(), value.clone());
        }
        super::LocalRequest::Query {
            statement,
            params: copied,
        }
    })
}

/// Only the replacement table is new: parameter strings/values move with their
/// original inbound custody. Both the old B-tree and the full new hash table
/// coexist until the source iterator has physically destroyed every node.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn query_params(
    params: std::collections::BTreeMap<String, contextdb_core::Value>,
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<Funded<std::collections::HashMap<String, contextdb_core::Value>>> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    let len = params.len();
    let buckets = if len == 0 {
        0
    } else if len < 4 {
        4
    } else if len < 8 {
        8
    } else {
        (mul(len, 8)? / 7)
            .checked_next_power_of_two()
            .ok_or_else(size_error)?
    };
    let bytes = if buckets == 0 {
        0
    } else {
        add(
            mul(
                buckets,
                std::mem::size_of::<(String, contextdb_core::Value)>(),
            )?,
            add(buckets, 32)?,
        )?
    };
    Funded::allocate_owned(Some(owner), bytes, || {
        let mut output = std::collections::HashMap::with_capacity(len);
        for (key, value) in params {
            output.insert(key, value);
        }
        output
    })
}

struct DisplaySize(usize);
impl std::fmt::Write for DisplaySize {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}
/// Format diagnostics only after their exact UTF-8 destination is admitted.
pub(crate) fn display_error(
    value: &impl std::fmt::Display,
    memory: &crate::read_image_memory::ImageMemoryHandle,
) -> Funded<contextdb_core::Error> {
    let mut size = DisplaySize(0);
    if std::fmt::write(&mut size, format_args!("{value}")).is_err() {
        return Funded::external(size_error());
    }
    let mut text = match Funded::allocate_owned(Some(memory.owner()), size.0, || {
        String::with_capacity(size.0)
    }) {
        Ok(text) => text,
        Err(error) => return Funded::external(error),
    };
    use std::fmt::Write;
    if write!(&mut text.value, "{value}").is_err() {
        return Funded::external(size_error());
    }
    Funded {
        value: contextdb_core::Error::Other(text.value.into()),
        credit: text.credit,
    }
}

/// The original decoded failure invoice stays live beside the original wire
/// document while its nested error is decoded by the SAME query ImageMemory.
pub(crate) fn query_error(
    failure: Funded<super::LocalEngineFailure>,
    memory: &crate::read_image_memory::ImageMemoryHandle,
    ceiling: u64,
) -> Funded<contextdb_core::Error> {
    match failure.value.decode_query_error(memory, ceiling) {
        Ok(error) => error,
        Err(error) => Funded::external(error),
    }
    // `failure` physically destroys the document before its original credit.
}

/// Admit only allocations made by the existing move conversion. Most error
/// classes move Strings verbatim and need no scratch. The exceptional trees
/// and formatted Unknown class receive their own pre-allocation invoice.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn error_conversion_bytes(
    document: &super::engine_answer::ReadChannelError,
) -> contextdb_core::Result<usize> {
    use super::engine_answer::ReadChannelError as E;
    match document {
        E::ContextScopeViolation { requested, allowed } => {
            super::engine_answer::query_context_conversion_bytes(requested, allowed.len())
        }
        E::ScopeLabelViolation { allowed, .. } => {
            tree_bytes::<contextdb_core::ScopeLabel, ()>(allowed.len())
        }
        E::Unknown {
            class_name,
            message,
        } => add(add(class_name.len(), message.len())?, 2),
        _ => Ok(0),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn owner_failure_responses(
    error: crate::owner_read::OwnerReadScaffoldError,
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    use crate::owner_read::OwnerReadScaffoldError as E;
    let response = match error {
        E::Refused(failure) => Funded::external(super::LocalResponse::Failure { failure }),
        E::Database(error) if memory_refusal_ceiling(&error).is_some() => {
            Funded::external(super::LocalResponse::Failure {
                failure: super::memory_ceiling_read_failure(
                    memory_refusal_ceiling(&error).expect("classified memory refusal"),
                    None,
                ),
            })
        }
        E::Database(error) => {
            let failure = super::LocalEngineFailure::from_query_error(&error, owner.clone())?;
            Funded {
                value: super::LocalResponse::EngineFailure {
                    failure: failure.value,
                },
                credit: failure.credit,
            }
        }
        E::LocalTransport(error) => {
            return match E::from_local(error) {
                other @ E::LocalTransport(_) => owner_diagnostic_response(&other, owner),
                other => owner_failure_responses(other, owner),
            };
        }
        other => return owner_diagnostic_response(&other, owner),
    };
    one_response(response, owner)
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn owner_diagnostic_response(
    error: &impl std::fmt::Display,
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    let mut size = DisplaySize(0);
    std::fmt::write(&mut size, format_args!("{error}")).map_err(|_| size_error())?;
    let mut text = Funded::allocate_owned(Some(owner.clone()), size.0, || {
        String::with_capacity(size.0)
    })?;
    use std::fmt::Write;
    write!(&mut text.value, "{error}").map_err(|_| size_error())?;
    let error = Funded {
        value: contextdb_core::Error::Other(text.value.into()),
        credit: text.credit,
    };
    let failure = super::LocalEngineFailure::from_query_error(&error.value, owner.clone())?;
    drop(error);
    one_response(
        Funded {
            value: super::LocalResponse::EngineFailure {
                failure: failure.value,
            },
            credit: failure.credit,
        },
        owner,
    )
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn one_response(
    mut response: Funded<super::LocalResponse>,
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    let mut output = Funded::allocate_owned(
        Some(owner),
        std::mem::size_of::<super::LocalResponse>(),
        || Vec::with_capacity(1),
    )?;
    output.value.push(response.value);
    if let Some(credit) = response.credit.take() {
        output
            .credit
            .as_mut()
            .expect("query response container is funded")
            .try_merge_prepaid(credit)?;
    }
    Ok(output)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn owner_panic_responses(
    payload: &(dyn std::any::Any + Send),
    owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let owner: contextdb_core::read_memory::ReadMemoryOwner = owner.into();

    struct Diagnostic<'a>(&'a (dyn std::any::Any + Send));
    impl std::fmt::Display for Diagnostic<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let reason = self
                .0
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("no reason recorded");
            write!(f, "the owner's read ended unexpectedly: {reason}")
        }
    }
    owner_diagnostic_response(&Diagnostic(payload), owner)
}

pub(crate) fn memory_refusal_ceiling(error: &contextdb_core::Error) -> Option<u64> {
    use contextdb_core::{Error, read_memory::NativeRefusal};
    match error.engine_diagnostic() {
        Error::MemoryBudgetExceeded {
            budget_limit_bytes, ..
        }
        | Error::NativeEngineOther(
            NativeRefusal::MemoryBudgetExceeded {
                budget_limit_bytes, ..
            }
            | NativeRefusal::ProcessMemoryBudgetExceeded {
                budget_limit_bytes, ..
            },
        ) => Some(*budget_limit_bytes as u64),
        Error::ReadFailure(failure) => match failure.detail() {
            contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail)
                if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Memory =>
            {
                Some(detail.value)
            }
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn transport_error(
    error: contextdb_core::Error,
    ceiling: u64,
) -> super::LocalTransportError {
    // Internal producers retain their original diagnostics. At the transport
    // boundary a real memory refusal keeps the established actionable tag.
    if crate::read_image_memory::is_memory_refusal(&error) {
        let ceiling = if ceiling == u64::MAX {
            memory_refusal_ceiling(&error).unwrap_or(ceiling)
        } else {
            ceiling
        };
        return super::LocalTransportError::Payload(
            super::PayloadViolation::MemoryCeilingExceeded {
                ceiling,
                required: None,
            },
        );
    }
    match error {
        contextdb_core::Error::ReadFailure(failure) => super::LocalTransportError::Refusal(failure),
        contextdb_core::Error::ReadCancelled => super::owner_disconnected(),
        _ => super::invalid_channel_data(),
    }
}

/// Exact native Arc backing layout, admitted before engine control creation.
pub(crate) fn arc_bytes<T>() -> usize {
    std::alloc::Layout::new::<(
        std::sync::atomic::AtomicUsize,
        std::sync::atomic::AtomicUsize,
    )>()
    .extend(std::alloc::Layout::new::<T>())
    .expect("Arc control layout")
    .0
    .pad_to_align()
    .size()
}
