//! Private query payloads retain their original invoice through physical
//! destruction. Public DTO conversion is an explicit consumer handoff.
use crate::read_image_memory::ImageMemory;
use contextdb_core::read_memory::{ReadMemoryBudget, ReadMemoryOwner, ReadCredit};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Funded<T> {
    // Declaration order is destruction order: payload, then its invoice.
    pub(crate) value: T,
    pub(crate) credit: Option<ReadCredit>,
}

impl<T> Funded<T> {
    pub(crate) fn external(value: T) -> Self { Self { value, credit: None } }

    pub(crate) fn allocate(
        owner: Option<Arc<dyn ReadMemoryBudget>>,
        bytes: usize,
        make: impl FnOnce() -> T,
    ) -> contextdb_core::Result<Self> {
        let credit = owner.map(|owner| ReadCredit::try_new(
            owner, bytes, "reader", "query_buffer",
            "Reduce simultaneous query working memory or raise the declared reader limit.",
        )).transpose()?;
        let value = make();
        Ok(Self { value, credit })
    }

    /// Only call at an actual public consumer boundary. The value moves;
    /// its engine invoice returns as custody leaves the engine.
    pub(crate) fn into_public(self) -> T { self.value }

    /// Move decoded payload into the SAME ImageMemory's retained answer.
    /// No reserve or release occurs: the original debit remains on that owner.
    pub(crate) fn into_retained(self) -> T {
        if let Some(credit) = self.credit { credit.transfer(); }
        self.value
    }
}

impl AsMut<[u8]> for Funded<Vec<u8>> {
    fn as_mut(&mut self) -> &mut [u8] { self.value.as_mut_slice() }
}

#[derive(Debug)]
pub(crate) struct FundedVec<T> {
    pub(crate) values: Vec<T>,
    credit: Option<ReadCredit>,
    owner: Option<Arc<dyn ReadMemoryBudget>>,
}

impl<T> Default for FundedVec<T> {
    fn default() -> Self { Self::new(None) }
}

impl<T> FundedVec<T> {
    pub(crate) fn new(owner: Option<Arc<dyn ReadMemoryBudget>>) -> Self {
        Self { values: Vec::new(), credit: None, owner }
    }
    pub(crate) fn owner(&self) -> Option<Arc<dyn ReadMemoryBudget>> { self.owner.clone() }
    pub(crate) fn clear(&mut self) {
        // clear retains Vec capacity, so retain its invoice too.
        self.values.clear();
    }
    pub(crate) fn push(&mut self, value: T) -> contextdb_core::Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| contextdb_core::Error::Other("query container overflow".into()))?;
            let bytes = capacity.checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| contextdb_core::Error::Other("query container size overflow".into()))?;
            // The complete replacement coexists with the old allocation.
            let mut replacement = Funded::allocate(self.owner.clone(), bytes,
                || Vec::with_capacity(capacity))?;
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
    pub(crate) fn into_public(mut self) -> contextdb_core::Result<Vec<super::LocalResponse>> {
        let bytes = self.values.len().checked_mul(std::mem::size_of::<super::LocalResponse>())
            .ok_or_else(|| contextdb_core::Error::Other("query handoff size overflow".into()))?;
        let mut output = Funded::allocate(self.owner.clone(), bytes,
            || Vec::with_capacity(self.values.len()))?;
        for response in &mut self.values {
            let value = std::mem::replace(&mut response.value,
                super::LocalResponse::CancelApplied { request_ordinal: 0 });
            output.value.push(value);
            if let Some(credit) = response.credit.take() { credit.transfer(); }
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
    owner: Arc<ImageMemory>,
    before: usize,
    armed: bool,
}
impl Drop for DecodeCredit {
    fn drop(&mut self) {
        if self.armed { self.owner.release(self.owner.held() - self.before); }
    }
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    owner: &Arc<ImageMemory>,
) -> contextdb_core::Result<Funded<T>> {
    // Input is already funded by the carrier/assembly, never charged twice.
    let mut pending = DecodeCredit { owner: owner.clone(), before: owner.held(), armed: true };
    let (value, consumed) = owner.scope(|| crate::read_image_memory::decode(bytes))?;
    if consumed != bytes.len() {
        drop(value);
        return Err(contextdb_core::Error::Other("query decode trailing bytes".into()));
    }
    let credit = ReadCredit::from_prepaid(owner.clone(), owner.held() - pending.before);
    pending.armed = false;
    Ok(Funded { value, credit: Some(credit) })
}

/// One private envelope around the genuine acquired query child. Every debit
/// still reaches that exact child; the counter only enforces this request's
/// narrower limit across the executor AND its later transport allocations.
#[derive(Debug)]
struct QueryOperationBudget {
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
enum QueryStop { Cancelled, OwnerTimeout, ActiveMs(u64) }
impl QueryStop {
    fn into_error(self) -> contextdb_core::Error {
        use contextdb_core::read_contract::{ReadFailure, ReadFailureKind, ReadFailureDetail,
            OwnerLimitExceededDetail, ReadFailureLimit};
        match self {
            Self::Cancelled => contextdb_core::Error::ReadCancelled,
            Self::OwnerTimeout => contextdb_core::Error::ReadFailure(
                ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
                    .expect("owner timeout has no detail")),
            Self::ActiveMs(value) => contextdb_core::Error::ReadFailure(
                ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::ActiveMs, value, required: None, statement: None,
                })),
        }
    }
}
impl QueryCheckpoint {
    fn check(&self) -> Result<(), QueryStop> {
        match self {
            Self::Owner(budget) => {
                if budget.cancellation.is_cancelled() { return Err(QueryStop::Cancelled); }
                if budget.elapsed() { return Err(QueryStop::ActiveMs(budget.active_ms)); }
            }
            Self::Client { clock, deadline_ms, cancellation } => {
                if cancellation.as_ref().is_some_and(|token| token.is_cancelled()) {
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
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> { None }
    fn try_reserve(&self, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str) -> contextdb_core::Result<()>
    {
        use std::sync::atomic::Ordering;
        if !self.terminal.load(Ordering::SeqCst) {
            let checkpoint = self.checkpoint.lock().unwrap_or_else(|p| p.into_inner()).clone();
            if let Some(checkpoint) = checkpoint {
                if let Err(stop) = checkpoint.check() {
                    *self.pending_stop.lock().unwrap_or_else(|p| p.into_inner()) = Some(stop);
                    return Err(stop.into_error());
                }
            }
        }
        if self.held.fetch_update(Ordering::SeqCst, Ordering::SeqCst,
            |held| held.checked_add(bytes).filter(|next| *next <= self.limit)).is_err()
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
        self.held.fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

/// External custody for the actual Arc allocation. All escaping budget aliases
/// are nested under this keeper (the worker result carries it to publication).
/// The original credit is not a field of the block it prices.
#[derive(Debug)]
pub(crate) struct QueryBudget {
    pub(crate) budget: Arc<dyn ReadMemoryBudget>,
    control: Arc<QueryOperationBudget>,
    _control: ReadCredit,
}

impl QueryBudget {
    pub(crate) fn bind_owner(&self, budget: crate::executor::ReadPublicationBudget) {
        *self.control.checkpoint.lock().unwrap_or_else(|p| p.into_inner()) = Some(QueryCheckpoint::Owner(budget));
    }
    pub(crate) fn bind_client(&self,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>, deadline_ms: u64,
        cancellation: Option<contextdb_core::read_contract::OwnerReadCancellation>,
    ) {
        *self.control.checkpoint.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(QueryCheckpoint::Client { clock, deadline_ms, cancellation });
    }
    /// A final refusal still owes its bytes after cancellation. Keep the
    /// original checkpoint/token, and fund only its terminal carrier under
    /// the SAME memory limit; this does not admit more execution work.
    pub(crate) fn terminal(&self) {
        self.control.terminal.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// Private codecs cannot put a core error into their public error enum.
    /// Preserve the actual query stop by value until its real terminal maps it.
    pub(crate) fn take_stop(&self) -> Option<contextdb_core::Error> {
        self.control.pending_stop.lock().unwrap_or_else(|p| p.into_inner())
            .take().map(QueryStop::into_error)
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
    pub(crate) fn funding_owner(&self) -> ReadMemoryOwner { self.owner.clone() }
    /// Prepay a surrounding keeper on the same genuine child. Its native
    /// invoice must not retain an Arc to the limiter stored inside that
    /// keeper. Seed the limiter with these already-debited bytes exactly once.
    pub(crate) fn prepay_control(&mut self, bytes: usize) -> contextdb_core::Result<ReadCredit> {
        let total = self.control_bytes.checked_add(bytes).ok_or_else(size_error)?;
        let credit = ReadCredit::try_new_owned(self.owner.clone(), bytes, "reader", "query_ingress",
            "Reduce simultaneous query memory or raise the declared reader limit.")?;
        self.control_bytes = total;
        Ok(credit)
    }
    pub(crate) fn native_with_controls(
        parent: &Arc<crate::vector_read_memory::DeclaredReadMemory>, bytes: usize,
    ) -> Result<(Self, ReadCredit), contextdb_core::read_memory::NativeRefusal> {
        let (receipt, controls) = parent.native_child_with_controls(bytes)?;
        let control_bytes = receipt.control_bytes() + controls.bytes();
        Ok((Self { owner: receipt.into_owner(), control_bytes }, controls))
    }

}
pub(crate) fn acquired_owner(
    provider: &dyn crate::vector_read_memory::ReadRequestBudgetProvider,
) -> contextdb_core::Result<AcquiredQueryOwner> {
    use crate::vector_read_memory::RequestReadRoute;
    Ok(match provider.request_budget_owner()?.into_route() {
        RequestReadRoute::Legacy(owner) => AcquiredQueryOwner {
            owner: ReadMemoryOwner::from_legacy(owner), control_bytes: 0,
        },
        RequestReadRoute::Native(receipt) => {
            let control_bytes = receipt.control_bytes();
            AcquiredQueryOwner { owner: receipt.into_owner(), control_bytes }
        }
    })
}

pub(crate) fn query_operation(
    acquired: AcquiredQueryOwner, limit: u64,
) -> contextdb_core::Result<QueryBudget> {
    query_operation_retained(&acquired, limit)
}

/// The caller keeps the acquired capability outside this fallible constructor.
/// Failure to construct the limiter is not failure to acquire its child.
pub(crate) fn query_operation_retained(
    acquired: &AcquiredQueryOwner, limit: u64,
) -> contextdb_core::Result<QueryBudget> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let bytes = std::mem::size_of::<QueryOperationBudget>() + 2 * std::mem::size_of::<usize>();
    let held = bytes.checked_add(acquired.control_bytes).ok_or_else(size_error)?;
    if held > limit { return Err(query_limit_error(limit)); }
    let parent = acquired.owner.clone();
    // Listener invoices retain this exact child, avoiding a token/limiter cycle.
    // Bind the existing request ceiling at that source, including its fixed cell.
    parent.constrain_operation_memory(limit);
    let control = ReadCredit::try_new_owned(parent.clone(), bytes, "reader", "query_owner",
        "Raise the declared reader limit to admit a query owner.")?;
    let budget = Arc::new(QueryOperationBudget { parent, limit,
        held: std::sync::atomic::AtomicUsize::new(held),
        checkpoint: std::sync::Mutex::new(None),
        terminal: std::sync::atomic::AtomicBool::new(false),
        pending_stop: std::sync::Mutex::new(None) });
    Ok(QueryBudget { budget: budget.clone(), control: budget, _control: control })
}

/// Kept on the terminal caller's stack, outside every allocation it funds.
/// Declaration order makes the ImageMemory Arc, accountant and all nested
/// controls physically disappear before their ORIGINAL control credit settles.
#[derive(Debug)]
pub(crate) struct QueryMemory {
    memory: Funded<Arc<ImageMemory>>,
    _accountant: Funded<Arc<crate::memory_accounting::MemoryAccountant>>,
    _operation: QueryBudget,
    _root: crate::vector_read_memory::QueryReadRoot,
}
impl QueryMemory {
    pub(crate) fn new(limit: u64) -> contextdb_core::Result<Self> {
        let root = crate::vector_read_memory::DeclaredReadMemory::query_root(limit)?;
        let acquired = acquired_owner(&root.root)?;
        let operation = query_operation(acquired, limit)?;
        let budget = operation.budget.clone();
        let controls = std::mem::size_of::<crate::memory_accounting::MemoryAccountant>()
            + std::mem::size_of::<std::sync::atomic::AtomicUsize>()
            + std::mem::size_of::<crate::memory_accounting::WriterCounter>()
            + 6 * std::mem::size_of::<usize>();
        let accountant = Funded::allocate(Some(budget.clone()), controls, ||
            Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                usize::try_from(limit).unwrap_or(usize::MAX))))?;
        let memory = ImageMemory::query_reader_for(accountant.value.clone().into(), budget)?;
        Ok(Self { memory, _accountant: accountant, _operation: operation, _root: root })
    }
    pub(crate) fn image(&self) -> Arc<ImageMemory> { self.memory.value.clone() }
    pub(crate) fn bind_client(&self,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>, deadline_ms: u64,
        cancellation: Option<contextdb_core::read_contract::OwnerReadCancellation>,
    ) { self._operation.bind_client(clock, deadline_ms, cancellation); }
    pub(crate) fn terminal(&self) { self._operation.terminal(); }
    pub(crate) fn take_stop(&self) -> Option<contextdb_core::Error> { self._operation.take_stop() }
}

fn query_limit_error(limit: usize) -> contextdb_core::Error {
    use contextdb_core::read_contract::{ReadFailure, OwnerLimitExceededDetail, ReadFailureLimit};
    contextdb_core::Error::ReadFailure(ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::Memory, value: u64::try_from(limit).unwrap_or(u64::MAX),
        required: None, statement: None,
    }))
}

fn size_error() -> contextdb_core::Error {
    contextdb_core::Error::Other("query allocation size overflow".into())
}
fn add(a: usize, b: usize) -> contextdb_core::Result<usize> {
    a.checked_add(b).ok_or_else(size_error)
}
fn mul(a: usize, b: usize) -> contextdb_core::Result<usize> {
    a.checked_mul(b).ok_or_else(size_error)
}

/// Rust's existing B-tree node envelope: at most eleven key/value slots,
/// twelve child links, the parent/index/length header and alignment padding.
/// The occupancy lower bound (five entries outside the root) bounds ALL live
/// nodes, including internal nodes. This does not change a consumer limit.
pub(crate) fn tree_bytes<K, V>(len: usize) -> contextdb_core::Result<usize> {
    if len == 0 { return Ok(0); }
    let node = add(mul(11, add(std::mem::size_of::<K>(), std::mem::size_of::<V>())?)?,
        mul(16, std::mem::size_of::<usize>())?)?;
    mul(1 + (len - 1) / 5, node)
}

/// The private client clones only after admitting every destination buffer.
/// Insert explicitly to avoid collect's hidden sorting Vec overlapping nodes.
pub(crate) fn query_request(
    statement: &str,
    params: &std::collections::HashMap<String, contextdb_core::Value>,
    memory: Arc<ImageMemory>,
) -> contextdb_core::Result<Funded<super::LocalRequest>> {
    let mut bytes = add(statement.len(), tree_bytes::<String, contextdb_core::Value>(params.len())?)?;
    for (key, value) in params {
        bytes = add(bytes, key.len())?;
        let heap = match value {
            contextdb_core::Value::Text(text) => text.len(),
            contextdb_core::Value::Vector(vector) => mul(vector.len(), std::mem::size_of::<f32>())?,
            contextdb_core::Value::Json(value) => crate::read_image_memory::json_clone_heap_bytes(value),
            _ => 0,
        };
        bytes = add(bytes, heap)?;
    }
    Funded::allocate(Some(memory), bytes, || {
        let statement = statement.to_owned();
        let mut copied = std::collections::BTreeMap::new();
        for (key, value) in params { copied.insert(key.clone(), value.clone()); }
        super::LocalRequest::Query { statement, params: copied }
    })
}

/// Only the replacement table is new: parameter strings/values move with their
/// original inbound custody. Both the old B-tree and the full new hash table
/// coexist until the source iterator has physically destroyed every node.
pub(crate) fn query_params(
    params: std::collections::BTreeMap<String, contextdb_core::Value>,
    owner: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<Funded<std::collections::HashMap<String, contextdb_core::Value>>> {
    let len = params.len();
    let buckets = if len == 0 { 0 } else if len < 4 { 4 } else if len < 8 { 8 }
        else { (mul(len, 8)? / 7).checked_next_power_of_two().ok_or_else(size_error)? };
    let bytes = if buckets == 0 { 0 } else {
        add(mul(buckets, std::mem::size_of::<(String, contextdb_core::Value)>())?, add(buckets, 32)?)?
    };
    Funded::allocate(Some(owner), bytes, || {
        let mut output = std::collections::HashMap::with_capacity(len);
        for (key, value) in params { output.insert(key, value); }
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
    value: &impl std::fmt::Display, memory: &Arc<ImageMemory>,
) -> Funded<contextdb_core::Error> {
    let mut size = DisplaySize(0);
    if std::fmt::write(&mut size, format_args!("{value}")).is_err() {
        return Funded::external(size_error());
    }
    let mut text = match Funded::allocate(Some(memory.clone()), size.0,
        || String::with_capacity(size.0)) {
        Ok(text) => text,
        Err(error) => return Funded::external(error),
    };
    use std::fmt::Write;
    if write!(&mut text.value, "{value}").is_err() {
        return Funded::external(size_error());
    }
    Funded { value: contextdb_core::Error::Other(text.value.into()), credit: text.credit }
}

/// The original decoded failure invoice stays live beside the original wire
/// document while its nested error is decoded by the SAME query ImageMemory.
pub(crate) fn query_error(
    failure: Funded<super::LocalEngineFailure>, memory: &Arc<ImageMemory>, ceiling: u64,
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
pub(crate) fn error_conversion_bytes(
    document: &super::engine_answer::ReadChannelError,
) -> contextdb_core::Result<usize> {
    use super::engine_answer::ReadChannelError as E;
    match document {
        E::ContextScopeViolation { requested, allowed } =>
            super::engine_answer::query_context_conversion_bytes(requested, allowed.len()),
        E::ScopeLabelViolation { allowed, .. } =>
            tree_bytes::<contextdb_core::ScopeLabel, ()>(allowed.len()),
        E::Unknown { class_name, message } => add(add(class_name.len(), message.len())?, 2),
        _ => Ok(0),
    }
}

pub(crate) fn owner_failure_responses(
    error: crate::owner_read::OwnerReadScaffoldError,
    owner: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    use crate::owner_read::OwnerReadScaffoldError as E;
    let response = match error {
        E::Refused(failure) => Funded::external(super::LocalResponse::Failure { failure }),
        E::Database(error) => {
            let failure = super::LocalEngineFailure::from_query_error(&error, owner.clone())?;
            Funded { value: super::LocalResponse::EngineFailure { failure: failure.value },
                credit: failure.credit }
        }
        E::LocalTransport(error) => {
            return match E::from_local(error) {
                E::Refused(failure) => owner_failure_responses(E::Refused(failure), owner),
                other => owner_diagnostic_response(&other, owner),
            };
        }
        other => return owner_diagnostic_response(&other, owner),
    };
    one_response(response, owner)
}
fn owner_diagnostic_response(
    error: &impl std::fmt::Display, owner: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let mut size = DisplaySize(0);
    std::fmt::write(&mut size, format_args!("{error}")).map_err(|_| size_error())?;
    let mut text = Funded::allocate(Some(owner.clone()), size.0, || String::with_capacity(size.0))?;
    use std::fmt::Write;
    write!(&mut text.value, "{error}").map_err(|_| size_error())?;
    let error = Funded { value: contextdb_core::Error::Other(text.value.into()), credit: text.credit };
    let failure = super::LocalEngineFailure::from_query_error(&error.value, owner.clone())?;
    drop(error);
    one_response(Funded { value: super::LocalResponse::EngineFailure { failure: failure.value },
        credit: failure.credit }, owner)
}
fn one_response(
    mut response: Funded<super::LocalResponse>, owner: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    let mut output = Funded::allocate(Some(owner), std::mem::size_of::<super::LocalResponse>(),
        || Vec::with_capacity(1))?;
    output.value.push(response.value);
    if let Some(credit) = response.credit.take() {
        output.credit.as_mut().expect("query response container is funded")
            .try_merge_prepaid(credit)?;
    }
    Ok(output)
}

pub(crate) fn owner_panic_responses(
    payload: &(dyn std::any::Any + Send), owner: Arc<dyn ReadMemoryBudget>,
) -> contextdb_core::Result<Funded<Vec<super::LocalResponse>>> {
    struct Diagnostic<'a>(&'a (dyn std::any::Any + Send));
    impl std::fmt::Display for Diagnostic<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let reason = self.0.downcast_ref::<&str>().copied()
                .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("no reason recorded");
            write!(f, "the owner's read ended unexpectedly: {reason}")
        }
    }
    owner_diagnostic_response(&Diagnostic(payload), owner)
}

pub(crate) fn transport_error(error: contextdb_core::Error, ceiling: u64) -> super::LocalTransportError {
    match error {
        contextdb_core::Error::ReadFailure(failure) => super::LocalTransportError::Refusal(failure),
        contextdb_core::Error::ReadCancelled => super::owner_disconnected(),
        error if crate::read_image_memory::is_memory_refusal(&error) =>
            super::LocalTransportError::Payload(super::PayloadViolation::MemoryCeilingExceeded {
                ceiling, required: None,
            }),
        _ => super::invalid_channel_data(),
    }
}
