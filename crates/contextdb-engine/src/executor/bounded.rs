#![allow(clippy::needless_range_loop)] // Existing compatibility and test seam shape.
use super::*;
use crate::database::CursorIdleWindow;
use crate::plugin::QueryOutcome;
use crate::read_contract::CanonicalQueryResult;
use bincode::enc::write::Writer;
use contextdb_core::read_contract::{
    CursorExpiryKind, OwnerLimitExceededDetail, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, RequiredBytesSetting, StatementRemedy,
};
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_planner::{ProjectColumn, SortKey};
use contextdb_relational::mem::{BoundedOrderedRowCursor, BoundedPhysicalCursor};
use serde::Serialize;
use std::cell::Cell;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

#[cfg(feature = "test-seams")]
mod vector_search_receipt;

mod grouped_count;

// One public continuation fetch may replay several internal kernel pages.
// Their work and deadlines belong to the same caller request.
struct CursorReplayBudget {
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    started_ms: u64,
    created_ms: u64,
    last_used_ms: u64,
    work: Cell<u64>,
}

std::thread_local! {
    static CURSOR_REPLAY_BUDGET: std::cell::RefCell<Option<std::rc::Rc<CursorReplayBudget>>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn with_cursor_replay_budget<T>(
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    started_ms: u64,
    created_ms: u64,
    last_used_ms: u64,
    operation: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<std::rc::Rc<CursorReplayBudget>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURSOR_REPLAY_BUDGET.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let budget = std::rc::Rc::new(CursorReplayBudget {
        limits,
        clock,
        started_ms,
        created_ms,
        last_used_ms,
        work: Cell::new(0),
    });
    let _restore = Restore(CURSOR_REPLAY_BUDGET.with(|slot| slot.replace(Some(budget))));
    operation()
}

std::thread_local! {
    static CAPTURE_CURSOR_OPEN_CONTEXT: Cell<bool> = const { Cell::new(false) };
    static CURSOR_OPEN_CONTEXT: std::cell::RefCell<Option<Arc<RequestContext>>> = const { std::cell::RefCell::new(None) };
}

pub(crate) struct CursorOpenObservation(Arc<RequestContext>);
impl CursorOpenObservation {
    pub(crate) fn into_observation(
        self,
    ) -> std::result::Result<Option<CursorObservation>, BoundedExecutionError> {
        let (mut observation, overflow) = self.0.take_final_observation();
        if overflow {
            return Err(Error::Other("bounded rows-examined counter overflow".to_string()).into());
        }
        // A failed open published no page to its caller, including when first
        // page preparation succeeded before registration later refused.
        if let Some(observation) = observation.as_mut() {
            for group in &mut Arc::get_mut(&mut observation.trace)
                .expect("failed-open receipt is private until release")
                .groups
            {
                group.returned_rows = 0;
            }
        }
        Ok(observation)
    }
}

pub(crate) fn with_cursor_open_work<T>(
    operation: impl FnOnce() -> T,
) -> (
    T,
    std::result::Result<u64, BoundedExecutionError>,
    Option<CursorOpenObservation>,
) {
    struct Restore(Option<Arc<RequestContext>>, bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURSOR_OPEN_CONTEXT.with(|slot| *slot.borrow_mut() = self.0.take());
            CAPTURE_CURSOR_OPEN_CONTEXT.with(|slot| slot.set(self.1));
        }
    }
    let _restore = Restore(
        CURSOR_OPEN_CONTEXT.with(|slot| slot.replace(None)),
        CAPTURE_CURSOR_OPEN_CONTEXT.with(|slot| slot.replace(true)),
    );
    let result = operation();
    let examined = CURSOR_OPEN_CONTEXT.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(Ok(0), |context| context.cumulative_rows_examined())
    });
    let observation = CURSOR_OPEN_CONTEXT.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|context| CursorOpenObservation(Arc::clone(context)))
    });
    (result, examined, observation)
}

pub(crate) fn cursor_replay_work_available() -> bool {
    CURSOR_REPLAY_BUDGET.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_none_or(|budget| budget.work.get() < budget.limits.work)
    })
}

pub(crate) fn cursor_replay_checkpoint() -> std::result::Result<(), BoundedExecutionError> {
    charge_cursor_replay(0)
}

fn charge_cursor_replay(units: u64) -> std::result::Result<(), BoundedExecutionError> {
    let budget = CURSOR_REPLAY_BUDGET.with(|slot| slot.borrow().clone());
    let Some(budget) = budget else {
        return Ok(());
    };
    let now = budget.clock.now_ms();
    for (started, limit, kind) in [
        (
            budget.created_ms,
            budget.limits.cursor_lifetime_ms,
            CursorExpiryKind::Lifetime,
        ),
        (
            budget.last_used_ms,
            budget.limits.cursor_idle_ms,
            CursorExpiryKind::Idle,
        ),
    ] {
        if now.saturating_sub(started) > limit {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                kind,
            )));
        }
    }
    let elapsed = now.saturating_sub(budget.started_ms);
    if elapsed > budget.limits.active_ms || (units > 0 && elapsed == budget.limits.active_ms) {
        return Err(RequestContext::limit_failure(
            ReadFailureLimit::ActiveMs,
            budget.limits.active_ms,
        ));
    }
    if units > 0 {
        let work = budget
            .work
            .get()
            .checked_add(units)
            .filter(|work| *work <= budget.limits.work)
            .ok_or_else(|| {
                RequestContext::limit_failure(ReadFailureLimit::Work, budget.limits.work)
            })?;
        budget.work.set(work);
    }
    Ok(())
}

struct RequestState {
    started_ms: u64,
    cancellation: OwnerReadCancellation,
    telemetry: BoundedExecutionTelemetry,
    touches: BTreeMap<BoundedSourceTouch, u64>,
    completed_rows_examined: u64,
    groups: Vec<GroupObserver>,
    group_observer_bytes: usize,
    // Unique until terminal release: changes are admitted before group work.
    final_observation: Option<CursorObservation>,
    group_publication_generation: u64,
    /// Next private descriptor ordinal; retained inline with the request.
    next_group_descriptor: u64,
    memory: ReadCredit,
    membership_bytes: usize,
    // What the answer being assembled holds right now, and how much work had
    // been done when its progress was last reported. These describe the
    // in-flight answer a caller is waiting on, which is exactly what the
    // finished telemetry cannot say: `encoded_bytes` is only true once the
    // read has stopped.
    rows_so_far: u64,
    bytes_so_far: u64,
    reported_work: u64,
    reported_rows: u64,
    // Sources release through infallible callbacks so a charge still travels
    // its release path while an error unwinds. A release can only fail by
    // underflowing the live charge, so that fault is parked here and raised
    // by the next boundary check instead of being dropped.
    release_fault: Option<BoundedExecutionError>,
}

/// Whether the read this context governs can hand an operator back its own
/// unfinished state and be asked again.
#[derive(Clone, Copy)]
enum ReadResumption {
    /// A one-shot read. The answer is assembled once, so an operator that
    /// cannot finish inside the ceilings the caller declared is refused with
    /// the typed refusal that names the ceiling.
    OneShot,
    /// A cursor. Each fetch is its own work budget over the one snapshot the
    /// cursor was opened on, so an operator that has spent this fetch's
    /// budget keeps what it has and finishes on a later fetch.
    AcrossFetches,
}

/// Identity assigned to the window descriptor, independent of any SQL column.
#[derive(Clone, Copy, PartialEq, Eq)]
struct GroupDescriptorId(u64);

/// A stable slot in this statement's observation owner.
#[derive(Clone, Copy, PartialEq, Eq)]
struct GroupBinding(usize);

struct GroupObserver {
    descriptor: GroupDescriptorId,
    key: Vec<Value>,
    disclosure: crate::database::GroupDisclosure,
    unpublished_rows: u64,
}

/// Actual group returns from one kernel page, never a count inferred from rows.
/// Copies of an observation share this receipt, making withdrawal idempotent.
pub(crate) struct GroupPageReturns {
    generation: u64,
    rows: Box<[Option<GroupBinding>]>,
    _charge: Arc<SourcePageCharge>,
}

/// Clears unpublished return attribution on every error/cancellation exit.
struct GroupPublicationAttempt<'a>(&'a RequestContext);
impl Drop for GroupPublicationAttempt<'_> {
    fn drop(&mut self) {
        self.0.discard_group_returns();
    }
}

#[derive(Debug)]
pub(super) struct TraceMemoryOwner(pub(super) Arc<dyn ReadMemoryBudget>);
impl contextdb_vector::MemoryBudget for TraceMemoryOwner {
    fn try_allocate_for(&self, bytes: usize, _: &str, _: &str, _: &str) -> Result<()> {
        self.0.try_reserve(
            bytes,
            "vector_search",
            "trace",
            "Reduce query scope or raise the reader memory limit.",
        )
    }
    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}
struct PrepaidTraceCredit {
    owner: Arc<dyn contextdb_vector::MemoryBudget>,
    credit: ReadCredit,
}
impl contextdb_vector::PrepaidVectorCopyCharge for PrepaidTraceCredit {
    fn accountant(&self) -> Arc<dyn contextdb_vector::MemoryBudget> {
        Arc::clone(&self.owner)
    }
    fn retained_bytes(&self) -> usize {
        self.credit.bytes()
    }
    fn transfer(self: Box<Self>) {
        self.credit.transfer();
    }
}

struct RequestContext {
    limits: ReadLimits,
    vector_copies: Mutex<contextdb_vector::VectorCopyAdmission>,
    /// The first validated vector capture fixes intent and roots across fetches.
    vector_read_capture: Mutex<Option<Arc<crate::persistence::vector_membership_source::VectorReadOperation>>>,
    /// How this read ends when an operator runs out of budget mid-answer:
    /// refused, or paused for the next fetch.
    resumption: ReadResumption,
    /// The transaction this read is running inside, decided ONCE where the
    /// read is entered. A caller holding an explicit transaction handle is not
    /// the session's transaction, so asking the store for "the active one"
    /// answers `None` and the read silently drops to committed state.
    transaction: Option<TxId>,
    clock: Arc<dyn DeadlineClock>,
    state: Mutex<RequestState>,
    /// Storage pages this read was admitted to hold that are still alive
    /// outside the store's page cache. Each such page carries its own charge
    /// (`SourcePageCharge`) and gives these bytes back when its last holder
    /// drops it, so the read's memory ceiling counts them for exactly as long
    /// as they exist.
    live_source_pages: Arc<std::sync::atomic::AtomicUsize>,
    /// The caller-supplied budget retained by this executing request.
    reader_budget: Arc<dyn ReadMemoryBudget>,
    #[cfg(feature = "test-seams")]
    probe: Mutex<Option<Arc<dyn BoundedExecutionProbe>>>,
}

/// The charge one storage page carries for as long as it lives outside the
/// store's page cache. Dropping it -- which happens when the last holder of
/// the page drops it -- returns the bytes to the declared owner and to the read
/// that admitted the page. The vector load transfer callback carries one for
/// its own allocation, which can likewise outlive the call that admitted it.
pub(crate) struct SourcePageCharge {
    _credit: ReadCredit,
    live: Arc<std::sync::atomic::AtomicUsize>,
    bytes: usize,
}

impl Drop for SourcePageCharge {
    fn drop(&mut self) {
        self.live
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Active preparation for a committed reader uses the bounded kernel's
/// existing cancellation and deadline checks and its declared request owner.
#[derive(Clone)]
pub(crate) struct ReadOpenContext(Arc<RequestContext>);

/// An operation retained by this request cannot hold a strong request back-reference.
/// Upgrading uses the same cancellation, deadline and aggregate work state.
pub(crate) struct VectorReadCheckpoint(std::sync::Weak<RequestContext>);

impl VectorReadCheckpoint {
    pub(crate) fn checkpoint(&self) -> contextdb_core::Result<()> {
        self.0.upgrade().ok_or(Error::ReadCancelled)?
            .checkpoint().map_err(ReadOpenContext::engine_error)
    }

    pub(crate) fn belongs_to(&self, context: &ReadOpenContext) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &Arc::downgrade(&context.0))
    }
}

impl ReadOpenContext {
    // Wrap the actual executing request. Constructing a second RequestContext
    // would reset its cancellation, active deadline and aggregate work.
    fn from_request(context: &Arc<RequestContext>) -> Self {
        Self(Arc::clone(context))
    }

    pub(crate) fn new(
        read_memory: Arc<dyn ReadMemoryBudget>,
        limits: ReadLimits,
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
    ) -> contextdb_core::Result<Self> {
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_commit_directory_open_funding(&read_memory);
        RequestContext::new(
            read_memory,
            limits,
            ReadResumption::OneShot,
            clock,
            cancellation,
            None,
            #[cfg(feature = "test-seams")]
            None,
        )
        .map(Self)
        .map_err(Self::engine_error)
    }

    pub(crate) fn checkpoint(&self) -> contextdb_core::Result<()> {
        self.0.checkpoint().map_err(Self::engine_error)
    }

    pub(crate) fn read_memory(&self) -> Arc<dyn ReadMemoryBudget> {
        self.0.reader_budget.clone()
    }

    pub(crate) fn vector_checkpoint(&self) -> VectorReadCheckpoint {
        VectorReadCheckpoint(Arc::downgrade(&self.0))
    }

    pub(crate) fn vector_read_capture(
        &self,
    ) -> Option<Arc<crate::persistence::vector_membership_source::VectorReadOperation>> {
        self.0.vector_read_capture.lock().unwrap_or_else(|poison| poison.into_inner()).clone()
    }

    pub(crate) fn retain_vector_read_capture(
        &self,
        capture: Arc<crate::persistence::vector_membership_source::VectorReadOperation>,
    ) -> contextdb_core::Result<()> {
        let mut slot = self.0.vector_read_capture.lock().unwrap_or_else(|poison| poison.into_inner());
        if slot.as_ref().is_some_and(|prior| !Arc::ptr_eq(prior, &capture)) {
            return Err(Error::StoreCorrupted { path: "vector_partition_memberships".into(),
                reason: "request attempted to replace its immutable vector capture".into() });
        }
        *slot = Some(capture);
        Ok(())
    }

    fn engine_error(error: BoundedExecutionError) -> Error {
        match error {
            BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
            BoundedExecutionError::Cancelled => Error::ReadCancelled,
            BoundedExecutionError::Engine(error) => error,
            BoundedExecutionError::Unimplemented => {
                Error::Other("bounded read preparation is not implemented".into())
            }
        }
    }
}

impl RequestContext {
    fn new(
        reader_budget: Arc<dyn ReadMemoryBudget>,
        limits: ReadLimits,
        resumption: ReadResumption,
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
        transaction: Option<TxId>,
        #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    ) -> std::result::Result<Arc<Self>, BoundedExecutionError> {
        Self::check_effective_limits(&limits)?;
        let started_ms = clock.now_ms();
        let context = Arc::new_cyclic(|context| Self {
            vector_read_capture: Mutex::new(None),
            vector_copies: Mutex::new(crate::vector_read_memory::admission(Arc::new(
                WeakSourceDecodeBudget(context.clone()),
            ))),
            limits,
            resumption,
            transaction,
            clock,
            state: Mutex::new(RequestState {
                started_ms,
                cancellation,
                telemetry: BoundedExecutionTelemetry::default(),
                touches: BTreeMap::new(),
                completed_rows_examined: 0,
                groups: Vec::new(),
                group_observer_bytes: 0,
                final_observation: None,
                group_publication_generation: 0,
                next_group_descriptor: 0,
                memory: ReadCredit::new(Arc::clone(&reader_budget)),
                membership_bytes: 0,
                rows_so_far: 0,
                bytes_so_far: 0,
                reported_work: 0,
                reported_rows: 0,
                release_fault: None,
            }),
            live_source_pages: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            reader_budget,
            #[cfg(feature = "test-seams")]
            probe: Mutex::new(probe),
        });
        context.replace_final_trace(0, |_| {})?;
        Ok(context)
    }

    /// Keep a private, already admitted terminal carrier. It is never shared
    /// while work can still change its counters; page snapshots remain immutable.
    fn replace_final_trace(
        &self,
        payload_bytes: usize,
        update: impl FnOnce(&mut QueryTrace),
    ) -> std::result::Result<(), BoundedExecutionError> {
        if !matches!(self.resumption, ReadResumption::AcrossFetches) {
            return Ok(());
        }
        let bytes = checked_size_add(
            checked_size_mul(
                checked_size_add(
                    std::mem::size_of::<QueryTrace>(),
                    payload_bytes,
                    "terminal trace",
                )?,
                2,
                "terminal trace copies",
            )?,
            std::mem::size_of::<SourcePageCharge>() + 4 * std::mem::size_of::<usize>(),
            "terminal trace owners",
        )?;
        self.reserve(BoundedWorkSource::TableScan, bytes)?;
        let credit = self.state().memory.try_split_off(bytes)?;
        self.live_source_pages
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        let charge = Arc::new(SourcePageCharge {
            _credit: credit,
            live: Arc::clone(&self.live_source_pages),
            bytes,
        });
        let mut state = self.state();
        let mut trace = state
            .final_observation
            .as_ref()
            .map_or_else(QueryTrace::default, |receipt| (*receipt.trace).clone());
        update(&mut trace);
        state.final_observation = Some(CursorObservation {
            trace: Arc::new(trace),
            rows_examined: 0,
            charge: Some(charge),
            group_page_returns: None,
        });
        Ok(())
    }

    fn prepare_final_observation_work(&self) -> std::result::Result<(), BoundedExecutionError> {
        let payload = {
            let state = self.state();
            let Some(receipt) = state.final_observation.as_ref() else {
                return Ok(());
            };
            if receipt
                .charge
                .as_ref()
                .is_none_or(|charge| Arc::strong_count(charge) == 1)
            {
                return Ok(());
            }
            query_trace_capacity_bytes(&receipt.trace)?
        };
        self.replace_final_trace(payload, |_| {})
    }

    fn final_observation_snapshot(
        &self,
    ) -> std::result::Result<CursorObservation, BoundedExecutionError> {
        let examined = self.cumulative_rows_examined()?;
        let mut state = self.state();
        let RequestState {
            final_observation,
            groups,
            ..
        } = &mut *state;
        let receipt = final_observation
            .as_mut()
            .expect("admitted terminal carrier");
        let trace =
            Arc::get_mut(&mut receipt.trace).expect("terminal receipt is private until release");
        for (disclosure, group) in trace.groups.iter_mut().zip(groups.iter()) {
            disclosure.examined_entries = group.disclosure.examined_entries;
            disclosure.returned_rows = group.disclosure.returned_rows;
            disclosure.route = group.disclosure.route;
        }
        // The carrier admission funds this one immutable outward trace copy.
        Ok(CursorObservation {
            trace: Arc::new(trace.clone()),
            rows_examined: examined,
            charge: receipt.charge.clone(),
            group_page_returns: None,
        })
    }

    fn bind_final_trace(
        &self,
        source: &QueryTrace,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let payload = checked_size_add(
            query_trace_capacity_bytes(source)?,
            self.group_snapshot_bytes()?,
            "terminal trace",
        )?;
        self.replace_final_trace(payload, |trace| {
            let groups = std::mem::take(&mut trace.groups);
            *trace = source.clone();
            trace.groups = groups;
        })
    }

    fn bind_final_vector(
        &self,
        vector: &VectorSearchDisclosure,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let current = {
            let state = self.state();
            let Some(receipt) = state.final_observation.as_ref() else {
                return Ok(());
            };
            query_trace_capacity_bytes(&receipt.trace)?
        };
        let payload = checked_size_add(
            current,
            vector_search_disclosure_capacity_bytes(vector)?,
            "terminal vector trace",
        )?;
        self.replace_final_trace(payload, |trace| trace.vector_search = Some(vector.clone()))
    }

    /// Move the unique admitted carrier; only inline counters change here.
    /// No allocation or memory admission is allowed at a terminal boundary.
    fn take_final_observation(&self) -> (Option<CursorObservation>, bool) {
        let mut state = self.state();
        let examined = Self::examined_touches(&state).and_then(|touches| {
            state
                .completed_rows_examined
                .checked_add(touches)
                .ok_or_else(|| {
                    Error::Other("bounded rows-examined counter overflow".to_string()).into()
                })
        });
        let mut receipt = state.final_observation.take();
        let overflow = examined.is_err();
        if let Some(receipt) = receipt.as_mut() {
            if let Ok(examined) = examined {
                receipt.rows_examined = examined;
            }
            let trace = Arc::get_mut(&mut receipt.trace)
                .expect("terminal receipt is private until release");
            for (disclosure, group) in trace.groups.iter_mut().zip(&state.groups) {
                disclosure.examined_entries = group.disclosure.examined_entries;
                disclosure.returned_rows = group.disclosure.returned_rows;
                disclosure.route = group.disclosure.route;
            }
        }
        (receipt, overflow)
    }

    /// Allocate one identity when a window descriptor is created in this request.
    fn new_group_descriptor(
        &self,
    ) -> std::result::Result<GroupDescriptorId, BoundedExecutionError> {
        let mut state = self.state();
        let descriptor = GroupDescriptorId(state.next_group_descriptor);
        state.next_group_descriptor = state
            .next_group_descriptor
            .checked_add(1)
            .ok_or_else(|| Error::PlanError("group descriptor ordinal overflow".to_string()))?;
        Ok(descriptor)
    }

    fn bind_group(
        &self,
        descriptor: GroupDescriptorId,
        key: &[Value],
        partition_key: &str,
        route: crate::database::GroupRoute,
    ) -> std::result::Result<GroupBinding, BoundedExecutionError> {
        {
            let state = self.state();
            if let Some(slot) = state
                .groups
                .iter()
                .position(|group| group.descriptor == descriptor && group.key.as_slice() == key)
            {
                return Ok(GroupBinding(slot));
            }
        }
        let (old_capacity, new_len) =
            {
                let state = self.state();
                (
                    state.groups.capacity(),
                    state.groups.len().checked_add(1).ok_or_else(|| {
                        Error::Other("group observation slot overflow".to_string())
                    })?,
                )
            };
        let container = checked_size_mul(
            new_len.saturating_sub(old_capacity),
            std::mem::size_of::<GroupObserver>(),
            "group observation slots",
        )?;
        let payload = checked_size_add(
            planned_values_clone_bytes(key)?,
            partition_key.len(),
            "group observation identity",
        )?;
        let planned = checked_size_add(container, payload, "group observation")?;
        self.reserve(BoundedWorkSource::TableScan, planned)?;
        let group = GroupObserver {
            descriptor,
            key: key.to_vec(),
            disclosure: crate::database::GroupDisclosure {
                ordinal: u64::try_from(new_len - 1)
                    .map_err(|_| Error::Other("group ordinal overflow".to_string()))?,
                partition_key: partition_key.to_owned(),
                examined_entries: 0,
                returned_rows: 0,
                route,
            },
            unpublished_rows: 0,
        };
        let actual_payload = checked_size_add(
            values_capacity_allocation_bytes(&group.key)?,
            group.disclosure.partition_key.capacity(),
            "group observation identity",
        )?;
        let (slot, actual_container) = {
            let mut state = self.state();
            let additional = new_len - state.groups.len();
            if state.groups.try_reserve_exact(additional).is_err() {
                drop(state);
                self.release(planned)?;
                return Err(Error::Other("group observation allocation failed".to_string()).into());
            }
            drop(state);
            if matches!(self.resumption, ReadResumption::AcrossFetches) {
                let current = {
                    let state = self.state();
                    query_trace_capacity_bytes(
                        &state
                            .final_observation
                            .as_ref()
                            .expect("admitted terminal carrier")
                            .trace,
                    )?
                };
                let additional = checked_size_add(
                    std::mem::size_of::<crate::database::GroupDisclosure>(),
                    group.disclosure.partition_key.len(),
                    "terminal group binding",
                )?;
                self.replace_final_trace(
                    checked_size_add(current, additional, "terminal groups")?,
                    |trace| {
                        let mut groups =
                            Box::<[crate::database::GroupDisclosure]>::new_uninit_slice(new_len);
                        for (target, prior) in groups.iter_mut().zip(trace.groups.iter()) {
                            target.write(prior.clone());
                        }
                        groups[new_len - 1].write(group.disclosure.clone());
                        // SAFETY: prior bindings and the new binding initialize every slot.
                        trace.groups = unsafe { groups.assume_init() };
                    },
                )?;
            }
            let mut state = self.state();
            let slot = state.groups.len();
            let actual_container = checked_size_mul(
                state.groups.capacity() - old_capacity,
                std::mem::size_of::<GroupObserver>(),
                "group observation slots",
            )?;
            state.groups.push(group);
            (slot, actual_container)
        };
        let actual = checked_size_add(actual_container, actual_payload, "group observation")?;
        reconcile_new_reservation(self, BoundedWorkSource::TableScan, planned, actual)?;
        let mut state = self.state();
        state.group_observer_bytes = checked_size_add(
            state.group_observer_bytes,
            actual,
            "group observation owner",
        )?;
        Ok(GroupBinding(slot))
    }

    /// Attribute real kernel touch deltas before propagating any operation error.
    fn with_group_work<T>(
        &self,
        binding: GroupBinding,
        operation: impl FnOnce() -> std::result::Result<T, BoundedExecutionError>,
    ) -> std::result::Result<T, BoundedExecutionError> {
        let before = self.cumulative_rows_examined()?;
        let outcome = operation();
        let delta = self
            .cumulative_rows_examined()?
            .checked_sub(before)
            .ok_or_else(|| Error::Other("group observation work counter underflow".to_string()))?;
        let mut state = self.state();
        let group = state
            .groups
            .get_mut(binding.0)
            .ok_or_else(|| Error::Other("unknown group observation binding".to_string()))?;
        group.disclosure.examined_entries = group
            .disclosure
            .examined_entries
            .checked_add(delta)
            .ok_or_else(|| Error::Other("group observation work counter overflow".to_string()))?;
        outcome
    }

    // One admitted source touch is consumed by one borrowed physical input.
    // Failed or unidentified inputs keep their work in the global telemetry.
    fn observe_physical_group(
        self: &Arc<Self>,
        source: &SourceInputGroup,
        scan_columns: &[String],
        row: &VersionedRow,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let null = Value::Null;
        let row_id =
            Value::Int64(i64::try_from(row.row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?);
        let value_at = |position: &PhysicalPartitionColumn| -> std::result::Result<&Value, BoundedExecutionError> {
            match position {
                PhysicalPartitionColumn::RowId => Ok(&row_id),
                PhysicalPartitionColumn::Schema(position) => {
                    let name = scan_columns.get(*position).ok_or_else(|| {
                        Error::PlanError("physical group column binding is missing".to_string())
                    })?;
                    Ok(row.values.get(name).unwrap_or(&null))
                }
            }
        };
        self.observe_source_group_values(source, value_at)
    }

    fn observe_index_group(
        self: &Arc<Self>,
        source: &SourceInputGroup,
        scan_columns: &[String],
        index_columns: &[(String, contextdb_core::SortDirection)],
        key: &IndexKey,
        row_id: RowId,
    ) -> std::result::Result<(), BoundedExecutionError> {
        // A posting only identifies partitions present in its decoded key.
        // Missing partition columns retain their actual work globally.
        let index_position = |position: &PhysicalPartitionColumn| -> Option<usize> {
            match position {
                PhysicalPartitionColumn::RowId => None,
                PhysicalPartitionColumn::Schema(position) => scan_columns
                    .get(*position)
                    .and_then(|name| index_columns.iter().position(|(column, _)| column == name)),
            }
        };
        if source.positions.iter().any(|position| {
            matches!(position, PhysicalPartitionColumn::Schema(_))
                && index_position(position)
                    .and_then(|position| key.get(position))
                    .is_none()
        }) {
            return Ok(());
        }
        let row_id =
            Value::Int64(i64::try_from(row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?);
        self.observe_source_group_values(source, |position| match position {
            PhysicalPartitionColumn::RowId => Ok(&row_id),
            PhysicalPartitionColumn::Schema(_) => {
                let value = index_position(position)
                    .and_then(|position| key.get(position))
                    .ok_or_else(|| {
                        Error::Other("source index partition binding is missing".to_string())
                    })?;
                Ok(match value {
                    DirectedValue::Asc(value) => &value.0,
                    DirectedValue::Desc(value) => &value.0,
                })
            }
        })
    }

    fn observe_source_group_values<'a>(
        self: &Arc<Self>,
        source: &SourceInputGroup,
        value_at: impl Fn(
            &PhysicalPartitionColumn,
        ) -> std::result::Result<&'a Value, BoundedExecutionError>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let planned = source.positions.iter().try_fold(
            checked_size_mul(
                source.positions.len(),
                std::mem::size_of::<Value>(),
                "source partition",
            )?,
            |bytes, position| {
                checked_size_add(
                    bytes,
                    planned_value_clone_bytes(value_at(position)?)?,
                    "source partition",
                )
            },
        )?;
        self.reserve(BoundedWorkSource::TableScan, planned)?;
        let mut scratch = ReservedBytes::new(self, planned);
        let mut key = Vec::new();
        key.try_reserve_exact(source.positions.len())
            .map_err(|_| Error::Other("source partition allocation failed".to_string()))?;
        for position in &source.positions {
            key.push(value_at(position)?.clone());
        }
        let actual = values_capacity_allocation_bytes(&key)?;
        scratch.bytes = 0;
        reconcile_new_reservation(self, BoundedWorkSource::TableScan, planned, actual)?;
        scratch.bytes = actual;
        let binding = self.bind_group(
            source.descriptor,
            &key,
            "<redacted>",
            crate::database::GroupRoute::Sort,
        )?;
        let mut state = self.state();
        let group = state
            .groups
            .get_mut(binding.0)
            .ok_or_else(|| Error::Other("unknown source group binding".to_string()))?;
        group.disclosure.examined_entries = group
            .disclosure
            .examined_entries
            .checked_add(1)
            .ok_or_else(|| Error::Other("source group work overflow".to_string()))?;
        Ok(())
    }

    fn stage_group_return(
        &self,
        binding: GroupBinding,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut state = self.state();
        let group = state
            .groups
            .get_mut(binding.0)
            .ok_or_else(|| Error::Other("unknown group observation binding".to_string()))?;
        group.unpublished_rows = group
            .unpublished_rows
            .checked_add(1)
            .ok_or_else(|| Error::Other("group observation return counter overflow".to_string()))?;
        Ok(())
    }

    fn unstage_group_return(
        &self,
        binding: GroupBinding,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut state = self.state();
        let group = state
            .groups
            .get_mut(binding.0)
            .ok_or_else(|| Error::Other("unknown group observation binding".to_string()))?;
        group.unpublished_rows = group.unpublished_rows.checked_sub(1).ok_or_else(|| {
            Error::Other("group observation return counter underflow".to_string())
        })?;
        Ok(())
    }

    fn prepare_group_page_returns(
        &self,
        rows: &[(usize, Option<GroupBinding>)],
    ) -> std::result::Result<Option<Arc<GroupPageReturns>>, BoundedExecutionError> {
        let state = self.state();
        if state.groups.is_empty() {
            if rows.iter().any(|(_, binding)| binding.is_some()) {
                return Err(Error::Other("page binding has no group observer".to_string()).into());
            }
            return Ok(None);
        }
        let generation = state
            .group_publication_generation
            .checked_add(1)
            .ok_or_else(|| Error::Other("group publication generation overflow".to_string()))?;
        for (_, binding) in rows {
            if binding.is_some_and(|binding| binding.0 >= state.groups.len()) {
                return Err(
                    Error::Other("page contains an unknown group binding".to_string()).into(),
                );
            }
        }
        for (index, group) in state.groups.iter().enumerate() {
            let actual = rows
                .iter()
                .filter(|(_, binding)| *binding == Some(GroupBinding(index)))
                .count();
            let actual = u64::try_from(actual)
                .map_err(|_| Error::Other("group page row count exceeds u64".to_string()))?;
            if actual != group.unpublished_rows {
                return Err(Error::Other(
                    "page bindings and staged group returns diverged".to_string(),
                )
                .into());
            }
        }
        drop(state);
        let len = rows.len();
        let bytes = checked_size_add(
            checked_size_mul(
                len,
                std::mem::size_of::<Option<GroupBinding>>(),
                "group page bindings",
            )?,
            std::mem::size_of::<GroupPageReturns>()
                + std::mem::size_of::<SourcePageCharge>()
                + 4 * std::mem::size_of::<usize>(),
            "group page return owners",
        )?;
        self.reserve(BoundedWorkSource::TableScan, bytes)?;
        let credit = self.state().memory.try_split_off(bytes)?;
        self.live_source_pages
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        let charge = Arc::new(SourcePageCharge {
            _credit: credit,
            live: Arc::clone(&self.live_source_pages),
            bytes,
        });
        let mut storage = Box::<[Option<GroupBinding>]>::new_uninit_slice(len);
        for (slot, (_, binding)) in storage.iter_mut().zip(rows) {
            slot.write(*binding);
        }
        // SAFETY: every exact slice slot was initialized from its actual page row.
        let rows = unsafe { storage.assume_init() };
        Ok(Some(Arc::new(GroupPageReturns {
            generation,
            rows,
            _charge: charge,
        })))
    }

    fn publish_group_returns(&self) -> std::result::Result<(), BoundedExecutionError> {
        let mut state = self.state();
        let generation = state
            .group_publication_generation
            .checked_add(1)
            .ok_or_else(|| Error::Other("group publication generation overflow".to_string()))?;
        // Validate every counter before changing any published counter.
        for group in &state.groups {
            group
                .disclosure
                .returned_rows
                .checked_add(group.unpublished_rows)
                .ok_or_else(|| {
                    Error::Other("group observation return counter overflow".to_string())
                })?;
        }
        for group in &mut state.groups {
            group.disclosure.returned_rows += group.unpublished_rows;
            group.unpublished_rows = 0;
        }
        state.group_publication_generation = generation;
        Ok(())
    }

    fn discard_group_returns(&self) {
        for group in &mut self.state().groups {
            group.unpublished_rows = 0;
        }
    }

    fn groups_match(
        &self,
        prior: &[crate::database::GroupDisclosure],
        include_unpublished: bool,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        let state = self.state();
        if state.groups.len() != prior.len() {
            return Ok(false);
        }
        for (group, prior) in state.groups.iter().zip(prior) {
            let returned = group
                .disclosure
                .returned_rows
                .checked_add(if include_unpublished {
                    group.unpublished_rows
                } else {
                    0
                })
                .ok_or_else(|| {
                    Error::Other("group observation return counter overflow".to_string())
                })?;
            if group.disclosure.partition_key != prior.partition_key
                || group.disclosure.route != prior.route
                || group.disclosure.examined_entries != prior.examined_entries
                || returned != prior.returned_rows
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn group_snapshot_bytes(&self) -> std::result::Result<usize, BoundedExecutionError> {
        let state = self.state();
        let mut bytes = checked_size_mul(
            state.groups.len(),
            std::mem::size_of::<crate::database::GroupDisclosure>(),
            "group trace slots",
        )?;
        for group in &state.groups {
            bytes = checked_size_add(
                bytes,
                group.disclosure.partition_key.len(),
                "group trace keys",
            )?;
        }
        Ok(bytes)
    }

    // The caller reserves the exact slice and key payloads before allocation.
    fn group_snapshot(
        &self,
        include_unpublished: bool,
    ) -> std::result::Result<Box<[crate::database::GroupDisclosure]>, BoundedExecutionError> {
        let state = self.state();
        if state.groups.is_empty() {
            return Ok(Box::default());
        }
        let mut storage =
            Box::<[crate::database::GroupDisclosure]>::new_uninit_slice(state.groups.len());
        // Initialize every slot with an allocation-free placeholder first, so
        // an unwind while cloning a key drops all previously cloned strings.
        for slot in &mut storage {
            slot.write(crate::database::GroupDisclosure {
                partition_key: String::new(),
                ordinal: 0,
                examined_entries: 0,
                returned_rows: 0,
                route: crate::database::GroupRoute::Sort,
            });
        }
        // SAFETY: every element above was initialized exactly once.
        let mut groups = unsafe { storage.assume_init() };
        for (disclosure, group) in groups.iter_mut().zip(&state.groups) {
            *disclosure = group.disclosure.clone();
            if include_unpublished {
                disclosure.returned_rows = disclosure
                    .returned_rows
                    .checked_add(group.unpublished_rows)
                    .ok_or_else(|| {
                        Error::Other("group observation return counter overflow".to_string())
                    })?;
            }
        }
        Ok(groups)
    }

    fn refresh_group_trace(
        &self,
        trace: &mut QueryTrace,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.groups_match(&trace.groups, true)? {
            return Ok(());
        }
        let planned = self.group_snapshot_bytes()?;
        self.reserve(BoundedWorkSource::TableScan, planned)?;
        let groups = self.group_snapshot(true)?;
        let actual = group_disclosure_capacity_bytes(&groups)?;
        reconcile_new_reservation(self, BoundedWorkSource::TableScan, planned, actual)?;
        let old = std::mem::replace(&mut trace.groups, groups);
        let old_bytes = group_disclosure_capacity_bytes(&old)?;
        drop(old);
        self.release(old_bytes)?;
        Ok(())
    }

    /// Check the limits this read will actually enforce.
    ///
    /// The kernel is handed *effective* limits — a set already narrowed field
    /// by field where the request was admitted. `ReadLimits::validate` also
    /// asserts relations between fields (a cursor page must fit inside the
    /// whole result, an idle window inside a lifetime); those describe a
    /// coherent limit *declaration*, and re-asserting them here refuses a read
    /// that merely asked for fewer result rows than the standing page ceiling
    /// — a stricter read, not an invalid one, and one that opens no cursor at
    /// all on the ordinary path. What the kernel needs from a limit is that it
    /// can admit something: a zero ceiling can never produce a row, a byte, or
    /// a unit of work.
    ///
    /// A limit the kernel cannot honour is reported as the same typed refusal
    /// every other ceiling produces, so a caller reads one vocabulary rather
    /// than an opaque engine string. The cursor-only ceilings carry their own
    /// typed refusals where the cursor path binds them (`default_page_rows`
    /// for a page that can hold no row, the expiry checks for a lifetime that
    /// has already elapsed).
    fn check_effective_limits(
        limits: &ReadLimits,
    ) -> std::result::Result<(), BoundedExecutionError> {
        for (value, limit) in [
            (limits.result_rows, ReadFailureLimit::ResultRows),
            (limits.result_bytes, ReadFailureLimit::ResultBytes),
            (limits.work, ReadFailureLimit::Work),
            (limits.active_ms, ReadFailureLimit::ActiveMs),
            (limits.memory, ReadFailureLimit::Memory),
            (limits.cursor_page_bytes, ReadFailureLimit::CursorPageBytes),
        ] {
            if value == 0 {
                return Err(Self::limit_failure(limit, value));
            }
        }
        Ok(())
    }

    fn with_vector_copies<T>(
        &self,
        operation: impl FnOnce(&mut contextdb_vector::VectorCopyAdmission) -> T,
    ) -> T {
        let mut owner = self
            .vector_copies
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        operation(&mut owner)
    }

    fn state(&self) -> MutexGuard<'_, RequestState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_fetch(
        &self,
        cancellation: OwnerReadCancellation,
        #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut state = self.state();
        let held = u64::try_from(state.memory.bytes())
            .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
        state.started_ms = self.clock.now_ms();
        state.cancellation = cancellation;
        state.telemetry = BoundedExecutionTelemetry {
            peak_temporary_bytes: held,
            ..BoundedExecutionTelemetry::default()
        };
        state.completed_rows_examined = state
            .completed_rows_examined
            .checked_add(Self::examined_touches(&state)?)
            .ok_or_else(|| Error::Other("bounded rows-examined counter overflow".to_string()))?;
        state.touches.clear();
        // A page is its own read as far as a waiting caller is concerned: it
        // starts with no rows, no bytes, and nothing reported.
        state.rows_so_far = 0;
        state.bytes_so_far = 0;
        state.reported_work = 0;
        state.reported_rows = 0;
        #[cfg(feature = "test-seams")]
        {
            let mut current = match self.probe.lock() {
                Ok(current) => current,
                Err(poisoned) => poisoned.into_inner(),
            };
            *current = probe;
        }
        Ok(())
    }

    #[cfg(feature = "test-seams")]
    fn probe(&self) -> Option<Arc<dyn BoundedExecutionProbe>> {
        match self.probe.lock() {
            Ok(probe) => probe.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Record what the answer being assembled holds right now.
    ///
    /// The caller waiting on this read wants to know how much of its answer
    /// exists, and the only place that is known is where a row has just been
    /// added and the answer re-measured.
    fn note_result_shape(&self, rows: usize, bytes: u64) {
        {
            let mut state = self.state();
            state.rows_so_far = u64::try_from(rows).unwrap_or(u64::MAX);
            state.bytes_so_far = bytes;
        }
        // Assembling the answer is the half of a read that examines nothing,
        // so it has to speak for itself: a read that sorted first arrives
        // here with every item already examined and not one row of the answer
        // reported.
        self.report_progress();
    }

    /// Tell whoever asked what this read has done so far, if it has done
    /// enough since the last report to have something new to say.
    ///
    /// The observer is called with no lock held: it is caller code, it may do
    /// anything, and it must not be able to deadlock the read that is
    /// reporting to it.
    fn report_progress(&self) {
        self.publish_progress(ProgressReport::WhileRunning);
    }

    /// Account for what this read did, now that it is finished.
    ///
    /// Exactly one total per completed read or published page, always. This is
    /// not a liveness report and does not become one: it goes to the
    /// accounting seam, which nothing on the local channel carries and whose
    /// default does nothing. The intervals stay exactly the promise they were
    /// -- a read that finishes inside one still tells a watching caller
    /// nothing -- while a caller adding up what reads did gets every read's
    /// number, including the read that ended on an interval boundary and the
    /// read that examined nothing.
    fn flush_final_progress(&self) {
        self.publish_progress(ProgressReport::AtCompletion);
    }

    fn publish_progress(&self, moment: ProgressReport) {
        let Some(observer) = crate::read_progress::observer_for_this_read() else {
            return;
        };
        let Some(progress) = self.progress_to_publish(moment) else {
            return;
        };
        // Liveness and accounting travel separately: a caller watching this
        // read run hears only what the intervals below admit, and the totals a
        // finished read owes go to the accounting seam whatever those
        // intervals happened to say.
        match moment {
            ProgressReport::WhileRunning => observer.progress(progress),
            ProgressReport::AtCompletion => observer.completed(progress),
        }
    }

    /// What to publish for this moment, or nothing when a LIVENESS report is
    /// not yet due.
    ///
    /// A completion total is always due. It is a statement of what one read
    /// cost, not news about a read in flight, so nothing about the liveness
    /// intervals decides whether it is made -- a read that ended exactly on an
    /// interval boundary, and a read that examined nothing at all, each owe
    /// their caller the same one total as any other. It neither reads nor
    /// moves the liveness bookkeeping for the same reason: a total is not a
    /// report the caller has now been told, and treating it as one would
    /// silence the next real report.
    fn progress_to_publish(
        &self,
        moment: ProgressReport,
    ) -> Option<crate::read_progress::ReadProgress> {
        let mut state = self.state();
        let work = state.telemetry.work_units;
        let rows = state.rows_so_far;
        if let ProgressReport::WhileRunning = moment {
            let due = work
                >= state
                    .reported_work
                    .saturating_add(crate::read_progress::WORK_REPORTING_INTERVAL)
                || rows
                    >= state
                        .reported_rows
                        .saturating_add(crate::read_progress::ROW_REPORTING_INTERVAL);
            if !due {
                return None;
            }
            state.reported_work = work;
            state.reported_rows = rows;
        }
        let active_ms = self.clock.now_ms().saturating_sub(state.started_ms);
        Some(crate::read_progress::ReadProgress {
            phase: crate::read_progress::ReadPhase::Executing,
            rows: state.rows_so_far,
            bytes: state.bytes_so_far,
            loaded_bytes: 0,
            total_bytes: None,
            work,
            active_ms,
        })
    }

    fn limit_failure(limit: ReadFailureLimit, value: u64) -> BoundedExecutionError {
        BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit,
                value,
                required: None,
                statement: None,
            },
        ))
    }

    /// The read's own declared memory ceiling refused this charge: answered in
    /// the read vocabulary (owner limit, memory), as the dev contract requires.
    /// A refusal of the database's standing budget is not this one; it keeps
    /// the store's own typed refusal from the store's credit.
    fn memory_limit_failure(
        &self,
        _held: usize,
        _requested: usize,
        _operation: &'static str,
    ) -> BoundedExecutionError {
        Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory)
    }

    /// Whatever must stop the request before it does more work: a parked
    /// release fault first, because it means the live charge no longer
    /// describes what the request holds, then cancellation.
    fn pending_failure(&self) -> Option<BoundedExecutionError> {
        let fault = {
            let mut state = self.state();
            state.release_fault.take()
        };
        if fault.is_some() {
            return fault;
        }
        self.cancellation()
    }

    /// Give bytes back from a path that cannot report an error. The first
    /// fault is kept; later ones would describe the same broken accounting.
    fn release_parked(&self, bytes: usize) {
        let Err(fault) = self.release(bytes) else {
            return;
        };
        let mut state = self.state();
        if state.release_fault.is_none() {
            state.release_fault = Some(fault);
        }
    }

    /// Move `bytes` of this request's load credit to `destination`, the owner
    /// that keeps a cold-loaded vector graph after this request ends. The
    /// destination admits first, with no lock of this request held, because
    /// admitting can reclaim vector caches; this request's credit is then split
    /// off and given back in one step, so the graph is never unfunded. The
    /// search that calls this holds `vector_copies`, so it is never touched
    /// here, and the move must hold past cancellation and the statement
    /// ceiling, so it never reserves.
    fn transfer_graph_credit(
        &self,
        destination: &crate::memory_accounting::MemoryAccountant,
        bytes: usize,
    ) -> contextdb_core::Result<()> {
        #[cfg(feature = "test-seams")]
        self.observe_graph_credit_transfer(
            GraphCreditTransferPhase::BeforeDestination,
            destination,
            bytes,
        );
        destination.transfer_vector_load_reservation(bytes, || {
            #[cfg(feature = "test-seams")]
            self.observe_graph_credit_transfer(
                GraphCreditTransferPhase::DestinationAdmitted,
                destination,
                bytes,
            );
            let split = {
                let mut state = self.state();
                state.memory.try_split_off(bytes)?
            };
            drop(split);
            #[cfg(feature = "test-seams")]
            self.observe_graph_credit_transfer(
                GraphCreditTransferPhase::SourceSplit,
                destination,
                bytes,
            );
            Ok(())
        })
    }

    /// Report where one graph credit move stands to this read's probe. It
    /// reads this request's credit under a brief `state` lock and the
    /// destination's usage, and calls the probe with no lock of this request
    /// held; it never touches `vector_copies`.
    #[cfg(feature = "test-seams")]
    fn observe_graph_credit_transfer(
        &self,
        phase: GraphCreditTransferPhase,
        destination: &crate::memory_accounting::MemoryAccountant,
        bytes: usize,
    ) {
        let Some(probe) = self.probe() else {
            return;
        };
        let request_credit = self.state().memory.bytes();
        probe.graph_credit_transfer(GraphCreditTransferCut {
            phase,
            bytes,
            request_credit,
            destination_used: destination.usage().used,
        });
    }

    /// The terminal answer for a caller that withdrew its read while the
    /// snapshot registration was parked behind an in-flight removal pass.
    fn withdrawn_failure(&self) -> BoundedExecutionError {
        self.pending_failure()
            .unwrap_or(BoundedExecutionError::Cancelled)
    }

    fn cancellation(&self) -> Option<BoundedExecutionError> {
        let mut state = self.state();
        if !state.cancellation.is_cancelled() {
            return None;
        }
        let first_observation = !state.telemetry.cancellation_observed;
        state.telemetry.cancellation_observed = true;
        #[allow(
            unused_variables,
            reason = "The same production measurement also feeds the optional read probe"
        )]
        let completed = state.telemetry.work_units;
        drop(state);
        #[cfg(not(feature = "test-seams"))]
        let _ = (first_observation, completed);
        #[cfg(feature = "test-seams")]
        if first_observation && let Some(probe) = self.probe() {
            probe.cancellation_observed(completed);
        }
        Some(BoundedExecutionError::Cancelled)
    }

    fn elapsed_since(&self, started_ms: u64) -> std::result::Result<u64, BoundedExecutionError> {
        self.clock.now_ms().checked_sub(started_ms).ok_or_else(|| {
            Error::Other("bounded deadline clock moved backwards".to_string()).into()
        })
    }

    fn check_final_boundary(&self) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        cursor_replay_checkpoint()?;
        let started_ms = self.state().started_ms;
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        Ok(())
    }

    fn publication_scope(
        &self,
        snapshot_lsn: Lsn,
        tables: BTreeSet<String>,
        inspection: bool,
    ) -> ReadPublicationScope {
        let state = self.state();
        let mut scope = ReadPublicationScope::bounded(
            snapshot_lsn,
            tables,
            self.limits,
            Arc::clone(&self.clock),
            state.cancellation.clone(),
            state.started_ms,
        );
        scope.inspection = inspection;
        scope
    }

    fn registration_budget(&self) -> crate::database::ReadRegistrationBudget {
        let started_ms = self.state().started_ms;
        crate::database::ReadRegistrationBudget::new(
            Arc::clone(&self.clock),
            started_ms,
            self.limits.active_ms,
        )
    }

    /// Check cancellation and the active deadline at an internal boundary
    /// whose work was already charged to the candidate or distance that owns
    /// it. A vector point score crosses several locks and allocation gates;
    /// each must still be interruptible without turning bookkeeping into a
    /// second unit of candidate work.
    fn checkpoint(&self) -> std::result::Result<(), BoundedExecutionError> {
        self.check_final_boundary()
    }

    /// Count one thing the read looked at, without charging for looking.
    ///
    /// The work a source does over an item is charged where it is done; this
    /// records that the item itself was read, once, so what the trace publishes
    /// as examined counts items rather than steps taken over them.
    fn note_touch(&self, touch: BoundedSourceTouch) {
        let mut state = self.state();
        let counted = state.touches.entry(touch).or_default();
        *counted = counted.saturating_add(1);
    }

    /// Observe a source item whose placement work was already charged while
    /// candidate groups were built. This preserves cancellation and probe
    /// visibility without billing the same candidate a second time at score.
    fn observe_precharged_touch(
        &self,
        touch: BoundedSourceTouch,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        #[allow(
            unused_variables,
            reason = "The source position also feeds the optional read probe"
        )]
        let (completed_source, started_ms) = {
            let state = self.state();
            let elapsed = self.elapsed_since(state.started_ms)?;
            if elapsed >= self.limits.active_ms {
                return Err(Self::limit_failure(
                    ReadFailureLimit::ActiveMs,
                    self.limits.active_ms,
                ));
            }
            (
                state.touches.get(&touch).copied().unwrap_or(0),
                state.started_ms,
            )
        };
        #[cfg(not(feature = "test-seams"))]
        let _ = completed_source;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            let _funding = crate::read_work_probe::observe_funding(
                crate::read_work_probe::funding_owner_name(&self.reader_budget),
                crate::read_work_probe::funding_owner_id(&self.reader_budget),
                self.state().memory.bytes(),
            );
            probe.before_source_touch(touch, completed_source);
        }
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        self.note_touch(touch);
        #[cfg(feature = "test-seams")]
        {
            crate::read_probe::note_bounded_source_touch();
            crate::read_probe::note_session_source_touch();
        }
        self.report_progress();
        Ok(())
    }

    fn charge(
        &self,
        source: BoundedWorkSource,
        touch: BoundedSourceTouch,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        #[allow(
            unused_variables,
            reason = "The same production measurement also feeds the optional read probe"
        )]
        let (completed_work, completed_source, started_ms) = {
            let state = self.state();
            if state.telemetry.work_units >= self.limits.work {
                return Err(Self::limit_failure(
                    ReadFailureLimit::Work,
                    self.limits.work,
                ));
            }
            let elapsed = self.elapsed_since(state.started_ms)?;
            if elapsed >= self.limits.active_ms {
                return Err(Self::limit_failure(
                    ReadFailureLimit::ActiveMs,
                    self.limits.active_ms,
                ));
            }
            (
                state.telemetry.work_units,
                state.touches.get(&touch).copied().map_or(0, |count| count),
                state.started_ms,
            )
        };

        #[cfg(not(feature = "test-seams"))]
        let _ = (completed_work, completed_source);
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_work(source, completed_work);
        }
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            let _funding = crate::read_work_probe::observe_funding(
                crate::read_work_probe::funding_owner_name(&self.reader_budget),
                crate::read_work_probe::funding_owner_id(&self.reader_budget),
                self.state().memory.bytes(),
            );
            probe.before_source_touch(touch, completed_source);
        }
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }

        charge_cursor_replay(1)?;
        let mut state = self.state();
        state.telemetry.work_units = state
            .telemetry
            .work_units
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        {
            let source_work = state.telemetry.source_work.entry(source).or_default();
            *source_work = source_work
                .checked_add(1)
                .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        }
        {
            let source_touches = state.touches.entry(touch).or_default();
            *source_touches = source_touches
                .checked_add(1)
                .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        }
        #[cfg(feature = "test-seams")]
        {
            crate::read_probe::note_bounded_source_touch();
            crate::read_probe::note_session_source_touch();
        }
        drop(state);
        // Reported from inside the loop, so a caller keeps hearing from a read
        // that is being cancelled right up to the moment it returns.
        self.report_progress();
        Ok(())
    }

    fn charge_operator(
        &self,
        source: BoundedWorkSource,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        #[allow(
            unused_variables,
            reason = "The same production measurement also feeds the optional read probe"
        )]
        let (completed_work, started_ms) = {
            let state = self.state();
            if state.telemetry.work_units >= self.limits.work {
                return Err(Self::limit_failure(
                    ReadFailureLimit::Work,
                    self.limits.work,
                ));
            }
            let elapsed = self.elapsed_since(state.started_ms)?;
            if elapsed >= self.limits.active_ms {
                return Err(Self::limit_failure(
                    ReadFailureLimit::ActiveMs,
                    self.limits.active_ms,
                ));
            }
            (state.telemetry.work_units, state.started_ms)
        };
        #[cfg(not(feature = "test-seams"))]
        let _ = completed_work;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_work(source, completed_work);
        }
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        charge_cursor_replay(1)?;
        let mut state = self.state();
        state.telemetry.work_units = state
            .telemetry
            .work_units
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        let source_work = state.telemetry.source_work.entry(source).or_default();
        *source_work = source_work
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        Ok(())
    }

    fn reserve(
        &self,
        source: BoundedWorkSource,
        requested: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if requested == 0 {
            return Ok(());
        }
        self.check_final_boundary()?;
        self.reserve_metadata(source, requested)
    }

    // Final cancellation observations own the same bounded memory as successful
    // observations, but are emitted after the request's cancellation boundary.
    fn reserve_metadata(
        &self,
        source: BoundedWorkSource,
        requested: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let requested_u64 = u64::try_from(requested)
            .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
        #[allow(
            unused_variables,
            reason = "The same production measurement also feeds the optional read probe"
        )]
        let held = {
            let state = self.state();
            let held = u64::try_from(self.held_memory_bytes(&state))
                .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
            if held
                .checked_add(requested_u64)
                .is_none_or(|after| after > self.limits.memory)
            {
                return Err(self.memory_limit_failure(
                    usize::try_from(held).unwrap_or(usize::MAX),
                    requested,
                    source.reservation_operation(),
                ));
            }
            held
        };
        #[cfg(not(feature = "test-seams"))]
        let _ = held;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_temporary_reservation(source, requested_u64, held);
        }
        #[allow(
            unused_variables,
            reason = "The same production measurement also feeds the optional read probe"
        )]
        let held_after = {
            let mut state = self.state();
            // Named, so a store-budget refusal tells the operator WHICH part of
            // the statement wanted the memory rather than only how much.
            state
                .memory
                .try_grow_for(
                    requested,
                    "bounded_read",
                    source.reservation_operation(),
                    "Lower the statement's LIMIT or width, or raise MEMORY_LIMIT.",
                )
                .map_err(BoundedExecutionError::from)?;
            let held_after = u64::try_from(self.held_memory_bytes(&state))
                .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
            state.telemetry.peak_temporary_bytes =
                state.telemetry.peak_temporary_bytes.max(held_after);
            state
                .telemetry
                .source_peak_temporary_bytes
                .entry(source)
                .and_modify(|peak| *peak = (*peak).max(held_after))
                .or_insert(held_after);
            held_after
        };
        #[cfg(not(feature = "test-seams"))]
        let _ = held_after;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.after_temporary_reservation(source, requested_u64, held_after);
        }
        Ok(())
    }

    /// The source adapter has already admitted move bytes into state.memory.
    /// Detaching its refund owner makes the pending row responsible for them.
    fn admit_source_row(
        &self,
        source: BoundedWorkSource,
        admission: crate::read_image_memory::RowReadAdmission<'_>,
    ) -> std::result::Result<usize, BoundedExecutionError> {
        match admission {
            crate::read_image_memory::RowReadAdmission::Copy(bytes) => {
                self.reserve(source, bytes)?;
                Ok(bytes)
            }
            crate::read_image_memory::RowReadAdmission::Move(memory) => {
                let bytes = memory.retained_payload_bytes();
                let credit = memory.take_retained_credit(bytes)?;
                // SourceDecodeBudget already recorded these exact bytes in
                // this request ledger. Transfer refund responsibility only.
                credit.transfer();
                Ok(bytes)
            }
        }
    }

    fn release(&self, bytes: usize) -> std::result::Result<(), BoundedExecutionError> {
        self.state()
            .memory
            .try_shrink(bytes)
            .map_err(BoundedExecutionError::from)
    }

    /// The final result adopts already admitted payload credit from this
    /// request. This transfers ownership once without re-admitting the bytes.
    fn own_vector_trace(
        &self,
        trace: contextdb_vector::mem::BoundedVectorSearchTrace,
    ) -> std::result::Result<contextdb_vector::OwnedVectorSearchTrace, BoundedExecutionError> {
        let bytes = trace.retained_bytes();
        let credit = self.state().memory.try_split_off(bytes)?;
        let owner: Arc<dyn contextdb_vector::MemoryBudget> =
            Arc::new(TraceMemoryOwner(Arc::clone(&self.reader_budget)));
        Ok(trace.into_owned(Arc::clone(&owner), PrepaidTraceCredit { owner, credit })?)
    }

    fn transfer_answer(
        &self,
        result: &mut crate::database::OwnedQueryResult,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut bytes = strings_capacity_bytes(&result.columns, "answer columns")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                result.rows.capacity(),
                std::mem::size_of::<Vec<Value>>(),
                "answer row container",
            )?,
            "answer payload",
        )?;
        for row in &result.rows {
            bytes = checked_size_add(bytes, values_capacity_allocation_bytes(row)?, "answer rows")?;
        }
        bytes = checked_size_add(
            bytes,
            query_trace_capacity_bytes(&result.trace)?,
            "answer trace",
        )?;
        let credit = self.state().memory.try_split_off(bytes)?;
        result.answer_charge = crate::read_image_memory::AnswerCharge::holding_reader(credit)?;
        Ok(())
    }

    /// What this read holds against its memory ceiling: its own reservation
    /// plus the storage pages it read that are still alive outside the page
    /// cache.
    fn held_memory_bytes(&self, state: &RequestState) -> usize {
        state.memory.bytes().saturating_add(
            self.live_source_pages
                .load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// Admit one storage page before its buffer is allocated, against the
    /// same ceiling and the same owner every other charge of this read
    /// uses, and hand back the charge the page will carry.
    fn admit_source_page(
        &self,
        source: BoundedWorkSource,
        bytes: usize,
    ) -> std::result::Result<SourcePageCharge, BoundedExecutionError> {
        self.admit_live_charge(source, bytes, "read_storage_page")
    }

    /// Admit `bytes` for an allocation that can outlive the call admitting it,
    /// before it exists. The charge it hands back stays in this read's held
    /// memory, and on the declared owner, until its last holder drops it;
    /// `operation` names the allocation in a refusal.
    fn admit_live_charge(
        &self,
        source: BoundedWorkSource,
        bytes: usize,
        operation: &'static str,
    ) -> std::result::Result<SourcePageCharge, BoundedExecutionError> {
        self.check_final_boundary()?;
        let mut state = self.state();
        let held = self.held_memory_bytes(&state);
        let within = held
            .checked_add(bytes)
            .and_then(|after| u64::try_from(after).ok())
            .is_some_and(|after| after <= self.limits.memory);
        if !within {
            return Err(self.memory_limit_failure(held, bytes, operation));
        }
        let credit = ReadCredit::try_new(
            Arc::clone(&self.reader_budget),
            bytes,
            "bounded_read",
            operation,
            "Lower the statement's LIMIT or width, or raise MEMORY_LIMIT.",
        )
        .map_err(BoundedExecutionError::from)?;
        self.live_source_pages
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        let held_after = u64::try_from(held.saturating_add(bytes)).unwrap_or(u64::MAX);
        state.telemetry.peak_temporary_bytes = state.telemetry.peak_temporary_bytes.max(held_after);
        state
            .telemetry
            .source_peak_temporary_bytes
            .entry(source)
            .and_modify(|peak| *peak = (*peak).max(held_after))
            .or_insert(held_after);
        Ok(SourcePageCharge {
            _credit: credit,
            live: Arc::clone(&self.live_source_pages),
            bytes,
        })
    }

    fn source_read_memory(
        self: &Arc<Self>,
        source: BoundedWorkSource,
    ) -> Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> {
        Arc::new(SourceDecodeBudget {
            context: Arc::clone(self),
            source,
        })
    }

    /// Run one row-source call with every storage page it has to read
    /// admitted against this read's memory before the page buffer exists.
    ///
    /// A stored row is read as the whole storage page that holds it, and the
    /// page's length is known from the page address before any byte of it is
    /// allocated. A value too large for the read's memory is therefore refused
    /// at that point, with the same typed memory refusal the row's own charge
    /// would have produced -- not after its page, its decoded copy and its
    /// authentication encoding already exist.
    ///
    /// An admitted page carries its charge for as long as it lives. A page
    /// the store's page cache keeps is paid for by the cache's own reserved
    /// budget and gives its charge back at once; a page the cache does not
    /// keep -- one larger than the cache -- keeps its charge until the last
    /// thing holding it (this call, a retained scan, a cursor) drops it. Pages
    /// served from the cache allocate nothing and are not charged.
    fn with_source_page_admission<T>(
        self: &Arc<Self>,
        source: BoundedWorkSource,
        read: impl FnOnce() -> std::result::Result<T, BoundedExecutionError>,
    ) -> std::result::Result<T, BoundedExecutionError> {
        let admission = {
            let context = Arc::clone(self);
            Arc::new(move |bytes: usize| {
                context
                    .admit_source_page(source, bytes)
                    .map(|charge| {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_worker_page_admitted(bytes, crate::read_work_probe::funding_owner_name(&context.reader_budget));
                        Box::new(charge) as redb::ReadPageCharge
                    })
                    .map_err(|error| {
                        let error = match error {
                            BoundedExecutionError::Engine(error) => error,
                            BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
                            BoundedExecutionError::Cancelled => Error::ReadCancelled,
                            BoundedExecutionError::Unimplemented => {
                                Error::ReadSessionNotImplemented
                            }
                        };
                        redb::StorageError::AdmissionRefused(Box::new(error))
                    })
            })
                as Arc<
                    dyn Fn(usize) -> std::result::Result<redb::ReadPageCharge, redb::StorageError>
                        + Send
                        + Sync,
                >
        };
        redb::with_read_page_charge_storage(admission, read)
    }

    fn set_encoded_bytes(&self, bytes: u64) {
        self.state().telemetry.encoded_bytes = bytes;
    }

    fn can_charge_more_work(&self) -> bool {
        self.state().telemetry.work_units < self.limits.work && cursor_replay_work_available()
    }

    /// Give the graph traversal a finite ceiling derived from the work this
    /// request still owns. The normal callbacks remain the aggregate guard;
    /// these values also prevent the filtered graph algorithm from wandering
    /// indefinitely while looking for allowed row ids.
    fn remaining_vector_graph_work(&self) -> std::result::Result<usize, BoundedExecutionError> {
        self.check_final_boundary()?;
        let remaining = {
            let state = self.state();
            self.limits
                .work
                .checked_sub(state.telemetry.work_units)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?
        };
        Ok(usize::try_from(remaining).unwrap_or(usize::MAX))
    }

    /// Whether an operator should stop here and finish on a later fetch.
    ///
    /// A cursor's work ceiling is charged per FETCH, so an operator that has
    /// spent this fetch's budget has not failed -- it has reached the end of
    /// what this page may pay for, and hands back what it has so far. A
    /// one-shot read has no later fetch to finish in, so it never stops here
    /// and meets the work ceiling as the typed refusal it always was.
    fn fetch_budget_spent(&self) -> bool {
        matches!(self.resumption, ReadResumption::AcrossFetches) && !self.can_charge_more_work()
    }

    /// Whether this refusal is this fetch's budget running out rather than a
    /// read that cannot be answered.
    ///
    /// A source charges as it walks, so it can reach the ceiling in the
    /// middle of one pull -- a filter reads several rows to hand back one.
    /// For a read that resumes, that refusal means "not in this page", not
    /// "no answer"; the operator that can carry its state forward absorbs it.
    fn refusal_pauses_this_fetch(&self, error: &BoundedExecutionError) -> bool {
        if !matches!(self.resumption, ReadResumption::AcrossFetches) {
            return false;
        }
        let BoundedExecutionError::Refused(failure) = error else {
            return false;
        };
        matches!(
            failure.detail(),
            ReadFailureDetail::OwnerLimitExceeded(detail)
                if detail.limit == ReadFailureLimit::Work
        )
    }

    fn held_bytes(&self) -> usize {
        self.state().memory.bytes()
    }

    fn attribute_live_memory(&self, source: BoundedWorkSource) {
        let mut state = self.state();
        let held = match u64::try_from(state.memory.bytes()) {
            Ok(held) => held,
            Err(_) => return,
        };
        state
            .telemetry
            .source_peak_temporary_bytes
            .entry(source)
            .and_modify(|peak| *peak = (*peak).max(held))
            .or_insert(held);
    }

    /// What this read looked at to answer, in the executor's own terms: rows
    /// it read, index entries it walked, adjacency entries it inspected.
    fn rows_examined(&self) -> std::result::Result<u64, BoundedExecutionError> {
        let state = self.state();
        Self::examined_touches(&state)
    }

    fn cumulative_rows_examined(&self) -> std::result::Result<u64, BoundedExecutionError> {
        let state = self.state();
        state
            .completed_rows_examined
            .checked_add(Self::examined_touches(&state)?)
            .ok_or_else(|| {
                Error::Other("bounded rows-examined counter overflow".to_string()).into()
            })
    }

    fn examined_touches(state: &RequestState) -> std::result::Result<u64, BoundedExecutionError> {
        [
            BoundedSourceTouch::TableRow,
            BoundedSourceTouch::IndexEntry,
            BoundedSourceTouch::AdjacencyEntry,
        ]
        .into_iter()
        .try_fold(0u64, |sum, touch| {
            sum.checked_add(state.touches.get(&touch).copied().unwrap_or(0))
        })
        .ok_or_else(|| Error::Other("bounded rows-examined counter overflow".to_string()).into())
    }

    /// Whether this read named any ceiling of its own. A read that named none
    /// is governed by its caller-supplied owner budget.
    fn declares_no_ceilings(&self) -> bool {
        self.limits.memory == u64::MAX
    }

    fn transaction(&self) -> Option<TxId> {
        self.transaction
    }

    fn telemetry(&self) -> BoundedExecutionTelemetry {
        self.state().telemetry.clone()
    }
}

/// Holds bytes a bounded source reserved on the caller's behalf for exactly
/// as long as the caller works with the value they cover. The source reports
/// the precise amount it reserved and never releases it itself, so every exit
/// from the owning block — including an error unwind — has to give it back.
/// Releasing from `Drop` is what makes that true of paths that never named
/// the reservation.
/// Why a report is being made, which decides both what makes one due and
/// which seam it goes to.
#[derive(Debug, Clone, Copy)]
enum ProgressReport {
    /// Liveness, mid-read: due once an interval's worth of new work exists,
    /// and told to whoever is watching this read run.
    WhileRunning,
    /// Accounting, at the end: due if anything at all is still untold, and
    /// told to the in-process seam rather than to the liveness surface.
    AtCompletion,
}

struct ReservedBytes<'a> {
    context: &'a Arc<RequestContext>,
    bytes: usize,
}

impl<'a> ReservedBytes<'a> {
    fn new(context: &'a Arc<RequestContext>, bytes: usize) -> Self {
        Self { context, bytes }
    }

    /// One authorization call holds only its largest inspected row, with
    /// cleanup on success, refusal and cancellation through this guard.
    fn charge_access_row(
        &mut self,
        bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.context.charge(
            BoundedWorkSource::AccessControl,
            BoundedSourceTouch::AccessRow,
        )?;
        let extra = bytes.saturating_sub(self.bytes);
        if extra != 0 {
            self.context
                .reserve(BoundedWorkSource::AccessControl, extra)?;
            self.bytes += extra;
        }
        self.context
            .attribute_live_memory(BoundedWorkSource::AccessControl);
        Ok(())
    }

    /// Give the bytes back where an accounting underflow can still be
    /// reported as the request's own failure.
    fn release(mut self) -> std::result::Result<(), BoundedExecutionError> {
        let bytes = std::mem::take(&mut self.bytes);
        self.context.release(bytes)
    }
}

/// A materialized row whose retained bytes stay charged to the request only
/// while this guard holds it.  Every way out of the block that pulled the row
/// — a refusal, a cancellation, an accounting fault — gives those bytes back;
/// handing the row on to the state that carries the charge from there is the
/// one exit that keeps them.
struct RetainedRow<'a> {
    context: &'a Arc<RequestContext>,
    row: PulledRow,
    charged: bool,
}

impl<'a> RetainedRow<'a> {
    fn new(context: &'a Arc<RequestContext>, row: PulledRow) -> Self {
        Self {
            context,
            row,
            charged: true,
        }
    }

    /// Give the bytes back where an accounting underflow can still be reported
    /// as the request's own failure.
    fn release(mut self) -> std::result::Result<(), BoundedExecutionError> {
        self.charged = false;
        let bytes = std::mem::take(&mut self.row.retained_bytes);
        drop(std::mem::take(&mut self.row.values));
        let _ = self.row.window.take();
        self.context.release(bytes)
    }

    /// Hand the row to the state that carries its charge from here on.
    fn into_row(mut self) -> PulledRow {
        self.charged = false;
        std::mem::replace(
            &mut self.row,
            PulledRow {
                window: None,
                values: Vec::new(),
                retained_bytes: 0,
                group_binding: None,
            },
        )
    }
}

impl std::ops::Deref for RetainedRow<'_> {
    type Target = PulledRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl std::ops::DerefMut for RetainedRow<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.row
    }
}

impl Drop for RetainedRow<'_> {
    fn drop(&mut self) {
        if !self.charged {
            return;
        }
        let bytes = std::mem::take(&mut self.row.retained_bytes);
        drop(std::mem::take(&mut self.row.values));
        let _ = self.row.window.take();
        if bytes != 0 {
            self.context.release_parked(bytes);
        }
    }
}

impl Drop for ReservedBytes<'_> {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        if bytes != 0 {
            self.context.release_parked(bytes);
        }
    }
}

// Ordinary execution still reaches these compatibility observers. The bounded
// path uses the fallible callbacks on the database helpers directly, so legacy
// callers retain their existing behavior and acquire no request budget.
pub(super) fn observe_access_control_row() {}

pub(super) fn observe_rank_candidate() {}

struct PullNode {
    columns: Vec<String>,
    trace: QueryTrace,
    kind: PullKind,
}

struct PulledRow {
    /// One specification per SELECT scope; Project consumes this typed namespace.
    /// Kept inline so no SQL identifier can address or shadow its value.
    window: Option<WindowRankValues>,
    values: Vec<Value>,
    retained_bytes: usize,
    group_binding: Option<GroupBinding>,
}

impl PulledRow {
    fn release(
        self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let bytes = self.retained_bytes;
        drop(self);
        context.release(bytes)
    }
}

#[allow(clippy::large_enum_variant)]
enum PullKind {
    Scan(ScanState),
    Graph(GraphState),
    // Admit this fixed native control separately from each recursive plan frame.
    Vector(Box<VectorState>),
    Project(ProjectState),
    Sort(SortState),
    Window(WindowState),
    Limit(LimitState),
    Filter(FilterState),
    Distinct(DistinctState),
    Join(JoinState),
    Static(StaticState),
    Union(UnionState),
}

/// What one plan layer of a BOUNDED CTE build costs in stack, pinned.
///
/// Scope: `PullKind`, `PullNode` and `ScanState` exist only inside the
/// bounded kernel, which serves `ReadSession` and the owner read channel.
/// `Database::execute` does NOT enter it -- it runs the eager
/// `executor::execute_plan_once` -- so this ceiling does not bound eager
/// recursive CTE execution through `Database::execute_at_snapshot`.
/// The eager frame on that path is pinned by
/// `executor::eager_plan_layer_frame`.
///
/// What this one protects is real on its own terms. `build_kernel_with_ctes`
/// recurses one frame per plan layer; it already returns `Box<PullNode>` for
/// that reason, but the node each layer BUILDS is materialized by value in
/// that layer's own frame first, and an unoptimised build elides none of
/// those copies. So a deep CTE read taken through the bounded kernel pays the
/// widest `PullKind` arm once per layer, and a field added to `TableMeta`
/// used to widen every one of them.
#[cfg(test)]
mod plan_layer_frame_size {
    use super::{PullKind, PullNode, ScanState};
    use contextdb_core::table_meta::TableMeta;

    /// What one layer of a bounded deep-CTE read may cost. A scan that owned
    /// its `TableMeta` by value measured 1848 bytes here; carrying a handle
    /// to it instead leaves room under this number for the ordinary growth of
    /// a scan's own state.
    const PLAN_LAYER_CEILING_BYTES: usize = 1600;

    /// What one ANSWER may cost. `QueryResult` carries `QueryTrace` inline and
    /// `execute_plan_once` returns it by value out of every recursive plan
    /// layer, so a rarely-populated disclosure added here is paid by every
    /// layer of a deep statement. The four-CTE query exercises eight layers.
    // The executed union receipt adds four optional counters and a terminal
    // word to the inline trace. Keep this bound close to the observed 784-byte
    // answer; the separate plan-layer bound still forbids copying TableMeta.
    const ANSWER_CEILING_BYTES: usize = 800;

    #[test]
    fn one_plan_layer_never_scales_with_the_table_catalog() {
        let pull_kind = std::mem::size_of::<PullKind>();
        let pull_node = std::mem::size_of::<PullNode>();
        let scan_state = std::mem::size_of::<ScanState>();
        let table_meta = std::mem::size_of::<TableMeta>();
        eprintln!(
            "plan layer frame: PullKind={pull_kind} PullNode={pull_node} \
             ScanState={scan_state} TableMeta={table_meta}"
        );
        let query_trace = std::mem::size_of::<crate::database::QueryTrace>();
        let query_result = std::mem::size_of::<crate::database::QueryResult>();
        eprintln!("answer frame: QueryTrace={query_trace} QueryResult={query_result}");
        assert!(
            query_result <= ANSWER_CEILING_BYTES,
            "one answer slot is {query_result} bytes, over the {ANSWER_CEILING_BYTES} a recursive \
             plan layer may spend on it; `execute_plan_once` returns `QueryResult` BY VALUE \
             through every layer of a deep statement, so each byte here is paid once per layer"
        );
        assert!(
            pull_kind <= PLAN_LAYER_CEILING_BYTES,
            "one layer of a bounded CTE read is {pull_kind} bytes, over the \
             {PLAN_LAYER_CEILING_BYTES} this kernel is sized for; a deep bounded read pays this \
             once per plan layer"
        );
        // And the ceiling stays tight enough to catch the way this broke:
        // if a scan could own its table's metadata by value again and still
        // fit, the ceiling has been raised into uselessness.
        assert!(
            scan_state + table_meta > PLAN_LAYER_CEILING_BYTES,
            "a scan could carry its {table_meta}-byte TableMeta by value again \
             ({scan_state} + {table_meta}) and still fit under {PLAN_LAYER_CEILING_BYTES}; that \
             is the regression this test exists to catch, so the ceiling is too slack"
        );
    }
}

/// The request owns these tokens, so the adapter uses a weak back-reference.
/// The request's reservation settles all remaining credit when it dies; a
/// token destroyed earlier releases through the same live request.
struct WeakSourceDecodeBudget(std::sync::Weak<RequestContext>);
impl std::fmt::Debug for WeakSourceDecodeBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RequestVectorBudget")
    }
}
impl contextdb_core::read_memory::ReadMemoryBudget for WeakSourceDecodeBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let context = self
            .0
            .upgrade()
            .ok_or_else(|| Error::Other("the requesting reader has ended".into()))?;
        contextdb_core::read_memory::ReadMemoryBudget::try_reserve(
            &SourceDecodeBudget {
                context,
                source: BoundedWorkSource::VectorCandidates,
            },
            bytes,
            subsystem,
            operation,
            hint,
        )
    }
    fn release(&self, bytes: usize) {
        if let Some(context) = self.0.upgrade() {
            context.release_parked(bytes);
        }
    }
}

/// Decoded source records belong to the requesting read. Admission shares
/// its aggregate memory and cancellation boundary; each window releases its
/// temporary charge.
struct SourceDecodeBudget {
    context: Arc<RequestContext>,
    source: BoundedWorkSource,
}

impl std::fmt::Debug for SourceDecodeBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SourceDecodeBudget")
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for SourceDecodeBudget {
    #[cfg(feature = "test-seams")]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        Some(&self.context.reader_budget)
    }

    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> Result<()> {
        self.context
            .reserve(self.source, bytes)
            .map_err(|error| match error {
                BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
                BoundedExecutionError::Cancelled => Error::ReadCancelled,
                BoundedExecutionError::Engine(error) => error,
                BoundedExecutionError::Unimplemented => {
                    Error::Other("unsupported source allocation".into())
                }
            })
    }

    fn release(&self, bytes: usize) {
        self.context.release_parked(bytes);
    }
}

struct MembershipBudget(Arc<RequestContext>);

impl std::fmt::Debug for MembershipBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MembershipBudget")
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for MembershipBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> Result<()> {
        self.0
            .reserve(BoundedWorkSource::IndexRange, bytes)
            .map_err(|error| match error {
                BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
                BoundedExecutionError::Cancelled => Error::ReadCancelled,
                BoundedExecutionError::Engine(error) => error,
                BoundedExecutionError::Unimplemented => {
                    Error::Other("unsupported membership allocation".into())
                }
            })?;
        self.0.state().membership_bytes += bytes;
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.0.state().membership_bytes -= bytes;
        self.0.release_parked(bytes);
    }
}

impl Drop for MembershipBudget {
    fn drop(&mut self) {
        let bytes = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        self.0.state().membership_bytes -= bytes;
        self.0.release_parked(bytes);
    }
}

fn capture_scan_membership(
    db: &dyn ReadExecutionTarget,
    table: &str,
    snapshot: SnapshotId,
    mode: &mut ScanMode,
    context: &Arc<RequestContext>,
) -> std::result::Result<(), BoundedExecutionError> {
    if matches!(mode, ScanMode::Physical { .. } | ScanMode::Empty) {
        return Ok(());
    }
    if db.retained_relational_membership_source() {
        return Ok(());
    }
    context.reserve(
        BoundedWorkSource::IndexRange,
        std::mem::size_of::<MembershipBudget>() + 2 * std::mem::size_of::<usize>(),
    )?;
    context.state().membership_bytes +=
        std::mem::size_of::<MembershipBudget>() + 2 * std::mem::size_of::<usize>();
    let budget = Arc::new(MembershipBudget(Arc::clone(context)));
    let touch = || {
        context.charge(
            BoundedWorkSource::IndexRange,
            BoundedSourceTouch::IndexEntry,
        )
    };
    match mode {
        ScanMode::Index { cursor, .. } | ScanMode::Ordered { cursor, .. } => cursor.capture(
            db.relational_membership_store(),
            table,
            snapshot,
            budget,
            touch,
        ),
        ScanMode::Exact { index, cursor } => cursor.capture(
            db.relational_membership_store(),
            table,
            index,
            snapshot,
            budget,
            touch,
        ),
        ScanMode::Physical { .. } | ScanMode::Empty => Ok(()),
    }
}

enum PhysicalPartitionColumn {
    RowId,
    Schema(usize),
}

struct SourceInputGroup {
    descriptor: GroupDescriptorId,
    positions: Vec<PhysicalPartitionColumn>,
}

struct ScanState {
    source_group: Option<SourceInputGroup>,
    identities_only: bool,
    /// Whether this candidate source's own index range already keeps the read
    /// inside every restriction its reader carries, so the row gate has
    /// nothing left to decide about a candidate.
    gate_answered_by_candidate_range: bool,
    vector_candidate_source: bool,
    table: String,
    /// An 8-byte handle to this scan's OWN clone of the catalog entry, not a
    /// share of the catalog's.
    ///
    /// `db.table_meta(table)` hands back an owned clone taken when the plan
    /// was built, exactly as before; the `Arc` wraps that private clone. So
    /// this buys size and nothing else -- `build_kernel_with_ctes`
    /// materializes the node each layer builds in that layer's own frame, and
    /// a `TableMeta` by value here made the frame scale with the catalog's
    /// shape. It is never mutated and `ScanState` is not `Clone`, so the
    /// refcount never leaves this state.
    ///
    /// Do NOT "close the gap" by handing out the live catalog's own `Arc`:
    /// that would replace this per-statement snapshot with a handle into the
    /// map a concurrent DDL rewrites, which is a staleness hazard this scan
    /// does not have today.
    meta: Arc<TableMeta>,
    schema_columns: Vec<String>,
    filter: Option<Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    mode: ScanMode,
    cursor_key_bytes: usize,
    residual_pick: Option<Box<IndexPick>>,
    /// Set when an exact whole-key probe over a single-column AUTOMATIC index
    /// was re-planned onto the authenticated ordered route. The source is an
    /// ordinary index run now, but the STATEMENT is still one that names the
    /// rows its keys identify, so refuse-vs-hide is decided the way an exact
    /// probe decides it and not the way a run does.
    authenticated_exact_index: Option<String>,
    /// Whether the committed source offers EVERY committed row of the table.
    /// A physical walk does. An index run offers only the rows its predicate
    /// names, so it cannot answer "did this identity have a committed
    /// original" merely by having passed it -- the store is asked instead.
    committed_source_offers_every_row: bool,
    pending_row: Option<(VersionedRow, usize, BoundedWorkSource)>,
    pending_vector_copy: Option<Box<contextdb_vector::VectorCopyAdmission>>,
    /// Whether the row in hand came from the committed source. An ordered
    /// read hands committed and staged rows back interleaved, so which one is
    /// in hand is recorded rather than inferred from how far the walk has got.
    pending_from_committed: bool,
    /// The quantized vector columns THIS answer names, so the scan fills
    /// those and no others. A column nobody asked for is not read out of the
    /// store and not charged: a search that orders by distance and projects
    /// only an id scores from the index and never touches a row's vector.
    supplement_columns: Vec<String>,
    /// The transaction's own rows folded into an ordered walk where their
    /// ordering value puts them. Present only for a read that PROMISED an
    /// order and has a transaction open; every other read hands the staged
    /// rows over after the committed ones, which is all an unordered answer
    /// asks for.
    ordered_merge: Option<OrderedMerge>,
    /// What the reading handle's own open transaction adds to and hides from
    /// this table. Absent when no transaction is open, which is every read of
    /// a committed file and every read by a handle that is not mid-write.
    transaction: Option<ScanTransactionOverlay>,
}

/// An ordered walk's view of the rows its own transaction staged.
///
/// An index run is the committed rows in order and nothing else. Reading the
/// whole table and sorting it answers the same question, but it turns a
/// five-row answer over a declared index into a walk of every row the table
/// holds the moment the reader has a transaction open -- so a work ceiling
/// sized on committed state refuses the identical query with a write in
/// flight. The transaction's own rows are few, already materialized, and
/// already charged, so they are put in this read's order once and folded into
/// the run as it walks.
struct OrderedMerge {
    /// Every SQL ordering key, in precedence order and with its requested
    /// direction. Pinned and tied leading keys still participate so the next
    /// key decides exactly as a full sort would.
    keys: Vec<(String, contextdb_core::SortDirection)>,
    /// The staged rows in this read's order. `None` until they have been read.
    staged: Option<VecDeque<(VersionedRow, usize)>>,
    /// A committed row already pulled and then held back, because a staged
    /// row's ordering value came first. It is the next row out.
    deferred: Option<(VersionedRow, usize, BoundedWorkSource)>,
    /// What the two above are holding, so a paused read is charged for them.
    held_bytes: usize,
}

fn ordered_merge_component(
    row: &VersionedRow,
    column: &str,
    direction: contextdb_core::SortDirection,
) -> DirectedValue {
    let value = row.values.get(column).cloned().unwrap_or(Value::Null);
    match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value)),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value)),
    }
}

/// Compare two rows by the complete ORDER BY tuple. Equal components keep
/// walking, including a pinned leading column, and NULL follows the same
/// directed total order as the declared index and ordinary SQL sort.
fn compare_ordered_merge_rows(
    left: &VersionedRow,
    right: &VersionedRow,
    keys: &[(String, contextdb_core::SortDirection)],
) -> Ordering {
    for (column, direction) in keys {
        let ordering = ordered_merge_component(left, column, *direction)
            .cmp(&ordered_merge_component(right, column, *direction));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn ordered_merge_keys_capacity_bytes(
    keys: &Vec<(String, contextdb_core::SortDirection)>,
) -> std::result::Result<usize, BoundedExecutionError> {
    keys.iter().try_fold(
        checked_size_mul(
            keys.capacity(),
            std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
            "ordered merge keys",
        )?,
        |bytes, (column, _)| checked_size_add(bytes, column.capacity(), "ordered merge key"),
    )
}

/// The open transaction this scan is reading inside, and how far through its
/// staged rows the scan has got.
struct OrderedStatementPosition {
    index: String,
    cursor: BoundedOrderedRowCursor,
    after: Option<(IndexKey, RowId)>,
}

fn staged_run_bound_bytes(bound: &Bound<IndexKey>) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => key
            .capacity()
            .saturating_mul(std::mem::size_of::<DirectedValue>())
            .saturating_add(
                key.iter()
                    .map(|value| match value {
                        DirectedValue::Asc(value) => value.0.estimated_bytes(),
                        DirectedValue::Desc(value) => value.0.estimated_bytes(),
                    })
                    .sum::<usize>(),
            ),
        Bound::Unbounded => 0,
    }
}

struct ScanTransactionOverlay {
    overlay: crate::executor::TransactionTableOverlay,
    predicate_deleted: Vec<RowId>,
    committed_predicates_resolved: bool,
    /// Identities of staged rows that also exist as committed rows this
    /// snapshot can see. A staged row whose committed original the
    /// transaction ALSO deleted is still published -- delete-then-reinsert
    /// leaves the reinserted row -- which is what this set is for.
    restaged_over_committed: std::collections::HashSet<RowId>,
    /// How many of the staged rows have been published.
    published: Option<usize>,
    ordered_staged: Option<OrderedStatementPosition>,
    staged_key_bytes: usize,
    /// Set once the committed source is exhausted and the staged rows start.
    committed_exhausted: bool,
}

impl ScanTransactionOverlay {
    /// Whether the committed row in hand is hidden by this transaction: it
    /// staged the row's deletion, it staged a deletion matching the row's
    /// values, or it restaged the row and its own version is published
    /// instead.
    fn hides_committed(&mut self, table: &str, row: &VersionedRow) -> bool {
        if self
            .overlay
            .staged_rows
            .as_ref()
            .is_some_and(|rows| rows.contains(&row.row_id))
        {
            // Recorded whether or not the row is also hidden: what makes a
            // staged row publishable is that its original exists here at all.
            let _ = self.restaged_over_committed.insert(row.row_id);
            return true;
        }
        if self.overlay.deleted.contains(&row.row_id) {
            return true;
        }
        if self.committed_predicates_resolved {
            return self.predicate_deleted.binary_search(&row.row_id).is_ok();
        }
        contextdb_tx::row_matches_delete_predicates(&self.overlay.delete_predicates, table, row)
    }
}

#[allow(clippy::large_enum_variant)]
enum ScanMode {
    Physical {
        cursor: BoundedPhysicalCursor,
    },
    Index {
        pick: Box<IndexPick>,
        column: String,
        direction: contextdb_core::SortDirection,
        cursor: BoundedOrderedRowCursor,
    },
    Ordered {
        /// The leading indexed column and physical direction the relational
        /// cursor needs to enter the B-tree.
        column: String,
        direction: contextdb_core::SortDirection,
        cursor: BoundedOrderedRowCursor,
    },
    /// An index built for exact lookup answers whole keys rather than runs,
    /// so the source asks it the keys the predicate names instead of walking
    /// an order it does not keep.
    Exact {
        index: String,
        cursor: contextdb_relational::mem::BoundedExactIndexCursor,
    },
    Empty,
}

struct OrderedIndexScan {
    column: String,
    direction: contextdb_core::SortDirection,
    keys: Vec<(String, contextdb_core::SortDirection)>,
    index: String,
    reverse: bool,
    predicate_pick: Option<IndexPick>,
    predicate_considered: Vec<crate::database::IndexCandidate>,
}

struct GraphState {
    start_alias: String,
    steps: Vec<GraphStepPlan>,
    filter: Option<Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    starts: VecDeque<uuid::Uuid>,
    start_candidates: Option<Box<PullNode>>,
    edge_source: Option<crate::database::gate::BoundedGraphEdgeCursor>,
    single_step_unpinned: bool,
    /// A single hop pinned only where it ENDS is walked backwards from that
    /// end: the pinned node is the one adjacency list worth opening, and the
    /// hop's own direction is reversed to reach the nodes that arrive at it.
    /// The bindings are then put back the way the statement wrote them.
    probes_backwards_from_target: bool,
    /// The standing visited ceiling this traversal keeps. `None` for a read
    /// that declared its own budget -- that budget replaces the ceiling, which
    /// is the whole point of a bounded read. `Some` for a read that declared
    /// none, which keeps the ceiling it always had.
    visited_cap: Option<usize>,
    /// Whether this traversal is a single hop walked out of named starts. It
    /// reports one row per matching edge. The one edge an undirected hop sees
    /// twice is a loop on its own start, once leaving and once arriving, so the
    /// types of the loops already reported out of the adjacency being walked
    /// are remembered here for as long as that one adjacency is being walked.
    single_step_probe: bool,
    probe_loops: Vec<EdgeType>,
    probe_seen_container_bytes: usize,
    seen_starts: Vec<uuid::Uuid>,
    seen_start_container_bytes: usize,
    /// The paths this traversal has already answered or already continued. In
    /// a longer match they are keyed by the nodes the pattern's target aliases
    /// are bound to, never by where the walk began: two starts that arrive at
    /// the same nodes are ONE answer, and the continuation out of those nodes
    /// is walked once. A single hop is not kept here: each matching edge out
    /// of its distinct starts is its own answer.
    seen_paths: Vec<Vec<uuid::Uuid>>,
    seen_path_container_bytes: usize,
    frontier: Vec<GraphPathState>,
    frontier_container_bytes: usize,
    row_memory: AmortizedRowMemory,
    active: Option<GraphActiveState>,
    /// Every start's first-step walk, open together and merged by depth and
    /// then start order, so a target is first reached at its shallowest depth
    /// across all starts and a tie goes to the earlier start. Used only when
    /// the first step's length varies; `None` until the starts are resolved.
    start_walks: Option<std::collections::BinaryHeap<GraphStartWalk>>,
    start_walk_container_bytes: usize,
    /// The node the merge chose from the active walk and that walk's start
    /// rank. Once the node is handled, the walk goes back to the merge.
    chosen: Option<contextdb_graph::mem::BoundedTraversalNode>,
    active_rank: Option<usize>,
}

/// Memory this traversal has been admitted for and not yet handed to a row.
///
/// A traversal reports one row after another, each needing a small allocation
/// of its own. Asking the request to admit every one of them separately makes
/// the number of admissions grow with the rows the traversal reports rather
/// than with the memory it holds. The traversal is admitted for a block, hands
/// that block out row by row, and asks again — for a larger block — only once
/// the block is spent, so every byte is still admitted before it is taken and
/// the admissions grow with the logarithm of the rows.
#[derive(Default)]
struct AmortizedRowMemory {
    available: usize,
    block: usize,
}

/// The largest block a traversal is admitted for in one step. Past this size a
/// larger block would hold more memory ahead of the rows than it saves in
/// admissions.
const AMORTIZED_ROW_MEMORY_BLOCK_CEILING: usize = 8 * 1024;

impl AmortizedRowMemory {
    fn take(
        &mut self,
        context: &Arc<RequestContext>,
        bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.available < bytes {
            let shortfall = bytes - self.available;
            let block = self
                .block
                .max(shortfall)
                .min(AMORTIZED_ROW_MEMORY_BLOCK_CEILING.max(shortfall));
            context.reserve(BoundedWorkSource::GraphTraversal, block)?;
            self.available = checked_size_add(self.available, block, "graph row memory")?;
            self.block = block
                .saturating_mul(2)
                .min(AMORTIZED_ROW_MEMORY_BLOCK_CEILING);
        }
        self.available -= bytes;
        Ok(())
    }

    /// Return bytes a row did not need. They stay admitted and are handed to
    /// the next row rather than released and re-admitted.
    fn give_back(&mut self, bytes: usize) -> std::result::Result<(), BoundedExecutionError> {
        self.available = checked_size_add(self.available, bytes, "graph row memory")?;
        Ok(())
    }

    fn release(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let held = std::mem::take(&mut self.available);
        self.block = 0;
        context.release(held)
    }
}

struct GraphPathState {
    bindings: Vec<uuid::Uuid>,
    node: uuid::Uuid,
    total_depth: u32,
    step_index: usize,
    retained_bytes: usize,
}

struct GraphActiveState {
    path: GraphPathState,
    cursor: contextdb_graph::mem::BoundedBfsCursor,
}

/// One start's first-step walk while every start's walk is merged: its rank
/// in start order, its path and cursor, and the next node it reports.
struct GraphStartWalk {
    rank: usize,
    active: GraphActiveState,
    next: contextdb_graph::mem::BoundedTraversalNode,
}

impl GraphStartWalk {
    fn order(&self) -> (u32, usize) {
        (self.next.node.depth, self.rank)
    }
}

// The merge is a max-heap, so the walk with the shallowest next node, and on
// a tie the earliest start, is the greatest.
impl Ord for GraphStartWalk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.order().cmp(&self.order())
    }
}

impl PartialOrd for GraphStartWalk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for GraphStartWalk {
    fn eq(&self, other: &Self) -> bool {
        self.order() == other.order()
    }
}

impl Eq for GraphStartWalk {}

enum VectorPreparation {
    Candidates(Option<Box<PullNode>>),
    Search,
    Materialize,
    Output,
    Exhausted,
}

enum VectorCandidateSource {
    Brute(contextdb_vector::mem::BoundedBruteForceCursor),
}

/// Own the request charge for one vector candidate partitioning until graph
/// execution finishes with it or exact execution transfers it into its cursor.
struct RetainedVectorCandidateGroups {
    groups: Option<contextdb_vector::mem::BoundedVectorCandidateGroups>,
    context: Arc<RequestContext>,
}

impl RetainedVectorCandidateGroups {
    fn new(
        groups: contextdb_vector::mem::BoundedVectorCandidateGroups,
        context: &Arc<RequestContext>,
    ) -> Self {
        Self {
            groups: Some(groups),
            context: Arc::clone(context),
        }
    }

    fn get(&self) -> &contextdb_vector::mem::BoundedVectorCandidateGroups {
        self.groups
            .as_ref()
            .expect("candidate groups remain owned until exact transfer")
    }

    fn take(&mut self) -> contextdb_vector::mem::BoundedVectorCandidateGroups {
        self.groups
            .take()
            .expect("candidate groups transfer to exactly one vector cursor")
    }
}

impl Drop for RetainedVectorCandidateGroups {
    fn drop(&mut self) {
        if let Some(groups) = self.groups.take() {
            self.context.release_parked(groups.into_retained_bytes());
        }
    }
}

struct RankedVectorRow {
    row_id: RowId,
    rank: f32,
    vector_score: f32,
    row: PulledRow,
}

struct VectorState {
    #[cfg(feature = "test-seams")]
    receipt: vector_search_receipt::SearchReceiptState,
    table: String,
    index: VectorIndexRef,
    query: Vec<f32>,
    _query_bytes: usize,
    k: usize,
    candidate_k: usize,
    partition_prefixes: Option<Vec<VectorPartitionKey>>,
    /// The one resolved route contract for this operation. SQL overrides are
    /// resolved against the durable column declaration before the pull state
    /// is created, so a resumed bounded read cannot reinterpret the mode.
    search_mode: contextdb_core::VectorSearchMode,
    /// AUTO retains its declared mode for disclosure while using exact work
    /// when the relational candidate source is not an indexed route.
    force_exact_for_relational_fallback: bool,
    sort_key: Option<String>,
    params: Arc<HashMap<String, Value>>,
    requested_search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
    query_expr: Expr,
    candidate_plan: Option<Box<PhysicalPlan>>,
    /// The declared index this search took its candidate identities from, when
    /// it took them as identities rather than as rows. Naming it is what lets
    /// a disagreement between a posting and the row it points at be reported
    /// as the damage it is.
    identity_source_index: Option<String>,
    restricted_candidates: bool,
    snapshot: SnapshotId,
    _schema: crate::database::VectorSchemaReadGuard,
    preparation: VectorPreparation,
    candidate_filter: bool,
    candidate_ids: Vec<u64>,
    candidate_bytes: usize,
    source: Option<VectorCandidateSource>,
    scores: Vec<(RowId, f32)>,
    score_bytes: usize,
    /// The table's column names, in order, so a row read by identity is
    /// projected exactly as the source used to project it.
    schema_columns: Vec<String>,
    /// The quantized vector columns THIS answer names. A search fetches and
    /// projects its own top-k rows, so it fills them itself -- otherwise a
    /// column the caller asked for comes back empty from a search while the
    /// same column read through a scan comes back whole.
    supplement_columns: Vec<String>,
    /// How many of the scored rows have been materialised.
    materialized: usize,
    ranked_rows: Vec<RankedVectorRow>,
    ranked_container_bytes: usize,
    output: VecDeque<PulledRow>,
    output_container_bytes: usize,
    used_hnsw: bool,
    disclosure: Option<VectorSearchDisclosure>,
    /// What this handle's own open transaction changes about the vectors this
    /// search can see. `None` whenever no transaction is open or it has
    /// touched nothing about this index -- which is every read of a committed
    /// file and every read by a handle that is not mid-write.
    vector_overlay: Option<super::TransactionVectorOverlay>,
    overlay_bytes: usize,
}

struct ProjectState {
    input: Box<PullNode>,
    columns: Vec<ProjectColumn>,
    params: Arc<HashMap<String, Value>>,
    aggregate_done: bool,
    aggregate: bool,
    pending: Option<PulledRow>,
    /// One running answer per projected aggregate, kept between cursor
    /// fetches so an aggregate too large for one page's work budget finishes
    /// on a later one.
    aggregates: Option<Vec<AggregateState>>,
    aggregate_state_bytes: usize,
}

struct SortState {
    input: Box<PullNode>,
    keys: Vec<SortKey>,
    params: Arc<HashMap<String, Value>>,
    rows: Vec<SortEntry>,
    prepared: bool,
    pending: Option<PulledRow>,
    /// What the buffered rows are holding, kept as a RUNNING total rather than
    /// summed on demand. A sort is asked what it holds twice a page, and a
    /// walk of the buffer would make that question cost what the buffer holds
    /// -- so answering it would grow with the answer still to be delivered,
    /// which is the shape paging exists to avoid. Moved at the two places the
    /// bytes themselves move: where a row is admitted into the buffer, and
    /// where it leaves.
    buffered_bytes: usize,
    container_bytes: usize,
    /// Set only below a ranked window whose rank filter caps every branch.
    rank_limit: Option<SortRankLimit>,
}

/// The highest rank any branch of a ranked window's filter can pass. A row
/// ranked past it within its partition can never be shown, so the sort may
/// give it up; the rows it keeps rank exactly as they would in the full sort.
#[derive(Clone, Copy)]
struct SortRankLimit {
    limit: usize,
    /// The leading sort keys are the window's partition columns, ascending.
    partition_len: usize,
}

struct SortEntry {
    keys: Vec<Value>,
    key_bytes: usize,
    row: PulledRow,
}

/// Window order: each key in its direction. The stable sorts that use it
/// keep rows with equal keys in arrival order.
fn sort_entry_order(keys: &[SortKey], left: &SortEntry, right: &SortEntry) -> Ordering {
    for (index, key) in keys.iter().enumerate() {
        let ordering = compare_sort_values(&left.keys[index], &right.keys[index], key.direction);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

struct WindowState {
    prepaid_construction: usize,
    descriptor: GroupDescriptorId,
    group_binding: Option<GroupBinding>,
    ceiling: Option<u64>,
    input: Box<PullNode>,
    binding: WindowBindingId,
    partition_positions: Vec<usize>,
    partition: Option<Vec<Value>>,
    partition_bytes: usize,
    rank: i64,
    pending: Option<PulledRow>,
}

impl WindowState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self
            .ceiling
            .is_some_and(|ceiling| self.rank as u64 >= ceiling)
        {
            return Ok(None);
        }
        if self.pending.is_none() {
            if let Some(ceiling) = self.ceiling {
                let remaining = ceiling
                    .checked_sub(self.rank as u64)
                    .ok_or_else(|| Error::Other("window prefix demand underflow".to_string()))?;
                if let PullKind::Scan(scan) = &mut self.input.kind {
                    if let ScanMode::Ordered { cursor, .. } = &mut scan.mode {
                        // A maintained overlay needs only the next committed
                        // comparator head, irrespective of remaining rank demand.
                        cursor.source_set_prefix_quota(Some(if scan.transaction.is_some() {
                            1
                        } else {
                            remaining
                        }));
                    }
                }
            }
            self.pending = match self.group_binding {
                Some(binding) if self.ceiling.is_some() => {
                    context.with_group_work(binding, || self.input.next(db, context))?
                }
                _ => self.input.next(db, context)?,
            };
        }
        let Some(row) = self.pending.as_ref() else {
            return Ok(None);
        };
        context.check_final_boundary()?;
        // Resolve positions once during binding; comparing a partition never
        // allocates a temporary copy of its payload or a named-column map.
        let value_at = |position: &usize| {
            row.values
                .get(*position)
                .ok_or_else(|| Error::PlanError("window partition value is missing".to_string()))
        };
        let changed = match self.partition.as_ref() {
            None => true,
            Some(previous) => self.partition_positions.iter().zip(previous).try_fold(
                false,
                |changed, (position, previous)| -> Result<bool> {
                    Ok(changed
                        || compare_sort_values(value_at(position)?, previous, SortDirection::Asc)
                            != Ordering::Equal)
                },
            )?,
        };
        if changed {
            let planned = self.partition_positions.iter().try_fold(
                checked_size_add(
                    std::mem::size_of::<Vec<Value>>(),
                    checked_size_mul(
                        self.partition_positions.len(),
                        std::mem::size_of::<Value>(),
                        "window partition",
                    )?,
                    "window partition",
                )?,
                |bytes, position| {
                    checked_size_add(
                        bytes,
                        planned_value_clone_bytes(value_at(position)?)?,
                        "window partition",
                    )
                },
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, planned)?;
            let mut partition_charge = ReservedBytes::new(context, planned);
            let mut partition = Vec::new();
            partition
                .try_reserve_exact(self.partition_positions.len())
                .map_err(|_| {
                    Error::Other("bounded window partition allocation failed".to_string())
                })?;
            for position in &self.partition_positions {
                partition.push(value_at(position)?.clone());
            }
            let actual = values_retained_bytes(&partition)?;
            // Release the temporary guard before reconciliation takes ownership.
            partition_charge.bytes = 0;
            reconcile_new_reservation(context, BoundedWorkSource::UnindexedSort, planned, actual)?;
            // Both partition allocations stay funded while binding can clone
            // the next identity. On refusal the local allocation drops before
            // its guard refunds it, leaving the old partition and pending row.
            partition_charge.bytes = actual;
            let group_binding = context.bind_group(
                self.descriptor,
                &partition,
                "<redacted>",
                if self.ceiling.is_some() {
                    crate::database::GroupRoute::Index
                } else {
                    crate::database::GroupRoute::Sort
                },
            )?;
            let old_partition = self.partition.replace(partition);
            let old_partition_bytes = std::mem::replace(&mut self.partition_bytes, actual);
            partition_charge.bytes = 0;
            self.group_binding = Some(group_binding);
            self.rank = 0;
            // Replacement transfers the new charge to this continuation;
            // refund the previous charge only after its allocation is gone.
            drop(old_partition);
            context.release(old_partition_bytes)?;
        }
        self.rank = self
            .rank
            .checked_add(1)
            .ok_or_else(|| Error::Other("window rank counter overflow".to_string()))?;
        let mut row = self
            .pending
            .take()
            .ok_or_else(|| Error::Other("window lost its ranked row".to_string()))?;
        row.window = Some(WindowRankValues::new(self.binding, self.rank));
        row.group_binding = self.group_binding;
        Ok(Some(row))
    }
}

struct LimitState {
    input: Box<PullNode>,
    count: u64,
    emitted: u64,
}

struct FilterState {
    input: Box<PullNode>,
    predicate: Expr,
    params: Arc<HashMap<String, Value>>,
    pending: Option<PulledRow>,
}

struct DistinctState {
    input: Box<PullNode>,
    seen: HashSet<Vec<u8>>,
    seen_bytes: usize,
    pending: Option<PulledRow>,
}

struct JoinState {
    left: Box<PullNode>,
    right: Box<PullNode>,
    condition: Expr,
    condition_columns: Vec<String>,
    right_column_count: usize,
    join_type: contextdb_planner::JoinType,
    params: Arc<HashMap<String, Value>>,
    right_rows: Vec<PulledRow>,
    right_pending: Option<PulledRow>,
    right_prepared: bool,
    left_row: Option<PulledRow>,
    right_position: usize,
    matched: bool,
}

struct StaticState {
    rows: VecDeque<PulledRow>,
}

struct UnionState {
    prepaid_construction: usize,
    inputs: Vec<PullNode>,
    input: usize,
    all: bool,
    seen: HashSet<Vec<u8>>,
    seen_bytes: usize,
    pending: Option<PulledRow>,
}

impl PullNode {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        match &mut self.kind {
            PullKind::Scan(state) => state.next(db, context),
            PullKind::Graph(state) => state.next(db, context),
            PullKind::Vector(state) => {
                let next = state.next(db, context);
                self.trace.vector_search = state.disclosure.clone();
                if state.used_hnsw {
                    // Runs on every row, so it has to say the same thing the
                    // second time it runs as the first. A catch-all that
                    // rewrote the label to the bare operator dropped the
                    // candidate source on the row after the one that earned
                    // the upgrade, which is how a read that scanned a table
                    // before searching it came back describing no scan at
                    // all. A label already upgraded is left exactly as it is.
                    self.trace.physical_plan = match self.trace.physical_plan {
                        "IndexScan -> VectorSearch" => "IndexScan -> HNSWSearch",
                        "Scan -> VectorSearch" => "Scan -> HNSWSearch",
                        "VectorSearch" => "HNSWSearch",
                        already_named => already_named,
                    };
                }
                next
            }
            PullKind::Project(state) => state.next(db, context),
            PullKind::Sort(state) => state.next(db, context),
            PullKind::Window(state) => state.next(db, context),
            PullKind::Limit(state) => state.next(db, context),
            PullKind::Filter(state) => state.next(db, context),
            PullKind::Distinct(state) => state.next(db, context),
            PullKind::Join(state) => state.next(db, context),
            PullKind::Static(state) => state.next(),
            PullKind::Union(state) => state.next(db, context),
        }
    }

    /// The plan word this node publishes, asked once the read has run.
    ///
    /// A wrapper node copies its input's description when it is BUILT, so a
    /// label the run itself decides -- an approximate vector index the source
    /// only discovers it can use once it touches the data -- never reaches a
    /// copy taken before the first row. Asking the node that owns the label,
    /// at the moment the answer is published, is what makes the two doors
    /// describe the same run. Each arm delegates to the same input the node
    /// copied its trace from when it was built.
    ///
    /// A sort is the one wrapper that has a word of its own: a source whose
    /// label already implies an order keeps it, and any other source is
    /// described as the sort it really became -- the same rule, from the same
    /// function, the executor applies.
    fn published_plan(&self) -> &'static str {
        match &self.kind {
            PullKind::Scan(_) | PullKind::Graph(_) | PullKind::Vector(_) | PullKind::Static(_) => {
                self.trace.physical_plan
            }
            PullKind::Project(state) => state.input.published_plan(),
            PullKind::Window(state) => {
                if state.ceiling.is_some() {
                    state.input.published_plan()
                } else {
                    "Sort"
                }
            }
            PullKind::Limit(state) => state.input.published_plan(),
            PullKind::Filter(state) => state.input.published_plan(),
            PullKind::Distinct(state) => state.input.published_plan(),
            PullKind::Join(state) => state.left.published_plan(),
            PullKind::Union(state) => state
                .inputs
                .first()
                .map_or(self.trace.physical_plan, PullNode::published_plan),
            PullKind::Sort(state) => {
                let input = state.input.published_plan();
                if super::trace_label_survives_sort(input) {
                    input
                } else {
                    "Sort"
                }
            }
        }
    }

    /// The vector source owns the actual route decision. Wrapper nodes copy a
    /// trace before the source runs, so publication walks back to that source
    /// just as [`Self::published_plan`] does for its physical-plan label.
    fn published_vector_search(&self) -> Option<&VectorSearchDisclosure> {
        match &self.kind {
            PullKind::Scan(_) | PullKind::Graph(_) | PullKind::Static(_) => {
                self.trace.vector_search.as_ref()
            }
            PullKind::Vector(_) => self.trace.vector_search.as_ref(),
            PullKind::Project(state) => state.input.published_vector_search(),
            PullKind::Sort(state) => state.input.published_vector_search(),
            PullKind::Window(state) => state.input.published_vector_search(),
            PullKind::Limit(state) => state.input.published_vector_search(),
            PullKind::Filter(state) => state.input.published_vector_search(),
            PullKind::Distinct(state) => state.input.published_vector_search(),
            PullKind::Join(state) => state.left.published_vector_search(),
            PullKind::Union(state) => state
                .inputs
                .first()
                .and_then(PullNode::published_vector_search),
        }
    }

    /// `Some` is an exact answer. `None` means filters/deduplication may need
    /// an admitted one-row lookahead before exhaustion can be published.
    fn has_more_hint(&self) -> Option<bool> {
        match &self.kind {
            PullKind::Scan(state) => state.has_more_hint(),
            PullKind::Graph(state) => state.has_more_hint(),
            PullKind::Vector(state) => state.has_more_hint(),
            PullKind::Project(state) => {
                if state.aggregate {
                    Some(!state.aggregate_done)
                } else {
                    state
                        .pending
                        .as_ref()
                        .map(|_| true)
                        .or_else(|| state.input.has_more_hint())
                }
            }
            PullKind::Window(state) => state.input.has_more_hint(),
            PullKind::Sort(state) if state.prepared => Some(!state.rows.is_empty()),
            PullKind::Sort(state) if state.pending.is_some() || !state.rows.is_empty() => {
                Some(true)
            }
            PullKind::Sort(state) => match state.input.has_more_hint() {
                Some(false) => Some(false),
                _ => None,
            },
            PullKind::Limit(state) => {
                if state.emitted >= state.count {
                    Some(false)
                } else {
                    state.input.has_more_hint()
                }
            }
            PullKind::Filter(state) => {
                if state.pending.is_some() {
                    None
                } else {
                    match state.input.has_more_hint() {
                        Some(false) => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Distinct(state) => {
                if state.pending.is_some() {
                    None
                } else {
                    match state.input.has_more_hint() {
                        Some(false) => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Join(state) => {
                if state.left_row.is_some() {
                    None
                } else {
                    match state.left.has_more_hint() {
                        Some(false) if state.right_prepared => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Static(state) => Some(!state.rows.is_empty()),
            PullKind::Union(state) => {
                if state.pending.is_some() {
                    None
                } else if state.input >= state.inputs.len() {
                    Some(false)
                } else if state.all {
                    let mut any_unknown = false;
                    for input in &state.inputs[state.input..] {
                        match input.has_more_hint() {
                            Some(true) => return Some(true),
                            Some(false) => {}
                            None => any_unknown = true,
                        }
                    }
                    (!any_unknown).then_some(false)
                } else {
                    None
                }
            }
        }
    }
}

fn bounded_size_error(operation: &str) -> BoundedExecutionError {
    Error::Other(format!(
        "bounded {operation} memory size exceeds the native address space"
    ))
    .into()
}

fn checked_size_add(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    left.checked_add(right)
        .ok_or_else(|| bounded_size_error(operation))
}

fn checked_size_mul(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    left.checked_mul(right)
        .ok_or_else(|| bounded_size_error(operation))
}

fn decimal_digits_u64(mut value: u64) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn reconcile_new_reservation(
    context: &RequestContext,
    source: BoundedWorkSource,
    planned: usize,
    actual: usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if actual > planned {
        if let Err(error) = context.reserve(source, actual - planned) {
            context.release(planned)?;
            return Err(error);
        }
        Ok(())
    } else if planned > actual {
        context.release(planned - actual)
    } else {
        Ok(())
    }
}

fn strings_capacity_bytes(
    values: &Vec<String>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    values.iter().try_fold(
        checked_size_mul(values.capacity(), std::mem::size_of::<String>(), operation)?,
        |bytes, value| checked_size_add(bytes, value.capacity(), operation),
    )
}

fn clone_strings_with_reservation(
    source: &[String],
    context: &Arc<RequestContext>,
    operation: &str,
) -> std::result::Result<(Vec<String>, usize), BoundedExecutionError> {
    let planned = source.iter().try_fold(
        checked_size_mul(source.len(), std::mem::size_of::<String>(), operation)?,
        |bytes, value| checked_size_add(bytes, value.len(), operation),
    )?;
    context.reserve(BoundedWorkSource::TableScan, planned)?;
    // Declared before the payload so every partial string and the container
    // are destroyed before an error refunds their admitted allocation.
    let mut reservation = ReservedBytes::new(context, planned);
    let mut values = Vec::new();
    if values.try_reserve_exact(source.len()).is_err() {
        return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
    }
    for source_value in source {
        let mut value = String::new();
        if value.try_reserve_exact(source_value.len()).is_err() {
            return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
        }
        value.push_str(source_value);
        values.push(value);
    }
    let actual = strings_capacity_bytes(&values, operation)?;
    if actual > planned {
        context.reserve(BoundedWorkSource::TableScan, actual - planned)?;
        reservation.bytes = actual;
    } else if planned > actual {
        context.release(planned - actual)?;
        reservation.bytes = actual;
    }
    // The returned payload now carries this reservation in its caller's
    // accounting; successful handoff must not refund its live allocation.
    reservation.bytes = 0;
    Ok((values, actual))
}

fn values_capacity_allocation_bytes(
    values: &Vec<Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        values.capacity(),
        std::mem::size_of::<Value>(),
        "result row values",
    )?;
    for value in values {
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "result row value")?;
    }
    Ok(bytes)
}

fn values_retained_bytes(values: &Vec<Value>) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        std::mem::size_of::<Vec<Value>>(),
        values_capacity_allocation_bytes(values)?,
        "result row",
    )
}

fn value_refs_retained_bytes<'a>(
    values: impl IntoIterator<Item = &'a Value>,
    count: usize,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<Vec<Value>>(),
        checked_size_mul(count, std::mem::size_of::<Value>(), "projected values")?,
        "projected row",
    )?;
    for value in values {
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "projected value")?;
    }
    Ok(bytes)
}

pub(crate) fn cursor_statement_bytes(
    sql: &str,
    params: &HashMap<String, Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        sql.len(),
        params_retained_bytes(params)?,
        "cursor continuation statement",
    )
}

fn params_retained_bytes(
    params: &HashMap<String, Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<HashMap<String, Value>>(),
        checked_size_mul(
            params.capacity(),
            std::mem::size_of::<(String, Value)>(),
            "parameter capacity",
        )?,
        "parameters",
    )?;
    for (name, value) in params {
        bytes = checked_size_add(bytes, name.capacity(), "parameter name")?;
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "parameter value")?;
    }
    Ok(bytes)
}

fn optional_string_capacity(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}

fn literal_capacity_bytes(literal: &Literal) -> std::result::Result<usize, BoundedExecutionError> {
    match literal {
        Literal::Text(value) => Ok(value.capacity()),
        Literal::Vector(values) => checked_size_mul(
            values.capacity(),
            std::mem::size_of::<f32>(),
            "literal vector capacity",
        ),
        Literal::Null | Literal::Bool(_) | Literal::Integer(_) | Literal::Real(_) => Ok(0),
    }
}

fn expr_capacity_bytes(expr: &Expr) -> std::result::Result<usize, BoundedExecutionError> {
    let boxed_expr = std::mem::size_of::<Expr>();
    match expr {
        Expr::RowNumber(window) => window_capacity_bytes(window),
        Expr::WindowRank(_) => Ok(0),
        Expr::Column(column) => checked_size_add(
            optional_string_capacity(&column.table),
            column.column.capacity(),
            "expression column",
        ),
        Expr::Literal(literal) => literal_capacity_bytes(literal),
        Expr::Parameter(parameter) => Ok(parameter.capacity()),
        Expr::Row(elements) => {
            let mut bytes = checked_size_mul(elements.capacity(), boxed_expr, "row value")?;
            for element in elements {
                bytes = checked_size_add(bytes, expr_capacity_bytes(element)?, "row value")?;
            }
            Ok(bytes)
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            let children = checked_size_add(
                expr_capacity_bytes(left)?,
                expr_capacity_bytes(right)?,
                "expression children",
            )?;
            checked_size_add(
                checked_size_mul(2, boxed_expr, "expression boxes")?,
                children,
                "expression boxes",
            )
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            checked_size_add(boxed_expr, expr_capacity_bytes(operand)?, "expression box")
        }
        Expr::FunctionCall { name, args } => {
            let mut bytes = checked_size_add(
                name.capacity(),
                checked_size_mul(args.capacity(), boxed_expr, "function arguments")?,
                "function expression",
            )?;
            for argument in args {
                bytes =
                    checked_size_add(bytes, expr_capacity_bytes(argument)?, "function expression")?;
            }
            Ok(bytes)
        }
        Expr::RowVectorSource { table, column, key } => {
            let strings =
                checked_size_add(table.capacity(), column.capacity(), "ROW_VECTOR expression")?;
            checked_size_add(
                strings,
                checked_size_add(boxed_expr, expr_capacity_bytes(key)?, "ROW_VECTOR key")?,
                "ROW_VECTOR expression",
            )
        }
        Expr::InList { expr, list, .. } => {
            let mut bytes =
                checked_size_add(boxed_expr, expr_capacity_bytes(expr)?, "IN expression")?;
            bytes = checked_size_add(
                bytes,
                checked_size_mul(list.capacity(), boxed_expr, "IN-list capacity")?,
                "IN expression",
            )?;
            for item in list {
                bytes = checked_size_add(bytes, expr_capacity_bytes(item)?, "IN expression")?;
            }
            Ok(bytes)
        }
        Expr::Like { expr, pattern, .. } => {
            let children = checked_size_add(
                expr_capacity_bytes(expr)?,
                expr_capacity_bytes(pattern)?,
                "LIKE expression",
            )?;
            checked_size_add(
                checked_size_mul(2, boxed_expr, "LIKE expression boxes")?,
                children,
                "LIKE expression",
            )
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let left = checked_size_add(
                boxed_expr,
                expr_capacity_bytes(expr)?,
                "subquery expression",
            )?;
            checked_size_add(
                left,
                checked_size_add(
                    std::mem::size_of::<contextdb_parser::ast::SelectBody>(),
                    select_body_capacity_bytes(subquery)?,
                    "subquery body",
                )?,
                "subquery expression",
            )
        }
    }
}

fn select_body_capacity_bytes(
    body: &contextdb_parser::ast::SelectBody,
) -> std::result::Result<usize, BoundedExecutionError> {
    use contextdb_parser::ast::{FromItem, SelectColumn};

    let mut bytes = checked_size_mul(
        body.columns.capacity(),
        std::mem::size_of::<SelectColumn>(),
        "subquery columns",
    )?;
    for column in &body.columns {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "subquery")?;
        bytes = checked_size_add(bytes, optional_string_capacity(&column.alias), "subquery")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.from.capacity(),
            std::mem::size_of::<FromItem>(),
            "subquery sources",
        )?,
        "subquery",
    )?;
    for source in &body.from {
        match source {
            FromItem::Table { name, alias } => {
                bytes = checked_size_add(bytes, name.capacity(), "subquery source")?;
                bytes =
                    checked_size_add(bytes, optional_string_capacity(alias), "subquery source")?;
            }
            FromItem::GraphTable {
                graph_name,
                match_clause,
                columns,
            } => {
                bytes = checked_size_add(bytes, graph_name.capacity(), "subquery graph")?;
                bytes = checked_size_add(
                    bytes,
                    match_clause_capacity_bytes(match_clause)?,
                    "subquery graph",
                )?;
                bytes = checked_size_add(
                    bytes,
                    checked_size_mul(
                        columns.capacity(),
                        std::mem::size_of::<contextdb_parser::ast::GraphTableColumn>(),
                        "subquery graph columns",
                    )?,
                    "subquery graph",
                )?;
                for column in columns {
                    bytes = checked_size_add(
                        bytes,
                        expr_capacity_bytes(&column.expr)?,
                        "subquery graph column",
                    )?;
                    bytes =
                        checked_size_add(bytes, column.alias.capacity(), "subquery graph column")?;
                }
            }
        }
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.joins.capacity(),
            std::mem::size_of::<contextdb_parser::ast::JoinClause>(),
            "subquery joins",
        )?,
        "subquery",
    )?;
    for join in &body.joins {
        bytes = checked_size_add(bytes, join.table.capacity(), "subquery join")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&join.alias),
            "subquery join",
        )?;
        bytes = checked_size_add(bytes, expr_capacity_bytes(&join.on)?, "subquery join")?;
    }
    if let Some(filter) = body.where_clause.as_ref() {
        bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "subquery filter")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.group_by.capacity(),
            std::mem::size_of::<Expr>(),
            "subquery grouping",
        )?,
        "subquery",
    )?;
    for item in &body.group_by {
        bytes = checked_size_add(bytes, expr_capacity_bytes(item)?, "subquery grouping")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.order_by.capacity(),
            std::mem::size_of::<contextdb_parser::ast::OrderByItem>(),
            "subquery ordering",
        )?,
        "subquery",
    )?;
    for ordering in &body.order_by {
        bytes = checked_size_add(
            bytes,
            expr_capacity_bytes(&ordering.expr)?,
            "subquery ordering",
        )?;
    }
    checked_size_add(bytes, optional_string_capacity(&body.use_rank), "subquery")
}

fn match_clause_capacity_bytes(
    clause: &contextdb_parser::ast::MatchClause,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = optional_string_capacity(&clause.graph_name);
    bytes = checked_size_add(
        bytes,
        node_pattern_capacity_bytes(&clause.pattern.start)?,
        "MATCH clause",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            clause.pattern.edges.capacity(),
            std::mem::size_of::<contextdb_parser::ast::EdgeStep>(),
            "MATCH edges",
        )?,
        "MATCH clause",
    )?;
    for edge in &clause.pattern.edges {
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&edge.edge_type),
            "MATCH edge",
        )?;
        bytes = checked_size_add(bytes, optional_string_capacity(&edge.alias), "MATCH edge")?;
        bytes = checked_size_add(
            bytes,
            node_pattern_capacity_bytes(&edge.target)?,
            "MATCH edge",
        )?;
    }
    if let Some(filter) = clause.where_clause.as_ref() {
        bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "MATCH filter")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            clause.return_cols.capacity(),
            std::mem::size_of::<contextdb_parser::ast::ReturnCol>(),
            "MATCH return columns",
        )?,
        "MATCH clause",
    )?;
    for column in &clause.return_cols {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "MATCH return")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.alias),
            "MATCH return",
        )?;
    }
    Ok(bytes)
}

fn node_pattern_capacity_bytes(
    node: &contextdb_parser::ast::NodePattern,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        node.alias.capacity(),
        optional_string_capacity(&node.label),
        "MATCH node",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            node.properties.capacity(),
            std::mem::size_of::<(String, Expr)>(),
            "MATCH properties",
        )?,
        "MATCH node",
    )?;
    for (name, value) in &node.properties {
        bytes = checked_size_add(bytes, name.capacity(), "MATCH property")?;
        bytes = checked_size_add(bytes, expr_capacity_bytes(value)?, "MATCH property")?;
    }
    Ok(bytes)
}

fn ctes_capacity_bytes(ctes: &Vec<Cte>) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(ctes.capacity(), std::mem::size_of::<Cte>(), "CTE plans")?;
    for cte in ctes {
        match cte {
            Cte::SqlCte { name, query } => {
                bytes = checked_size_add(bytes, name.capacity(), "CTE plan")?;
                bytes = checked_size_add(bytes, select_body_capacity_bytes(query)?, "CTE plan")?;
            }
            Cte::MatchCte { name, match_clause } => {
                bytes = checked_size_add(bytes, name.capacity(), "CTE plan")?;
                bytes = checked_size_add(
                    bytes,
                    match_clause_capacity_bytes(match_clause)?,
                    "CTE plan",
                )?;
            }
        }
    }
    Ok(bytes)
}

fn statement_capacity_bytes(
    statement: &Statement,
) -> std::result::Result<usize, BoundedExecutionError> {
    match statement {
        Statement::Select(select) => checked_size_add(
            ctes_capacity_bytes(&select.ctes)?,
            select_body_capacity_bytes(&select.body)?,
            "parsed SELECT",
        ),
        _ => Ok(0),
    }
}

fn physical_plan_capacity_bytes(
    plan: &PhysicalPlan,
) -> std::result::Result<usize, BoundedExecutionError> {
    let boxed_plan = std::mem::size_of::<PhysicalPlan>();
    match plan {
        PhysicalPlan::WindowRank { input, window, .. } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "window input",
            )?,
            window_capacity_bytes(window)?,
            "window plan",
        ),
        PhysicalPlan::Scan {
            table,
            alias,
            filter,
        } => {
            let mut bytes = checked_size_add(
                table.capacity(),
                optional_string_capacity(alias),
                "scan plan",
            )?;
            if let Some(filter) = filter.as_ref() {
                bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "scan plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
        } => {
            let mut bytes = checked_size_add(table.capacity(), index.capacity(), "index plan")?;
            bytes = checked_size_add(
                bytes,
                bound_value_capacity_bytes(&range.lower)?,
                "index plan",
            )?;
            bytes = checked_size_add(
                bytes,
                bound_value_capacity_bytes(&range.upper)?,
                "index plan",
            )?;
            if let Some(value) = range.equality.as_ref() {
                bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "index plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::GraphBfs {
            start_alias,
            start_expr,
            start_candidates,
            filter_ctes,
            steps,
            filter,
        } => {
            let mut bytes = checked_size_add(
                start_alias.capacity(),
                expr_capacity_bytes(start_expr)?,
                "graph plan",
            )?;
            if let Some(candidates) = start_candidates.as_deref() {
                bytes = checked_size_add(
                    bytes,
                    checked_size_add(
                        boxed_plan,
                        physical_plan_capacity_bytes(candidates)?,
                        "graph candidate plan",
                    )?,
                    "graph plan",
                )?;
            }
            bytes = checked_size_add(bytes, ctes_capacity_bytes(filter_ctes)?, "graph plan")?;
            bytes = checked_size_add(bytes, graph_steps_capacity_bytes(steps)?, "graph plan")?;
            if let Some(filter) = filter.as_ref() {
                bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "graph plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            candidates,
            sort_key,
            materialized_columns,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            candidates,
            sort_key,
            materialized_columns,
            ..
        } => {
            let mut bytes = checked_size_add(table.capacity(), column.capacity(), "vector plan")?;
            bytes = checked_size_add(bytes, expr_capacity_bytes(query_expr)?, "vector plan")?;
            bytes = checked_size_add(bytes, optional_string_capacity(sort_key), "vector plan")?;
            if let Some(materialized_columns) = materialized_columns {
                bytes = checked_size_add(
                    bytes,
                    string_vec_capacity_bytes(materialized_columns, "vector materialized columns")?,
                    "vector plan",
                )?;
            }
            if let Some(candidates) = candidates.as_deref() {
                bytes = checked_size_add(
                    bytes,
                    checked_size_add(
                        boxed_plan,
                        physical_plan_capacity_bytes(candidates)?,
                        "vector candidate plan",
                    )?,
                    "vector plan",
                )?;
            }
            Ok(bytes)
        }
        PhysicalPlan::Filter { input, predicate } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "filter input plan",
            )?,
            expr_capacity_bytes(predicate)?,
            "filter plan",
        ),
        PhysicalPlan::Project { input, columns } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "projection input plan",
            )?,
            project_columns_capacity_bytes(columns)?,
            "projection plan",
        ),
        PhysicalPlan::GroupedCount {
            input,
            group_by,
            columns,
        } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "grouped count input plan",
            )?,
            checked_size_add(
                grouped_count::group_by_capacity_bytes(group_by)?,
                project_columns_capacity_bytes(columns)?,
                "grouped count plan",
            )?,
            "grouped count plan",
        ),
        PhysicalPlan::Distinct { input } | PhysicalPlan::Limit { input, .. } => checked_size_add(
            boxed_plan,
            physical_plan_capacity_bytes(input)?,
            "unary plan",
        ),
        PhysicalPlan::Join {
            left,
            right,
            condition,
            left_alias,
            right_alias,
            ..
        } => {
            let mut bytes = checked_size_add(
                checked_size_add(boxed_plan, physical_plan_capacity_bytes(left)?, "join plan")?,
                checked_size_add(
                    boxed_plan,
                    physical_plan_capacity_bytes(right)?,
                    "join plan",
                )?,
                "join plan",
            )?;
            bytes = checked_size_add(bytes, expr_capacity_bytes(condition)?, "join plan")?;
            bytes = checked_size_add(bytes, optional_string_capacity(left_alias), "join plan")?;
            checked_size_add(bytes, optional_string_capacity(right_alias), "join plan")
        }
        PhysicalPlan::Sort { input, keys } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "sort input plan",
            )?,
            sort_keys_capacity_bytes(keys)?,
            "sort plan",
        ),
        PhysicalPlan::MaterializeCte { name, input } => checked_size_add(
            name.capacity(),
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "CTE input plan",
            )?,
            "CTE plan",
        ),
        PhysicalPlan::CteRef { name } => Ok(name.capacity()),
        PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => {
            let mut bytes = checked_size_mul(inputs.capacity(), boxed_plan, "multi-input plan")?;
            for input in inputs {
                bytes = checked_size_add(
                    bytes,
                    physical_plan_capacity_bytes(input)?,
                    "multi-input plan",
                )?;
            }
            Ok(bytes)
        }
        PhysicalPlan::ShowVectorPartitions { table, column, .. } => checked_size_add(
            optional_string_capacity(table),
            optional_string_capacity(column),
            "vector partition inspection plan",
        ),
        _ => Ok(0),
    }
}

fn string_vec_capacity_bytes(
    values: &Vec<String>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    strings_capacity_bytes(values, operation)
}

fn table_meta_capacity_bytes(
    meta: &TableMeta,
) -> std::result::Result<usize, BoundedExecutionError> {
    use contextdb_core::{PropagationRule, ScopeLabelKind};

    let mut bytes = checked_size_mul(
        meta.columns.capacity(),
        std::mem::size_of::<contextdb_core::ColumnDef>(),
        "table metadata columns",
    )?;
    for column in &meta.columns {
        bytes = checked_size_add(bytes, column.name.capacity(), "table metadata column")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.default),
            "table metadata column",
        )?;
        if let Some(reference) = column.references.as_ref() {
            bytes = checked_size_add(bytes, reference.table.capacity(), "foreign key")?;
            bytes = checked_size_add(bytes, reference.column.capacity(), "foreign key")?;
        }
        if let Some(rank) = column.rank_policy.as_ref() {
            for value in [
                &rank.joined_table,
                &rank.joined_column,
                &rank.anchor_column,
                &rank.sort_key,
                &rank.formula,
                &rank.protected_index,
            ] {
                bytes = checked_size_add(bytes, value.capacity(), "rank policy")?;
            }
        }
        if let Some(partition_key_columns) = column.partition_key_columns.as_ref() {
            bytes = checked_size_add(
                bytes,
                string_vec_capacity_bytes(partition_key_columns, "vector partition declaration")?,
                "table metadata column",
            )?;
        }
        if let Some(scope) = column.scope_label.as_ref() {
            match scope {
                ScopeLabelKind::Simple { write_labels } => {
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(write_labels, "scope labels")?,
                        "scope labels",
                    )?;
                }
                ScopeLabelKind::Split {
                    read_labels,
                    write_labels,
                } => {
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(read_labels, "scope labels")?,
                        "scope labels",
                    )?;
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(write_labels, "scope labels")?,
                        "scope labels",
                    )?;
                }
            }
        }
        if let Some(acl) = column.acl_ref.as_ref() {
            bytes = checked_size_add(bytes, acl.ref_table.capacity(), "ACL reference")?;
            bytes = checked_size_add(bytes, acl.ref_column.capacity(), "ACL reference")?;
        }
    }
    if let Some(machine) = meta.state_machine.as_ref() {
        bytes = checked_size_add(bytes, machine.column.capacity(), "state machine")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                machine.transitions.capacity(),
                std::mem::size_of::<(String, Vec<String>)>(),
                "state-machine transitions",
            )?,
            "state machine",
        )?;
        for (state, targets) in &machine.transitions {
            bytes = checked_size_add(bytes, state.capacity(), "state machine")?;
            bytes = checked_size_add(
                bytes,
                string_vec_capacity_bytes(targets, "state-machine targets")?,
                "state machine",
            )?;
        }
    }
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&meta.dag_edge_types, "DAG edge types")?,
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.unique_constraints.capacity(),
            std::mem::size_of::<Vec<String>>(),
            "unique constraints",
        )?,
        "table metadata",
    )?;
    for constraint in &meta.unique_constraints {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(constraint, "unique constraint")?,
            "unique constraint",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&meta.natural_key_column),
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.propagation_rules.capacity(),
            std::mem::size_of::<PropagationRule>(),
            "propagation rules",
        )?,
        "table metadata",
    )?;
    for rule in &meta.propagation_rules {
        match rule {
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                ..
            } => {
                for value in [
                    fk_column,
                    referenced_table,
                    referenced_column,
                    trigger_state,
                    target_state,
                ] {
                    bytes = checked_size_add(bytes, value.capacity(), "propagation rule")?;
                }
            }
            PropagationRule::Edge {
                edge_type,
                trigger_state,
                target_state,
                ..
            } => {
                for value in [edge_type, trigger_state, target_state] {
                    bytes = checked_size_add(bytes, value.capacity(), "propagation rule")?;
                }
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                bytes = checked_size_add(bytes, trigger_state.capacity(), "propagation rule")?;
            }
        }
    }
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&meta.expires_column),
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.indexes.capacity(),
            std::mem::size_of::<contextdb_core::IndexDecl>(),
            "table indexes",
        )?,
        "table metadata",
    )?;
    for index in &meta.indexes {
        bytes = checked_size_add(bytes, index.name.capacity(), "table index")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                index.columns.capacity(),
                std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
                "table index columns",
            )?,
            "table index",
        )?;
        for (column, _) in &index.columns {
            bytes = checked_size_add(bytes, column.capacity(), "table index column")?;
        }
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.composite_foreign_keys.capacity(),
            std::mem::size_of::<contextdb_core::CompositeForeignKey>(),
            "composite foreign keys",
        )?,
        "table metadata",
    )?;
    for foreign_key in &meta.composite_foreign_keys {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&foreign_key.child_columns, "foreign-key columns")?,
            "composite foreign key",
        )?;
        bytes = checked_size_add(
            bytes,
            foreign_key.parent_table.capacity(),
            "composite foreign key",
        )?;
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&foreign_key.parent_columns, "foreign-key columns")?,
            "composite foreign key",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&meta.primary_key_columns, "primary-key columns")?,
        "table metadata",
    )?;
    Ok(bytes)
}

/// What the ONE copy of a pulled row this read is holding really costs.
///
/// The store charges before it clones, using the retained-representation
/// estimate -- it has to, because a charge that arrives after the allocation
/// guarantees nothing. That estimate covers the worst any consumer retains
/// (a graph edge keeps a property map in both adjacency directions), and a
/// read is not that consumer: it holds one copy. So once the row is in hand
/// the reservation is trued up to this, which is measured off the row itself.
fn pulled_row_retained_bytes(
    row: &VersionedRow,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<VersionedRow>(),
        checked_size_mul(
            row.values.capacity(),
            std::mem::size_of::<(String, Value)>(),
            "pulled row entries",
        )?,
        "pulled row",
    )?;
    for (column, value) in &row.values {
        bytes = checked_size_add(bytes, column.capacity(), "pulled row column")?;
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "pulled row value")?;
    }
    Ok(bytes)
}

fn value_capacity_bytes(value: &Value) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        Value::Text(value) => Ok(value.capacity()),
        Value::Vector(values) => checked_size_mul(
            values.capacity(),
            std::mem::size_of::<f32>(),
            "value vector capacity",
        ),
        Value::Json(value) => Ok(contextdb_core::types::json_retained_heap_bytes(value)),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

fn bound_value_capacity_bytes(
    bound: &std::ops::Bound<Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    match bound {
        std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) => {
            value_capacity_bytes(value)
        }
        std::ops::Bound::Unbounded => Ok(0),
    }
}

fn index_pick_capacity_bytes(
    pick: &IndexPick,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<IndexPick>(),
        pick.name.capacity(),
        "owned index pick",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            pick.columns.capacity(),
            std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
            "index-pick columns",
        )?,
        "index pick",
    )?;
    for (column, _) in &pick.columns {
        bytes = checked_size_add(bytes, column.capacity(), "index-pick column")?;
    }
    bytes = checked_size_add(bytes, pick.pushed_column.capacity(), "index pick")?;
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&pick.pushed_columns, "pushed index columns")?,
        "index pick",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            pick.suffix_shapes.capacity(),
            std::mem::size_of::<IndexPredicateShape>(),
            "index suffix shapes",
        )?,
        "index pick",
    )?;
    for shape in &pick.suffix_shapes {
        bytes = checked_size_add(
            bytes,
            index_predicate_shape_capacity_bytes(shape)?,
            "index suffix shape",
        )?;
    }
    if let Some((lower, upper)) = &pick.trailing_range {
        for bound in [lower, upper] {
            if let Bound::Included(key) | Bound::Excluded(key) = bound {
                bytes = checked_size_add(
                    bytes,
                    checked_size_mul(
                        key.capacity(),
                        std::mem::size_of::<DirectedValue>(),
                        "index trailing range",
                    )?,
                    "index trailing range",
                )?;
                for value in key {
                    let value = match value {
                        DirectedValue::Asc(TotalOrdAsc(value))
                        | DirectedValue::Desc(TotalOrdDesc(value)) => value,
                    };
                    bytes =
                        checked_size_add(bytes, value.estimated_bytes(), "index trailing range")?;
                }
            }
        }
    }
    if let Some(tuples) = pick.tuple_prefix() {
        for tuple in tuples {
            bytes = checked_size_add(
                bytes,
                checked_size_mul(
                    tuple.capacity(),
                    std::mem::size_of::<Value>(),
                    "index tuple prefix",
                )?,
                "index tuple prefix",
            )?;
            for value in tuple {
                bytes = checked_size_add(bytes, value.estimated_bytes(), "index tuple prefix")?;
            }
        }
    }
    checked_size_add(
        bytes,
        index_predicate_shape_capacity_bytes(&pick.shape)?,
        "index predicate",
    )
}

fn index_predicate_shape_capacity_bytes(
    shape: &IndexPredicateShape,
) -> std::result::Result<usize, BoundedExecutionError> {
    match shape {
        IndexPredicateShape::Equality(value) | IndexPredicateShape::NotEqual(value) => {
            value_capacity_bytes(value)
        }
        IndexPredicateShape::Range { lower, upper } => checked_size_add(
            bound_value_capacity_bytes(lower)?,
            bound_value_capacity_bytes(upper)?,
            "index predicate",
        ),
        IndexPredicateShape::InList(values) => {
            let mut bytes = checked_size_mul(
                values.capacity(),
                std::mem::size_of::<Value>(),
                "index IN values",
            )?;
            for value in values.iter() {
                bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "index predicate")?;
            }
            Ok(bytes)
        }
        IndexPredicateShape::IsNull | IndexPredicateShape::IsNotNull => Ok(0),
    }
}

fn vector_search_disclosure_capacity_bytes(
    disclosure: &VectorSearchDisclosure,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = disclosure.auto_index_at_source.capacity();
    bytes = checked_size_add(
        bytes,
        strings_capacity_bytes(
            &disclosure.partition_key_columns,
            "trace vector partition-key columns",
        )?,
        "trace vector disclosure",
    )?;
    for value in [
        disclosure.fallback.as_ref(),
        disclosure.refusal.as_ref(),
        disclosure.recovery.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        bytes = checked_size_add(bytes, value.capacity(), "trace vector disclosure")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            disclosure.partition_hnsw.capacity(),
            std::mem::size_of::<crate::database::VectorPartitionHnswDisclosure>(),
            "trace vector partition disclosures",
        )?,
        "trace vector disclosure",
    )?;
    for partition in &disclosure.partition_hnsw {
        bytes = checked_size_add(
            bytes,
            partition.partition.capacity(),
            "trace vector partition disclosure",
        )?;
        bytes = checked_size_add(
            bytes,
            partition.ef_search_source.capacity(),
            "trace vector partition disclosure",
        )?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&partition.reason),
            "trace vector partition disclosure",
        )?;
    }
    Ok(bytes)
}

fn group_disclosure_capacity_bytes(
    groups: &[crate::database::GroupDisclosure],
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        groups.len(),
        std::mem::size_of::<crate::database::GroupDisclosure>(),
        "trace groups",
    )?;
    for group in groups {
        bytes = checked_size_add(bytes, group.partition_key.capacity(), "trace group key")?;
    }
    Ok(bytes)
}

pub(crate) fn query_trace_capacity_bytes(
    trace: &QueryTrace,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = trace.index_used.as_ref().map_or(0, String::capacity);
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            trace.groups.len(),
            std::mem::size_of::<crate::database::GroupDisclosure>(),
            "trace groups",
        )?,
        "query trace",
    )?;
    for group in &trace.groups {
        bytes = checked_size_add(bytes, group.partition_key.capacity(), "trace group key")?;
    }
    if trace.predicates_pushed.spilled() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                trace.predicates_pushed.capacity(),
                std::mem::size_of::<std::borrow::Cow<'static, str>>(),
                "trace predicates",
            )?,
            "query trace",
        )?;
    }
    for predicate in &trace.predicates_pushed {
        if let std::borrow::Cow::Owned(predicate) = predicate {
            bytes = checked_size_add(bytes, predicate.capacity(), "trace predicate")?;
        }
    }
    if trace.indexes_considered.spilled() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                trace.indexes_considered.capacity(),
                std::mem::size_of::<crate::database::IndexCandidate>(),
                "trace index candidates",
            )?,
            "query trace",
        )?;
    }
    for candidate in &trace.indexes_considered {
        bytes = checked_size_add(bytes, candidate.name.capacity(), "trace index candidate")?;
        if let std::borrow::Cow::Owned(reason) = &candidate.rejected_reason {
            bytes = checked_size_add(bytes, reason.capacity(), "trace rejection")?;
        }
    }
    if let Some(index) = trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, index.table.capacity(), "trace vector source")?;
        bytes = checked_size_add(bytes, index.column.capacity(), "trace vector source")?;
    }
    if let Some(disclosure) = trace.vector_search.as_ref() {
        bytes = checked_size_add(
            bytes,
            vector_search_disclosure_capacity_bytes(disclosure)?,
            "query trace",
        )?;
    }
    if let Some(count) = trace.maintained_count.as_ref() {
        use crate::maintained_counts::MaintainedCountDisclosure::*;
        let (table, columns, lists, list_capacity) = match count.as_ref() {
            Used { table, columns } | DeclaredNotServed { table, columns } => {
                (table, columns, &[][..], 0)
            }
            NotDeclared {
                table,
                columns,
                already_declared,
            } => (
                table,
                columns,
                already_declared.as_slice(),
                already_declared.capacity(),
            ),
        };
        bytes = checked_size_add(bytes, std::mem::size_of_val(count.as_ref()), "trace count")?;
        bytes = checked_size_add(bytes, table.capacity(), "trace count table")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                list_capacity,
                std::mem::size_of::<Vec<(String, contextdb_core::SortDirection)>>(),
                "trace count lists",
            )?,
            "trace count",
        )?;
        for columns in std::iter::once(columns).chain(lists.iter()) {
            bytes = checked_size_add(
                bytes,
                checked_size_mul(
                    columns.capacity(),
                    std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
                    "trace count columns",
                )?,
                "trace count",
            )?;
            for (name, _) in columns {
                bytes = checked_size_add(bytes, name.capacity(), "trace count name")?;
            }
        }
    }
    Ok(bytes)
}

fn graph_steps_capacity_bytes(
    steps: &Vec<GraphStepPlan>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        steps.capacity(),
        std::mem::size_of::<GraphStepPlan>(),
        "graph steps",
    )?;
    for step in steps {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&step.edge_types, "graph edge types")?,
            "graph step",
        )?;
        bytes = checked_size_add(bytes, step.target_alias.capacity(), "graph step")?;
    }
    Ok(bytes)
}

fn project_columns_capacity_bytes(
    columns: &Vec<ProjectColumn>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        columns.capacity(),
        std::mem::size_of::<ProjectColumn>(),
        "projection columns",
    )?;
    for column in columns {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "projection")?;
        bytes = checked_size_add(bytes, optional_string_capacity(&column.alias), "projection")?;
    }
    Ok(bytes)
}

fn sort_keys_capacity_bytes(
    keys: &Vec<SortKey>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(keys.capacity(), std::mem::size_of::<SortKey>(), "sort keys")?;
    for key in keys {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&key.expr)?, "sort key")?;
    }
    Ok(bytes)
}

fn boxed_pull_bytes(input: &PullNode) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        std::mem::size_of::<PullNode>(),
        pull_continuation_bytes(input)?,
        "boxed pull continuation",
    )
}

/// What a paused read is holding, part by part.
///
/// The settlement compares ONE number against the reader's ceiling, so a term
/// that is missing from it is memory the reader holds and is never asked to
/// pay for -- and from outside, a total that is too small looks exactly like a
/// read that is genuinely small. Naming the parts is what lets a caller say
/// which one is missing instead of only that the sum is wrong.
///
/// `total` is the production figure the cursor is settled to, taken from the
/// same walk the settlement uses, so a named part can never disagree with it:
/// `named` lists the parts a caller can ask about by name and `unnamed` is
/// whatever is left, so the parts always add up to the total.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuationBytes {
    pub total: usize,
    pub named: Vec<(&'static str, usize)>,
    pub unnamed: usize,
}

#[cfg(feature = "test-seams")]
impl ContinuationBytes {
    /// What the part by this name holds, or zero when this read holds no such
    /// part. A caller asking about a part a read does not have is asking a
    /// fair question and gets a number, not a panic.
    pub fn part(&self, name: &str) -> usize {
        self.named
            .iter()
            .find(|(part, _)| *part == name)
            .map_or(0, |(_, bytes)| *bytes)
    }
}

/// The name of the term every index run a list predicate still owes is
/// counted under.
#[cfg(feature = "test-seams")]
pub const PENDING_INDEX_RUNS: &str = "pending index runs";

#[cfg(feature = "test-seams")]
fn continuation_bytes_breakdown(
    node: &PullNode,
) -> std::result::Result<ContinuationBytes, BoundedExecutionError> {
    let mut named: Vec<(&'static str, usize)> = Vec::new();
    let mut pending_runs = 0_usize;
    let mut index_picks = 0_usize;
    let mut table_shapes = 0_usize;
    let mut filters = 0_usize;
    collect_continuation_parts(
        node,
        &mut pending_runs,
        &mut index_picks,
        &mut table_shapes,
        &mut filters,
    )?;
    named.push((PENDING_INDEX_RUNS, pending_runs));
    named.push(("index picks", index_picks));
    named.push(("table shapes", table_shapes));
    named.push(("row filters", filters));
    let total = pull_continuation_bytes(node)?;
    let accounted: usize = named.iter().map(|(_, bytes)| *bytes).sum();
    Ok(ContinuationBytes {
        total,
        named,
        unnamed: total.saturating_sub(accounted),
    })
}

/// Walk every source in the tree and add up the parts by name. The scan arm
/// reads the same fields the settlement reads, so a part named here is the
/// part the settlement counted.
#[cfg(feature = "test-seams")]
fn collect_continuation_parts(
    node: &PullNode,
    pending_runs: &mut usize,
    index_picks: &mut usize,
    table_shapes: &mut usize,
    filters: &mut usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if let PullKind::Scan(state) = &node.kind {
        *table_shapes = table_shapes.saturating_add(table_meta_capacity_bytes(&state.meta)?);
        if let Some(filter) = state.filter.as_ref() {
            *filters = filters.saturating_add(expr_capacity_bytes(filter)?);
        }
        match &state.mode {
            ScanMode::Index { pick, cursor, .. } => {
                *index_picks = index_picks.saturating_add(index_pick_capacity_bytes(pick)?);
                *pending_runs = pending_runs.saturating_add(cursor.pending_run_bytes());
            }
            ScanMode::Ordered { cursor, .. } => {
                *pending_runs = pending_runs.saturating_add(cursor.pending_run_bytes());
            }
            ScanMode::Physical { .. } | ScanMode::Exact { .. } | ScanMode::Empty => {}
        }
        if let Some(pick) = state.residual_pick.as_ref() {
            *index_picks = index_picks.saturating_add(index_pick_capacity_bytes(pick)?);
        }
    }
    for input in pull_node_inputs(&node.kind) {
        collect_continuation_parts(input, pending_runs, index_picks, table_shapes, filters)?;
    }
    Ok(())
}

/// Every node this one reads from. One place says what a source's children
/// are, so a walk over the tree cannot miss a branch the settlement counts.
#[cfg(feature = "test-seams")]
fn pull_node_inputs(kind: &PullKind) -> Vec<&PullNode> {
    match kind {
        PullKind::Graph(state) => state.start_candidates.as_deref().into_iter().collect(),
        PullKind::Vector(state) => match &state.preparation {
            VectorPreparation::Candidates(Some(candidates)) => vec![candidates.as_ref()],
            _ => Vec::new(),
        },
        PullKind::Project(state) => vec![state.input.as_ref()],
        PullKind::Sort(state) => vec![state.input.as_ref()],
        PullKind::Window(state) => vec![state.input.as_ref()],
        PullKind::Limit(state) => vec![state.input.as_ref()],
        PullKind::Filter(state) => vec![state.input.as_ref()],
        PullKind::Distinct(state) => vec![state.input.as_ref()],
        PullKind::Join(state) => vec![state.left.as_ref(), state.right.as_ref()],
        PullKind::Union(state) => state.inputs.iter().collect(),
        PullKind::Scan(_) | PullKind::Static(_) => Vec::new(),
    }
}

fn pull_continuation_bytes(node: &PullNode) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = strings_capacity_bytes(&node.columns, "pull output columns")?;
    bytes = checked_size_add(
        bytes,
        query_trace_capacity_bytes(&node.trace)?,
        "pull continuation",
    )?;
    let state_bytes = match &node.kind {
        PullKind::Scan(state) => {
            let mut state_bytes = checked_size_add(
                state.table.capacity(),
                table_meta_capacity_bytes(&state.meta)?,
                "scan continuation",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                strings_capacity_bytes(&state.schema_columns, "scan schema columns")?,
                "scan continuation",
            )?;
            if let Some(source) = state.source_group.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    checked_size_mul(
                        source.positions.capacity(),
                        std::mem::size_of::<PhysicalPartitionColumn>(),
                        "source group positions",
                    )?,
                    "scan observation",
                )?;
            }
            if let Some(filter) = state.filter.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, expr_capacity_bytes(filter)?, "scan filter")?;
            }
            let mode_bytes = match &state.mode {
                ScanMode::Physical { .. } | ScanMode::Empty => 0,
                // The probe keeps the keys it still owes plus the postings of
                // the one key it is reading, both already charged as they were
                // taken.
                ScanMode::Exact { index, cursor } => checked_size_add(
                    index.capacity(),
                    cursor.retained_bytes(),
                    "exact index continuation",
                )?,
                ScanMode::Index {
                    pick,
                    column,
                    cursor,
                    ..
                } => {
                    let mut mode_bytes = checked_size_add(
                        index_pick_capacity_bytes(pick)?,
                        column.capacity(),
                        "index scan continuation",
                    )?;
                    if let Some(index) = cursor.index_name() {
                        mode_bytes =
                            checked_size_add(mode_bytes, index.len(), "index cursor continuation")?;
                    }
                    checked_size_add(
                        mode_bytes,
                        cursor.pending_run_bytes(),
                        "index cursor continuation",
                    )?
                }
                ScanMode::Ordered { column, cursor, .. } => {
                    let mut mode_bytes = column.capacity();
                    if let Some(index) = cursor.index_name() {
                        mode_bytes = checked_size_add(
                            mode_bytes,
                            index.len(),
                            "ordered cursor continuation",
                        )?;
                    }
                    checked_size_add(
                        mode_bytes,
                        cursor.pending_run_bytes(),
                        "ordered cursor continuation",
                    )?
                }
            };
            state_bytes = checked_size_add(state_bytes, mode_bytes, "scan continuation")?;
            if let Some(merge) = state.ordered_merge.as_ref() {
                let mut fold_bytes = checked_size_add(
                    merge.held_bytes,
                    ordered_merge_keys_capacity_bytes(&merge.keys)?,
                    "ordered fold",
                )?;
                if let Some((_, deferred_bytes, _)) = merge.deferred.as_ref() {
                    fold_bytes = checked_size_add(fold_bytes, *deferred_bytes, "ordered fold")?;
                }
                state_bytes = checked_size_add(state_bytes, fold_bytes, "scan continuation")?;
            }
            if let Some(pick) = state.residual_pick.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    index_pick_capacity_bytes(pick)?,
                    "scan residual",
                )?;
            }
            if let Some(index) = state.authenticated_exact_index.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, index.capacity(), "scan exact probe index")?;
            }
            state_bytes
        }
        PullKind::Graph(state) => {
            let mut state_bytes = checked_size_add(
                state.start_alias.capacity(),
                graph_steps_capacity_bytes(&state.steps)?,
                "graph continuation",
            )?;
            if let Some(filter) = state.filter.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, expr_capacity_bytes(filter)?, "graph filter")?;
            }
            state_bytes = checked_size_add(
                state_bytes,
                checked_size_mul(
                    state.starts.capacity(),
                    std::mem::size_of::<uuid::Uuid>(),
                    "graph starts",
                )?,
                "graph continuation",
            )?;
            if let Some(candidates) = state.start_candidates.as_deref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    boxed_pull_bytes(candidates)?,
                    "graph candidates",
                )?;
            }
            // The parts of a traversal that GROW as it reads. Everything above
            // is fixed the moment the walk is planned; these are not, and they
            // are what the walk hands back when it finally runs dry. Leaving
            // them out let a settlement computed from this figure give those
            // bytes away while the traversal was still holding them, and the
            // walk's own hand-back at exhaustion then asked for more than the
            // request had left. Every one is a figure the traversal already
            // keeps, so asking costs the same however deep the walk has got.
            for growing in [
                state.seen_start_container_bytes,
                state.seen_path_container_bytes,
                state.probe_seen_container_bytes,
                state.frontier_container_bytes,
                state.row_memory.available,
                state
                    .edge_source
                    .as_ref()
                    .map_or(0, |source| source.retained_bytes()),
                state.start_walk_container_bytes,
                state.chosen.as_ref().map_or(0, |node| node.reserved_bytes),
            ] {
                state_bytes = checked_size_add(state_bytes, growing, "graph continuation")?;
            }
            if let Some(active) = state.active.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    active.cursor.retained_bytes(),
                    "graph traversal continuation",
                )?;
                state_bytes = checked_size_add(
                    state_bytes,
                    active.path.retained_bytes,
                    "graph traversal path",
                )?;
            }
            // Every start walk waiting in the merge holds its cursor, its path
            // and the next node it has already read.
            for walk in state.start_walks.iter().flatten() {
                for held in [
                    walk.active.cursor.retained_bytes(),
                    walk.active.path.retained_bytes,
                    walk.next.reserved_bytes,
                ] {
                    state_bytes = checked_size_add(state_bytes, held, "graph start walk")?;
                }
            }
            state_bytes
        }
        PullKind::Vector(state) => {
            let mut state_bytes = checked_size_add(
                checked_size_add(
                    size_of::<VectorState>(), state.table.capacity(), "vector control",
                )?,
                checked_size_add(
                    state.index.table.capacity(),
                    state.index.column.capacity(),
                    "vector index",
                )?,
                "vector continuation",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                optional_string_capacity(&state.sort_key),
                "vector continuation",
            )?;
            if let VectorPreparation::Candidates(Some(candidates)) = &state.preparation {
                state_bytes = checked_size_add(
                    state_bytes,
                    boxed_pull_bytes(candidates)?,
                    "vector candidates",
                )?;
            }
            // Held for as long as the search is deciding, so a settlement
            // computed from this figure cannot give it away underneath the
            // search that is still reading it.
            state_bytes =
                checked_size_add(state_bytes, state.overlay_bytes, "vector continuation")?;
            // The parts of a SEARCH that grow as it reads, and that it keeps
            // between fetches: the candidate identities it admitted, the scores
            // it computed for them, the ranked and output queues it hands rows
            // out of, and the brute-force continuation it is stepping. Each is
            // charged to this request where it is taken and released by the
            // search itself when it is done with it. Leaving them out of this
            // figure let the per-page settlement give those bytes away while
            // the search was still holding them, and the fetch that finally
            // released them asked the request for more than it had left --
            // exactly the fault the traversal arm above is written against.
            for growing in [
                state.candidate_bytes,
                state.score_bytes,
                state.ranked_container_bytes,
                state.output_container_bytes,
                state.source.as_ref().map_or(0, |source| match source {
                    VectorCandidateSource::Brute(cursor) => cursor.retained_bytes(),
                }),
            ] {
                state_bytes = checked_size_add(state_bytes, growing, "vector continuation")?;
            }
            // The rows in those two queues are charged one at a time as they
            // are materialised and stay charged until they are published, so
            // they move with the queue rather than with the page.
            for ranked in &state.ranked_rows {
                state_bytes =
                    checked_size_add(state_bytes, ranked.row.retained_bytes, "vector ranked rows")?;
            }
            for row in &state.output {
                state_bytes =
                    checked_size_add(state_bytes, row.retained_bytes, "vector output rows")?;
            }
            state_bytes
        }
        PullKind::Project(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.input)?,
                project_columns_capacity_bytes(&state.columns)?,
                "projection continuation",
            )?;
            // An aggregate a cursor opened keeps its running answer -- and
            // the input row it has pulled but not folded in -- between
            // fetches. A settlement that cannot see them gives them away, and
            // the fetch that finally releases them under-releases by exactly
            // that much.
            state_bytes = checked_size_add(
                state_bytes,
                state.aggregate_state_bytes,
                "projection continuation",
            )?;
            if let Some(pending) = state.pending.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    pending.retained_bytes,
                    "projection continuation",
                )?;
            }
            state_bytes
        }
        PullKind::Sort(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.input)?,
                sort_keys_capacity_bytes(&state.keys)?,
                "sort continuation",
            )?;
            // A sort answers nothing until it has read everything, so between
            // its first row and its last it is holding the whole answer: the
            // key bytes it reserved for each waiting row, and the row's own
            // bytes charged by the input. Read as the running total the sort
            // keeps, never walked -- this question is asked twice a page, and
            // a walk would make answering it cost what is still buffered, so
            // the bookkeeping would grow with the answer still to be
            // delivered. Counted for the same reason a traversal's frontier
            // is: what a settlement cannot see, it gives away.
            state_bytes = checked_size_add(state_bytes, state.container_bytes, "sort container")?;
            state_bytes =
                checked_size_add(state_bytes, state.buffered_bytes, "sort buffered rows")?;
            if let Some(pending) = state.pending.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, pending.retained_bytes, "sort pending row")?;
            }
            state_bytes
        }
        PullKind::Window(state) => {
            let position_bytes = checked_size_mul(
                state.partition_positions.capacity(),
                std::mem::size_of::<usize>(),
                "window positions",
            )?;
            let held = checked_size_add(
                boxed_pull_bytes(&state.input)?,
                checked_size_add(state.partition_bytes, position_bytes, "window bindings")?,
                "window continuation",
            )?;
            checked_size_add(
                held,
                state.pending.as_ref().map_or(0, |row| row.retained_bytes),
                "window pending",
            )?
        }
        PullKind::Limit(state) => boxed_pull_bytes(&state.input)?,
        PullKind::Filter(state) => checked_size_add(
            boxed_pull_bytes(&state.input)?,
            expr_capacity_bytes(&state.predicate)?,
            "filter continuation",
        )?,
        PullKind::Distinct(state) => boxed_pull_bytes(&state.input)?,
        PullKind::Join(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.left)?,
                boxed_pull_bytes(&state.right)?,
                "join inputs",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                expr_capacity_bytes(&state.condition)?,
                "join condition",
            )?;
            checked_size_add(
                state_bytes,
                strings_capacity_bytes(&state.condition_columns, "join columns")?,
                "join continuation",
            )?
        }
        PullKind::Static(state) => {
            let mut state_bytes = checked_size_mul(
                state.rows.capacity(),
                std::mem::size_of::<PulledRow>(),
                "static rows",
            )?;
            for row in &state.rows {
                state_bytes = checked_size_add(
                    state_bytes,
                    values_retained_bytes(&row.values)?,
                    "static row",
                )?;
            }
            state_bytes
        }
        PullKind::Union(state) => {
            let mut state_bytes = checked_size_mul(
                state.inputs.capacity(),
                std::mem::size_of::<PullNode>(),
                "UNION inputs",
            )?;
            for input in &state.inputs {
                state_bytes = checked_size_add(
                    state_bytes,
                    pull_continuation_bytes(input)?,
                    "UNION continuation",
                )?;
            }
            state_bytes
        }
    };
    bytes = checked_size_add(bytes, state_bytes, "pull continuation")?;
    Ok(bytes)
}

fn expr_nodes(expr: &Expr) -> std::result::Result<usize, BoundedExecutionError> {
    let children = match expr {
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            checked_size_add(expr_nodes(left)?, expr_nodes(right)?, "expression nodes")?
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => expr_nodes(operand)?,
        Expr::FunctionCall { args, .. } => args.iter().try_fold(0usize, |total, arg| {
            checked_size_add(total, expr_nodes(arg)?, "expression nodes")
        })?,
        Expr::InList { expr, list, .. } => {
            let list_nodes = list.iter().try_fold(0usize, |total, item| {
                checked_size_add(total, expr_nodes(item)?, "expression nodes")
            })?;
            checked_size_add(expr_nodes(expr)?, list_nodes, "expression nodes")?
        }
        Expr::Like { expr, pattern, .. } => {
            checked_size_add(expr_nodes(expr)?, expr_nodes(pattern)?, "expression nodes")?
        }
        Expr::Row(elements) => elements.iter().try_fold(0usize, |total, element| {
            checked_size_add(total, expr_nodes(element)?, "expression nodes")
        })?,
        Expr::InSubquery { .. } => 0,
        Expr::RowVectorSource { key, .. } => expr_nodes(key)?,
        Expr::RowNumber(window) => checked_size_add(
            window.partition_by.len(),
            window.order_by.len(),
            "window column nodes",
        )?,
        Expr::WindowRank(_) | Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => 0,
    };
    checked_size_add(children, 1, "expression nodes")
}

/// The parameter names an expression reads.
fn collect_expr_parameters(expr: &Expr, into: &mut std::collections::BTreeSet<String>) {
    match expr {
        Expr::Parameter(name) => {
            into.insert(name.clone());
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            collect_expr_parameters(left, into);
            collect_expr_parameters(right, into);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_parameters(arg, into);
            }
        }
        Expr::UnaryOp { operand, .. } => collect_expr_parameters(operand, into),
        Expr::IsNull { expr, .. } => collect_expr_parameters(expr, into),
        Expr::Like { expr, .. } => collect_expr_parameters(expr, into),
        Expr::InList { expr, list, .. } => {
            collect_expr_parameters(expr, into);
            for item in list {
                collect_expr_parameters(item, into);
            }
        }
        Expr::InSubquery { expr, .. } => collect_expr_parameters(expr, into),
        Expr::Row(elements) => {
            for element in elements {
                collect_expr_parameters(element, into);
            }
        }
        _ => {}
    }
}

/// What the named parameters cost, container included.
fn referenced_params_retained_bytes(
    params: &HashMap<String, Value>,
    referenced: &std::collections::BTreeSet<String>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = std::mem::size_of::<HashMap<String, Value>>();
    for name in referenced {
        let Some(value) = params.get(name) else {
            continue;
        };
        bytes = checked_size_add(
            bytes,
            checked_size_add(
                std::mem::size_of::<(String, Value)>(),
                checked_size_add(name.capacity(), value_capacity_bytes(value)?, "parameter")?,
                "parameter",
            )?,
            "parameters",
        )?;
    }
    Ok(bytes)
}

fn expression_temporary_bytes<'a>(
    row: &Vec<Value>,
    params: &HashMap<String, Value>,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut referenced = std::collections::BTreeSet::new();
    let mut nodes = 0usize;
    for expression in expressions {
        collect_expr_parameters(expression, &mut referenced);
        nodes = checked_size_add(nodes, expr_nodes(expression)?, "expression evaluation")?;
    }
    let referenced = &referenced;
    let row_bytes = values_retained_bytes(row)?;
    // Only the parameters these expressions actually READ. A statement's
    // parameter map is charged where it is held; charging the whole of it
    // again for every row an expression is evaluated over makes a projection
    // of one small column cost whatever the largest parameter is -- a
    // 128-dimension query vector, say, doubled, per row, for a projection
    // that never mentions it.
    let parameter_bytes = referenced_params_retained_bytes(params, referenced)?;
    let working_values = checked_size_mul(
        checked_size_add(row_bytes, parameter_bytes, "expression inputs")?,
        2,
        "expression inputs",
    )?;
    checked_size_add(
        working_values,
        checked_size_mul(
            nodes,
            std::mem::size_of::<Value>(),
            "expression temporaries",
        )?,
        "expression evaluation",
    )
}

// A direct projection allocates the selected value clones, not two copies
// of every input field. Charge each occurrence plus Vec collection growth.
fn projection_temporary_bytes(
    row: &Vec<Value>,
    columns: &[String],
    params: &HashMap<String, Value>,
    projected: &[ProjectColumn],
) -> std::result::Result<usize, BoundedExecutionError> {
    if !projected
        .iter()
        .all(|column| matches!(column.expr, Expr::Column(_)))
    {
        return expression_temporary_bytes(
            row,
            params,
            projected.iter().map(|column| &column.expr),
        );
    }
    let capacity = if projected.is_empty() {
        0
    } else {
        projected
            .len()
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(|| bounded_size_error("projection slots"))?
    };
    // Collect may grow from the preceding capacity while retaining its old
    // buffer. This includes that temporary overlap before evaluating anything.
    let slots = capacity
        .checked_add(capacity / 2)
        .ok_or_else(|| bounded_size_error("projection slots"))?;
    let mut bytes = checked_size_add(
        std::mem::size_of::<Vec<Value>>(),
        checked_size_mul(slots, std::mem::size_of::<Value>(), "projection slots")?,
        "projection output",
    )?;
    for column in projected {
        let Expr::Column(name) = &column.expr else {
            unreachable!()
        };
        let position = match resolve_query_result_column(columns, name) {
            QueryResultColumn::Found(position) => position,
            resolution => return Err(query_result_column_error(name, &resolution).into()),
        };
        let value = row
            .get(position)
            .ok_or_else(|| Error::PlanError("projection value is missing".to_string()))?;
        bytes = checked_size_add(
            bytes,
            planned_value_clone_bytes(value)?,
            "projection clones",
        )?;
    }
    Ok(bytes)
}

// Direct column keys can repeat arbitrarily in a window specification. Fund
// every retained clone before evaluation, rather than estimating multiplicity
// from the input row's size. Other expressions retain their existing workspace.
fn sort_key_temporary_bytes(
    row: &Vec<Value>,
    columns: &[String],
    params: &HashMap<String, Value>,
    keys: &[SortKey],
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut exact = checked_size_mul(keys.len(), std::mem::size_of::<Value>(), "sort keys")?;
    let mut direct = true;
    for key in keys {
        match &key.expr {
            Expr::Column(column) => match resolve_query_result_column(columns, column) {
                QueryResultColumn::Found(position) => {
                    let value = row
                        .get(position)
                        .ok_or_else(|| Error::PlanError("sort key value is missing".to_string()))?;
                    exact = checked_size_add(
                        exact,
                        planned_value_clone_bytes(value)?,
                        "sort key clones",
                    )?;
                }
                resolution => return Err(query_result_column_error(column, &resolution).into()),
            },
            Expr::WindowRank(_) => {}
            _ => direct = false,
        }
    }
    if direct {
        Ok(exact)
    } else {
        checked_size_add(
            exact,
            expression_temporary_bytes(row, params, keys.iter().map(|key| &key.expr))?,
            "sort evaluation",
        )
    }
}

struct CheckedCountingWriter<'a> {
    bytes: &'a Cell<usize>,
}

impl Writer for CheckedCountingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::result::Result<(), bincode::error::EncodeError> {
        let next =
            self.bytes
                .get()
                .checked_add(bytes.len())
                .ok_or(bincode::error::EncodeError::Other(
                    "bounded canonical size overflow",
                ))?;
        self.bytes.set(next);
        Ok(())
    }
}

pub(super) fn serialized_size<T: Serialize + ?Sized>(
    value: &T,
) -> std::result::Result<usize, BoundedExecutionError> {
    let bytes = Cell::new(0usize);
    bincode::serde::encode_into_writer(
        value,
        CheckedCountingWriter { bytes: &bytes },
        bincode::config::standard(),
    )
    .map_err(|_| Error::Other("canonical read encoding failed".to_string()))?;
    Ok(bytes.get())
}

fn encoded_row_key(
    row: &[Value],
    size: usize,
) -> std::result::Result<Vec<u8>, BoundedExecutionError> {
    let mut key = vec![0u8; size];
    let written =
        bincode::serde::encode_into_slice(row, key.as_mut_slice(), bincode::config::standard())
            .map_err(|_| Error::Other("bounded row-key encoding failed".to_string()))?;
    if written != size {
        return Err(
            Error::Other("bounded row-key size changed during encoding".to_string()).into(),
        );
    }
    Ok(key)
}

#[derive(Debug, Clone, Copy)]
enum BorrowedScalar<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(&'a str),
    Uuid(&'a uuid::Uuid),
    Timestamp(i64),
    Json(&'a serde_json::Value),
    Vector(&'a [f32]),
    TxId(TxId),
}

impl<'a> BorrowedScalar<'a> {
    fn from_value(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Text(value) => Self::Text(value),
            Value::Uuid(value) => Self::Uuid(value),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Json(value) => Self::Json(value),
            Value::Vector(value) => Self::Vector(value),
            Value::TxId(value) => Self::TxId(*value),
        }
    }

    fn from_literal(literal: &'a Literal) -> Self {
        match literal {
            Literal::Null => Self::Null,
            Literal::Bool(value) => Self::Bool(*value),
            Literal::Integer(value) => Self::Int64(*value),
            Literal::Real(value) => Self::Float64(*value),
            Literal::Text(value) => Self::Text(value),
            Literal::Vector(value) => Self::Vector(value),
        }
    }

    fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    fn into_owned(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(value),
            Self::Int64(value) => Value::Int64(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Text(value) => Value::Text(value.to_owned()),
            Self::Uuid(value) => Value::Uuid(*value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Json(value) => Value::Json(value.clone()),
            Self::Vector(value) => Value::Vector(value.to_vec()),
            Self::TxId(value) => Value::TxId(value),
        }
    }
}

fn borrowed_scalar_truth(value: BorrowedScalar<'_>) -> bool {
    matches!(value, BorrowedScalar::Bool(true))
}

fn borrowed_scalar_exact_eq(left: BorrowedScalar<'_>, right: BorrowedScalar<'_>) -> bool {
    match (left, right) {
        (BorrowedScalar::Null, BorrowedScalar::Null) => true,
        (BorrowedScalar::Bool(left), BorrowedScalar::Bool(right)) => left == right,
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => left == right,
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => left == right,
        (BorrowedScalar::Text(left), BorrowedScalar::Text(right)) => left == right,
        (BorrowedScalar::Uuid(left), BorrowedScalar::Uuid(right)) => left == right,
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Timestamp(right)) => left == right,
        (BorrowedScalar::Json(left), BorrowedScalar::Json(right)) => left == right,
        (BorrowedScalar::Vector(left), BorrowedScalar::Vector(right)) => left == right,
        (BorrowedScalar::TxId(left), BorrowedScalar::TxId(right)) => left == right,
        _ => false,
    }
}

fn borrowed_scalar_cmp(
    left: BorrowedScalar<'_>,
    right: BorrowedScalar<'_>,
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match (left, right) {
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => {
            Some(left.total_cmp(&right))
        }
        (BorrowedScalar::Text(left), BorrowedScalar::Text(right)) => Some(left.cmp(right)),
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Timestamp(right)) => {
            Some(left.cmp(&right))
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::Float64(right)) => {
            Some((left as f64).total_cmp(&right))
        }
        (BorrowedScalar::Float64(left), BorrowedScalar::Int64(right)) => {
            Some(left.total_cmp(&(right as f64)))
        }
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Int64(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Int64(left), BorrowedScalar::Timestamp(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Bool(left), BorrowedScalar::Bool(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Uuid(left), BorrowedScalar::Uuid(right)) => Some(left.cmp(right)),
        (BorrowedScalar::Uuid(left), BorrowedScalar::Text(right)) => right
            .parse::<uuid::Uuid>()
            .ok()
            .map(|right| left.cmp(&right)),
        (BorrowedScalar::Text(left), BorrowedScalar::Uuid(right)) => {
            left.parse::<uuid::Uuid>().ok().map(|left| left.cmp(right))
        }
        (BorrowedScalar::TxId(left), BorrowedScalar::TxId(right)) => Some(left.0.cmp(&right.0)),
        (BorrowedScalar::TxId(left), BorrowedScalar::Int64(right)) => {
            if right < 0 {
                Some(Ordering::Greater)
            } else {
                Some(left.0.cmp(&(right as u64)))
            }
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::TxId(right)) => {
            if left < 0 {
                Some(Ordering::Less)
            } else {
                Some((left as u64).cmp(&right.0))
            }
        }
        (BorrowedScalar::TxId(_), BorrowedScalar::Timestamp(_))
        | (BorrowedScalar::Timestamp(_), BorrowedScalar::TxId(_))
        | (BorrowedScalar::Null, _)
        | (_, BorrowedScalar::Null) => None,
        _ => None,
    }
}

fn borrowed_scalar_sql_eq(left: BorrowedScalar<'_>, right: BorrowedScalar<'_>) -> bool {
    borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Equal)
        || borrowed_scalar_exact_eq(left, right)
}

fn bounded_eval_arithmetic<'a>(
    name: &str,
    left: BorrowedScalar<'a>,
    right: BorrowedScalar<'a>,
) -> Result<BorrowedScalar<'a>> {
    if left.is_null() || right.is_null() {
        return Ok(BorrowedScalar::Null);
    }
    match (left, right) {
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => match name {
            "__add" => left
                .checked_add(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__sub" => left
                .checked_sub(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__mul" => left
                .checked_mul(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__div" => {
                if right == 0 {
                    Err(Error::PlanError("division by zero".to_string()))
                } else {
                    left.checked_div(right)
                        .map(BorrowedScalar::Int64)
                        .ok_or_else(|| {
                            Error::PlanError(format!("integer overflow: {left} / {right}"))
                        })
                }
            }
            _ => Err(Error::PlanError(format!("unknown function: {name}"))),
        },
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => {
            bounded_eval_float_arithmetic(name, left, right)
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::Float64(right)) => {
            bounded_eval_float_arithmetic(name, left as f64, right)
        }
        (BorrowedScalar::Float64(left), BorrowedScalar::Int64(right)) => {
            bounded_eval_float_arithmetic(name, left, right as f64)
        }
        _ => Err(Error::PlanError(format!(
            "function {name} expects numeric arguments"
        ))),
    }
}

fn bounded_eval_float_arithmetic<'a>(
    name: &str,
    left: f64,
    right: f64,
) -> Result<BorrowedScalar<'a>> {
    match name {
        "__add" => Ok(BorrowedScalar::Float64(left + right)),
        "__sub" => Ok(BorrowedScalar::Float64(left - right)),
        "__mul" => Ok(BorrowedScalar::Float64(left * right)),
        "__div" if right == 0.0 => Err(Error::PlanError("division by zero".to_string())),
        "__div" => Ok(BorrowedScalar::Float64(left / right)),
        _ => Err(Error::PlanError(format!("unknown function: {name}"))),
    }
}

fn bounded_eval_function<'a>(
    name: &str,
    args: &'a [Expr],
    mut evaluate: impl FnMut(&'a Expr) -> Result<BorrowedScalar<'a>>,
) -> Result<BorrowedScalar<'a>> {
    let mut first = None;
    let mut second = None;
    let mut coalesced = None;
    for (position, argument) in args.iter().enumerate() {
        let value = evaluate(argument)?;
        if position == 0 {
            first = Some(value);
        } else if position == 1 {
            second = Some(value);
        }
        if coalesced.is_none() && !value.is_null() {
            coalesced = Some(value);
        }
    }

    if ["__add", "__sub", "__mul", "__div"]
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
    {
        if args.len() != 2 {
            return Err(Error::PlanError(format!(
                "function {name} expects 2 arguments"
            )));
        }
        let (Some(left), Some(right)) = (first, second) else {
            return Err(Error::PlanError(format!(
                "function {name} expects 2 arguments"
            )));
        };
        return bounded_eval_arithmetic(name, left, right);
    }
    if name.eq_ignore_ascii_case("coalesce") {
        return Ok(coalesced.unwrap_or(BorrowedScalar::Null));
    }
    if name.eq_ignore_ascii_case("now") {
        // The name is already identified here. Dispatching through the shared
        // evaluator would first normalize it into a fresh String that no
        // request budget admitted, so the clock is read directly.
        return Ok(BorrowedScalar::Timestamp(now_timestamp_seconds()?));
    }
    Err(Error::PlanError(format!("unknown function: {name}")))
}

fn bounded_resolve_scalar<'a>(
    expr: &'a Expr,
    params: &'a HashMap<String, Value>,
) -> Result<BorrowedScalar<'a>> {
    match expr {
        Expr::Literal(literal) => Ok(BorrowedScalar::from_literal(literal)),
        Expr::Parameter(name) => params
            .get(name)
            .map(BorrowedScalar::from_value)
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {name}"))),
        Expr::Column(column) => Ok(BorrowedScalar::Text(&column.column)),
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match bounded_resolve_scalar(operand, params)? {
                BorrowedScalar::Int64(value) => value
                    .checked_neg()
                    .map(BorrowedScalar::Int64)
                    .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
                BorrowedScalar::Float64(value) => Ok(BorrowedScalar::Float64(-value)),
                _ => Err(Error::PlanError(
                    "cannot negate non-numeric value".to_string(),
                )),
            },
            UnaryOp::Not => Err(Error::PlanError(
                "boolean NOT requires row context".to_string(),
            )),
        },
        Expr::FunctionCall { name, args } => bounded_eval_function(name, args, |argument| {
            bounded_resolve_scalar(argument, params)
        }),
        Expr::CosineDistance { right, .. } => bounded_resolve_scalar(right, params),
        _ => Err(Error::PlanError("unsupported expression".to_string())),
    }
}

fn bounded_eval_scalar_in_row<'a>(
    row: &'a VersionedRow,
    expr: &'a Expr,
    params: &'a HashMap<String, Value>,
) -> Result<BorrowedScalar<'a>> {
    match expr {
        Expr::Column(column) => {
            if column.column == "row_id" {
                Ok(BorrowedScalar::Int64(row.row_id.0 as i64))
            } else {
                Ok(row
                    .values
                    .get(&column.column)
                    .map(BorrowedScalar::from_value)
                    .unwrap_or(BorrowedScalar::Null))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = bounded_eval_scalar_in_row(row, left, params)?;
            let right = bounded_eval_scalar_in_row(row, right, params)?;
            if matches!(op, BinOp::And | BinOp::Or) {
                let left = if left.is_null() {
                    None
                } else {
                    Some(borrowed_scalar_truth(left))
                };
                let right = if right.is_null() {
                    None
                } else {
                    Some(borrowed_scalar_truth(right))
                };
                let truth = match op {
                    BinOp::And => match (left, right) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    },
                    BinOp::Or => match (left, right) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    },
                    _ => unreachable!(),
                };
                return Ok(truth
                    .map(BorrowedScalar::Bool)
                    .unwrap_or(BorrowedScalar::Null));
            }
            if left.is_null() || right.is_null() {
                return Ok(BorrowedScalar::Null);
            }
            let value = match op {
                BinOp::Eq => {
                    !left.is_null() && !right.is_null() && borrowed_scalar_sql_eq(left, right)
                }
                BinOp::Neq => {
                    !left.is_null() && !right.is_null() && !borrowed_scalar_sql_eq(left, right)
                }
                BinOp::Lt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Less),
                BinOp::Lte => matches!(
                    borrowed_scalar_cmp(left, right),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                BinOp::Gt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Greater),
                BinOp::Gte => matches!(
                    borrowed_scalar_cmp(left, right),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                BinOp::And => borrowed_scalar_truth(left) && borrowed_scalar_truth(right),
                BinOp::Or => borrowed_scalar_truth(left) || borrowed_scalar_truth(right),
            };
            Ok(BorrowedScalar::Bool(value))
        }
        Expr::UnaryOp { op, operand } => {
            let value = bounded_eval_scalar_in_row(row, operand, params)?;
            match op {
                UnaryOp::Not => Ok(if value.is_null() {
                    BorrowedScalar::Null
                } else {
                    BorrowedScalar::Bool(!borrowed_scalar_truth(value))
                }),
                UnaryOp::Neg => match value {
                    BorrowedScalar::Int64(value) => value
                        .checked_neg()
                        .map(BorrowedScalar::Int64)
                        .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
                    BorrowedScalar::Float64(value) => Ok(BorrowedScalar::Float64(-value)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => bounded_eval_function(name, args, |argument| {
            bounded_eval_scalar_in_row(row, argument, params)
        }),
        Expr::IsNull { expr, negated } => {
            let is_null = bounded_eval_scalar_in_row(row, expr, params)?.is_null();
            Ok(BorrowedScalar::Bool(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if matches!(expr.as_ref(), Expr::Row(_)) => {
            Ok(match bounded_row_value_in_list(row, expr, list, params)? {
                Some(matched) => BorrowedScalar::Bool(matched != *negated),
                None => BorrowedScalar::Null,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = bounded_eval_scalar_in_row(row, expr, params)?;
            if needle.is_null() {
                return Ok(BorrowedScalar::Null);
            }
            let mut saw_unknown = false;
            for item in list {
                let candidate = bounded_eval_scalar_in_row(row, item, params)?;
                if candidate.is_null() {
                    saw_unknown = true;
                } else if borrowed_scalar_cmp(needle, candidate) == Some(std::cmp::Ordering::Equal)
                    || borrowed_scalar_exact_eq(needle, candidate)
                {
                    return Ok(BorrowedScalar::Bool(!*negated));
                }
            }
            Ok(if saw_unknown {
                BorrowedScalar::Null
            } else {
                BorrowedScalar::Bool(*negated)
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let value = bounded_eval_scalar_in_row(row, expr, params)?;
            let pattern = bounded_eval_scalar_in_row(row, pattern, params)?;
            let matched = match (value, pattern) {
                (BorrowedScalar::Null, _) | (_, BorrowedScalar::Null) => {
                    return Ok(BorrowedScalar::Null);
                }
                (BorrowedScalar::Text(value), BorrowedScalar::Text(pattern)) => {
                    bounded_like_matches(value, pattern)
                }
                _ => false,
            };
            Ok(BorrowedScalar::Bool(if *negated {
                !matched
            } else {
                matched
            }))
        }
        _ => bounded_resolve_scalar(expr, params),
    }
}

fn next_utf8_char(value: &str, position: usize) -> Option<(char, usize)> {
    let suffix = value.get(position..)?;
    let character = suffix.chars().next()?;
    Some((character, position + character.len_utf8()))
}

fn bounded_like_matches(value: &str, pattern: &str) -> bool {
    let (mut value_position, mut pattern_position) = (0usize, 0usize);
    let mut star_pattern_position = None;
    let mut star_value_position = 0usize;

    while value_position < value.len() {
        let Some((value_character, next_value_position)) = next_utf8_char(value, value_position)
        else {
            return false;
        };
        if let Some((pattern_character, next_pattern_position)) =
            next_utf8_char(pattern, pattern_position)
        {
            if pattern_character == '_' || pattern_character == value_character {
                value_position = next_value_position;
                pattern_position = next_pattern_position;
                continue;
            }
            if pattern_character == '%' {
                star_pattern_position = Some(next_pattern_position);
                star_value_position = value_position;
                pattern_position = next_pattern_position;
                continue;
            }
        }
        let Some(after_star) = star_pattern_position else {
            return false;
        };
        let Some((_, next_star_value_position)) = next_utf8_char(value, star_value_position) else {
            return false;
        };
        star_value_position = next_star_value_position;
        value_position = next_star_value_position;
        pattern_position = after_star;
    }

    while let Some((pattern_character, next_pattern_position)) =
        next_utf8_char(pattern, pattern_position)
    {
        if pattern_character != '%' {
            return false;
        }
        pattern_position = next_pattern_position;
    }
    pattern_position == pattern.len()
}

/// `(a, b, ...) IN ((x, y, ...), ...)` over one row, under SQL's
/// three-valued row comparison; the same rule as the executor's
/// `row_value_in_list`, read through borrowed values.
fn bounded_row_value_in_list(
    row: &VersionedRow,
    left: &Expr,
    list: &[Expr],
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    let Expr::Row(columns) = left else {
        return Err(Error::PlanError(
            "a row-value IN needs a row on its left".to_string(),
        ));
    };
    let mut unknown = false;
    for item in list {
        let Expr::Row(values) = item else {
            return Err(Error::PlanError(
                "IN compares a row value against a single value".to_string(),
            ));
        };
        if values.len() != columns.len() {
            return Err(Error::PlanError(format!(
                "IN compares a row of {} columns against a row of {} values",
                columns.len(),
                values.len()
            )));
        }
        let mut row_unknown = false;
        let mut row_differs = false;
        for (column, value) in columns.iter().zip(values) {
            let needle = bounded_eval_scalar_in_row(row, column, params)?;
            let value = bounded_eval_scalar_in_row(row, value, params)?;
            if needle.is_null() || value.is_null() {
                row_unknown = true;
            } else if borrowed_scalar_cmp(needle, value) != Some(std::cmp::Ordering::Equal)
                && !borrowed_scalar_exact_eq(needle, value)
            {
                row_differs = true;
                break;
            }
        }
        if row_differs {
            continue;
        }
        if !row_unknown {
            return Ok(Some(true));
        }
        unknown = true;
    }
    Ok((!unknown).then_some(false))
}

fn bounded_eval_bool_expr(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = bounded_eval_scalar_in_row(row, left, params)?;
                let right = bounded_eval_scalar_in_row(row, right, params)?;
                if left.is_null() || right.is_null() {
                    return Ok(None);
                }
                let result = match op {
                    BinOp::Eq => borrowed_scalar_sql_eq(left, right),
                    BinOp::Neq => !borrowed_scalar_sql_eq(left, right),
                    BinOp::Lt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Less),
                    BinOp::Lte => matches!(
                        borrowed_scalar_cmp(left, right),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    ),
                    BinOp::Gt => {
                        borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Greater)
                    }
                    BinOp::Gte => matches!(
                        borrowed_scalar_cmp(left, right),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ),
                    BinOp::And | BinOp::Or => {
                        return Err(Error::Other(
                            "bounded boolean evaluator reached a non-comparison operator"
                                .to_string(),
                        ));
                    }
                };
                Ok(Some(result))
            }
            BinOp::And => {
                let left = bounded_eval_bool_expr(row, left, params)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = bounded_eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = bounded_eval_bool_expr(row, left, params)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = bounded_eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(bounded_eval_bool_expr(row, operand, params)?.map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } if matches!(expr.as_ref(), Expr::Row(_)) => {
            Ok(bounded_row_value_in_list(row, expr, list, params)?
                .map(|matched| matched != *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = bounded_eval_scalar_in_row(row, expr, params)?;
            if needle.is_null() {
                return Ok(None);
            }
            let mut saw_unknown = false;
            for item in list {
                let candidate = bounded_eval_scalar_in_row(row, item, params)?;
                if candidate.is_null() {
                    saw_unknown = true;
                } else if borrowed_scalar_cmp(needle, candidate) == Some(std::cmp::Ordering::Equal)
                    || borrowed_scalar_exact_eq(needle, candidate)
                {
                    return Ok(Some(!*negated));
                }
            }
            Ok(if saw_unknown { None } else { Some(*negated) })
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before execution".to_string(),
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let left = bounded_eval_scalar_in_row(row, expr, params)?;
            let right = bounded_eval_scalar_in_row(row, pattern, params)?;
            let matched = match (left, right) {
                (BorrowedScalar::Null, _) | (_, BorrowedScalar::Null) => None,
                (BorrowedScalar::Text(value), BorrowedScalar::Text(pattern)) => {
                    Some(bounded_like_matches(value, pattern))
                }
                _ => Some(false),
            };
            Ok(matched.map(|matched| matched != *negated))
        }
        Expr::IsNull { expr, negated } => {
            let is_null = bounded_eval_scalar_in_row(row, expr, params)?.is_null();
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::Literal(Literal::Bool(value)) => Ok(Some(*value)),
        Expr::Column(_)
        | Expr::Parameter(_)
        | Expr::Literal(Literal::Null)
        | Expr::FunctionCall { .. } => match bounded_eval_scalar_in_row(row, expr, params)? {
            BorrowedScalar::Bool(value) => Ok(Some(value)),
            BorrowedScalar::Null => Ok(None),
            other => Err(Error::PlanError(format!(
                "WHERE expression must be boolean, got {other:?}: {expr:?}"
            ))),
        },
        _ => Err(Error::PlanError(format!(
            "unsupported WHERE expression: {expr:?}"
        ))),
    }
}

fn bounded_row_matches(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
    Ok(bounded_eval_bool_expr(row, expr, params)?.unwrap_or(false))
}

impl ScanState {
    fn drain_verified_vector_candidate_identities(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        candidate_ids: &mut Vec<u64>,
        candidate_bytes: &mut usize,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        if !self.identities_only
            || !self.vector_candidate_source
            || self.pending_row.is_some()
            || self.transaction.is_some()
        {
            return Ok(false);
        }
        let ScanMode::Index { cursor, pick, .. } = &mut self.mode else {
            return Ok(false);
        };
        let _attribution = contextdb_core::find_attribution::stage("candidate_drain");
        loop {
            let visited = db.bounded_visit_verified_ordered_identities(
                &self.table,
                &pick.name,
                self.snapshot,
                cursor,
                &mut || {
                    context.charge(
                        BoundedWorkSource::IndexRange,
                        BoundedSourceTouch::IndexEntry,
                    )
                },
                &mut |row_id| {
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::VectorCandidates,
                        candidate_ids,
                        "vector candidate ids",
                    )?;
                    *candidate_bytes = candidate_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("vector candidate ids"))?;
                    candidate_ids.push(row_id.0);
                    Ok(())
                },
                context.source_read_memory(BoundedWorkSource::IndexRange),
            )?;
            if !visited {
                return Ok(false);
            }
            if cursor.is_exhausted() {
                return Ok(true);
            }
        }
    }

    /// The next row this handle's own open transaction staged, once the
    /// committed rows are done.
    ///
    /// A staged row is charged exactly as a committed one is: the touch
    /// before it is read, the bytes before they are copied. A row the
    /// transaction staged and then deleted is dropped -- unless it had a
    /// committed original, which is what a delete followed by a reinsert
    /// leaves behind.
    fn next_staged_row(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<(VersionedRow, usize)>, BoundedExecutionError> {
        let Some(state) = self.transaction.as_mut() else {
            return Ok(None);
        };
        state.committed_exhausted = true;
        loop {
            let staged = if let Some(rows) = state.overlay.staged_rows.as_ref() {
                if let Some(ordered) = state.ordered_staged.as_mut() {
                    loop {
                        let (start, end, prefix_len) = ordered.cursor.source_run_bounds();
                        let scratch = staged_run_bound_bytes(start)
                            .saturating_add(staged_run_bound_bytes(end))
                            .saturating_add(state.staged_key_bytes)
                            .saturating_add(128);
                        context.reserve(BoundedWorkSource::TableScan, scratch)?;
                        let next = rows.next_ordered_in_run(
                            &ordered.index,
                            ordered.cursor.source_reverse(),
                            ordered.after.as_ref(),
                            start,
                            end,
                            prefix_len,
                        );
                        context.release(scratch)?;
                        if let Some((key, row)) = next {
                            break Some((Some(key), None, row));
                        }
                        ordered.after = None;
                        if !ordered.cursor.source_finish_run() {
                            break None;
                        }
                    }
                } else {
                    rows.next_unordered(state.published)
                        .map(|(sequence, row)| (None, Some(sequence), row))
                }
            } else {
                None
            };
            let Some((key, sequence, staged)) = staged else {
                let held = std::mem::take(&mut state.overlay.bytes);
                state.ordered_staged = None;
                context.release(held)?;
                context.release(std::mem::take(&mut state.staged_key_bytes))?;
                return Ok(None);
            };
            context.charge(BoundedWorkSource::TableScan, BoundedSourceTouch::TableRow)?;
            if let Some(source) = &self.source_group {
                context.observe_physical_group(source, &self.schema_columns, staged)?;
            }
            let reserved = Cell::new(staged.estimated_bytes());
            context.reserve(BoundedWorkSource::TableScan, reserved.get())?;
            if let Some(key) = key {
                let bytes = key
                    .0
                    .iter()
                    .map(|v| match v {
                        DirectedValue::Asc(v) => v.0.estimated_bytes(),
                        DirectedValue::Desc(v) => v.0.estimated_bytes(),
                    })
                    .sum::<usize>()
                    .saturating_add(
                        key.0
                            .len()
                            .saturating_mul(std::mem::size_of::<DirectedValue>()),
                    )
                    .saturating_add(32);
                context.reserve(BoundedWorkSource::TableScan, bytes)?;
                if let Some(ordered) = &mut state.ordered_staged {
                    ordered.after = Some(key.clone());
                }
                context.release(state.staged_key_bytes)?;
                state.staged_key_bytes = bytes;
            }
            state.published = sequence;
            let row = staged.clone();
            #[cfg(feature = "test-seams")]
            if self.vector_candidate_source {
                db.note_vector_candidate_materialized();
            }

            // Same trade as a committed row: charged before the clone against
            // the retained estimate, trued up to the copy this read holds.
            let held = pulled_row_retained_bytes(&row)?;
            reconcile_new_reservation(context, BoundedWorkSource::TableScan, reserved.get(), held)?;
            reserved.set(held);
            if state.overlay.deleted.contains(&row.row_id)
                && !state.restaged_over_committed.contains(&row.row_id)
            {
                // The committed source does not always offer every row of the
                // table: an index run offers only the rows its predicate
                // names, so a delete-then-reinsert whose COMMITTED version the
                // predicate excludes never passes through the walk, and
                // inferring "no committed original" from that would drop a row
                // the transaction can plainly see it inserted. Ask the store.
                let mut had_committed = false;
                if !self.committed_source_offers_every_row {
                    let probe = Cell::new(0_usize);
                    let existing =
                        context.with_source_page_admission(BoundedWorkSource::TableScan, || {
                            db.bounded_row_by_identity(
                                context.source_read_memory(BoundedWorkSource::TableScan),
                                None,
                                &self.table,
                                row.row_id,
                                self.snapshot,
                                &mut || {
                                    context.charge(
                                        BoundedWorkSource::TableScan,
                                        BoundedSourceTouch::TableRow,
                                    )
                                },
                                &mut |bytes| {
                                    let bytes = context
                                        .admit_source_row(BoundedWorkSource::TableScan, bytes)?;
                                    probe.set(bytes);
                                    Ok(())
                                },
                            )
                        })?;
                    // The probe asks only whether the original is there; the
                    // copy it made is handed straight back.
                    had_committed = existing.is_some();
                    drop(existing);
                    context.release(probe.get())?;
                }
                if !had_committed {
                    drop(row);
                    context.release(reserved.get())?;
                    continue;
                }
            }
            return Ok(Some((row, reserved.get())));
        }
    }

    /// Keep one matching staged head in the committed cursor's run and order.
    /// An index that does not cover every frozen row uses the admitted sorted
    /// fallback, preserving rows staged before that index was declared.
    fn materialize_ordered_staged(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        match self.ordered_merge.as_ref() {
            Some(merge) if merge.staged.is_none() => {}
            Some(merge)
                if merge.staged.as_ref().is_some_and(VecDeque::is_empty)
                    && self
                        .transaction
                        .as_ref()
                        .is_some_and(|tx| tx.ordered_staged.is_some()) =>
            {
                if let Some(staged) = self.next_staged_row(db, context)? {
                    let merge = self.ordered_merge.as_mut().expect("ordered merge is held");
                    merge.held_bytes = merge.held_bytes.saturating_add(staged.1);
                    merge
                        .staged
                        .as_mut()
                        .expect("ordered merge is initialized")
                        .push_back(staged);
                }
                return Ok(());
            }
            _ => return Ok(()),
        }
        let mut maintained_order = false;
        if let (Some(transaction), ScanMode::Ordered { cursor, .. }) =
            (&mut self.transaction, &self.mode)
            && let Some(index) = cursor.index_name()
            && transaction
                .overlay
                .staged_rows
                .as_ref()
                .is_some_and(|rows| rows.has_index_for(index, &self.meta))
        {
            let (start, end, _) = cursor.source_run_bounds();
            let control = std::mem::size_of::<OrderedStatementPosition>()
                .saturating_add(index.len())
                .saturating_add(staged_run_bound_bytes(start))
                .saturating_add(staged_run_bound_bytes(end))
                .saturating_add(cursor.pending_run_bytes().saturating_mul(2));
            context.reserve(BoundedWorkSource::TableScan, control)?;
            transaction.overlay.bytes = transaction.overlay.bytes.saturating_add(control);
            transaction.ordered_staged = Some(OrderedStatementPosition {
                index: index.to_owned(),
                cursor: cursor.clone(),
                after: None,
            });
            maintained_order = true;
        }
        let mut rows = Vec::new();
        if maintained_order {
            if let Some(staged) = self.next_staged_row(db, context)? {
                rows.push(staged);
            }
        } else {
            while let Some(staged) = self.next_staged_row(db, context)? {
                rows.push(staged);
            }
        }
        let Some(merge) = self.ordered_merge.as_mut() else {
            return Ok(());
        };
        // newest_event_merges_staged_order_without_sort observes the real
        // bounded-kernel overlay sort, as well as the eager executor's sort.
        // Only staged rows the maintained index does not order are sorted. A
        // transaction that staged no rows for this table, such as one that
        // only deleted committed rows, has nothing here and sorts nothing.
        if !maintained_order && !rows.is_empty() {
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_query_test_seams::note_staged_sort(rows.len());
            rows.sort_by(|left, right| compare_ordered_merge_rows(&left.0, &right.0, &merge.keys));
        }
        merge.held_bytes = rows.iter().map(|(_, bytes)| *bytes).sum();
        merge.staged = Some(rows.into());
        Ok(())
    }

    /// Hand back a staged row ahead of the committed row in hand, when its
    /// ordering value comes first. The committed row is held, not dropped:
    /// it is the next row out.
    fn fold_in_earlier_staged_row(&mut self) {
        let Some(merge) = self.ordered_merge.as_mut() else {
            return;
        };
        if merge.deferred.is_some() {
            return;
        }
        let Some(committed) = self.pending_row.as_ref() else {
            return;
        };
        let comes_first = match merge.staged.as_ref().and_then(VecDeque::front) {
            Some((head, _)) => compare_ordered_merge_rows(head, &committed.0, &merge.keys).is_lt(),
            None => false,
        };
        if !comes_first {
            return;
        }
        let Some((row, bytes)) = merge.staged.as_mut().and_then(VecDeque::pop_front) else {
            return;
        };
        merge.held_bytes = merge.held_bytes.saturating_sub(bytes);
        merge.deferred = self.pending_row.take();
        self.pending_row = Some((row, bytes, BoundedWorkSource::TableScan));
        self.pending_from_committed = false;
    }

    /// The next staged row of an ordered walk, once the committed run is done.
    fn next_ordered_staged_row(&mut self) -> Option<(VersionedRow, usize)> {
        let merge = self.ordered_merge.as_mut()?;
        let (row, bytes) = merge.staged.as_mut().and_then(VecDeque::pop_front)?;
        merge.held_bytes = merge.held_bytes.saturating_sub(bytes);
        Some((row, bytes))
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending_row.is_none() {
                self.pending_vector_copy = None;
            }
            self.materialize_ordered_staged(db, context)?;
            if self.pending_row.is_none()
                && let Some(merge) = self.ordered_merge.as_mut()
                && let Some(held) = merge.deferred.take()
            {
                self.pending_row = Some(held);
                self.pending_from_committed = true;
            }
            if self.pending_row.is_none() {
                let reserved = Cell::new(0usize);
                let (row, source) = match &mut self.mode {
                    ScanMode::Physical { cursor } => {
                        let receipt = Cell::new(false);
                        let pulled = context.with_source_page_admission(
                            BoundedWorkSource::TableScan,
                            || {
                                db.bounded_physical_table_row_next(
                                    context.source_read_memory(BoundedWorkSource::TableScan),
                                    self.table.as_str(),
                                    self.snapshot,
                                    cursor,
                                    &mut || {
                                        context.observe_precharged_touch(
                                            BoundedSourceTouch::StorageStep,
                                        )?;
                                        receipt.set(true);
                                        Ok(())
                                    },
                                    &mut |row| {
                                        context.charge(
                                            BoundedWorkSource::TableScan,
                                            BoundedSourceTouch::TableRow,
                                        )?;
                                        if !receipt.replace(false) {
                                            return Err(Error::Other(
                                                "physical input has no admitted work receipt"
                                                    .to_string(),
                                            )
                                            .into());
                                        }
                                        if let Some(source) = &self.source_group {
                                            context.observe_physical_group(
                                                source,
                                                &self.schema_columns,
                                                row,
                                            )?;
                                        }
                                        Ok(())
                                    },
                                    &mut |bytes| {
                                        let bytes = context.admit_source_row(
                                            BoundedWorkSource::TableScan,
                                            bytes,
                                        )?;
                                        reserved.set(bytes);
                                        Ok(())
                                    },
                                )
                            },
                        )?;
                        (pulled, BoundedWorkSource::TableScan)
                    }
                    ScanMode::Ordered {
                        column,
                        direction,
                        cursor,
                        ..
                    } => {
                        let receipt = Cell::new(false);
                        let old_key_bytes = cursor.retained_key_bytes();
                        let old_key_generation = cursor.key_generation();
                        let index_columns = self
                            .meta
                            .indexes
                            .iter()
                            .find(|index| Some(index.name.as_str()) == cursor.index_name())
                            .map(|index| index.columns.as_slice())
                            .ok_or_else(|| {
                                Error::Other("source index declaration is missing".to_string())
                            })?;
                        let pulled = context.with_source_page_admission(
                            BoundedWorkSource::IndexRange,
                            || {
                                db.bounded_ordered_table_row_next_observed(
                                    context.source_read_memory(BoundedWorkSource::IndexRange),
                                    &self.table,
                                    column,
                                    *direction,
                                    self.snapshot,
                                    cursor,
                                    &mut || context.check_final_boundary(),
                                    &mut || {
                                        context.observe_precharged_touch(
                                            BoundedSourceTouch::StorageStep,
                                        )?;
                                        receipt.set(true);
                                        Ok(())
                                    },
                                    &mut |key, row_id| {
                                        context.charge(
                                            BoundedWorkSource::IndexRange,
                                            BoundedSourceTouch::IndexEntry,
                                        )?;
                                        if !receipt.replace(false) {
                                            return Err(Error::Other(
                                                "index input has no admitted work receipt"
                                                    .to_string(),
                                            )
                                            .into());
                                        }
                                        if let Some(source) = &self.source_group {
                                            context.observe_index_group(
                                                source,
                                                &self.schema_columns,
                                                index_columns,
                                                key,
                                                row_id,
                                            )?;
                                        }
                                        Ok(())
                                    },
                                    &mut |bytes| {
                                        let bytes = context.admit_source_row(
                                            BoundedWorkSource::IndexRange,
                                            bytes,
                                        )?;
                                        reserved.set(bytes);
                                        Ok(())
                                    },
                                )
                            },
                        )?;
                        if cursor.key_generation() != old_key_generation {
                            context.release(old_key_bytes)?;
                            let row_bytes = reserved
                                .get()
                                .checked_sub(cursor.retained_key_bytes())
                                .ok_or_else(|| {
                                    Error::Other(
                                        "bounded ordered row charge omitted its cursor key"
                                            .to_string(),
                                    )
                                })?;
                            reserved.set(row_bytes);
                            self.cursor_key_bytes = cursor.retained_key_bytes();
                        }
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Index {
                        column,
                        direction,
                        cursor,
                        ..
                    } => {
                        let receipt = Cell::new(false);
                        let old_key_bytes = cursor.retained_key_bytes();
                        let old_key_generation = cursor.key_generation();
                        let index_columns = self
                            .meta
                            .indexes
                            .iter()
                            .find(|index| Some(index.name.as_str()) == cursor.index_name())
                            .map(|index| index.columns.as_slice())
                            .ok_or_else(|| {
                                Error::Other("source index declaration is missing".to_string())
                            })?;
                        let pulled = context.with_source_page_admission(
                            BoundedWorkSource::IndexRange,
                            || {
                                db.bounded_ordered_table_row_next_observed(
                                    context.source_read_memory(BoundedWorkSource::IndexRange),
                                    &self.table,
                                    column,
                                    *direction,
                                    self.snapshot,
                                    cursor,
                                    &mut || context.check_final_boundary(),
                                    &mut || {
                                        context.observe_precharged_touch(
                                            BoundedSourceTouch::StorageStep,
                                        )?;
                                        receipt.set(true);
                                        Ok(())
                                    },
                                    &mut |key, row_id| {
                                        context.charge(
                                            BoundedWorkSource::IndexRange,
                                            BoundedSourceTouch::IndexEntry,
                                        )?;
                                        if !receipt.replace(false) {
                                            return Err(Error::Other(
                                                "index input has no admitted work receipt"
                                                    .to_string(),
                                            )
                                            .into());
                                        }
                                        if let Some(source) = &self.source_group {
                                            context.observe_index_group(
                                                source,
                                                &self.schema_columns,
                                                index_columns,
                                                key,
                                                row_id,
                                            )?;
                                        }
                                        Ok(())
                                    },
                                    &mut |bytes| {
                                        let bytes = context.admit_source_row(
                                            BoundedWorkSource::IndexRange,
                                            bytes,
                                        )?;
                                        reserved.set(bytes);
                                        Ok(())
                                    },
                                )
                            },
                        )?;
                        if cursor.key_generation() != old_key_generation {
                            context.release(old_key_bytes)?;
                            let row_bytes = reserved
                                .get()
                                .checked_sub(cursor.retained_key_bytes())
                                .ok_or_else(|| {
                                    Error::Other(
                                        "bounded index row charge omitted its cursor key"
                                            .to_string(),
                                    )
                                })?;
                            reserved.set(row_bytes);
                            self.cursor_key_bytes = cursor.retained_key_bytes();
                        }
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Exact { index, cursor } => {
                        let pulled = context.with_source_page_admission(
                            BoundedWorkSource::IndexRange,
                            || {
                                db.bounded_exact_index_row_next(
                                    context.source_read_memory(BoundedWorkSource::IndexRange),
                                    &self.table,
                                    index,
                                    self.snapshot,
                                    cursor,
                                    &mut || {
                                        context.observe_precharged_touch(
                                            BoundedSourceTouch::StorageStep,
                                        )
                                    },
                                    &mut || {
                                        context.charge(
                                            BoundedWorkSource::IndexRange,
                                            BoundedSourceTouch::IndexEntry,
                                        )
                                    },
                                    &mut |bytes| {
                                        context.reserve(BoundedWorkSource::IndexRange, bytes)
                                    },
                                    &mut |bytes| context.release(bytes),
                                    &mut |bytes| {
                                        let bytes = context.admit_source_row(
                                            BoundedWorkSource::IndexRange,
                                            bytes,
                                        )?;
                                        reserved.set(bytes);
                                        Ok(())
                                    },
                                )
                            },
                        )?;
                        self.cursor_key_bytes = cursor.retained_bytes();
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Empty => return Ok(None),
                };
                let row = if let Some(identity) = row {
                    let identity_source = matches!(&self.mode,
                        ScanMode::Ordered { cursor, .. } if cursor.source_identities_only());
                    if identity_source && identity.values.is_empty() {
                        // Suppression is decided on identity before selected
                        // body work. Hidden postings never consume a rank.
                        if self
                            .transaction
                            .as_mut()
                            .is_some_and(|state| state.hides_committed(&self.table, &identity))
                        {
                            context.release(reserved.get())?;
                            continue;
                        }
                        let identity_bytes = reserved.get();
                        let selected_bytes = Cell::new(0);
                        let selected = context.with_source_page_admission(source, || {
                            db.bounded_row_by_identity(
                                context.source_read_memory(source),
                                None,
                                &self.table,
                                identity.row_id,
                                self.snapshot,
                                &mut || context.charge(source, BoundedSourceTouch::TableRow),
                                &mut |bytes| {
                                    let bytes = context.admit_source_row(source, bytes)?;
                                    selected_bytes.set(bytes);
                                    Ok(())
                                },
                            )
                        })?;
                        context.release(identity_bytes)?;
                        reserved.set(selected_bytes.get());
                        if selected.is_none() {
                            return Err(Error::StoreCorrupted {
                                path: db.bounded_store_path(),
                                reason: "authenticated current posting has no visible row"
                                    .to_string(),
                            }
                            .into());
                        }
                        selected
                    } else {
                        Some(identity)
                    }
                } else {
                    None
                };
                // The source charged its retained-representation estimate
                // before it cloned. This read holds exactly one copy of what
                // came back, so the reservation becomes what that copy costs.
                let row = match row {
                    Some(row) => {
                        let held = pulled_row_retained_bytes(&row)?;
                        if held > reserved.get() {
                            if let Err(error) = context.reserve(source, held - reserved.get()) {
                                drop(row);
                                context.release(reserved.get())?;
                                return Err(error);
                            }
                        } else if reserved.get() > held {
                            context.release(reserved.get() - held)?;
                        }
                        reserved.set(held);
                        Some(row)
                    }
                    None => None,
                };
                let Some(row) = row else {
                    let source_exhausted = match &self.mode {
                        ScanMode::Physical { cursor } => cursor.is_exhausted(),
                        ScanMode::Index { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Ordered { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Exact { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Empty => true,
                    };
                    if source_exhausted {
                        // The committed rows are done. If this handle has a
                        // transaction open, the rows it staged are part of
                        // its own answer and come next, each charged the same
                        // way a committed row is. An ordered walk has already
                        // read them and hands back whatever the fold has left.
                        if let Some((row, bytes)) = self.next_ordered_staged_row() {
                            self.pending_row = Some((row, bytes, source));
                            self.pending_from_committed = false;
                            continue;
                        }
                        if self.ordered_merge.is_none()
                            && let Some(staged) = self.next_staged_row(db, context)?
                        {
                            self.pending_row = Some((staged.0, staged.1, source));
                            self.pending_from_committed = false;
                            continue;
                        }
                        context.release(self.cursor_key_bytes)?;
                        self.cursor_key_bytes = 0;
                        return Ok(None);
                    }
                    continue;
                };
                #[cfg(feature = "test-seams")]
                if self.vector_candidate_source && !self.identities_only {
                    db.note_vector_candidate_materialized();
                }
                self.pending_row = Some((row, reserved.get(), source));
                self.pending_from_committed = true;
            }
            // A staged row whose ordering value comes before the committed row
            // in hand is handed back first; the committed row waits its turn.
            if self.pending_from_committed {
                self.fold_in_earlier_staged_row();
            }
            if !self.supplement_columns.is_empty() {
                if self.pending_vector_copy.is_none() {
                    self.pending_vector_copy = Some(Box::new(
                        crate::vector_read_memory::admission(Arc::new(SourceDecodeBudget {
                            context: context.clone(),
                            source: BoundedWorkSource::TableScan,
                        })),
                    ));
                }
                let (row_identity, row_lsn) = self
                    .pending_row
                    .as_ref()
                    .map(|(row, _, _)| (row.row_id, row.lsn))
                    .ok_or_else(|| Error::Other("bounded scan lost its row".to_string()))?;
                let mut filled = Vec::new();
                for column in &self.supplement_columns {
                    if !self.pending_row.as_ref().is_some_and(|(row, _, _)| {
                        matches!(row.values.get(column), Some(Value::Null) | None)
                    }) {
                        continue;
                    }
                    if let Some(vector) = db.load_row_vector_for_column(
                        context.transaction(),
                        &self.table,
                        column,
                        row_identity,
                        row_lsn,
                        self.snapshot,
                        self.pending_vector_copy
                            .as_deref_mut()
                            .expect("pending vector owner"),
                        Arc::clone(&context.reader_budget),
                    )? {
                        filled.push((column.clone(), Value::Vector(vector)));
                    }
                }
                if let Some((row, _, _)) = self.pending_row.as_mut() {
                    for (column, value) in filled {
                        row.values.insert(column, value);
                    }
                }
            }

            let (source_bytes, source) = self
                .pending_row
                .as_ref()
                .map(|(_, bytes, source)| (*bytes, *source))
                .ok_or_else(|| Error::Other("bounded scan lost its pending row".to_string()))?;
            let row = &self
                .pending_row
                .as_ref()
                .ok_or_else(|| Error::Other("bounded scan lost its pending row".to_string()))?
                .0;
            // A row this handle's own transaction staged is not committed and
            // never will be at this snapshot, so the committed-visibility rule
            // is not the one that decides it -- the transaction it belongs to
            // is. Only a row that came from the committed source is judged
            // here.
            let from_committed = if self.ordered_merge.is_some() {
                self.pending_from_committed
            } else {
                self.transaction
                    .as_ref()
                    .is_none_or(|state| !state.committed_exhausted)
            };
            if from_committed && !row.visible_at(self.snapshot) {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost an invisible row".to_string())
                })?;
                context.release(bytes)?;
                continue;
            }
            if from_committed
                && let Some(state) = self.transaction.as_mut()
                && state.hides_committed(&self.table, row)
            {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost a row its transaction hides".to_string())
                })?;
                context.release(bytes)?;
                continue;
            }

            let index_pick = if self.identities_only && from_committed {
                None
            } else {
                match &self.mode {
                    ScanMode::Index { pick, .. } => Some(pick.as_ref()),
                    _ => self.residual_pick.as_deref(),
                }
            };
            if let Some(pick) = index_pick {
                let key = contextdb_relational::index_key_for_row(&pick.columns, &row.values);
                if !index_key_matches_pick(&key, pick) {
                    let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                        Error::Other("bounded index scan lost a rejected row".to_string())
                    })?;
                    context.release(bytes)?;
                    continue;
                }
            }

            // A read that PINS the row it asks about is refused when the
            // reader is not entitled to it, with the reason the caller
            // branches on: answering "no such row" about a row that exists
            // tells the caller the store does not hold what it holds. A read
            // that merely scans is asking what it MAY see, and hiding is the
            // honest answer -- so the two are told apart by the executor's own
            // line, asked here rather than drawn again.
            // An exact whole-key probe is the same statement whether it is
            // answered by the exact source or by the authenticated run it was
            // re-planned onto, so the same question is asked either way.
            let exact_index = match &self.mode {
                ScanMode::Exact { index, .. } => Some(index.as_str()),
                _ => self.authenticated_exact_index.as_deref(),
            };
            // The statement decides whether it names one row, on every target and source; only a
            // source that carries its keys with no statement filter is asked about itself.
            let statement_pins = self
                .filter
                .as_ref()
                .map(|filter| crate::executor::statement_names_one_row(&self.meta, filter, &self.params));
            let names_one_row = statement_pins.unwrap_or_else(|| match (exact_index, index_pick) {
                // An exact-lookup source asked a whole key: it is already
                // reading the row that key names.
                (Some(index), _) => db.read_index_names_one_row(
                    &self.table,
                    index,
                    self.filter.as_ref(),
                    &self.params,
                ),
                (_, Some(pick)) => db.read_pick_names_one_row(
                    &self.table,
                    pick,
                    self.filter.as_ref(),
                    &self.params,
                ),
                _ => false,
            });
            let mut charge_access = |_candidate_bytes: usize| {
                context.charge(
                    BoundedWorkSource::AccessControl,
                    BoundedSourceTouch::AccessRow,
                )?;
                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                Ok::<(), BoundedExecutionError>(())
            };
            let mut refused = None;
            let mut hidden = false;
            // A candidate this scan is deriving for a vector search is not a
            // row anyone is served. It was drawn from an index range that the
            // reader's own restrictions already bound (see
            // `select_vector_candidate_identity_source`), so there is nothing
            // left for the row gate to decide about it -- and deciding it
            // anyway would mean reading a stored row version per candidate to
            // publish k of them. The k rows that ARE published go through the
            // gate where they are published, unchanged.
            let candidate_range_answers_gate =
                self.vector_candidate_source && self.gate_answered_by_candidate_range;
            if candidate_range_answers_gate {
                note_vector_candidate_identity("candidate range answers the gate")?;
            } else if names_one_row {
                // Only a row that matches the complete statement can be the row it names; any
                // other row is not an answer at all, so it is skipped before the refusing gate.
                if let Some(filter) = self.filter.as_ref()
                    && !(self.identities_only && from_committed)
                {
                    context.check_final_boundary()?;
                    let matched = bounded_row_matches(row, filter, &self.params);
                    context.check_final_boundary()?;
                    if !matched? {
                        let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                            Error::Other("bounded scan lost a filtered row".to_string())
                        })?;
                        context.release(bytes)?;
                        continue;
                    }
                }
                refused = db.bounded_read_denial_for_row(
                    // The request's transaction, not this scan's row
                    // overlay: a transaction that staged only a GRANT row has
                    // no overlay on the table being scanned, and its own grant
                    // still has to admit its own rows.
                    context.transaction(),
                    &self.table,
                    &self.meta,
                    row,
                    self.snapshot,
                    &mut charge_access,
                    context.source_read_memory(BoundedWorkSource::AccessControl),
                )?;
            } else {
                hidden = !db.bounded_read_allowed_for_row(
                    // The request's transaction, not this scan's row
                    // overlay: a transaction that staged only a GRANT row has
                    // no overlay on the table being scanned, and its own grant
                    // still has to admit its own rows.
                    context.transaction(),
                    &self.table,
                    &self.meta,
                    row,
                    self.snapshot,
                    &mut charge_access,
                    context.source_read_memory(BoundedWorkSource::AccessControl),
                )?;
            }
            if hidden || refused.is_some() {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost a row the gate withheld".to_string())
                })?;
                context.release(bytes)?;
                if let Some(denial) = refused {
                    return Err(BoundedExecutionError::from(denial));
                }
                continue;
            }

            if let Some(filter) = self.filter.as_ref()
                && !(self.identities_only && from_committed)
            {
                context.check_final_boundary()?;
                let matched = bounded_row_matches(row, filter, &self.params);
                context.check_final_boundary()?;
                if !matched? {
                    let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                        Error::Other("bounded scan lost a filtered row".to_string())
                    })?;
                    context.release(bytes)?;
                    continue;
                }
            }

            let row_id = i64::try_from(row.row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?;
            let row_id_value = Value::Int64(row_id);
            let output_capacity = self
                .schema_columns
                .len()
                .checked_add(1)
                .ok_or_else(|| bounded_size_error("scan projection"))?;
            let planned_output_bytes = checked_size_add(
                std::mem::size_of::<Vec<Value>>(),
                checked_size_mul(
                    output_capacity,
                    std::mem::size_of::<Value>(),
                    "scan projection slots",
                )?,
                "scan projection",
            )?;
            let vector_credit = self
                .pending_vector_copy
                .as_ref()
                .map_or(0, |owner| owner.retained_bytes());
            let funded_output_bytes = checked_size_add(
                checked_size_add(source_bytes, vector_credit, "source projection credit")?,
                planned_output_bytes,
                "projection credit",
            )?;
            context.reserve(source, planned_output_bytes)?;
            let mut values = Vec::new();
            if values.try_reserve_exact(output_capacity).is_err() {
                context.release(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded scan projection allocation failed".to_string()).into(),
                );
            }
            let (source_row, retained_source_bytes, _) = self
                .pending_row
                .take()
                .ok_or_else(|| Error::Other("bounded scan lost its projected row".to_string()))?;
            if source_bytes != retained_source_bytes {
                return Err(Error::Other(
                    "bounded scan source charge changed before release".to_string(),
                )
                .into());
            }
            // The output row takes the source row's values rather than copies
            // of them: the source row is released right here, so a copy would
            // only put a second instance of every value -- one very large
            // value included -- in memory for the length of this step.
            let mut source_values = source_row.values;
            values.push(row_id_value);
            values.extend(
                self.schema_columns
                    .iter()
                    .map(|column| source_values.remove(column).unwrap_or(Value::Null)),
            );
            drop(source_values);
            let output_bytes = values_retained_bytes(&values)?;
            if let Some(owner) = self.pending_vector_copy.take() {
                (*owner).transfer();
            }
            // The old HashMap, discarded fields and source owner are gone;
            // reconcile all transferred credit only now.
            if output_bytes > funded_output_bytes {
                if let Err(error) = context.reserve(source, output_bytes - funded_output_bytes) {
                    drop(values);
                    context.release(funded_output_bytes)?;
                    return Err(error);
                }
            } else if funded_output_bytes > output_bytes {
                context.release(funded_output_bytes - output_bytes)?;
            }
            if let ScanMode::Physical { cursor } = &mut self.mode {
                // The emitted anchor is what a later pull re-anchors against
                // after maintenance compacts the vector; an emitted row is
                // visible at this scan's registered snapshot, so maintenance
                // never removes it while the continuation is suspended.
                cursor.note_emitted();
            }
            return Ok(Some(PulledRow {
                window: None,
                values,
                retained_bytes: output_bytes,
                group_binding: None,
            }));
        }
    }

    fn has_more_hint(&self) -> Option<bool> {
        if self.pending_row.is_some() {
            return None;
        }
        // Rows the fold is still holding are part of this answer, whether or
        // not the committed run has anything left.
        if self.ordered_merge.as_ref().is_some_and(|merge| {
            merge.deferred.is_some()
                || merge
                    .staged
                    .as_ref()
                    .is_some_and(|staged| !staged.is_empty())
        }) {
            return None;
        }
        match &self.mode {
            ScanMode::Physical { cursor } if cursor.is_exhausted() => Some(false),
            ScanMode::Index { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Ordered { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Exact { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Empty => Some(false),
            ScanMode::Physical { .. }
            | ScanMode::Index { .. }
            | ScanMode::Ordered { .. }
            | ScanMode::Exact { .. } => None,
        }
    }
}

impl GraphState {
    fn next_unpinned_edge(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<(uuid::Uuid, uuid::Uuid)>, BoundedExecutionError> {
        loop {
            let Some(source) = self.edge_source.as_mut() else {
                return Ok(None);
            };
            let first_step = self.steps.first().ok_or_else(|| {
                Error::Other("bounded graph plan has no traversal step".to_string())
            })?;
            let edge_types =
                (!first_step.edge_types.is_empty()).then_some(first_step.edge_types.as_slice());
            let edge = {
                let mut access_memory = ReservedBytes::new(context, 0);
                db.bounded_graph_edge_next(
                    source,
                    edge_types,
                    context.transaction(),
                    &mut || {
                        context.charge(
                            BoundedWorkSource::GraphTraversal,
                            BoundedSourceTouch::GraphEdge,
                        )
                    },
                    &mut |bytes| access_memory.charge_access_row(bytes),
                    &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                    &mut |bytes| context.release(bytes),
                    &mut || context.note_touch(BoundedSourceTouch::AdjacencyEntry),
                    context.source_read_memory(BoundedWorkSource::GraphTraversal),
                )
            }?;
            let Some(edge) = edge else {
                let source = self.edge_source.take().ok_or_else(|| {
                    Error::Other("bounded graph lost its exhausted edge source".to_string())
                })?;
                context.release(source.retained_bytes())?;
                return Ok(None);
            };
            if self.single_step_unpinned {
                // A single hop answers once per matching edge, and the scan
                // reports each edge once, so every edge it reports is a row.
                return Ok(Some(edge));
            }
            let start = edge.0;
            match self.seen_starts.binary_search(&start) {
                Ok(_) => continue,
                Err(position) => {
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::GraphTraversal,
                        &mut self.seen_starts,
                        "graph start deduplication",
                    )?;
                    self.seen_start_container_bytes = self
                        .seen_start_container_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("graph start deduplication"))?;
                    self.seen_starts.insert(position, start);
                    return Ok(Some(edge));
                }
            }
        }
    }

    fn next_start(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<uuid::Uuid>, BoundedExecutionError> {
        self.resolve_next_start(db, context)
    }

    fn resolve_next_start(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<uuid::Uuid>, BoundedExecutionError> {
        // A start named twice is one start: its walk, and every row out of it,
        // happens once.
        loop {
            let start = if let Some(start) = self.starts.pop_front() {
                start
            } else if let Some(start) = self.next_candidate_start(db, context)? {
                start
            } else {
                return Ok(self.next_unpinned_edge(db, context)?.map(|edge| edge.0));
            };
            match self.seen_starts.binary_search(&start) {
                Ok(_) => continue,
                Err(position) => {
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::GraphTraversal,
                        &mut self.seen_starts,
                        "graph start deduplication",
                    )?;
                    self.seen_start_container_bytes = self
                        .seen_start_container_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("graph start deduplication"))?;
                    self.seen_starts.insert(position, start);
                    return Ok(Some(start));
                }
            }
        }
    }

    fn next_candidate_start(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<uuid::Uuid>, BoundedExecutionError> {
        let Some(candidates) = self.start_candidates.as_mut() else {
            return Ok(None);
        };
        loop {
            let Some(row) = candidates.next(db, context)? else {
                self.start_candidates = None;
                return Ok(None);
            };
            let start = row.values.iter().find_map(|value| match value {
                Value::Uuid(id) => Some(Ok(*id)),
                Value::Text(text) => Some(uuid::Uuid::parse_str(text).map_err(|_| {
                    Error::Other(format!(
                        "bounded graph start candidate is not a UUID: {text}"
                    ))
                })),
                _ => None,
            });
            row.release(context)?;
            if let Some(start) = start {
                return start.map(Some).map_err(BoundedExecutionError::from);
            }
        }
    }

    fn next_unpinned_single_step(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            let Some((start, target)) = self.next_unpinned_edge(db, context)? else {
                self.row_memory.release(context)?;
                return Ok(None);
            };
            let binding_capacity = 2usize;
            let planned_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_add(
                    checked_size_mul(
                        binding_capacity,
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    checked_size_add(
                        self.start_alias.len(),
                        self.steps[0].target_alias.len(),
                        "graph binding aliases",
                    )?,
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            self.row_memory.take(context, planned_binding_bytes)?;
            let mut bindings = HashMap::new();
            if bindings.try_reserve(binding_capacity).is_err() {
                self.row_memory.give_back(planned_binding_bytes)?;
                return Err(
                    Error::Other("bounded graph binding allocation failed".to_string()).into(),
                );
            }
            bindings.insert(self.start_alias.clone(), start);
            bindings.insert(self.steps[0].target_alias.clone(), target);
            let actual_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_add(
                    checked_size_mul(
                        bindings.capacity(),
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    bindings.keys().try_fold(0usize, |bytes, alias| {
                        checked_size_add(bytes, alias.capacity(), "graph binding aliases")
                    })?,
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            if actual_binding_bytes > planned_binding_bytes {
                if let Err(error) = self
                    .row_memory
                    .take(context, actual_binding_bytes - planned_binding_bytes)
                {
                    self.row_memory.give_back(planned_binding_bytes)?;
                    return Err(error);
                }
            } else {
                self.row_memory
                    .give_back(planned_binding_bytes - actual_binding_bytes)?;
            }
            let mut predicate_bytes = 0usize;
            let mut predicate_access_bytes = 0usize;
            let mut predicate_failure = None;
            let matches = self
                .filter
                .as_ref()
                .map(|filter| {
                    db.graph_filter_matches_bindings_with_work(
                        filter,
                        &self.params,
                        // The reader's own transaction: a WHERE on node
                        // metadata asks what this handle can see, and what it
                        // staged is part of that. Everything below this call
                        // already takes the transaction and already composes
                        // the overlay; only this door was never handing it
                        // over.
                        context.transaction(),
                        self.snapshot,
                        &bindings,
                        &mut |work| {
                            let result = match work {
                                GraphPredicateWork::Checkpoint => context.charge(
                                    BoundedWorkSource::GraphTraversal,
                                    BoundedSourceTouch::AccessRow,
                                ),
                                GraphPredicateWork::AccessRow(bytes) => context
                                    .charge(
                                        BoundedWorkSource::AccessControl,
                                        BoundedSourceTouch::AccessRow,
                                    )
                                    .and_then(|()| {
                                        let extra = bytes.saturating_sub(predicate_access_bytes);
                                        context.reserve(BoundedWorkSource::AccessControl, extra)?;
                                        predicate_access_bytes = predicate_access_bytes.max(bytes);
                                        predicate_bytes = predicate_bytes.saturating_add(extra);
                                        Ok(())
                                    }),
                            };
                            if let Err(error) = result {
                                predicate_failure = Some(error);
                                return Err(Error::ReadCancelled);
                            }
                            Ok(())
                        },
                        Arc::new(SourceDecodeBudget {
                            context: context.clone(),
                            source: BoundedWorkSource::GraphTraversal,
                        }),
                    )
                })
                .transpose();
            context.release(predicate_bytes)?;
            if let Some(error) = predicate_failure {
                self.row_memory.give_back(actual_binding_bytes)?;
                return Err(error);
            }
            let matches = match matches {
                Ok(Some(matches)) => matches,
                Ok(None) => true,
                Err(error) => {
                    self.row_memory.give_back(actual_binding_bytes)?;
                    return Err(error.into());
                }
            };
            self.row_memory.give_back(actual_binding_bytes)?;
            if !matches {
                continue;
            }
            let planned_values = 4usize;
            let planned_output_bytes = checked_size_add(
                std::mem::size_of::<Vec<Value>>(),
                checked_size_mul(
                    planned_values,
                    std::mem::size_of::<Value>(),
                    "graph projection",
                )?,
                "graph projection",
            )?;
            self.row_memory.take(context, planned_output_bytes)?;
            let mut projected = Vec::new();
            if projected.try_reserve_exact(planned_values).is_err() {
                self.row_memory.give_back(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded graph projection allocation failed".to_string()).into(),
                );
            }
            projected.push(Value::Uuid(start));
            projected.push(Value::Uuid(target));
            projected.push(Value::Uuid(target));
            projected.push(Value::Int64(1));
            let output_bytes = values_retained_bytes(&projected)?;
            if output_bytes > planned_output_bytes {
                if let Err(error) = self
                    .row_memory
                    .take(context, output_bytes - planned_output_bytes)
                {
                    self.row_memory.give_back(planned_output_bytes)?;
                    return Err(error);
                }
            } else {
                self.row_memory
                    .give_back(planned_output_bytes - output_bytes)?;
            }
            return Ok(Some(PulledRow {
                window: None,
                values: projected,
                retained_bytes: output_bytes,
                group_binding: None,
            }));
        }
    }

    fn initial_path(
        &self,
        start: uuid::Uuid,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<GraphPathState, BoundedExecutionError> {
        let capacity = self
            .steps
            .len()
            .checked_add(1)
            .ok_or_else(|| bounded_size_error("graph path bindings"))?;
        let planned_bytes = checked_size_mul(
            capacity,
            std::mem::size_of::<uuid::Uuid>(),
            "graph path bindings",
        )?;
        context.reserve(BoundedWorkSource::GraphTraversal, planned_bytes)?;
        let mut bindings = Vec::new();
        if bindings.try_reserve_exact(capacity).is_err() {
            context.release(planned_bytes)?;
            return Err(Error::Other("bounded graph path allocation failed".to_string()).into());
        }
        bindings.push(start);
        let retained_bytes = checked_size_mul(
            bindings.capacity(),
            std::mem::size_of::<uuid::Uuid>(),
            "graph path bindings",
        )?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::GraphTraversal,
            planned_bytes,
            retained_bytes,
        )?;
        Ok(GraphPathState {
            bindings,
            node: start,
            total_depth: 0,
            step_index: 0,
            retained_bytes,
        })
    }

    fn open_active(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        path: GraphPathState,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(active) = self.open_walk(db, context, path)? {
            self.active = Some(active);
        }
        Ok(())
    }

    /// Open the walk out of one path's node for its step. `None` when there is
    /// nothing to walk, with the path's charge given back.
    fn open_walk(
        &self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        path: GraphPathState,
    ) -> std::result::Result<Option<GraphActiveState>, BoundedExecutionError> {
        let step = self.steps.get(path.step_index).ok_or_else(|| {
            Error::Other("bounded graph continuation has no traversal step".to_string())
        })?;
        let edge_types = (!step.edge_types.is_empty()).then_some(step.edge_types.as_slice());
        let direction = if self.probes_backwards_from_target {
            super::reverse_graph_probe_direction(step.direction)
        } else {
            step.direction
        };
        let cursor = {
            let mut access_memory = ReservedBytes::new(context, 0);
            db.bounded_graph_bfs_cursor(
                context.transaction(),
                path.node,
                edge_types,
                direction,
                step.min_depth,
                step.max_depth,
                self.snapshot,
                &mut |bytes| access_memory.charge_access_row(bytes),
                &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                &mut |bytes| context.release_parked(bytes),
                context.source_read_memory(BoundedWorkSource::GraphTraversal),
            )
        }?;
        let Some(cursor) = cursor else {
            context.release(path.retained_bytes)?;
            return Ok(None);
        };
        Ok(Some(GraphActiveState { path, cursor }))
    }

    /// Read the next node a walk reports, charged as every traversal read is.
    fn advance_walk(
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        cursor: &mut contextdb_graph::mem::BoundedBfsCursor,
        visited_cap: Option<usize>,
    ) -> std::result::Result<
        Option<contextdb_graph::mem::BoundedTraversalNode>,
        BoundedExecutionError,
    > {
        let mut access_memory = ReservedBytes::new(context, 0);
        db.bounded_graph_bfs_next(
            cursor,
            &mut || {
                context.charge(
                    BoundedWorkSource::GraphTraversal,
                    BoundedSourceTouch::GraphEdge,
                )
            },
            // A path clone is traversal work, not an edge touch, so it is
            // charged without a source touch and `rows_examined` keeps its
            // meaning.
            &mut || context.charge_operator(BoundedWorkSource::GraphTraversal),
            &mut |bytes| access_memory.charge_access_row(bytes),
            &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
            &mut |bytes| context.release_parked(bytes),
            // Stepping over one staged edge is a source touch like reading
            // one committed adjacency entry, and the one name copied out of
            // the transaction is reserved on the same channel that releases
            // it.
            &mut || {
                context.charge(
                    BoundedWorkSource::GraphTraversal,
                    BoundedSourceTouch::GraphEdge,
                )
            },
            &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
            &mut |bytes| context.release_parked(bytes),
            &mut || context.note_touch(BoundedSourceTouch::AdjacencyEntry),
            visited_cap,
            context.transaction(),
            context.source_read_memory(BoundedWorkSource::GraphTraversal),
        )
    }

    /// Whether the first step's length varies, so starts can reach one target
    /// at different depths and their walks are merged.
    fn merges_starts(&self) -> bool {
        self.steps
            .first()
            .is_some_and(|step| step.min_depth < step.max_depth)
    }

    /// Resolve every start and open each one's first-step walk with its first
    /// node in hand. The starts are ranked as given, or ascending by id when
    /// the pattern names none.
    fn open_start_walks(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let unnamed = self.edge_source.is_some();
        let mut starts: Vec<uuid::Uuid> = Vec::new();
        let mut starts_bytes = 0usize;
        loop {
            let start = match self.next_start(db, context) {
                Ok(Some(start)) => start,
                Ok(None) => break,
                Err(error) => {
                    context.release(starts_bytes)?;
                    return Err(error);
                }
            };
            let added = match reserve_vector_slot(
                context,
                BoundedWorkSource::GraphTraversal,
                &mut starts,
                "graph starts",
            ) {
                Ok(added) => added,
                Err(error) => {
                    context.release(starts_bytes)?;
                    return Err(error);
                }
            };
            starts_bytes = checked_size_add(starts_bytes, added, "graph starts")?;
            starts.push(start);
        }
        if unnamed {
            starts.sort_unstable();
        }
        let mut walks: Vec<GraphStartWalk> = Vec::new();
        let mut walks_bytes = 0usize;
        for (rank, start) in starts.iter().copied().enumerate() {
            let opened = self
                .initial_path(start, context)
                .and_then(|path| self.open_walk(db, context, path));
            let mut active = match opened {
                Ok(Some(active)) => active,
                Ok(None) => continue,
                Err(error) => {
                    Self::dispose_walks(context, walks, walks_bytes);
                    context.release_parked(starts_bytes);
                    return Err(error);
                }
            };
            let next = match Self::advance_walk(db, context, &mut active.cursor, self.visited_cap) {
                Ok(Some(next)) => next,
                Ok(None) => {
                    context.release(active.cursor.into_retained_bytes())?;
                    context.release(active.path.retained_bytes)?;
                    continue;
                }
                Err(error) => {
                    context.release_parked(active.cursor.into_retained_bytes());
                    context.release_parked(active.path.retained_bytes);
                    Self::dispose_walks(context, walks, walks_bytes);
                    context.release_parked(starts_bytes);
                    return Err(error);
                }
            };
            let added = match reserve_vector_slot(
                context,
                BoundedWorkSource::GraphTraversal,
                &mut walks,
                "graph start walks",
            ) {
                Ok(added) => added,
                Err(error) => {
                    context.release_parked(next.reserved_bytes);
                    context.release_parked(active.cursor.into_retained_bytes());
                    context.release_parked(active.path.retained_bytes);
                    Self::dispose_walks(context, walks, walks_bytes);
                    context.release_parked(starts_bytes);
                    return Err(error);
                }
            };
            walks_bytes = checked_size_add(walks_bytes, added, "graph start walks")?;
            walks.push(GraphStartWalk { rank, active, next });
        }
        drop(starts);
        context.release(starts_bytes)?;
        // The heap keeps the vector's allocation, and a walk only ever goes
        // back after it was taken out, so it never grows past what was charged.
        self.start_walk_container_bytes = walks_bytes;
        self.start_walks = Some(std::collections::BinaryHeap::from(walks));
        Ok(())
    }

    /// Make the merge's next walk the active one: the walk whose next node is
    /// shallowest, the earlier start on a tie. The first call opens them.
    fn take_start_walk(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.start_walks.is_none() {
            self.open_start_walks(db, context)?;
        }
        if let Some(walk) = self.start_walks.as_mut().and_then(|walks| walks.pop()) {
            self.active = Some(walk.active);
            self.chosen = Some(walk.next);
            self.active_rank = Some(walk.rank);
        }
        Ok(())
    }

    /// Put the active walk back into the merge once its chosen node has been
    /// handled, with the next node it reports, or close it when it has none.
    fn return_chosen_walk(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let Some(rank) = self.active_rank.take() else {
            return Ok(());
        };
        let mut active = self
            .active
            .take()
            .ok_or_else(|| Error::Other("bounded graph lost its merged start walk".to_string()))?;
        match Self::advance_walk(db, context, &mut active.cursor, self.visited_cap) {
            Ok(Some(next)) => {
                let walks = self.start_walks.as_mut().ok_or_else(|| {
                    Error::Other("bounded graph lost its start walk merge".to_string())
                })?;
                walks.push(GraphStartWalk { rank, active, next });
                Ok(())
            }
            Ok(None) => {
                context.release(active.cursor.into_retained_bytes())?;
                context.release(active.path.retained_bytes)?;
                self.release_probe_seen(context)
            }
            Err(error) => {
                context.release_parked(active.cursor.into_retained_bytes());
                context.release_parked(active.path.retained_bytes);
                self.dispose_start_walks(context);
                Err(error)
            }
        }
    }

    /// Give back everything the merge holds, for a traversal that is failing.
    fn dispose_start_walks(&mut self, context: &Arc<RequestContext>) {
        if let Some(walks) = self.start_walks.as_mut() {
            let walks = std::mem::take(walks);
            let held = std::mem::take(&mut self.start_walk_container_bytes);
            Self::dispose_walks(context, walks, held);
        }
        if let Some(chosen) = self.chosen.take() {
            context.release_parked(chosen.reserved_bytes);
        }
    }

    fn dispose_walks(
        context: &Arc<RequestContext>,
        walks: impl IntoIterator<Item = GraphStartWalk>,
        container_bytes: usize,
    ) {
        for walk in walks {
            context.release_parked(walk.next.reserved_bytes);
            context.release_parked(walk.active.cursor.into_retained_bytes());
            context.release_parked(walk.active.path.retained_bytes);
        }
        context.release_parked(container_bytes);
    }

    /// Forget the loops a finished probe reported, and give back what
    /// remembering them was charged for.
    fn release_probe_seen(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let held = std::mem::take(&mut self.probe_seen_container_bytes);
        self.probe_loops = Vec::new();
        context.release(held)
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.single_step_unpinned {
            return self.next_unpinned_single_step(db, context);
        }
        loop {
            // A merged start walk whose chosen node has been handled goes back
            // to the merge with its next node before anything else is read.
            if self.active_rank.is_some() && self.chosen.is_none() {
                self.return_chosen_walk(db, context)?;
            }
            if self.active.is_none() {
                if self.merges_starts() {
                    // Every start's first step is read, shallowest first, before
                    // any later step continues, as one start's walk always was.
                    self.take_start_walk(db, context)?;
                }
                let path = if self.active.is_some() {
                    None
                } else if let Some(path) = self.frontier.pop() {
                    Some(path)
                } else if self.merges_starts() {
                    None
                } else if let Some(start) = self.next_start(db, context)? {
                    Some(self.initial_path(start, context)?)
                } else {
                    None
                };
                if let Some(path) = path {
                    self.open_active(db, context, path)?;
                    if self.active.is_none() {
                        continue;
                    }
                } else if self.active.is_none() {
                    context.release(self.frontier_container_bytes)?;
                    self.frontier_container_bytes = 0;
                    self.frontier = Vec::new();
                    context.release(self.seen_start_container_bytes)?;
                    self.seen_start_container_bytes = 0;
                    self.seen_starts = Vec::new();
                    context.release(self.seen_path_container_bytes)?;
                    self.seen_path_container_bytes = 0;
                    self.seen_paths = Vec::new();
                    if self.start_walks.is_some() {
                        context.release(self.start_walk_container_bytes)?;
                        self.start_walk_container_bytes = 0;
                        self.start_walks = Some(std::collections::BinaryHeap::new());
                    }
                    self.release_probe_seen(context)?;
                    return Ok(None);
                }
            }

            let active = self.active.as_mut().ok_or_else(|| {
                Error::Other("bounded graph lost its active continuation".to_string())
            })?;
            let next = match self.chosen.take() {
                Some(chosen) => Ok(Some(chosen)),
                None => Self::advance_walk(db, context, &mut active.cursor, self.visited_cap),
            };
            // A continuation that is abandoned keeps its charge until the
            // caller gives it back, so a refusal or cancellation from the
            // traversal has to dispose of the cursor before it propagates.
            let next = match next {
                Ok(next) => next,
                Err(error) => {
                    if let Some(active) = self.active.take() {
                        context.release_parked(active.cursor.into_retained_bytes());
                        context.release_parked(active.path.retained_bytes);
                    }
                    self.dispose_start_walks(context);
                    return Err(error);
                }
            };
            let Some(node) = next else {
                let active = self.active.take().ok_or_else(|| {
                    Error::Other("bounded graph lost its exhausted continuation".to_string())
                })?;
                context.release(active.cursor.into_retained_bytes())?;
                context.release(active.path.retained_bytes)?;
                // The adjacency this probe was reporting out of is finished,
                // so what it reported is no longer worth remembering: the next
                // start's own probe answers about its own neighbours.
                self.release_probe_seen(context)?;
                continue;
            };
            // The traversal reserved these bytes for this one projected row
            // and released nothing; every exit below returns them.
            let node_reservation = ReservedBytes::new(context, node.reserved_bytes);
            // A single hop answers once per matching edge. The one edge an
            // undirected hop sees twice is a loop on the node it walks out of,
            // once leaving and once arriving, so a loop is reported once per
            // edge type over the one adjacency being walked. The entry was
            // still read, so it is still examined.
            let walked_from = self.active.as_ref().map(|active| active.path.node);
            let loop_type = if self.single_step_probe && walked_from == Some(node.node.id) {
                node.node
                    .path
                    .last()
                    .map(|(_, edge_type)| edge_type.as_str())
            } else {
                None
            };
            if let Some(edge_type) = loop_type {
                match self
                    .probe_loops
                    .binary_search_by(|seen| seen.as_str().cmp(edge_type))
                {
                    Ok(_) => {
                        node_reservation.release()?;
                        continue;
                    }
                    Err(position) => {
                        let added = match reserve_vector_slot(
                            context,
                            BoundedWorkSource::GraphTraversal,
                            &mut self.probe_loops,
                            "graph probe loops",
                        ) {
                            Ok(added) => added,
                            Err(error) => {
                                node_reservation.release()?;
                                return Err(error);
                            }
                        };
                        let Some(total) = self.probe_seen_container_bytes.checked_add(added) else {
                            node_reservation.release()?;
                            return Err(bounded_size_error("graph probe loops"));
                        };
                        self.probe_seen_container_bytes = total;
                        if let Err(error) =
                            context.reserve(BoundedWorkSource::GraphTraversal, edge_type.len())
                        {
                            node_reservation.release()?;
                            return Err(error);
                        }
                        let Some(total) =
                            self.probe_seen_container_bytes.checked_add(edge_type.len())
                        else {
                            node_reservation.release()?;
                            return Err(bounded_size_error("graph probe loops"));
                        };
                        self.probe_seen_container_bytes = total;
                        let mut kept = String::with_capacity(edge_type.len());
                        kept.push_str(edge_type);
                        self.probe_loops.insert(position, kept);
                    }
                }
            }
            // Answers are keyed by the pattern's target bindings, including
            // single-hop probes that reach one neighbour from both directions.
            // The existing admitted path-key set owns each distinct answer.
            {
                let key = {
                    let active = self.active.as_ref().ok_or_else(|| {
                        Error::Other("bounded graph lost its emitted path".to_string())
                    })?;
                    let key_len = active
                        .path
                        .step_index
                        .checked_add(1)
                        .ok_or_else(|| bounded_size_error("graph path deduplication"))?;
                    let key_bytes = match checked_size_mul(
                        key_len,
                        std::mem::size_of::<uuid::Uuid>(),
                        "graph path deduplication",
                    ) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            node_reservation.release()?;
                            return Err(error);
                        }
                    };
                    if let Err(error) =
                        context.reserve(BoundedWorkSource::GraphTraversal, key_bytes)
                    {
                        node_reservation.release()?;
                        return Err(error);
                    }
                    let mut key: Vec<uuid::Uuid> = Vec::new();
                    if key.try_reserve_exact(key_len).is_err() {
                        context.release(key_bytes)?;
                        node_reservation.release()?;
                        return Err(Error::Other(
                            "bounded graph path deduplication allocation failed".to_string(),
                        )
                        .into());
                    }
                    key.extend(active.path.bindings.iter().skip(1).copied());
                    key.push(node.node.id);
                    (key, key_bytes)
                };
                let (key, key_bytes) = key;
                match self.seen_paths.binary_search(&key) {
                    Ok(_) => {
                        context.release(key_bytes)?;
                        node_reservation.release()?;
                        continue;
                    }
                    Err(position) => {
                        let added = match reserve_vector_slot(
                            context,
                            BoundedWorkSource::GraphTraversal,
                            &mut self.seen_paths,
                            "graph path deduplication",
                        ) {
                            Ok(added) => added,
                            Err(error) => {
                                context.release(key_bytes)?;
                                node_reservation.release()?;
                                return Err(error);
                            }
                        };
                        let total = self
                            .seen_path_container_bytes
                            .checked_add(added)
                            .and_then(|bytes| bytes.checked_add(key_bytes));
                        let Some(total) = total else {
                            node_reservation.release()?;
                            return Err(bounded_size_error("graph path deduplication"));
                        };
                        self.seen_path_container_bytes = total;
                        self.seen_paths.insert(position, key);
                    }
                }
            }
            let active = self
                .active
                .as_ref()
                .ok_or_else(|| Error::Other("bounded graph lost its emitted path".to_string()))?;
            let step_index = active.path.step_index;
            let total_depth = active
                .path
                .total_depth
                .checked_add(node.node.depth)
                .ok_or_else(|| Error::Other("bounded graph total depth overflow".to_string()))?;
            let final_step = step_index
                .checked_add(1)
                .ok_or_else(|| Error::Other("bounded graph step position overflow".to_string()))?
                == self.steps.len();
            if !final_step {
                let planned_bytes = active.path.retained_bytes;
                context.reserve(BoundedWorkSource::GraphTraversal, planned_bytes)?;
                let mut bindings = Vec::new();
                if bindings
                    .try_reserve_exact(active.path.bindings.capacity())
                    .is_err()
                {
                    context.release(planned_bytes)?;
                    return Err(Error::Other(
                        "bounded graph continuation allocation failed".to_string(),
                    )
                    .into());
                }
                bindings.extend(active.path.bindings.iter().copied());
                bindings.push(node.node.id);
                let retained_bytes = checked_size_mul(
                    bindings.capacity(),
                    std::mem::size_of::<uuid::Uuid>(),
                    "graph path bindings",
                )?;
                reconcile_new_reservation(
                    context,
                    BoundedWorkSource::GraphTraversal,
                    planned_bytes,
                    retained_bytes,
                )?;
                let path = GraphPathState {
                    bindings,
                    node: node.node.id,
                    total_depth,
                    step_index: step_index.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded graph step position overflow".to_string())
                    })?,
                    retained_bytes,
                };
                let added = reserve_vector_slot(
                    context,
                    BoundedWorkSource::GraphTraversal,
                    &mut self.frontier,
                    "graph continuation stack",
                );
                let added = match added {
                    Ok(added) => added,
                    Err(error) => {
                        context.release(path.retained_bytes)?;
                        return Err(error);
                    }
                };
                self.frontier_container_bytes = self
                    .frontier_container_bytes
                    .checked_add(added)
                    .ok_or_else(|| bounded_size_error("graph continuation stack"))?;
                self.frontier.push(path);
                continue;
            }

            let binding_capacity = self
                .steps
                .len()
                .checked_add(1)
                .ok_or_else(|| bounded_size_error("graph bindings"))?;
            let planned_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_mul(
                    binding_capacity,
                    std::mem::size_of::<(String, uuid::Uuid)>(),
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            let alias_bytes =
                self.steps
                    .iter()
                    .try_fold(self.start_alias.len(), |bytes, step| {
                        checked_size_add(bytes, step.target_alias.len(), "graph binding aliases")
                    })?;
            let planned_binding_bytes =
                checked_size_add(planned_binding_bytes, alias_bytes, "graph bindings")?;
            context.reserve(BoundedWorkSource::GraphTraversal, planned_binding_bytes)?;
            let mut bindings = HashMap::new();
            if bindings.try_reserve(binding_capacity).is_err() {
                context.release(planned_binding_bytes)?;
                return Err(
                    Error::Other("bounded graph binding allocation failed".to_string()).into(),
                );
            }
            let start = active.path.bindings.first().copied().ok_or_else(|| {
                Error::Other("bounded graph path has no start binding".to_string())
            })?;
            if self.probes_backwards_from_target {
                // Walked backwards out of the pinned end, so what the walk
                // calls its start is the hop's TARGET and what it reached is
                // the hop's start. Named here the way the statement named
                // them, so the answer reads as the statement was written.
                let target_alias = self
                    .steps
                    .first()
                    .map(|step| step.target_alias.clone())
                    .ok_or_else(|| {
                        Error::Other("bounded graph backwards probe has no step".to_string())
                    })?;
                bindings.insert(self.start_alias.clone(), node.node.id);
                bindings.insert(target_alias, start);
            } else {
                bindings.insert(self.start_alias.clone(), start);
            }
            if !self.probes_backwards_from_target {
                for (step, id) in self.steps.iter().zip(
                    active
                        .path
                        .bindings
                        .iter()
                        .skip(1)
                        .copied()
                        .chain(std::iter::once(node.node.id)),
                ) {
                    bindings.insert(step.target_alias.clone(), id);
                }
            }
            let actual_binding_bytes = bindings.keys().try_fold(
                checked_size_add(
                    std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                    checked_size_mul(
                        bindings.capacity(),
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    "graph bindings",
                )?,
                |bytes, alias| checked_size_add(bytes, alias.capacity(), "graph binding aliases"),
            )?;
            reconcile_new_reservation(
                context,
                BoundedWorkSource::GraphTraversal,
                planned_binding_bytes,
                actual_binding_bytes,
            )?;
            let mut predicate_bytes = 0usize;
            let mut predicate_access_bytes = 0usize;
            let mut predicate_failure = None;
            let matches = self
                .filter
                .as_ref()
                .map(|filter| {
                    db.graph_filter_matches_bindings_with_work(
                        filter,
                        &self.params,
                        // The reader's own transaction: a WHERE on node
                        // metadata asks what this handle can see, and what it
                        // staged is part of that. Everything below this call
                        // already takes the transaction and already composes
                        // the overlay; only this door was never handing it
                        // over.
                        context.transaction(),
                        self.snapshot,
                        &bindings,
                        &mut |work| {
                            let result = match work {
                                GraphPredicateWork::Checkpoint => context.charge(
                                    BoundedWorkSource::GraphTraversal,
                                    BoundedSourceTouch::AccessRow,
                                ),
                                GraphPredicateWork::AccessRow(bytes) => context
                                    .charge(
                                        BoundedWorkSource::AccessControl,
                                        BoundedSourceTouch::AccessRow,
                                    )
                                    .and_then(|()| {
                                        let extra = bytes.saturating_sub(predicate_access_bytes);
                                        context.reserve(BoundedWorkSource::AccessControl, extra)?;
                                        predicate_access_bytes = predicate_access_bytes.max(bytes);
                                        predicate_bytes = predicate_bytes.saturating_add(extra);
                                        Ok(())
                                    }),
                            };
                            if let Err(error) = result {
                                predicate_failure = Some(error);
                                return Err(Error::ReadCancelled);
                            }
                            Ok(())
                        },
                        Arc::new(SourceDecodeBudget {
                            context: context.clone(),
                            source: BoundedWorkSource::GraphTraversal,
                        }),
                    )
                })
                .transpose();
            context.release(predicate_bytes)?;
            if let Some(error) = predicate_failure {
                context.release(actual_binding_bytes)?;
                return Err(error);
            }
            let matches = match matches {
                Ok(Some(matches)) => matches,
                Ok(None) => true,
                Err(error) => {
                    context.release(actual_binding_bytes)?;
                    return Err(error.into());
                }
            };
            context.release(actual_binding_bytes)?;
            if !matches {
                continue;
            }
            let projected_capacity = binding_capacity
                .checked_add(2)
                .ok_or_else(|| bounded_size_error("graph projection"))?;
            let planned_output_bytes = checked_size_add(
                std::mem::size_of::<Vec<Value>>(),
                checked_size_mul(
                    projected_capacity,
                    std::mem::size_of::<Value>(),
                    "graph projection",
                )?,
                "graph projection",
            )?;
            context.reserve(BoundedWorkSource::GraphTraversal, planned_output_bytes)?;
            let mut projected = Vec::new();
            if projected.try_reserve_exact(projected_capacity).is_err() {
                context.release(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded graph projection allocation failed".to_string()).into(),
                );
            }
            if self.probes_backwards_from_target {
                // The walk went out of the hop's pinned END, so its own idea
                // of start and reached is the statement's idea of them
                // reversed. The answer is published the way the statement was
                // written: the node the walk reached is where the hop starts,
                // and the pinned node is where it ends.
                projected.push(Value::Uuid(node.node.id));
                projected.extend(active.path.bindings.iter().copied().map(Value::Uuid));
            } else {
                projected.extend(active.path.bindings.iter().copied().map(Value::Uuid));
                projected.push(Value::Uuid(node.node.id));
            }
            projected.push(Value::Uuid(node.node.id));
            projected.push(Value::Int64(i64::from(total_depth)));
            let output_bytes = values_retained_bytes(&projected)?;
            reconcile_new_reservation(
                context,
                BoundedWorkSource::GraphTraversal,
                planned_output_bytes,
                output_bytes,
            )?;
            node_reservation.release()?;
            return Ok(Some(PulledRow {
                window: None,
                values: projected,
                retained_bytes: output_bytes,
                group_binding: None,
            }));
        }
    }

    fn has_more_hint(&self) -> Option<bool> {
        if self.active.is_some()
            || !self.frontier.is_empty()
            || !self.starts.is_empty()
            || self.start_candidates.is_some()
            || self.edge_source.is_some()
            || self.chosen.is_some()
            || self
                .start_walks
                .as_ref()
                .is_some_and(|walks| !walks.is_empty())
        {
            None
        } else {
            Some(false)
        }
    }
}

fn bounded_graph_static_starts(
    filter: &Expr,
    params: &HashMap<String, Value>,
    start_alias: &str,
) -> std::result::Result<Option<Vec<uuid::Uuid>>, BoundedExecutionError> {
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_graph_start_column_ref(left, start_alias, "id")
            || is_graph_start_column_ref(right, start_alias, "id") =>
        {
            let value = if is_graph_start_column_ref(left, start_alias, "id") {
                right
            } else {
                left
            };
            Ok(
                resolve_graph_static_uuid_expr(value, params, "graph start identifier in filter")?
                    .map(|id| vec![id]),
            )
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_graph_start_column_ref(expr, start_alias, "id") => {
            let mut ids = Vec::with_capacity(list.len());
            for item in list {
                let Some(id) = resolve_graph_static_uuid_expr(
                    item,
                    params,
                    "graph start identifier in filter",
                )?
                else {
                    return Ok(None);
                };
                ids.push(id);
            }
            Ok(Some(ids))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left = bounded_graph_static_starts(left, params, start_alias)?;
            let right = bounded_graph_static_starts(right, params, start_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => {
                    let right = right.into_iter().collect::<BTreeSet<_>>();
                    Some(left.into_iter().filter(|id| right.contains(id)).collect())
                }
                (Some(ids), None) | (None, Some(ids)) => Some(ids),
                (None, None) => None,
            })
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let left = bounded_graph_static_starts(left, params, start_alias)?;
            let right = bounded_graph_static_starts(right, params, start_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(
                    left.into_iter()
                        .chain(right)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
                _ => None,
            })
        }
        _ => Ok(None),
    }
}

#[cfg(feature = "test-seams")]
struct BoundedHnswProbeObserver {
    probe: Arc<dyn BoundedExecutionProbe>,
}

#[cfg(feature = "test-seams")]
impl contextdb_vector::hnsw::HnswCandidateObserver for BoundedHnswProbeObserver {
    fn before_candidate_distance(&self, event: contextdb_vector::hnsw::HnswCandidateDistanceEvent) {
        self.probe.before_hnsw_candidate_distance(event);
    }

    fn after_residual_rejection(&self, event: contextdb_vector::hnsw::HnswResidualRejectionEvent) {
        self.probe.after_hnsw_residual_rejection(event);
    }
}

/// The capacity an accumulating collection starts at before doubling. Small
/// enough that a read accumulating a handful of items pays for a handful.
const AMORTIZED_GROWTH_FLOOR: usize = 4;

fn reserve_vector_slot<T>(
    context: &Arc<RequestContext>,
    source: BoundedWorkSource,
    values: &mut Vec<T>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    if values.len() < values.capacity() {
        return Ok(0);
    }
    let previous_capacity = values.capacity();
    // Growing by one slot at a time reallocates and copies everything already
    // accumulated on every append, so a read that examines many items costs
    // far more than the declared work ceiling counts. Asking for the larger
    // capacity before growing into it keeps the charge ahead of the memory
    // and keeps the ceiling describing what the read does.
    let target = previous_capacity
        .saturating_mul(2)
        .max(AMORTIZED_GROWTH_FLOOR);
    let additional = target.saturating_sub(previous_capacity).max(1);
    let bytes = checked_size_mul(additional, std::mem::size_of::<T>(), operation)?;
    context.reserve(source, bytes)?;
    if values.try_reserve_exact(additional).is_err() {
        context.release(bytes)?;
        return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
    }
    let actual = match checked_size_mul(values.capacity(), std::mem::size_of::<T>(), operation) {
        Ok(actual) => actual,
        Err(error) => {
            context.release(bytes)?;
            return Err(error);
        }
    };
    let prior = match checked_size_mul(previous_capacity, std::mem::size_of::<T>(), operation) {
        Ok(prior) => prior,
        Err(error) => {
            context.release(bytes)?;
            return Err(error);
        }
    };
    let delta = match actual.checked_sub(prior) {
        Some(delta) => delta,
        None => {
            context.release(bytes)?;
            return Err(
                Error::Other(format!("bounded {operation} capacity moved backwards")).into(),
            );
        }
    };
    reconcile_new_reservation(context, source, bytes, delta)?;
    Ok(delta)
}

/// Which column of a candidate set names the rows it picked.
///
/// `row_id` is the direct name and is preferred wherever the plan carries it.
/// A candidate set that came through named queries carries the row's own `id`
/// instead, which names the same row one lookup away.
enum VectorCandidateIdentity {
    RowId(usize),
    NaturalId(usize),
}

fn vector_candidate_identity(columns: &[String]) -> Option<VectorCandidateIdentity> {
    if let Some(position) = columns
        .iter()
        .position(|column| column == "row_id" || column.rsplit('.').next() == Some("row_id"))
    {
        return Some(VectorCandidateIdentity::RowId(position));
    }
    columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .map(VectorCandidateIdentity::NaturalId)
}

impl VectorState {
    fn with_partition_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        let operation = || match self.sort_key.as_ref() {
            Some(_) => contextdb_vector::mem::with_search_policy_limit(self.k, operation),
            None => operation(),
        };
        match self.partition_prefixes.as_ref() {
            Some(prefixes) => contextdb_vector::mem::with_selected_partition_prefixes(
                &self.index,
                prefixes.clone(),
                operation,
            ),
            None => operation(),
        }
    }

    fn record_disclosure(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        aggregate_allowed_vectors: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        trace: &contextdb_vector::mem::BoundedVectorSearchTrace,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let disclosure = context.with_vector_copies(|vector_copy_owner| {
            db.completed_vector_search_disclosure(
                self.index.clone(),
                self.k,
                self.restricted_candidates,
                self.candidate_plan.as_deref(),
                self.requested_search_mode,
                &self.query_expr,
                self.snapshot,
                &self.params,
                aggregate_allowed_vectors,
                groups,
                trace.partition_routes(),
                trace.used_hnsw(),
                trace.fallback_reason(),
                trace.budget_receipt(),
                vector_copy_owner,
                context.reader_budget.clone(),
            )
        })?;
        context.bind_final_vector(&disclosure)?;
        self.disclosure = Some(disclosure);
        Ok(())
    }

    /// The row one scored candidate names, projected the way the source used
    /// to project it: the row id first, then the table's columns in order.
    ///
    /// `None` where the row is gone or this reader may not have it -- the same
    /// two outcomes the scan produced, decided per published row, so what the
    /// search admitted and what the reader is entitled to stay separate
    /// questions.
    /// Whether a row body satisfies the statement's own predicate -- the
    /// filter its candidate source carries. One rule, asked both of a row this
    /// transaction staged and of a committed row about to be published.
    ///
    /// A predicate that reads another table cannot be answered from a row, so
    /// it is not asked here: the candidate source already ran it against the
    /// rows it admitted, and re-asking it with only a row in hand would not
    /// reach the subquery's answer. What is left is what a row can settle for
    /// itself, which is the part a posting could have lied about.
    fn row_matches_candidate_predicate(
        &self,
        row: &VersionedRow,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        let mut plan = self.candidate_plan.as_deref();
        while let Some(PhysicalPlan::Project { input, .. }) = plan {
            plan = Some(input);
        }
        let Some(PhysicalPlan::Scan {
            filter: Some(filter),
            ..
        }) = plan
        else {
            return Ok(true);
        };
        if !predicate_is_answerable_from_one_row(filter) {
            return Ok(true);
        }
        Ok(super::eval_bool_expr(filter, &|inner| {
            super::eval_expr_value(row, inner, &self.params)
        })? == Some(true))
    }

    fn materialize_scored_row(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        row_id: RowId,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        let mut vector_copy_owner =
            crate::vector_read_memory::admission(Arc::new(SourceDecodeBudget {
                context: context.clone(),
                source: BoundedWorkSource::TableScan,
            }));
        let mut retained = 0usize;
        let row = context.with_source_page_admission(BoundedWorkSource::TableScan, || {
            db.bounded_row_by_identity(
                context.source_read_memory(BoundedWorkSource::TableScan),
                context.transaction(),
                &self.table,
                row_id,
                self.snapshot,
                // Publishing a row the search already scored is charged as the
                // work it is, and it is NOT a source touch: the candidate source
                // already counted the rows this statement examined, and counting
                // the answer again would make a filtered search report more rows
                // read than the filter admitted -- one more per row returned. The
                // other door resolves the same row through the version index
                // (`RelationalStore::row_by_id`) and counts nothing, and
                // `rows_examined` is one figure both doors owe the same answer to.
                &mut || context.charge_operator(BoundedWorkSource::TableScan),
                &mut |bytes| {
                    retained = context.admit_source_row(BoundedWorkSource::TableScan, bytes)?;
                    Ok(())
                },
            )
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let Some(meta) = db.table_meta(&self.table) else {
            drop(row);
            context.release(retained)?;
            return Ok(None);
        };
        // The gate still decides every published row, exactly as it did when
        // these rows arrived from a scan: hiding what this reader may not see
        // rather than refusing the statement, because a search over a
        // collection is a set read.
        let allowed = db.bounded_read_allowed_for_row(
            context.transaction(),
            &self.table,
            &meta,
            &row,
            self.snapshot,
            &mut |_bytes| {
                context.charge(
                    BoundedWorkSource::AccessControl,
                    BoundedSourceTouch::AccessRow,
                )?;
                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                Ok(())
            },
            context.source_read_memory(BoundedWorkSource::AccessControl),
        )?;
        // The gate decides what this reader MAY see; it does not decide what
        // the statement ASKED for. On the identity route nothing has read this
        // row body yet, so `context_id`, `generation_id` and `is_current` have
        // been believed from the index posting alone up to this point.
        //
        // Measured: on the authenticated route neither divergence can reach
        // here -- a tampered row body is refused by the content-identity
        // authentication at load, and a moved posting by the per-window
        // reconciliation that compares the canonical posting tree against the
        // physical index. This check is the last of those three, not the first,
        // and it costs one predicate evaluation per PUBLISHED row.
        let matches = self.row_matches_candidate_predicate(&row)?;
        if !allowed || !matches {
            // A posting and the row version it names are written and retired
            // by the same transaction, so inside ONE snapshot they cannot
            // disagree -- unless this reader's own open transaction has staged
            // a change the committed index has not seen, which is ordinary and
            // keeps the old hide-and-continue. With no such transaction, an
            // index naming a row whose body contradicts it is store damage,
            // and damage is reported, never served: publishing a different row
            // in its place would hide it, and dropping this one would answer
            // short of the LIMIT with nothing to say.
            if let Some(index) = self.identity_source_index.as_deref()
                && context.transaction().is_none()
            {
                drop(row);
                context.release(retained)?;
                return Err(Error::StoreCorrupted {
                    path: db.bounded_store_path(),
                    reason: format!(
                        "relational index {index} on table {} names row {} as {}, and the row \
                         version stored at this snapshot disagrees",
                        self.table,
                        row_id.0,
                        if allowed {
                            "one the statement selected"
                        } else {
                            "one this reader may see"
                        },
                    ),
                }
                .into());
            }
            context.release(retained)?;
            return Ok(None);
        }
        let mut row = row;
        for column in &self.supplement_columns {
            if !matches!(row.values.get(column), Some(Value::Null) | None) {
                continue;
            }
            if let Some(vector) = db.load_row_vector_for_column(
                context.transaction(),
                &self.table,
                column,
                row.row_id,
                row.lsn,
                self.snapshot,
                &mut vector_copy_owner,
                Arc::clone(&context.reader_budget),
            )? {
                row.values.insert(column.clone(), Value::Vector(vector));
            }
        }
        let row_id_value =
            Value::Int64(i64::try_from(row.row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?);
        let capacity = self
            .schema_columns
            .len()
            .checked_add(1)
            .ok_or_else(|| bounded_size_error("vector row projection"))?;
        let planned = checked_size_add(
            std::mem::size_of::<Vec<Value>>(),
            checked_size_mul(
                capacity,
                std::mem::size_of::<Value>(),
                "vector projection slots",
            )?,
            "vector projection",
        )?;
        let vector_credit = vector_copy_owner.retained_bytes();
        let funded = checked_size_add(
            checked_size_add(retained, vector_credit, "vector source credit")?,
            planned,
            "vector projection credit",
        )?;
        context.reserve(BoundedWorkSource::TableScan, planned)?;
        let mut values = Vec::new();
        if values.try_reserve_exact(capacity).is_err() {
            drop(row);
            context.release(planned)?;
            context.release(retained)?;
            return Err(Error::Other("bounded vector row allocation failed".to_string()).into());
        }
        // The source row is released below, so its values are moved into the
        // output rather than copied: a copy would hold every value twice.
        let mut source_values = row.values;
        values.push(row_id_value);
        values.extend(
            self.schema_columns
                .iter()
                .map(|column| source_values.remove(column).unwrap_or(Value::Null)),
        );
        drop(source_values);
        let actual = values_retained_bytes(&values)?;
        vector_copy_owner.transfer();
        if actual > funded {
            if let Err(error) = context.reserve(BoundedWorkSource::TableScan, actual - funded) {
                drop(values);
                context.release(funded)?;
                return Err(error);
            }
        } else if funded > actual {
            context.release(funded - actual)?;
        }
        // Payload credit moved into this output; only dead map/field credit
        // was returned by reconciliation above.
        Ok(Some(PulledRow {
            window: None,
            values,
            retained_bytes: actual,
            group_binding: None,
        }))
    }

    /// Which identity a committed entry is published under, if at all, once
    /// this transaction's own vector work is taken into account.
    ///
    /// A move is a re-key rather than a re-read: the score the source just
    /// produced is the score of that vector wherever it is filed.
    fn overlay_publishes(&self, row_id: RowId) -> Option<RowId> {
        let Some(overlay) = self.vector_overlay.as_ref() else {
            return Some(row_id);
        };
        // A row this transaction restaged is published from what it staged,
        // not from the committed entry that version replaced.
        if overlay.removed.contains(&row_id)
            || overlay.staged_identities.contains(&row_id)
            || overlay.moved.contains_key(&row_id)
        {
            return None;
        }
        let published = overlay.moved.get(&row_id).copied().unwrap_or(row_id);
        if overlay.removed.contains(&published) || overlay.staged_identities.contains(&published) {
            return None;
        }
        self.candidate_admits(published).then_some(published)
    }

    /// Let the source yield the committed entries that this transaction has
    /// moved INTO the candidate set.
    ///
    /// The source filters and truncates by the identity an entry is filed
    /// under, which for a moved vector is the identity the statement never
    /// asked about. Adding those source identities to the list the source sees
    /// is what keeps a moved row reachable; it cannot widen the answer,
    /// because what is published is still judged on the identity it is
    /// published under.
    fn admit_move_sources(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if !self.candidate_filter {
            return Ok(());
        }
        let Some(overlay) = self.vector_overlay.as_ref() else {
            return Ok(());
        };
        let mut sources: Vec<u64> = overlay
            .moved
            .iter()
            .filter(|(from_row_id, to_row_id)| {
                self.candidate_ids.binary_search(&to_row_id.0).is_ok()
                    && self.candidate_ids.binary_search(&from_row_id.0).is_err()
            })
            .map(|(from_row_id, _)| from_row_id.0)
            .collect();
        if sources.is_empty() {
            return Ok(());
        }
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            let added = reserve_vector_slot(
                context,
                BoundedWorkSource::VectorCandidates,
                &mut self.candidate_ids,
                "vector candidate ids",
            )?;
            self.candidate_bytes = self
                .candidate_bytes
                .checked_add(added)
                .ok_or_else(|| bounded_size_error("vector candidate ids"))?;
            self.candidate_ids.push(source);
        }
        self.candidate_ids.sort_unstable();
        self.candidate_ids.dedup();
        Ok(())
    }

    /// Whether the candidate set this statement named still admits a row.
    fn candidate_admits(&self, row_id: RowId) -> bool {
        if !self.candidate_filter || self.vector_overlay.is_none() {
            return true;
        }
        self.candidate_ids.binary_search(&row_id.0).is_ok()
    }

    /// Keep one scored candidate, admitted before the slot that holds it.
    fn keep_score(
        &mut self,
        context: &Arc<RequestContext>,
        row_id: RowId,
        score: f32,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let added = reserve_vector_slot(
            context,
            BoundedWorkSource::VectorCandidates,
            &mut self.scores,
            "brute-force result scores",
        )?;
        self.score_bytes = self
            .score_bytes
            .checked_add(added)
            .ok_or_else(|| bounded_size_error("brute-force result scores"))?;
        self.scores.push((row_id, score));
        Ok(())
    }

    fn staged_row_admitted(
        &self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        row_id: RowId,
    ) -> std::result::Result<bool, BoundedExecutionError> {
        if !self.candidate_admits(row_id) {
            return Ok(false);
        }
        let mut retained = 0;
        let row =
            context.with_source_page_admission(BoundedWorkSource::VectorCandidates, || {
                db.bounded_row_by_identity(
                    context.source_read_memory(BoundedWorkSource::VectorCandidates),
                    context.transaction(),
                    &self.table,
                    row_id,
                    self.snapshot,
                    &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                    &mut |bytes| {
                        retained =
                            context.admit_source_row(BoundedWorkSource::VectorCandidates, bytes)?;
                        Ok(())
                    },
                )
            })?;
        let admitted = (|| {
            let Some(row) = row.as_ref() else {
                return Ok(false);
            };
            if !self.row_matches_candidate_predicate(row)? {
                return Ok(false);
            }
            let meta = db
                .table_meta(&self.table)
                .ok_or_else(|| Error::TableNotFound(self.table.clone()))?;
            db.bounded_read_allowed_for_row(
                context.transaction(),
                &self.table,
                &meta,
                row,
                self.snapshot,
                &mut |_| context.charge_operator(BoundedWorkSource::AccessControl),
                context.source_read_memory(BoundedWorkSource::AccessControl),
            )
        })();
        drop(row);
        context.release(retained)?;
        admitted
    }

    /// Score the vectors this transaction staged, after the committed entries
    /// are done.
    ///
    /// One touch is charged for each staged vector before it is read and its
    /// payload before it is copied, so a vector this handle staged costs what
    /// a committed entry of the same width costs; the copy is handed back as
    /// soon as the score is taken, because a score is all that is kept.
    fn score_staged_vectors(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.vector_overlay.is_none() {
            return Ok(());
        }
        let Some(tx) = context.transaction() else {
            return Ok(());
        };
        // Move the already-accounted overlay into this local scope so scoring
        // can update the result heap without allocating a second move map.
        let overlay = self
            .vector_overlay
            .take()
            .expect("a transaction delta is present");
        let moved_result = (|| -> std::result::Result<(), BoundedExecutionError> {
            for (&from, &to) in &overlay.moved {
                if overlay.removed.contains(&to)
                    || overlay.staged_identities.contains(&to)
                    || !self.staged_row_admitted(db, context, to)?
                {
                    continue;
                }
                context.charge(
                    BoundedWorkSource::VectorCandidates,
                    BoundedSourceTouch::BruteForceVectorCandidate,
                )?;
                let score = context.with_vector_copies(|vector_copy_owner| {
                    db.bounded_score_committed_vector_row(
                        &self.index,
                        from,
                        self.snapshot,
                        &self.query,
                        &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                        &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                        &mut |bytes| context.reserve(BoundedWorkSource::VectorCandidates, bytes),
                        &mut |bytes| context.release_parked(bytes),
                        vector_copy_owner,
                        context.reader_budget.clone(),
                    )
                });
                if let Some(score) = score? {
                    self.keep_score(context, to, score)?;
                }
            }
            Ok(())
        })();
        self.vector_overlay = Some(overlay);
        moved_result?;
        let mut staged = 0usize;
        loop {
            let Some(capability) = self
                .vector_overlay
                .as_ref()
                .and_then(|overlay| overlay.staged_vectors.get(staged).cloned())
            else {
                return Ok(());
            };
            staged = staged.saturating_add(1);
            let mut retained = 0usize;
            let entry = db.transaction_staged_vector(
                tx,
                &capability,
                &mut || {
                    context.charge(
                        BoundedWorkSource::VectorCandidates,
                        BoundedSourceTouch::BruteForceVectorCandidate,
                    )
                },
                &mut |bytes| {
                    retained = bytes;
                    context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                },
            )?;
            let Some((row_id, vector)) = entry else {
                continue;
            };
            let scored = self
                .staged_row_admitted(db, context, row_id)?
                .then(|| contextdb_vector::cosine_similarity(&vector, &self.query));
            drop(vector);
            context.release(retained)?;
            if let Some(score) = scored {
                self.keep_score(context, row_id, score)?;
            }
        }
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        #[cfg(feature = "test-seams")]
        {
            self.receipt.begin();
            let before = vector_search_receipt::exact_candidates(context);
            let observer = self.receipt.observer();
            let insertion_observer = self.receipt.insertion_observer();
            // The graph walk/build is synchronous on this request worker.
            // Restore its prior TLS observer before returning across a fetch.
            let result = match observer {
                Some(observer) => {
                    contextdb_vector::hnsw::with_additional_hnsw_build_observer(observer, || {
                        match insertion_observer {
                            Some(observer) => {
                                contextdb_vector::hnsw::with_additional_hnsw_insert_observer(
                                    observer,
                                    || self.next_unobserved(db, context),
                                )
                            }
                            None => self.next_unobserved(db, context),
                        }
                    })
                }
                None => self.next_unobserved(db, context),
            };
            let terminal = result
                .as_ref()
                .err()
                .is_some_and(|error| !context.refusal_pauses_this_fetch(error));
            let completed = matches!(
                self.preparation,
                VectorPreparation::Output | VectorPreparation::Exhausted
            );
            self.receipt.finish_step(
                vector_search_receipt::exact_candidates(context).saturating_sub(before),
                self.disclosure.as_ref(),
                completed || terminal,
            );
            result
        }
        #[cfg(not(feature = "test-seams"))]
        self.next_unobserved(db, context)
    }

    fn next_unobserved(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            match self.preparation {
                VectorPreparation::Candidates(_) => {
                    let candidate = match &mut self.preparation {
                        VectorPreparation::Candidates(candidate) => candidate,
                        _ => {
                            return Err(Error::Other(
                                "bounded vector candidate state changed unexpectedly".to_string(),
                            )
                            .into());
                        }
                    };
                    let Some(root) = candidate.as_mut() else {
                        self.preparation = VectorPreparation::Search;
                        continue;
                    };
                    if let PullKind::Scan(scan) = &mut root.kind
                        && scan.drain_verified_vector_candidate_identities(
                            db,
                            context,
                            &mut self.candidate_ids,
                            &mut self.candidate_bytes,
                        )?
                    {
                        self.candidate_ids.sort_unstable();
                        self.candidate_ids.dedup();
                        self.preparation = VectorPreparation::Search;
                        continue;
                    }
                    // How this candidate set names the rows it picked. A plan
                    // that reads the table directly projects the table's own
                    // `row_id`. A candidate set written as named queries -- a
                    // CTE projecting `id`, joined and filtered -- has no
                    // `row_id` to project, and the identity it does carry is
                    // the row's own `id`. The executor reads that second shape
                    // too (`executor.rs`, the `id_idx` arm of the candidate
                    // bitmap), so refusing it here leaves the caller with no
                    // smaller question to fall back on: the narrowing is the
                    // whole question.
                    let identity = vector_candidate_identity(&root.columns).ok_or_else(|| {
                        Error::Other(
                            "bounded vector candidate plan must name its rows by row_id or id"
                                .to_string(),
                        )
                    })?;
                    match root.next(db, context)? {
                        Some(row) => {
                            let resolved = match identity {
                                VectorCandidateIdentity::RowId(position) => {
                                    match row.values.get(position) {
                                        Some(Value::Int64(row_id)) => {
                                            Some(u64::try_from(*row_id).map_err(|_| {
                                                Error::Other(
                                                    "bounded vector candidate row_id is negative"
                                                        .to_string(),
                                                )
                                            })?)
                                        }
                                        _ => None,
                                    }
                                }
                                VectorCandidateIdentity::NaturalId(position) => {
                                    match row.values.get(position) {
                                        Some(key @ Value::Uuid(_)) => {
                                            // One index entry, charged before it
                                            // is read, to turn the row's own id
                                            // into the identity the vector index
                                            // keys by. Resolved one candidate at
                                            // a time inside the caller's own
                                            // transaction -- the eager helper
                                            // builds a map of the whole table,
                                            // which is exactly the unbounded read
                                            // a ceiling exists to prevent.
                                            context.charge(
                                                BoundedWorkSource::VectorCandidates,
                                                BoundedSourceTouch::IndexEntry,
                                            )?;
                                            db.row_id_for_natural_key_in_tx(
                                                context.transaction(),
                                                &self.table,
                                                "id",
                                                key,
                                                self.snapshot,
                                                context.reader_budget.clone(),
                                            )?
                                            .map(|row_id| row_id.0)
                                        }
                                        _ => None,
                                    }
                                }
                            };
                            let Some(row_id) = resolved else {
                                row.release(context)?;
                                continue;
                            };
                            let added = reserve_vector_slot(
                                context,
                                BoundedWorkSource::VectorCandidates,
                                &mut self.candidate_ids,
                                "vector candidate ids",
                            )?;
                            self.candidate_bytes = self
                                .candidate_bytes
                                .checked_add(added)
                                .ok_or_else(|| bounded_size_error("vector candidate ids"))?;
                            self.candidate_ids.push(row_id);
                            row.release(context)?;
                            continue;
                        }
                        None => {
                            self.candidate_ids.sort_unstable();
                            self.candidate_ids.dedup();
                            self.preparation = VectorPreparation::Search;
                            continue;
                        }
                    }
                }
                VectorPreparation::Search => {
                    // A move re-keys a score AFTER the source has yielded it,
                    // so a row whose moved-to identity the statement asked for
                    // has to survive the source's own filtering under the
                    // identity it is still filed under. The list the source
                    // gets therefore carries those source identities as well;
                    // what is finally published is judged against the
                    // statement's own list, on the identity it is published
                    // under.
                    self.admit_move_sources(context)?;
                    let candidates = self
                        .candidate_filter
                        .then_some(self.candidate_ids.as_slice());
                    let groups = self.with_partition_scope(|| {
                        context.with_vector_copies(|vector_copy_owner| {
                            db.bounded_vector_candidate_groups(
                                &self.index,
                                candidates,
                                self.snapshot,
                                &mut || context.checkpoint(),
                                &mut || {
                                    context.charge_operator(BoundedWorkSource::VectorCandidates)
                                },
                                &mut |bytes| {
                                    context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                                },
                                &mut |bytes| context.release_parked(bytes),
                                vector_copy_owner,
                                context.reader_budget.clone(),
                            )
                        })
                    })?;
                    let mut groups = RetainedVectorCandidateGroups::new(groups, context);
                    let aggregate_allowed_count = groups.get().aggregate_allowed_count();
                    self.candidate_k = context
                        .with_vector_copies(|vector_copy_owner| {
                            db.bounded_vector_candidate_k(
                                &self.index,
                                self.k,
                                self.sort_key.as_deref(),
                                groups.get(),
                                &mut || context.checkpoint(),
                                vector_copy_owner,
                                context.reader_budget.clone(),
                            )
                        })?
                        .saturating_add(
                            self.vector_overlay
                                .as_ref()
                                .map_or(0, |overlay| overlay.searchable_entry_count),
                        );
                    let hnsw = {
                        let remaining_graph_work = context.remaining_vector_graph_work()?;
                        let allowed_search_limits =
                            contextdb_vector::hnsw::HnswAllowedSearchLimits {
                                max_visited_nodes: remaining_graph_work,
                                max_vector_evaluations: remaining_graph_work,
                            };
                        // Funded before the search starts, so a cold graph load
                        // inside it can hand its credit to the graph's owner.
                        let load_transfer = funded_graph_credit_transfer(context)?;
                        let hnsw_search = || {
                            context.with_vector_copies(|vector_copy_owner| {
                                db.bounded_hnsw_vector_search(
                                    &self.index,
                                    &self.query,
                                    self.candidate_k,
                                    groups.get(),
                                    if self.force_exact_for_relational_fallback {
                                        contextdb_core::VectorSearchMode::Exact
                                    } else {
                                        self.search_mode
                                    },
                                    &reader_memory_refusal,
                                    allowed_search_limits,
                                    &mut || context.checkpoint(),
                                    &mut || {
                                        context.charge(
                                            BoundedWorkSource::VectorCandidates,
                                            BoundedSourceTouch::BruteForceVectorCandidate,
                                        )
                                    },
                                    &mut || {
                                        context.charge_operator(BoundedWorkSource::VectorCandidates)
                                    },
                                    &mut |bytes| {
                                        context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                                    },
                                    &mut |bytes| context.release_parked(bytes),
                                    vector_copy_owner,
                                    context.reader_budget.clone(),
                                    Arc::clone(&load_transfer),
                                )
                            })
                        };
                        #[cfg(feature = "test-seams")]
                        let searched = if let Some(probe) = context.probe() {
                            contextdb_vector::hnsw::with_hnsw_candidate_observer(
                                Arc::new(BoundedHnswProbeObserver { probe }),
                                hnsw_search,
                            )
                        } else {
                            hnsw_search()
                        }?;
                        #[cfg(not(feature = "test-seams"))]
                        let searched = hnsw_search()?;
                        searched
                    };
                    match hnsw {
                        super::BoundedVectorSearchOutcome::Complete(
                            (mut rows, retained_bytes),
                            mut trace,
                        ) => {
                            if self.force_exact_for_relational_fallback {
                                trace.override_exact_reason("filtered_route_exact");
                            }
                            self.used_hnsw = trace.used_hnsw();
                            if self.vector_overlay.is_some() {
                                rows.retain_mut(|(row_id, _score)| {
                                    let Some(published) = self.overlay_publishes(*row_id) else {
                                        return false;
                                    };
                                    *row_id = published;
                                    true
                                });
                            }
                            self.score_bytes = retained_bytes;
                            self.scores = rows;
                            // INDEXED searches committed rows through the
                            // maintained graph and scores only this
                            // transaction's bounded staged delta directly.
                            if let Err(error) =
                                self.score_staged_vectors(db, context).and_then(|()| {
                                    self.record_disclosure(
                                        db,
                                        context,
                                        aggregate_allowed_count,
                                        groups.get(),
                                        &trace,
                                    )
                                })
                            {
                                context.release(trace.retained_bytes())?;
                                return Err(error);
                            }
                            let trace = context.own_vector_trace(trace)?;
                            db.publish_vector_search_trace(trace);
                            self.preparation = VectorPreparation::Materialize;
                            continue;
                        }
                        super::BoundedVectorSearchOutcome::CompleteEmpty => {
                            self.used_hnsw = true;
                            self.scores.clear();
                            self.score_bytes = 0;
                            self.score_staged_vectors(db, context)?;
                            self.preparation = VectorPreparation::Materialize;
                            continue;
                        }
                        super::BoundedVectorSearchOutcome::ExactRequired(mut trace) => {
                            if self.force_exact_for_relational_fallback {
                                trace.override_exact_reason("filtered_route_exact");
                            }
                            if let Err(error) = self.record_disclosure(
                                db,
                                context,
                                aggregate_allowed_count,
                                groups.get(),
                                &trace,
                            ) {
                                context.release(trace.retained_bytes())?;
                                return Err(error);
                            }
                            let trace = context.own_vector_trace(trace)?;
                            db.publish_vector_search_trace(trace);
                        }
                        super::BoundedVectorSearchOutcome::Unavailable
                        | super::BoundedVectorSearchOutcome::Incomplete => {}
                    }
                    if self.search_mode == contextdb_core::VectorSearchMode::Indexed {
                        return Err(Error::VectorIndexedRouteUnavailable {
                            index: self.index.clone(),
                        }
                        .into());
                    }
                    self.scores = Vec::new();
                    self.score_bytes = 0;
                    // The exact route is resolved here -- asked for outright,
                    // forced by a relational fallback, chosen below the
                    // automatic crossover, or reached because the graph could
                    // not serve this read. A ranked read on that route promises
                    // the formula over every allowed stored vector before the
                    // final LIMIT, so its pool is the whole visible set: the
                    // indexed route's bounded cap would cut the best-scoring
                    // row before the formula ever saw it. Every retained score
                    // is still charged to this read as it is held.
                    let exact_candidate_k = if self.sort_key.is_some() {
                        let staged_entries = self
                            .vector_overlay
                            .as_ref()
                            .map_or(0, |overlay| overlay.searchable_entry_count);
                        context
                            .with_vector_copies(|owner| {
                                db.bounded_vector_entry_count(&self.index, self.snapshot, owner)
                            })?
                            .saturating_add(staged_entries)
                            .max(self.candidate_k)
                    } else {
                        self.candidate_k
                    };
                    let cursor = context.with_vector_copies(|vector_copy_owner| {
                        db.bounded_brute_force_vector_cursor(
                            self.index.clone(),
                            &self.query,
                            exact_candidate_k,
                            groups.take(),
                            &mut || context.checkpoint(),
                            &mut |bytes| {
                                context
                                    .reserve(BoundedWorkSource::VectorCandidates, bytes)
                                    .map_err(|error| match error {
                                        BoundedExecutionError::Engine(
                                            Error::MemoryBudgetExceeded {
                                                available_bytes, ..
                                            },
                                        ) => Error::VectorExactSearchBudgetExceeded {
                                            index: self.index.clone(),
                                            required_bytes: bytes as u64,
                                            available_bytes: available_bytes as u64,
                                        }
                                        .into(),
                                        other => other,
                                    })
                            },
                            &mut |bytes| context.release_parked(bytes),
                            vector_copy_owner,
                            context.reader_budget.clone(),
                        )
                    })?;
                    // Automatic exhaustive work, and an exact comparison over a
                    // committed image, answer inside the memory they have by
                    // reading one stored body at a time where a whole partition
                    // does not fit. A writable store's exact search keeps the
                    // whole-partition load and its typed refusal.
                    let cursor = if self.search_mode == contextdb_core::VectorSearchMode::Auto
                        || (self.search_mode == contextdb_core::VectorSearchMode::Exact
                            && cursor.exact_comparison_streams_when_partition_does_not_fit())
                    {
                        cursor.streaming_bodies_it_cannot_hold()
                    } else {
                        cursor
                    };
                    self.source = Some(VectorCandidateSource::Brute(cursor));
                    self.preparation = VectorPreparation::Materialize;
                    continue;
                }
                VectorPreparation::Materialize if self.source.is_some() => {
                    let step = match self.source.as_mut() {
                        Some(VectorCandidateSource::Brute(cursor)) => {
                            context.with_vector_copies(|vector_copy_owner| {
                                db.bounded_brute_force_vector_step(
                                    cursor,
                                    &mut || context.checkpoint(),
                                    &mut || {
                                        context.observe_precharged_touch(
                                            BoundedSourceTouch::BruteForceVectorCandidate,
                                        )
                                    },
                                    &mut || {
                                        context.charge(
                                            BoundedWorkSource::VectorCandidates,
                                            BoundedSourceTouch::BruteForceVectorCandidate,
                                        )
                                    },
                                    &mut || {
                                        context.charge_operator(BoundedWorkSource::VectorCandidates)
                                    },
                                    &mut |bytes| {
                                        context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                                    },
                                    &mut |bytes| context.release_parked(bytes),
                                    vector_copy_owner,
                                    context.reader_budget.clone(),
                                )
                            })
                        }
                        None => {
                            return Err(Error::Other(
                                "bounded vector lost its candidate source".to_string(),
                            )
                            .into());
                        }
                    };
                    // An abandoned continuation keeps its charge, so a refusal
                    // or cancellation from the step disposes of it first.
                    let step = match step {
                        Ok(step) => step,
                        Err(error) => {
                            if let Some(VectorCandidateSource::Brute(cursor)) = self.source.take() {
                                context.release_parked(cursor.into_retained_bytes());
                            }
                            return Err(error);
                        }
                    };
                    match step {
                        contextdb_vector::mem::BoundedVectorStep::Pending => continue,
                        contextdb_vector::mem::BoundedVectorStep::Row(row_id, score) => {
                            let Some(row_id) = self.overlay_publishes(row_id) else {
                                continue;
                            };
                            self.keep_score(context, row_id, score)?;
                            continue;
                        }
                        contextdb_vector::mem::BoundedVectorStep::Exhausted => {
                            let Some(VectorCandidateSource::Brute(cursor)) = self.source.take()
                            else {
                                return Err(Error::Other(
                                    "bounded vector lost its exhausted brute-force cursor"
                                        .to_string(),
                                )
                                .into());
                            };
                            context.release(cursor.into_retained_bytes())?;
                            // The committed entries are done. The vectors this
                            // transaction staged are part of its own answer and
                            // come next, each charged the way a committed entry
                            // is.
                            self.score_staged_vectors(db, context)?;
                            continue;
                        }
                    }
                }
                VectorPreparation::Materialize => {
                    // The search already knows WHICH rows it wants, so it
                    // asks for those rows. Walking the table to find them
                    // costs what the table has ever been written -- every
                    // superseded version of every row, inspected and thrown
                    // away, for each row published -- so a store that has been
                    // updated for a while answers the same query more and more
                    // slowly for no reason visible in its data.
                    let Some((row_id, vector_score)) = self.scores.get(self.materialized).copied()
                    else {
                        self.finish_materialization(context)?;
                        self.preparation = VectorPreparation::Output;
                        continue;
                    };
                    self.materialized = self.materialized.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded vector materialization overflow".to_string())
                    })?;
                    let Some(row) = self.materialize_scored_row(db, context, row_id)? else {
                        continue;
                    };
                    // From here the row's retained bytes are held by a guard,
                    // so no exit out of this block leaves them charged.
                    let mut row = RetainedRow::new(context, row);
                    let rank = if let Some(sort_key) = self.sort_key.as_deref() {
                        let schema_columns = self.schema_columns.clone();
                        let anchor_capacity = schema_columns.len();
                        let mut anchor_bytes = checked_size_add(
                            std::mem::size_of::<HashMap<String, Value>>(),
                            checked_size_mul(
                                anchor_capacity,
                                std::mem::size_of::<(String, Value)>(),
                                "rank anchor map",
                            )?,
                            "rank anchor map",
                        )?;
                        for (column, value) in schema_columns.iter().zip(row.values.iter().skip(1))
                        {
                            anchor_bytes = checked_size_add(
                                anchor_bytes,
                                checked_size_add(
                                    column.len(),
                                    value_capacity_bytes(value)?,
                                    "rank anchor",
                                )?,
                                "rank anchor",
                            )?;
                        }
                        context.reserve(BoundedWorkSource::RankCandidates, anchor_bytes)?;
                        let mut anchor = HashMap::new();
                        if anchor.try_reserve(anchor_capacity).is_err() {
                            context.release(anchor_bytes)?;
                            row.release()?;
                            return Err(Error::Other(
                                "bounded rank-anchor allocation failed".to_string(),
                            )
                            .into());
                        }
                        anchor.extend(
                            schema_columns
                                .iter()
                                .cloned()
                                .zip(row.values.iter().skip(1).cloned()),
                        );
                        let actual_anchor_bytes = anchor.iter().try_fold(
                            checked_size_add(
                                std::mem::size_of::<HashMap<String, Value>>(),
                                checked_size_mul(
                                    anchor.capacity(),
                                    std::mem::size_of::<(String, Value)>(),
                                    "rank anchor map",
                                )?,
                                "rank anchor map",
                            )?,
                            |bytes, (column, value)| {
                                checked_size_add(
                                    bytes,
                                    checked_size_add(
                                        column.capacity(),
                                        value_capacity_bytes(value)?,
                                        "rank anchor",
                                    )?,
                                    "rank anchor",
                                )
                            },
                        )?;
                        if let Err(error) = reconcile_new_reservation(
                            context,
                            BoundedWorkSource::RankCandidates,
                            anchor_bytes,
                            actual_anchor_bytes,
                        ) {
                            row.release()?;
                            return Err(error);
                        }
                        let ranked = db.bounded_rank_candidate(
                            context.source_read_memory(BoundedWorkSource::RankCandidates),
                            &self.index,
                            sort_key,
                            row_id,
                            &anchor,
                            vector_score,
                            self.snapshot,
                            context.transaction(),
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::RankCandidates,
                                    BoundedSourceTouch::RankCandidate,
                                )?;
                                context.attribute_live_memory(BoundedWorkSource::RankCandidates);
                                Ok(())
                            },
                            &mut |_bytes| {
                                context.charge(
                                    BoundedWorkSource::AccessControl,
                                    BoundedSourceTouch::AccessRow,
                                )?;
                                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                                Ok(())
                            },
                            &mut |bytes| context.reserve(BoundedWorkSource::RankCandidates, bytes),
                            &mut |bytes| context.release(bytes),
                        );
                        context.release(actual_anchor_bytes)?;
                        let (rank, joined_bytes) = ranked?;
                        context.release(joined_bytes)?;
                        let Some(rank) = rank else {
                            row.release()?;
                            continue;
                        };
                        rank
                    } else {
                        vector_score
                    };
                    let score_value = Value::Float64(rank as f64);
                    let old_capacity = row.values.capacity();
                    let planned_slot_growth = if row.values.len() == old_capacity {
                        std::mem::size_of::<Value>()
                    } else {
                        0
                    };
                    let reserve_bytes = checked_size_add(
                        planned_slot_growth,
                        value_capacity_bytes(&score_value)?,
                        "vector score",
                    )?;
                    context.reserve(BoundedWorkSource::VectorCandidates, reserve_bytes)?;
                    if row.values.try_reserve_exact(1).is_err() {
                        context.release(reserve_bytes)?;
                        row.release()?;
                        return Err(Error::Other(
                            "bounded vector output allocation failed".to_string(),
                        )
                        .into());
                    }
                    let new_capacity = row.values.capacity();
                    let slot_growth = checked_size_mul(
                        new_capacity
                            .checked_sub(old_capacity)
                            .ok_or_else(|| bounded_size_error("vector output capacity"))?,
                        std::mem::size_of::<Value>(),
                        "vector output capacity",
                    )?;
                    let actual_growth = checked_size_add(
                        slot_growth,
                        value_capacity_bytes(&score_value)?,
                        "vector score",
                    )?;
                    if actual_growth > reserve_bytes {
                        context.reserve(
                            BoundedWorkSource::VectorCandidates,
                            actual_growth - reserve_bytes,
                        )?;
                    } else if reserve_bytes > actual_growth {
                        context.release(reserve_bytes - actual_growth)?;
                    }
                    row.values.push(score_value);
                    row.retained_bytes = row
                        .retained_bytes
                        .checked_add(actual_growth)
                        .ok_or_else(|| bounded_size_error("vector output row"))?;
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::RankCandidates,
                        &mut self.ranked_rows,
                        "ranked vector rows",
                    )?;
                    self.ranked_container_bytes = self
                        .ranked_container_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("ranked vector rows"))?;
                    self.ranked_rows.push(RankedVectorRow {
                        row_id,
                        rank,
                        vector_score,
                        row: row.into_row(),
                    });
                    self.drop_rows_out_of_contention(context)?;
                    continue;
                }
                VectorPreparation::Output => {
                    if let Some(row) = self.output.pop_front() {
                        return Ok(Some(row));
                    }
                    context.release(self.output_container_bytes)?;
                    self.output_container_bytes = 0;
                    self.preparation = VectorPreparation::Exhausted;
                    continue;
                }
                VectorPreparation::Exhausted => return Ok(None),
            }
        }
    }

    /// Release the materialised rows that can no longer reach the answer.
    ///
    /// Once twice the answer's size is held, the rows are put in answer order
    /// and everything past `LIMIT` is released, so a rank over the whole
    /// table holds at most two answers' worth of row values at any moment
    /// rather than every row it scored. Amortised over the read this is one
    /// sort per `LIMIT` candidates, and the answer is the same top rows the
    /// final sort would have kept.
    fn drop_rows_out_of_contention(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let contention_ceiling = self.k.saturating_mul(2).max(self.k.saturating_add(1));
        if self.ranked_rows.len() < contention_ceiling {
            return Ok(());
        }
        self.sort_ranked_rows_into_answer_order();
        self.release_ranked_rows_past_limit(context)
    }

    fn sort_ranked_rows_into_answer_order(&mut self) {
        if self.sort_key.is_some() {
            self.ranked_rows.sort_by(|left, right| {
                bounded_rank_float_desc(left.rank, right.rank)
                    .then_with(|| bounded_rank_float_desc(left.vector_score, right.vector_score))
                    .then_with(|| left.row_id.cmp(&right.row_id))
            });
        } else {
            self.ranked_rows.sort_by(|left, right| {
                right
                    .vector_score
                    .total_cmp(&left.vector_score)
                    .then_with(|| left.row_id.cmp(&right.row_id))
            });
        }
    }

    fn release_ranked_rows_past_limit(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        while self.ranked_rows.len() > self.k {
            let removed = self.ranked_rows.pop().ok_or_else(|| {
                Error::Other("bounded vector truncation lost its tail row".to_string())
            })?;
            removed.row.release(context)?;
        }
        Ok(())
    }

    fn finish_materialization(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.sort_ranked_rows_into_answer_order();
        self.release_ranked_rows_past_limit(context)?;
        let planned_output_bytes = checked_size_mul(
            self.ranked_rows.len(),
            std::mem::size_of::<PulledRow>(),
            "vector output queue",
        )?;
        context.reserve(BoundedWorkSource::VectorCandidates, planned_output_bytes)?;
        let mut output = VecDeque::new();
        if output.try_reserve_exact(self.ranked_rows.len()).is_err() {
            context.release(planned_output_bytes)?;
            return Err(
                Error::Other("bounded vector output-queue allocation failed".to_string()).into(),
            );
        }
        let output_bytes = checked_size_mul(
            output.capacity(),
            std::mem::size_of::<PulledRow>(),
            "vector output queue",
        )?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::VectorCandidates,
            planned_output_bytes,
            output_bytes,
        )?;
        for ranked in self.ranked_rows.drain(..) {
            output.push_back(ranked.row);
        }
        self.ranked_rows = Vec::new();
        context.release(self.ranked_container_bytes)?;
        self.ranked_container_bytes = 0;
        self.output = output;
        self.output_container_bytes = output_bytes;
        self.scores = Vec::new();
        context.release(self.score_bytes)?;
        self.score_bytes = 0;
        self.candidate_ids = Vec::new();
        context.release(self.candidate_bytes)?;
        self.candidate_bytes = 0;
        // The transaction's own vector work has been read and applied; what
        // remembering it cost goes back with the rest of the search state.
        self.vector_overlay = None;
        context.release(self.overlay_bytes)?;
        self.overlay_bytes = 0;
        Ok(())
    }

    fn has_more_hint(&self) -> Option<bool> {
        match self.preparation {
            VectorPreparation::Output if !self.output.is_empty() => Some(true),
            VectorPreparation::Exhausted => Some(false),
            _ => None,
        }
    }
}

fn bounded_rank_float_desc(left: f32, right: f32) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => right.total_cmp(&left),
    }
}

impl ProjectState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.aggregate {
            if self.aggregate_done {
                return Ok(None);
            }
            if self.aggregates.is_none() {
                let state_bytes = checked_size_add(
                    std::mem::size_of::<Vec<AggregateState>>(),
                    checked_size_mul(
                        self.columns.len(),
                        std::mem::size_of::<AggregateState>(),
                        "aggregate accumulators",
                    )?,
                    "aggregate accumulators",
                )?;
                context.reserve(BoundedWorkSource::TableScan, state_bytes)?;
                let mut running = Vec::new();
                if running.try_reserve_exact(self.columns.len()).is_err() {
                    context.release(state_bytes)?;
                    return Err(Error::Other(
                        "bounded aggregate accumulator allocation failed".to_string(),
                    )
                    .into());
                }
                for column in &self.columns {
                    let Some(aggregate) = column_aggregate(column) else {
                        context.release(state_bytes)?;
                        return Err(mixed_aggregate_error().into());
                    };
                    running.push(AggregateState::new(aggregate));
                }
                self.aggregates = Some(running);
                self.aggregate_state_bytes = state_bytes;
            }

            loop {
                // This fetch has spent the work it was given. The running
                // answer and the row already pulled stay exactly where they
                // are, and the page goes back with no row on it -- a cursor
                // finishes the aggregate on the next fetch rather than being
                // refused for an input larger than one page may pay for.
                if context.fetch_budget_spent() {
                    return Ok(None);
                }
                if self.pending.is_none() {
                    match self.input.next(db, context) {
                        Ok(pulled) => self.pending = pulled,
                        // The source reached the ceiling part-way through
                        // this pull -- a filter walks several rows to find
                        // one. Same answer as above: the fetch stops here,
                        // charged for exactly what it examined, and the next
                        // one carries on from the row the source has not
                        // touched yet.
                        Err(error) if context.refusal_pauses_this_fetch(&error) => {
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
                let Some(row) = self.pending.as_ref() else {
                    let running = self.aggregates.as_ref().ok_or_else(|| {
                        Error::Other("bounded aggregate lost its accumulators".to_string())
                    })?;
                    let planned_output_bytes = checked_size_add(
                        std::mem::size_of::<Vec<Value>>(),
                        checked_size_mul(
                            running.len(),
                            std::mem::size_of::<Value>(),
                            "aggregate output",
                        )?,
                        "aggregate output",
                    )?;
                    context.reserve(BoundedWorkSource::TableScan, planned_output_bytes)?;
                    let mut output = Vec::new();
                    if output.try_reserve_exact(running.len()).is_err() {
                        context.release(planned_output_bytes)?;
                        return Err(Error::Other(
                            "bounded aggregate output allocation failed".to_string(),
                        )
                        .into());
                    }
                    let mut output_bytes = match values_retained_bytes(&output) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            context.release(planned_output_bytes)?;
                            return Err(error);
                        }
                    };
                    reconcile_new_reservation(
                        context,
                        BoundedWorkSource::TableScan,
                        planned_output_bytes,
                        output_bytes,
                    )?;
                    for state in running {
                        match state.finish() {
                            Ok(value) => output.push(value),
                            Err(error) => {
                                context.release(output_bytes)?;
                                return Err(error.into());
                            }
                        }
                    }
                    let final_output_bytes = match values_retained_bytes(&output) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            context.release(output_bytes)?;
                            return Err(error);
                        }
                    };
                    reconcile_new_reservation(
                        context,
                        BoundedWorkSource::TableScan,
                        output_bytes,
                        final_output_bytes,
                    )?;
                    output_bytes = final_output_bytes;
                    self.aggregates = None;
                    context.release(self.aggregate_state_bytes)?;
                    self.aggregate_state_bytes = 0;
                    self.aggregate_done = true;
                    return Ok(Some(PulledRow {
                        window: None,
                        values: output,
                        retained_bytes: output_bytes,
                        group_binding: None,
                    }));
                };

                // Folding a row the source has already been charged for is
                // not a second look at it: `work` is what a read EXAMINES, so
                // billing the fold too would make a declared ceiling of a
                // thousand admit five hundred rows. The boundary the charge
                // used to carry -- cancellation and the active-time ceiling --
                // is still checked here, on every row.
                context.check_final_boundary()?;
                let arguments = self.columns.iter().filter_map(|column| {
                    let Expr::FunctionCall { args, .. } = &column.expr else {
                        return None;
                    };
                    match aggregate_input(args) {
                        AggregateInput::Value(argument) => Some(argument),
                        AggregateInput::Row | AggregateInput::Nothing => None,
                    }
                });
                let temporary_bytes =
                    expression_temporary_bytes(&row.values, &self.params, arguments)?;
                context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
                let folded = (|| {
                    let running = self.aggregates.as_mut().ok_or_else(|| {
                        Error::Other("bounded aggregate lost its accumulators".to_string())
                    })?;
                    for (column, state) in self.columns.iter().zip(running.iter_mut()) {
                        let Expr::FunctionCall { args, .. } = &column.expr else {
                            return Err(mixed_aggregate_error());
                        };
                        match aggregate_input(args) {
                            AggregateInput::Row => state.admit(AggregateItem::Row)?,
                            AggregateInput::Nothing => state.admit(AggregateItem::Nothing)?,
                            AggregateInput::Value(argument) => {
                                let value = eval_query_result_expr_with_window(
                                    argument,
                                    &row.values,
                                    &self.input.columns,
                                    &self.params,
                                    row.window,
                                )?;
                                state.admit(AggregateItem::Value(&value))?;
                            }
                        }
                    }
                    Ok(())
                })();
                context.release(temporary_bytes)?;
                match folded {
                    Ok(()) => {}
                    Err(error) => {
                        let row = self.pending.take().ok_or_else(|| {
                            Error::Other("bounded aggregate lost its input row".to_string())
                        })?;
                        row.release(context)?;
                        return Err(error.into());
                    }
                }
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded aggregate lost its input row".to_string())
                })?;
                row.release(context)?;
            }
        }

        if self.pending.is_none() {
            self.pending = self.input.next(db, context)?;
        }
        let Some(row) = self.pending.as_ref() else {
            return Ok(None);
        };
        let temporary_bytes = projection_temporary_bytes(
            &row.values,
            &self.input.columns,
            &self.params,
            &self.columns,
        )?;
        context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
        let output = self
            .columns
            .iter()
            .map(|column| {
                eval_project_expr_with_window(
                    &column.expr,
                    &row.values,
                    &self.input.columns,
                    &self.params,
                    row.window,
                )
            })
            .collect::<Result<Vec<_>>>();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                context.release(temporary_bytes)?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded projection lost its input row".to_string())
                })?;
                row.release(context)?;
                return Err(error.into());
            }
        };
        // The headroom above was reserved FOR this output; the output is what
        // it turned into, not something held beside it. Reserving the output
        // on top and only then releasing the headroom charged the read for
        // both at once -- a row's worth of double-count at the moment a
        // projection is at its widest, which is exactly when a memory ceiling
        // is decided.
        let output_bytes = values_retained_bytes(&output)?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::TableScan,
            temporary_bytes,
            output_bytes,
        )?;
        let input = self
            .pending
            .take()
            .ok_or_else(|| Error::Other("bounded projection lost its input row".to_string()))?;
        let group_binding = input.group_binding;
        input.release(context)?;
        Ok(Some(PulledRow {
            window: None,
            values: output,
            retained_bytes: output_bytes,
            group_binding,
        }))
    }
}

impl SortState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if !self.prepared {
            loop {
                if self.pending.is_none() {
                    self.pending = self.input.next(db, context)?;
                }
                let Some(row) = self.pending.as_ref() else {
                    let sort_scratch_bytes = checked_size_mul(
                        self.rows.len(),
                        std::mem::size_of::<SortEntry>(),
                        "sort scratch",
                    )?;
                    context.reserve(BoundedWorkSource::UnindexedSort, sort_scratch_bytes)?;
                    self.rows
                        .sort_by(|left, right| sort_entry_order(&self.keys, left, right));
                    if let Some(limit) = self.rank_limit {
                        self.release_ranks_past(limit, context)?;
                    }
                    self.rows.reverse();
                    context.release(sort_scratch_bytes)?;
                    self.prepared = true;
                    break;
                };
                context.charge(
                    BoundedWorkSource::UnindexedSort,
                    BoundedSourceTouch::SortCandidate,
                )?;
                let temporary_bytes = sort_key_temporary_bytes(
                    &row.values,
                    &self.input.columns,
                    &self.params,
                    &self.keys,
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, temporary_bytes)?;
                let values = self
                    .keys
                    .iter()
                    .map(|key| {
                        eval_query_result_expr_with_window(
                            &key.expr,
                            &row.values,
                            &self.input.columns,
                            &self.params,
                            row.window,
                        )
                    })
                    .collect::<Result<Vec<_>>>();
                let values = match values {
                    Ok(values) => values,
                    Err(error) => {
                        context.release(temporary_bytes)?;
                        let row = self.pending.take().ok_or_else(|| {
                            Error::Other("bounded sort lost its candidate".to_string())
                        })?;
                        row.release(context)?;
                        return Err(error.into());
                    }
                };
                let key_bytes = values_retained_bytes(&values)?;
                if let Err(error) = context.reserve(BoundedWorkSource::UnindexedSort, key_bytes) {
                    context.release(temporary_bytes)?;
                    return Err(error);
                }
                context.release(temporary_bytes)?;
                let mut key_charge = ReservedBytes::new(context, key_bytes);
                if let Some(limit) = self.rank_limit {
                    if limit.limit == 0 {
                        // The rank filter passes no row at all.
                        drop(values);
                        drop(key_charge);
                        let row = self.pending.take().ok_or_else(|| {
                            Error::Other("bounded sort lost its candidate".to_string())
                        })?;
                        row.release(context)?;
                        continue;
                    }
                    if self.rows.len() == self.rows.capacity() && self.rows.len() > limit.limit {
                        // Before the buffer grows, give up the rows the rank
                        // filter can never pass. It grows only if more than
                        // half stays full, which is never sooner than the
                        // full sort's buffer would have grown.
                        self.compact_ranks_past(limit, context)?;
                        if self.rows.len() > self.rows.capacity() / 2 {
                            let growth = double_sort_rows(context, &mut self.rows)?;
                            self.container_bytes =
                                checked_size_add(self.container_bytes, growth, "sort container")?;
                        }
                    }
                }
                let growth = reserve_vector_slot(
                    context,
                    BoundedWorkSource::UnindexedSort,
                    &mut self.rows,
                    "sort rows",
                )?;
                self.container_bytes =
                    checked_size_add(self.container_bytes, growth, "sort container")?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded sort lost its admitted candidate".to_string())
                })?;
                self.buffered_bytes = self
                    .buffered_bytes
                    .checked_add(key_bytes)
                    .and_then(|held| held.checked_add(row.retained_bytes))
                    .ok_or_else(|| bounded_size_error("sort buffer"))?;
                self.rows.push(SortEntry {
                    keys: values,
                    key_bytes,
                    row,
                });
                key_charge.bytes = 0;
            }
        }
        if self.rows.is_empty() {
            return Ok(None);
        }
        // Handing back a row buffered by an earlier page is work this read is
        // doing now. Charging it is what lets a resumed page be stopped by its
        // work ceiling and interrupted by its caller, exactly like every other
        // operator that emits from a buffer it filled earlier.
        context.charge_operator(BoundedWorkSource::UnindexedSort)?;
        let Some(entry) = self.rows.pop() else {
            return Ok(None);
        };
        self.buffered_bytes = self
            .buffered_bytes
            .checked_sub(entry.key_bytes)
            .and_then(|bytes| bytes.checked_sub(entry.row.retained_bytes))
            .ok_or_else(|| Error::Other("bounded sort retained bytes underflow".to_string()))?;
        context.release(entry.key_bytes)?;
        Ok(Some(entry.row))
    }

    /// Sort the buffer in window order and give up every row ranked past the
    /// limit within its partition. The scratch is funded as the final sort
    /// funds its own, for no more rows than the full sort would hold then.
    fn compact_ranks_past(
        &mut self,
        limit: SortRankLimit,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let scratch = checked_size_mul(
            self.rows.len(),
            std::mem::size_of::<SortEntry>(),
            "sort scratch",
        )?;
        context.reserve(BoundedWorkSource::UnindexedSort, scratch)?;
        self.rows
            .sort_by(|left, right| sort_entry_order(&self.keys, left, right));
        self.release_ranks_past(limit, context)?;
        context.release(scratch)
    }

    /// In a buffer already in window order, keep each partition's first
    /// `limit` rows in place and release the rest: the rank filter can pass
    /// none of them, and the kept rows keep the ranks the full sort gives them.
    fn release_ranks_past(
        &mut self,
        limit: SortRankLimit,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut kept = 0usize;
        let mut rank = 0usize;
        for read in 0..self.rows.len() {
            let same_partition = kept > 0
                && self.rows[read].keys[..limit.partition_len]
                    .iter()
                    .zip(&self.rows[kept - 1].keys[..limit.partition_len])
                    .all(|(left, right)| {
                        compare_sort_values(left, right, SortDirection::Asc) == Ordering::Equal
                    });
            rank = if same_partition { rank + 1 } else { 1 };
            if rank <= limit.limit {
                self.rows.swap(kept, read);
                kept += 1;
            }
        }
        for released in self.rows.drain(kept..) {
            self.buffered_bytes = self
                .buffered_bytes
                .checked_sub(released.key_bytes)
                .and_then(|held| held.checked_sub(released.row.retained_bytes))
                .ok_or_else(|| Error::Other("bounded sort retained bytes underflow".to_string()))?;
            let key_bytes = released.key_bytes;
            drop(released.keys);
            context.release(key_bytes)?;
            released.row.release(context)?;
        }
        Ok(())
    }
}

/// Double the sort buffer's capacity, funded first, by the rule
/// `reserve_vector_slot` follows when the buffer is full.
fn double_sort_rows(
    context: &Arc<RequestContext>,
    rows: &mut Vec<SortEntry>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let previous_capacity = rows.capacity();
    let target = previous_capacity
        .saturating_mul(2)
        .max(AMORTIZED_GROWTH_FLOOR);
    let additional = target.saturating_sub(previous_capacity).max(1);
    let bytes = checked_size_mul(additional, std::mem::size_of::<SortEntry>(), "sort rows")?;
    context.reserve(BoundedWorkSource::UnindexedSort, bytes)?;
    if rows
        .try_reserve_exact(target.saturating_sub(rows.len()))
        .is_err()
    {
        context.release(bytes)?;
        return Err(Error::Other("bounded sort rows allocation failed".to_string()).into());
    }
    let delta = checked_size_mul(
        rows.capacity(),
        std::mem::size_of::<SortEntry>(),
        "sort rows",
    )
    .and_then(|actual| {
        checked_size_mul(
            previous_capacity,
            std::mem::size_of::<SortEntry>(),
            "sort rows",
        )
        .map(|prior| actual.checked_sub(prior))
    });
    let delta = match delta {
        Ok(Some(delta)) => delta,
        Ok(None) => {
            context.release(bytes)?;
            return Err(
                Error::Other("bounded sort rows capacity moved backwards".to_string()).into(),
            );
        }
        Err(error) => {
            context.release(bytes)?;
            return Err(error);
        }
    };
    reconcile_new_reservation(context, BoundedWorkSource::UnindexedSort, bytes, delta)?;
    Ok(delta)
}

impl LimitState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.emitted >= self.count {
            return Ok(None);
        }
        let row = self.input.next(db, context)?;
        if row.is_some() {
            self.emitted = self
                .emitted
                .checked_add(1)
                .ok_or_else(|| Error::Other("bounded LIMIT counter overflow".to_string()))?;
        }
        Ok(row)
    }
}

impl FilterState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                self.pending = self.input.next(db, context)?;
            }
            let Some(row) = self.pending.as_ref() else {
                return Ok(None);
            };
            let temporary_bytes = expression_temporary_bytes(
                &row.values,
                &self.params,
                std::iter::once(&self.predicate),
            )?;
            context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
            let matches = eval_bool_expr(&self.predicate, &|inner| {
                eval_query_result_expr_with_window(
                    inner,
                    &row.values,
                    &self.input.columns,
                    &self.params,
                    row.window,
                )
            })
            .map(|truth| truth.unwrap_or(false));
            context.release(temporary_bytes)?;
            let matches = match matches {
                Ok(matches) => matches,
                Err(error) => {
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded filter lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error.into());
                }
            };
            if matches {
                return self.pending.take().map(Some).ok_or_else(|| {
                    Error::Other("bounded filter lost its match".to_string()).into()
                });
            }
            let row = self
                .pending
                .take()
                .ok_or_else(|| Error::Other("bounded filter lost its rejected row".to_string()))?;
            row.release(context)?;
        }
    }
}

impl DistinctState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                self.pending = self.input.next(db, context)?;
            }
            let Some(row) = self.pending.as_ref() else {
                context.release(self.seen_bytes)?;
                self.seen_bytes = 0;
                self.seen.clear();
                return Ok(None);
            };
            context.charge_operator(BoundedWorkSource::UnindexedSort)?;
            let encoded_bytes = serialized_size(&row.values)?;
            let retained = checked_size_add(
                encoded_bytes,
                checked_size_add(
                    std::mem::size_of::<Vec<u8>>(),
                    checked_size_mul(2, std::mem::size_of::<usize>(), "DISTINCT key")?,
                    "DISTINCT key",
                )?,
                "DISTINCT key",
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, retained)?;
            let key = match encoded_row_key(&row.values, encoded_bytes) {
                Ok(key) => key,
                Err(error) => {
                    context.release(retained)?;
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded DISTINCT lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error);
                }
            };
            if self.seen.contains(&key) {
                context.release(retained)?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded DISTINCT lost its duplicate".to_string())
                })?;
                row.release(context)?;
                continue;
            }
            self.seen.insert(key);
            self.seen_bytes = self
                .seen_bytes
                .checked_add(retained)
                .ok_or_else(|| bounded_size_error("DISTINCT state"))?;
            return self
                .pending
                .take()
                .map(Some)
                .ok_or_else(|| Error::Other("bounded DISTINCT lost its row".to_string()).into());
        }
    }
}

impl JoinState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if !self.right_prepared {
            loop {
                if self.right_pending.is_none() {
                    self.right_pending = self.right.next(db, context)?;
                }
                let Some(_) = self.right_pending.as_ref() else {
                    self.right_prepared = true;
                    break;
                };
                let entry_bytes = std::mem::size_of::<PulledRow>();
                context.reserve(BoundedWorkSource::UnindexedSort, entry_bytes)?;
                let mut row = self
                    .right_pending
                    .take()
                    .ok_or_else(|| Error::Other("bounded join lost its right input".to_string()))?;
                row.retained_bytes = row
                    .retained_bytes
                    .checked_add(entry_bytes)
                    .ok_or_else(|| bounded_size_error("join right buffer"))?;
                self.right_rows.push(row);
            }
        }

        loop {
            if self.left_row.is_none() {
                let Some(row) = self.left.next(db, context)? else {
                    for row in self.right_rows.drain(..) {
                        row.release(context)?;
                    }
                    return Ok(None);
                };
                self.left_row = Some(row);
                self.right_position = 0;
                self.matched = false;
            }

            while self.right_position < self.right_rows.len() {
                context.charge_operator(BoundedWorkSource::UnindexedSort)?;
                let right = self.right_rows.get(self.right_position).ok_or_else(|| {
                    Error::Other("bounded join lost its admitted right pair".to_string())
                })?;
                let left = self
                    .left_row
                    .as_ref()
                    .ok_or_else(|| Error::Other("bounded join lost its left input".to_string()))?;
                let output_bytes = value_refs_retained_bytes(
                    left.values.iter().chain(right.values.iter()),
                    left.values
                        .len()
                        .checked_add(right.values.len())
                        .ok_or_else(|| bounded_size_error("join output"))?,
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, output_bytes)?;
                let combined = concatenate_rows(&left.values, &right.values);
                let temporary_bytes = expression_temporary_bytes(
                    &combined,
                    &self.params,
                    std::iter::once(&self.condition),
                )?;
                if let Err(error) =
                    context.reserve(BoundedWorkSource::UnindexedSort, temporary_bytes)
                {
                    context.release(output_bytes)?;
                    return Err(error);
                }
                let matches = query_result_row_matches(
                    &combined,
                    &self.condition_columns,
                    &self.condition,
                    &self.params,
                );
                context.release(temporary_bytes)?;
                let matches = match matches {
                    Ok(matches) => matches,
                    Err(error) => {
                        context.release(output_bytes)?;
                        return Err(error.into());
                    }
                };
                self.right_position = self
                    .right_position
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("bounded join position overflow".to_string()))?;
                if matches {
                    self.matched = true;
                    return Ok(Some(PulledRow {
                        window: None,
                        values: combined,
                        retained_bytes: output_bytes,
                        group_binding: None,
                    }));
                }
                context.release(output_bytes)?;
            }

            if !self.matched && matches!(self.join_type, contextdb_planner::JoinType::Left) {
                let left = self.left_row.as_mut().ok_or_else(|| {
                    Error::Other("bounded join lost its unmatched row".to_string())
                })?;
                let old_capacity = left.values.capacity();
                let required_len = left
                    .values
                    .len()
                    .checked_add(self.right_column_count)
                    .ok_or_else(|| bounded_size_error("left join NULL extension"))?;
                let planned_slots = if required_len > old_capacity {
                    required_len
                        .checked_sub(old_capacity)
                        .ok_or_else(|| bounded_size_error("left join NULL extension"))?
                } else {
                    0
                };
                let planned_growth = checked_size_mul(
                    planned_slots,
                    std::mem::size_of::<Value>(),
                    "left join NULL extension",
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, planned_growth)?;
                if left
                    .values
                    .try_reserve_exact(self.right_column_count)
                    .is_err()
                {
                    context.release(planned_growth)?;
                    return Err(Error::Other(
                        "bounded left join NULL extension allocation failed".to_string(),
                    )
                    .into());
                }
                let actual_slots = match left.values.capacity().checked_sub(old_capacity) {
                    Some(slots) => slots,
                    None => {
                        context.release(planned_growth)?;
                        return Err(Error::Other(
                            "bounded left join capacity moved backwards".to_string(),
                        )
                        .into());
                    }
                };
                let actual_growth = match checked_size_mul(
                    actual_slots,
                    std::mem::size_of::<Value>(),
                    "left join NULL extension",
                ) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        context.release(planned_growth)?;
                        return Err(error);
                    }
                };
                let retained_bytes = match left.retained_bytes.checked_add(actual_growth) {
                    Some(bytes) => bytes,
                    None => {
                        context.release(planned_growth)?;
                        return Err(bounded_size_error("left join output"));
                    }
                };
                reconcile_new_reservation(
                    context,
                    BoundedWorkSource::UnindexedSort,
                    planned_growth,
                    actual_growth,
                )?;
                left.values
                    .extend(std::iter::repeat_n(Value::Null, self.right_column_count));
                left.retained_bytes = retained_bytes;
                return Ok(self.left_row.take());
            }
            let left = self
                .left_row
                .take()
                .ok_or_else(|| Error::Other("bounded join lost its completed row".to_string()))?;
            left.release(context)?;
        }
    }
}

impl UnionState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                let Some(input) = self.inputs.get_mut(self.input) else {
                    context.release(self.seen_bytes)?;
                    self.seen_bytes = 0;
                    self.seen.clear();
                    return Ok(None);
                };
                self.pending = input.next(db, context)?;
                if self.pending.is_none() {
                    self.input = self.input.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded UNION input position overflow".to_string())
                    })?;
                    continue;
                }
            }
            context.charge_operator(BoundedWorkSource::UnindexedSort)?;
            if self.all {
                return self
                    .pending
                    .take()
                    .map(Some)
                    .ok_or_else(|| Error::Other("bounded UNION lost its row".to_string()).into());
            }
            let row = self
                .pending
                .as_ref()
                .ok_or_else(|| Error::Other("bounded UNION lost its pending row".to_string()))?;
            let encoded_bytes = serialized_size(&row.values)?;
            let retained = checked_size_add(
                encoded_bytes,
                checked_size_add(
                    std::mem::size_of::<Vec<u8>>(),
                    checked_size_mul(2, std::mem::size_of::<usize>(), "UNION key")?,
                    "UNION key",
                )?,
                "UNION key",
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, retained)?;
            let key = match encoded_row_key(&row.values, encoded_bytes) {
                Ok(key) => key,
                Err(error) => {
                    context.release(retained)?;
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded UNION lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error);
                }
            };
            if self.seen.contains(&key) {
                context.release(retained)?;
                let row = self
                    .pending
                    .take()
                    .ok_or_else(|| Error::Other("bounded UNION lost its duplicate".to_string()))?;
                row.release(context)?;
                continue;
            }
            self.seen.insert(key);
            self.seen_bytes = self
                .seen_bytes
                .checked_add(retained)
                .ok_or_else(|| bounded_size_error("UNION state"))?;
            return self
                .pending
                .take()
                .map(Some)
                .ok_or_else(|| Error::Other("bounded UNION lost its row".to_string()).into());
        }
    }
}

impl StaticState {
    fn next(&mut self) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        Ok(self.rows.pop_front())
    }
}

fn ensure_supported_plan(plan: &PhysicalPlan) -> std::result::Result<(), BoundedExecutionError> {
    match plan {
        PhysicalPlan::Scan { .. } => Ok(()),
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::WindowRank { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::MaterializeCte { input, .. } => ensure_supported_plan(input),
        PhysicalPlan::Join { left, right, .. } => {
            ensure_supported_plan(left)?;
            ensure_supported_plan(right)
        }
        PhysicalPlan::Union { inputs, .. } => {
            for input in inputs {
                ensure_supported_plan(input)?;
            }
            Ok(())
        }
        PhysicalPlan::GraphBfs {
            start_candidates, ..
        } => start_candidates
            .as_deref()
            .map_or(Ok(()), ensure_supported_plan),
        PhysicalPlan::VectorSearch { candidates, .. }
        | PhysicalPlan::HnswSearch { candidates, .. } => {
            candidates.as_deref().map_or(Ok(()), ensure_supported_plan)
        }
        PhysicalPlan::IndexScan { .. } | PhysicalPlan::CteRef { .. } => Ok(()),
        PhysicalPlan::GroupedCount { input, .. } => match input.as_ref() {
            PhysicalPlan::Scan { .. } => Ok(()),
            _ => Err(Error::PlanError(
                "bounded execution accepts read-only SELECT plans".to_string(),
            )
            .into()),
        },
        // A store answering questions about itself: no rows read, nothing
        // changed, and classified a READ, so a reading session runs it.
        PhysicalPlan::ShowMemoryLimit
        | PhysicalPlan::ShowDiskLimit
        | PhysicalPlan::ShowContentSize
        | PhysicalPlan::ShowMaintenancePollInterval
        | PhysicalPlan::ShowVectorServingCache
        | PhysicalPlan::ShowSyncConflictPolicy
        | PhysicalPlan::ShowVectorIndexes
        | PhysicalPlan::ShowTenantTablePolicy { .. }
        | PhysicalPlan::ShowSyncBindings
        | PhysicalPlan::ShowDeliveryOutcomes(_)
        | PhysicalPlan::ShowVectorPartitions { .. } => Ok(()),
        PhysicalPlan::Pipeline(plans) => {
            for plan in plans {
                ensure_supported_plan(plan)?;
            }
            Ok(())
        }
        _ => Err(
            Error::PlanError("bounded execution accepts read-only SELECT plans".to_string()).into(),
        ),
    }
}

/// Resolve `IN (subquery)` for a bounded read by reading the inner answer the
/// bounded way.
///
/// The inner query is a read like any other: it touches sources, it retains
/// rows, and a caller who set a ceiling meant it to cover the whole statement,
/// not just the outer half. Handing the inner half to the eager executor reads
/// it with no ceiling at all, keeps the whole result in memory uncharged, and
/// cannot notice a cancellation until it is over -- so a one-row answer over a
/// million-row subquery escapes both limits it was given.
///
/// Everything except the row reading is the executor's: the same correlation
/// refusal, the same planner, the same literal conversion, the same walk.
fn bounded_resolve_in_subqueries(
    db: &dyn ReadExecutionTarget,
    expr: &Expr,
    params: &Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &[Cte],
    context: &Arc<RequestContext>,
) -> std::result::Result<Expr, BoundedExecutionError> {
    let Some(database) = db.as_database_for_subquery_plan() else {
        // Only the in-process target can plan a subquery; any other target
        // never carries one, so there is nothing to resolve.
        return Ok(expr.clone());
    };
    let mut drain = |plan: &PhysicalPlan,
                     select: &Expr|
     -> std::result::Result<Vec<Value>, BoundedExecutionError> {
        let mut node = build_kernel(db, plan, Arc::clone(params), snapshot, context)?;
        {
            let named: std::collections::BTreeSet<String> = node.columns.iter().cloned().collect();
            declare_supplemented_columns(db, &mut node, &named);
        }
        let mut values = Vec::new();
        // Reserved before the value exists and released on every exit,
        // including the unwind, so a refusal mid-drain leaves the accountant
        // where it started.
        let mut retained = 0usize;
        let outcome = (|| -> std::result::Result<(), BoundedExecutionError> {
            while let Some(row) = pull(db, context, &mut node)? {
                let projected =
                    super::eval_project_expr(select, &row.values, &node.columns, params)?;
                // What the list really costs to hold: the payload, the slot it
                // sits in while it is a value, and the slot it sits in again
                // once it becomes a literal. Charging the payload alone leaves
                // a scalar id looking free, and a million free ids is how the
                // ceiling gets passed without ever being consulted.
                let bytes = planned_value_clone_bytes(&projected)?
                    .checked_add(std::mem::size_of::<Value>())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Expr>()))
                    .ok_or_else(|| bounded_size_error("bounded subquery literal"))?;
                context.reserve(BoundedWorkSource::TableScan, bytes)?;
                retained = retained.saturating_add(bytes);
                values.push(projected);
                context.release(row.retained_bytes)?;
            }
            Ok(())
        })();
        if outcome.is_err() {
            let _ = context.release(retained);
        }
        outcome?;
        Ok(values)
    };
    super::resolve_in_subqueries_with_drain(database, expr, params.as_ref(), ctes, &mut drain)
}

/// Resolve a traversal's start against the store the bounded way.
///
/// Finding a start by something other than its id reads rows -- a whole node
/// table when no index covers the predicate. Reading them through the eager
/// helper puts an unbounded scan and an unbounded candidate set inside a read
/// that was given a ceiling, and the ceiling only learns about it afterwards.
///
/// Only the row reading moves here. Which filter names a start, which tables
/// can describe one, which predicates were pushed and the per-candidate
/// confirmation all stay in the executor's own resolver.
fn bounded_resolve_graph_start_nodes(
    db: &dyn ReadExecutionTarget,
    filter: &Expr,
    params: &Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    start_alias: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<super::GraphStartResolution>, BoundedExecutionError> {
    let Some(database) = db.as_database_for_subquery_plan() else {
        return Ok(None);
    };
    let tx = context.transaction();
    let mut read = |table: &str,
                    pick: Option<&IndexPick>|
     -> std::result::Result<(Vec<uuid::Uuid>, u64), BoundedExecutionError> {
        // The index the executor's own resolver picked for this table, read
        // the bounded way. Ignoring it walks every row of a node table to find
        // a start the index names in one run, charges the reader for all of
        // them, and reports a figure the other door never reports.
        let mut node = build_scan(
            db,
            table,
            None,
            Arc::clone(params),
            snapshot,
            None,
            pick.cloned(),
            context,
        )?;
        let id_column = node.columns.iter().position(|column| column == "id");
        let mut ids = Vec::new();
        let mut examined = 0u64;
        let mut retained = 0usize;
        let outcome = (|| -> std::result::Result<(), BoundedExecutionError> {
            while let Some(row) = pull(db, context, &mut node)? {
                examined = examined.saturating_add(1);
                if let Some(position) = id_column
                    && let Some(Value::Uuid(id)) = row.values.get(position)
                {
                    // Reserved before the id is kept, so a candidate set that
                    // outgrows the ceiling is refused while it is growing.
                    let bytes = std::mem::size_of::<uuid::Uuid>();
                    context.reserve(BoundedWorkSource::TableScan, bytes)?;
                    retained = retained.saturating_add(bytes);
                    ids.push(*id);
                }
                context.release(row.retained_bytes)?;
            }
            Ok(())
        })();
        if outcome.is_err() {
            let _ = context.release(retained);
        }
        outcome?;
        Ok((ids, examined))
    };
    super::resolve_graph_start_nodes_with_reader(
        database,
        filter,
        params.as_ref(),
        tx,
        snapshot,
        start_alias,
        &mut read,
        Arc::new(SourceDecodeBudget {
            context: context.clone(),
            source: BoundedWorkSource::GraphTraversal,
        }),
    )
    .map(Some)
}

/// Validate the complete declared inputs before building a kernel, including
/// sources whose join/candidate branch execution can subsequently bypass.
fn preflight_plan_indexes(
    db: &dyn ReadExecutionTarget, plan: &PhysicalPlan, params: &HashMap<String, Value>,
    snapshot: SnapshotId, context: &Arc<RequestContext>,
) -> std::result::Result<(), BoundedExecutionError> {
    let memory = context.reader_budget.clone();
    if db.ordered_prefix_snapshot_is_current(snapshot, memory.clone())? { return Ok(()); }
    let preflight = |table: &str, index: &str| context.with_source_page_admission(
        BoundedWorkSource::IndexRange, || db.preflight_historical_index(table, index, snapshot,
            memory.clone(), &mut || context.charge_operator(BoundedWorkSource::IndexRange)));
    match plan {
        PhysicalPlan::Limit { count: 0, .. } => Ok(()),
        PhysicalPlan::Scan { table, filter, .. } => {
            if let Some(filter) = filter.as_ref()
                && let Some(meta) = db.table_meta(table)
                && let Some(pick) = analyze_filter_for_index(table, &meta, filter, params).pick {
                preflight(table, &pick.name)?;
            }
            Ok(())
        }
        PhysicalPlan::IndexScan { table, index, .. } => preflight(table, index),
        PhysicalPlan::Sort { input, keys } => {
            if let Some((table, _, ordered)) = ordered_scan(db, input, keys, params) {
                preflight(table, &ordered.index)?;
            }
            preflight_plan_indexes(db, input, params, snapshot, context)
        }
        PhysicalPlan::WindowRank { input, .. } | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. } | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. } | PhysicalPlan::MaterializeCte { input, .. }
        | PhysicalPlan::GroupedCount { input, .. } => preflight_plan_indexes(db, input, params, snapshot, context),
        PhysicalPlan::Join { left, right, .. } => {
            preflight_plan_indexes(db, left, params, snapshot, context)?;
            preflight_plan_indexes(db, right, params, snapshot, context)
        }
        PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => {
            for input in inputs { preflight_plan_indexes(db, input, params, snapshot, context)?; }
            Ok(())
        }
        PhysicalPlan::GraphBfs { start_candidates, .. } => {
            db.preflight_historical_graph(snapshot,
                context.reader_budget.clone(),
                &mut || context.charge_operator(BoundedWorkSource::GraphTraversal))?;
            if let Some(input) = start_candidates { preflight_plan_indexes(db, input, params, snapshot, context)?; }
            Ok(())
        }
        PhysicalPlan::VectorSearch { candidates, .. } | PhysicalPlan::HnswSearch { candidates, .. } => {
            if let Some(candidates) = candidates { preflight_plan_indexes(db, candidates, params, snapshot, context)?; }
            Ok(())
        }
        _ => Ok(()),
    }
}

// The guard is thread-local and lives only while this request is executing.
// Cursor fetches re-enter beneath the cursor's existing immutable writer root.
fn enter_vector_read(
    db: &dyn ReadExecutionTarget,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
    required: bool,
) -> std::result::Result<Option<crate::persistence::vector_membership_source::VectorReadScope>, BoundedExecutionError> {
    if !required { return Ok(None); }
    context.with_vector_copies(|copies| db.vector_read_preflight(
        snapshot, context.reader_budget.clone(), copies,
        Some(ReadOpenContext::from_request(context)),
    )).map_err(BoundedExecutionError::from)
}

// Include branches that execution can bypass after deriving empty candidates.
fn plan_needs_vector_read(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Limit { count: 0, .. } => false,
        PhysicalPlan::VectorSearch { k, .. } | PhysicalPlan::HnswSearch { k, .. } => *k != 0,
        PhysicalPlan::WindowRank { input, .. } | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. } | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. } | PhysicalPlan::MaterializeCte { input, .. }
        | PhysicalPlan::GroupedCount { input, .. } | PhysicalPlan::Sort { input, .. } => plan_needs_vector_read(input),
        PhysicalPlan::Join { left, right, .. } => plan_needs_vector_read(left) || plan_needs_vector_read(right),
        PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => inputs.iter().any(plan_needs_vector_read),
        PhysicalPlan::GraphBfs { start_candidates, .. } => start_candidates.as_deref().is_some_and(plan_needs_vector_read),
        _ => false,
    }
}

fn build_kernel(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    preflight_plan_indexes(db, plan, &params, snapshot, context)?;
    build_kernel_with_ctes(db, plan, params, snapshot, &HashMap::new(), context).map(|node| *node)
}

/// Serve a `COUNT(*)` over an exact index prefix from the maintained count,
/// when the table declares one over exactly those columns.
///
/// `disclosure` is an out-parameter rather than a return value because the
/// two answers travel on different traces: a served count carries its own
/// node and trace, while a NOT-declared count returns `Ok(None)` and the
/// ordinary index-range aggregate the caller builds next is what carries the
/// disclosure. Both answers are exact; only their cost differs, and this is
/// what lets a consumer see which one it got.
fn build_authenticated_index_count(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    columns: &[ProjectColumn],
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
    disclosure: &mut Option<crate::maintained_counts::MaintainedCountDisclosure>,
) -> std::result::Result<Option<Box<PullNode>>, BoundedExecutionError> {
    if context.transaction().is_some() || columns.len() != 1 {
        return Ok(None);
    }
    let Expr::FunctionCall { name, args } = &columns[0].expr else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case("count") || !matches!(aggregate_input(args), AggregateInput::Row)
    {
        return Ok(None);
    }
    let PhysicalPlan::Scan {
        table,
        filter: Some(filter),
        ..
    } = input
    else {
        return Ok(None);
    };
    db.assert_table_read_allowed(table)?;
    if db.bounded_read_requires_candidate_filter(table)? {
        return Ok(None);
    }
    let Some(meta) = db.table_meta(table) else {
        return Err(Error::TableNotFound(table.clone()).into());
    };
    let analysis = analyze_filter_for_index(table, &meta, filter, params);
    let Some(pick) = analysis.pick else {
        return Ok(None);
    };
    let pick = match db.coerce_pick_shape_to_column_type(table, &pick) {
        Ok(pick) => pick,
        Err(_) => return Ok(None),
    };
    let conjuncts = split_conjuncts(filter);
    if conjuncts.len() != pick.pushed_columns.len() {
        return Ok(None);
    }
    let mut covered = HashSet::new();
    for conjunct in conjuncts {
        let Some((column, shape)) = classify_index_predicate(&conjunct, params) else {
            return Ok(None);
        };
        if !matches!(
            shape,
            IndexPredicateShape::Equality(_) | IndexPredicateShape::IsNull
        ) || !pick.pushed_columns.iter().any(|pushed| pushed == &column)
            || !covered.insert(column)
        {
            return Ok(None);
        }
    }
    if !matches!(
        pick.shape,
        IndexPredicateShape::Equality(_) | IndexPredicateShape::IsNull
    ) || pick.suffix_shapes.iter().any(|shape| {
        !matches!(
            shape,
            IndexPredicateShape::Equality(_) | IndexPredicateShape::IsNull
        )
    }) {
        return Ok(None);
    }
    let mut runs = crate::executor::expanded_index_runs(&pick)?;
    if runs.len() != 1 {
        return Ok(None);
    }
    let (Bound::Included(prefix), _) = runs.pop().expect("one exact index run") else {
        return Ok(None);
    };
    if prefix.len() != pick.pushed_columns.len() {
        return Ok(None);
    }
    // A count over a column list a primary key or unique constraint already
    // bounds at 0 or 1 is not maintained: the same answer comes from the index
    // range itself, which holds at most one posting for such a key. Falling
    // through to the ordinary aggregate reads that one posting and returns the
    // same number, authenticated by the same postings.
    if crate::persistence::relational_index_prefix_is_constraint_bounded(
        &meta,
        &pick.columns[..prefix.len()],
    ) {
        return Ok(None);
    }
    // Whether a count is MAINTAINED is the declaration's answer, and it is the
    // only thing an `ALTER TABLE` can change. Whether this read route can SERVE
    // it is a separate question the route answers, so the two are reported
    // separately: advising a declaration that is already in force would send a
    // consumer to change DDL that is not what is costing it anything.
    let declared =
        crate::maintained_counts::maintained_count_member(&meta, &pick.columns[..prefix.len()]);
    if !declared {
        // Nothing maintains a count over this list, so there is no record to
        // consult and no record's absence to misread. Name the declaration
        // that would make it O(1) and let the ordinary aggregate answer.
        *disclosure = Some(
            crate::maintained_counts::MaintainedCountDisclosure::NotDeclared {
                table: table.clone(),
                columns: pick.columns[..prefix.len()].to_vec(),
                // `SET` replaces the whole declared set, so the advice carries
                // what the table already declares alongside the missing list.
                already_declared: crate::maintained_counts::declared_lists(&meta),
            },
        );
        return Ok(None);
    }
    let Some(count) = db.authenticated_relational_index_prefix_count(
        table,
        &pick.columns[..prefix.len()],
        &prefix,
        context.source_read_memory(BoundedWorkSource::IndexRange),
    )?
    else {
        // The shape was eligible and the answer is still exact — the ordinary
        // index-range aggregate the caller builds next opens the same exact
        // prefix range and counts live postings.
        // The table DOES declare this count and the write path keeps it
        // current; this read route simply did not serve the answer from it.
        // That is not a declaration problem and no DDL changes it.
        *disclosure = Some(
            crate::maintained_counts::MaintainedCountDisclosure::DeclaredNotServed {
                table: table.clone(),
                columns: pick.columns[..prefix.len()].to_vec(),
            },
        );
        return Ok(None);
    };
    *disclosure = Some(crate::maintained_counts::MaintainedCountDisclosure::Used {
        table: table.clone(),
        columns: pick.columns[..prefix.len()].to_vec(),
    });
    let count =
        i64::try_from(count).map_err(|_| Error::PlanError("integer out of range".to_owned()))?;
    context.charge(
        BoundedWorkSource::IndexRange,
        BoundedSourceTouch::IndexEntry,
    )?;
    let values = vec![Value::Int64(count)];
    let retained_bytes = value_refs_retained_bytes(values.iter(), values.len())?;
    context.reserve(BoundedWorkSource::IndexRange, retained_bytes)?;
    Ok(Some(Box::new(PullNode {
        columns: project_output_columns(columns),
        trace: QueryTrace {
            physical_plan: "IndexScan",
            index_used: Some(pick.name),
            predicates_pushed: pick
                .pushed_columns
                .into_iter()
                .map(std::borrow::Cow::Owned)
                .collect(),
            indexes_considered: analysis.considered.into_iter().collect(),
            maintained_count: disclosure.clone().map(Box::new),
            ..Default::default()
        },
        kind: PullKind::Static(StaticState {
            rows: VecDeque::from([PulledRow {
                window: None,
                values,
                retained_bytes,
                group_binding: None,
            }]),
        }),
    })))
}

/// One finite partition and its independent visible rank demand.
struct RankedPrefixGroup {
    key: Vec<Value>,
    ceiling: u64,
}

fn ranked_prefix_shape<'a>(
    input: &'a PhysicalPlan,
) -> Option<(
    &'a PhysicalPlan,
    &'a [ProjectColumn],
    &'a contextdb_parser::RowNumberWindow,
    WindowBindingId,
)> {
    let input = match input {
        PhysicalPlan::MaterializeCte { input, .. } => input.as_ref(),
        other => other,
    };
    let PhysicalPlan::Project { input, columns } = input else {
        return None;
    };
    if projection_aggregates(columns) {
        return None;
    }
    let PhysicalPlan::WindowRank {
        input,
        window,
        binding,
    } = input.as_ref()
    else {
        return None;
    };
    if !matches!(input.as_ref(), PhysicalPlan::Scan { .. }) || window.partition_by.is_empty() {
        return None;
    }
    // Project is an alias binding boundary. No projection, limit, distinct or
    // join below the window is crossed by this lowering.
    if columns
        .iter()
        .any(|column| !matches!(column.expr, Expr::Column(_) | Expr::WindowRank(_)))
    {
        return None;
    }
    Some((input, columns, window, *binding))
}

fn ranked_prefix_constant(expr: &Expr, params: &HashMap<String, Value>) -> Result<Option<Value>> {
    match expr {
        Expr::Literal(literal) => super::literal_to_value(literal).map(Some),
        Expr::Parameter(name) => Ok(params.get(name).cloned()),
        _ => Ok(None),
    }
}

fn ranked_prefix_project_expr<'a>(
    reference: &contextdb_parser::ColumnRef,
    columns: &'a [ProjectColumn],
) -> Option<&'a Expr> {
    let mut matched = None;
    for column in columns {
        let name = column.alias.as_deref().or_else(|| match &column.expr {
            Expr::Column(source) => Some(source.column.as_str()),
            _ => None,
        });
        if name == Some(reference.column.as_str()) {
            if matched.is_some() {
                return None;
            }
            matched = Some(&column.expr);
        }
    }
    matched
}

#[allow(clippy::too_many_arguments)]
fn ranked_prefix_conjunction(
    predicate: &Expr,
    columns: &[ProjectColumn],
    window: &contextdb_parser::RowNumberWindow,
    binding: WindowBindingId,
    meta: &TableMeta,
    params: &HashMap<String, Value>,
    key: &mut [Option<Value>],
    ceiling: &mut Option<u64>,
    dead: &mut bool,
) -> Result<bool> {
    if let Expr::BinaryOp {
        left,
        op: BinOp::And,
        right,
    } = predicate
    {
        return Ok(ranked_prefix_conjunction(
            left, columns, window, binding, meta, params, key, ceiling, dead,
        )? && ranked_prefix_conjunction(
            right, columns, window, binding, meta, params, key, ceiling, dead,
        )?);
    }
    let Expr::BinaryOp { left, op, right } = predicate else {
        return Ok(false);
    };
    let (reference, constant, operation) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(reference), constant) => (reference, constant, *op),
        (constant, Expr::Column(reference)) => (
            reference,
            constant,
            match op {
                BinOp::Eq => BinOp::Eq,
                BinOp::Gte => BinOp::Lte,
                _ => return Ok(false),
            },
        ),
        _ => return Ok(false),
    };
    let Some(expression) = ranked_prefix_project_expr(reference, columns) else {
        return Ok(false);
    };
    let Some(value) = ranked_prefix_constant(constant, params)? else {
        return Ok(false);
    };
    if let Expr::WindowRank(found) = expression {
        if *found != binding || operation != BinOp::Lte {
            return Ok(false);
        }
        let bound = match value {
            Value::Null => {
                *dead = true;
                return Ok(true);
            }
            Value::Int64(bound) => {
                if bound <= 0 {
                    0
                } else {
                    bound as u64
                }
            }
            // Keep the existing scalar comparison for non-integral bounds.
            _ => return Ok(false),
        };
        *ceiling = Some(ceiling.map_or(bound, |existing| existing.min(bound)));
        return Ok(true);
    }
    let Expr::Column(source) = expression else {
        return Ok(false);
    };
    if operation != BinOp::Eq {
        return Ok(false);
    }
    let Some(position) = window
        .partition_by
        .iter()
        .position(|column| column == source)
    else {
        return Ok(false);
    };
    let Some(declared) = meta
        .columns
        .iter()
        .find(|column| column.name == source.column)
    else {
        return Ok(false);
    };
    if matches!(value, Value::Null) {
        *dead = true;
        return Ok(true);
    }
    let value = match super::partition_key_literal(&declared.column_type, &value) {
        super::PartitionKeyLiteral::Exact(value) => value,
        super::PartitionKeyLiteral::NoPartition => {
            *dead = true;
            return Ok(true);
        }
        super::PartitionKeyLiteral::Unresolved | super::PartitionKeyLiteral::Unbound => {
            return Ok(false);
        }
    };
    if let Some(previous) = &key[position] {
        if super::compare_values(previous, &value) != Some(Ordering::Equal) {
            *dead = true;
        }
    } else {
        key[position] = Some(value);
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn ranked_prefix_branches(
    predicate: &Expr,
    columns: &[ProjectColumn],
    window: &contextdb_parser::RowNumberWindow,
    binding: WindowBindingId,
    meta: &TableMeta,
    params: &HashMap<String, Value>,
    groups: &mut Vec<RankedPrefixGroup>,
) -> Result<bool> {
    if let Expr::BinaryOp {
        left,
        op: BinOp::Or,
        right,
    } = predicate
    {
        return Ok(
            ranked_prefix_branches(left, columns, window, binding, meta, params, groups)?
                && ranked_prefix_branches(right, columns, window, binding, meta, params, groups)?,
        );
    }
    let mut key = vec![None; window.partition_by.len()];
    let mut ceiling = None;
    let mut dead = false;
    if !ranked_prefix_conjunction(
        predicate,
        columns,
        window,
        binding,
        meta,
        params,
        &mut key,
        &mut ceiling,
        &mut dead,
    )? {
        return Ok(false);
    }
    if dead {
        return Ok(true);
    }
    let Some(ceiling) = ceiling else {
        return Ok(false);
    };
    if key.iter().any(Option::is_none) {
        return Ok(false);
    }
    if ceiling == 0 {
        return Ok(true);
    }
    let key: Vec<Value> = key.into_iter().flatten().collect();
    if let Some(previous) = groups.iter_mut().find(|group| {
        group
            .key
            .iter()
            .zip(&key)
            .all(|(left, right)| super::compare_values(left, right) == Some(Ordering::Equal))
    }) {
        previous.ceiling = previous.ceiling.max(ceiling);
    } else {
        groups.push(RankedPrefixGroup { key, ceiling });
    }
    Ok(true)
}

/// Visit every conjunct of an AND tree.
fn rank_filter_conjuncts<'a>(expr: &'a Expr, visit: &mut dyn FnMut(&'a Expr)) {
    if let Expr::BinaryOp {
        left,
        op: BinOp::And,
        right,
    } = expr
    {
        rank_filter_conjuncts(left, visit);
        rank_filter_conjuncts(right, visit);
        return;
    }
    visit(expr);
}

/// Visit every branch of an OR tree, stopping at the first `false`.
fn rank_filter_branches<'a>(expr: &'a Expr, visit: &mut dyn FnMut(&'a Expr) -> bool) -> bool {
    if let Expr::BinaryOp {
        left,
        op: BinOp::Or,
        right,
    } = expr
    {
        return rank_filter_branches(left, visit) && rank_filter_branches(right, visit);
    }
    visit(expr)
}

/// The rank cap one conjunct states: `rank <= c` or `c >= rank` with an
/// integer `c` (a negative `c` caps at 0), or NULL, which no rank satisfies.
/// Any other conjunct states no cap.
fn rank_filter_cap(
    conjunct: &Expr,
    columns: &[ProjectColumn],
    binding: WindowBindingId,
    params: &HashMap<String, Value>,
) -> Option<u64> {
    let Expr::BinaryOp { left, op, right } = conjunct else {
        return None;
    };
    let (reference, constant) = match (left.as_ref(), op, right.as_ref()) {
        (Expr::Column(reference), BinOp::Lte, constant)
        | (constant, BinOp::Gte, Expr::Column(reference)) => (reference, constant),
        _ => return None,
    };
    let Expr::WindowRank(found) = ranked_prefix_project_expr(reference, columns)? else {
        return None;
    };
    if *found != binding {
        return None;
    }
    let bound = match constant {
        Expr::Literal(contextdb_parser::Literal::Null) => return Some(0),
        Expr::Literal(contextdb_parser::Literal::Integer(bound)) => *bound,
        Expr::Parameter(name) => match params.get(name)? {
            Value::Null => return Some(0),
            Value::Int64(bound) => *bound,
            _ => return None,
        },
        _ => return None,
    };
    Some(u64::try_from(bound).unwrap_or(0))
}

/// The highest rank the rank filter can pass, proven without allocating:
/// every OR branch must cap the rank, and the filter's cap is the largest
/// branch cap. `None` when any branch leaves the rank uncapped.
fn rank_filter_limit(
    predicate: &Expr,
    columns: &[ProjectColumn],
    binding: WindowBindingId,
    params: &HashMap<String, Value>,
) -> Option<u64> {
    let mut limit = 0u64;
    let capped = rank_filter_branches(predicate, &mut |branch| {
        let mut cap: Option<u64> = None;
        rank_filter_conjuncts(branch, &mut |conjunct| {
            if let Some(bound) = rank_filter_cap(conjunct, columns, binding, params) {
                cap = Some(cap.map_or(bound, |cap| cap.min(bound)));
            }
        });
        let Some(cap) = cap else {
            return false;
        };
        limit = limit.max(cap);
        true
    });
    capped.then_some(limit)
}

/// A value the filter reads without any way to fail: a column the filter's
/// input resolves, any literal, or a bound parameter.
fn rank_filter_operand_cannot_raise(
    expr: &Expr,
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> bool {
    match expr {
        Expr::Column(column) => matches!(
            resolve_query_result_column(input_columns, column),
            QueryResultColumn::Found(_)
        ),
        Expr::Literal(_) => true,
        Expr::Parameter(name) => params.contains_key(name),
        _ => false,
    }
}

/// Whether the filter evaluates on every row without raising: AND, OR and NOT
/// of comparisons, IN lists and IS NULL over operands that cannot raise, and
/// TRUE or FALSE. A row the cap gives up is never shown to the filter, so any
/// other shape, which might raise on exactly that row, keeps the full sort.
fn rank_filter_cannot_raise(
    expr: &Expr,
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And | BinOp::Or,
            right,
        } => {
            rank_filter_cannot_raise(left, input_columns, params)
                && rank_filter_cannot_raise(right, input_columns, params)
        }
        Expr::BinaryOp { left, right, .. } => {
            rank_filter_operand_cannot_raise(left, input_columns, params)
                && rank_filter_operand_cannot_raise(right, input_columns, params)
        }
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => rank_filter_cannot_raise(operand, input_columns, params),
        Expr::InList { expr, list, .. } => {
            rank_filter_operand_cannot_raise(expr, input_columns, params)
                && list
                    .iter()
                    .all(|item| rank_filter_operand_cannot_raise(item, input_columns, params))
        }
        Expr::IsNull { expr, .. } => rank_filter_operand_cannot_raise(expr, input_columns, params),
        Expr::Literal(contextdb_parser::Literal::Bool(_)) => true,
        _ => false,
    }
}

/// Below a rank filter that caps every branch, the general ranked route's
/// sort keeps only each partition's rows within the cap. Any other filter, or
/// a window the index route serves, keeps the full sort.
fn limit_ranked_sort(
    plan: &PhysicalPlan,
    predicate: &Expr,
    params: &HashMap<String, Value>,
    built: &mut PullNode,
) {
    let Some((_, columns, window, binding)) = ranked_prefix_shape(plan) else {
        return;
    };
    // The filter must be unable to raise on any row, because the rows the cap
    // gives up are never evaluated. Checked against the columns the filter
    // itself evaluates over.
    if !rank_filter_cannot_raise(predicate, &built.columns, params) {
        return;
    }
    let PullKind::Project(project) = &mut built.kind else {
        return;
    };
    let PullKind::Window(ranked) = &mut project.input.kind else {
        return;
    };
    if ranked.binding != binding || ranked.ceiling.is_some() {
        return;
    }
    let PullKind::Sort(sort) = &mut ranked.input.kind else {
        return;
    };
    if sort.rank_limit.is_some() || sort.prepared || sort.pending.is_some() || !sort.rows.is_empty()
    {
        return;
    }
    // A cap of 0 would hand the filter no row at all; the full sort keeps
    // every evaluation the filter makes today.
    let Some(limit) =
        rank_filter_limit(predicate, columns, binding, params).filter(|limit| *limit > 0)
    else {
        return;
    };
    sort.rank_limit = Some(SortRankLimit {
        limit: usize::try_from(limit).unwrap_or(usize::MAX),
        partition_len: window.partition_by.len(),
    });
}

/// Test the finite source scope independently of timestamp restrictions.
/// Unsupported predicates preserve the ordinary rank-then-filter plan.
fn ranked_prefix_inner_scope(
    predicate: &Expr,
    window: &contextdb_parser::RowNumberWindow,
    key: &[Value],
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let (Some(left), Some(right)) = (
                ranked_prefix_inner_scope(left, window, key, params)?,
                ranked_prefix_inner_scope(right, window, key, params)?,
            ) else {
                return Ok(None);
            };
            Ok(Some(left && right))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Expr::Column(column) = expr.as_ref() else {
                return Ok(None);
            };
            let Some(position) = window
                .partition_by
                .iter()
                .position(|partition| partition == column)
            else {
                return Ok(None);
            };
            let mut matched = false;
            for item in list {
                let Some(value) = ranked_prefix_constant(item, params)? else {
                    return Ok(None);
                };
                matched |= super::compare_values(&key[position], &value) == Some(Ordering::Equal);
            }
            Ok(Some(matched))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let (column, constant) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), constant) | (constant, Expr::Column(column)) => {
                    (column, constant)
                }
                _ => return Ok(None),
            };
            let Some(position) = window
                .partition_by
                .iter()
                .position(|partition| partition == column)
            else {
                return Ok(None);
            };
            let Some(value) = ranked_prefix_constant(constant, params)? else {
                return Ok(None);
            };
            Ok(Some(
                super::compare_values(&key[position], &value) == Some(Ordering::Equal),
            ))
        }
        Expr::IsNull { expr, .. } if matches!(expr.as_ref(), Expr::Column(_)) => Ok(Some(true)),
        _ => Ok(None),
    }
}

fn ranked_prefix_null_restriction(
    predicate: &Expr,
) -> Option<(&contextdb_parser::ColumnRef, bool)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => ranked_prefix_null_restriction(left).or_else(|| ranked_prefix_null_restriction(right)),
        Expr::IsNull { expr, negated } => match expr.as_ref() {
            Expr::Column(column) => Some((column, *negated)),
            _ => None,
        },
        _ => None,
    }
}

/// The inner partition scope was already intersected with this selected key.
/// Replace its finite IN selector with this one seek while retaining every
/// other predicate on the source.
fn ranked_prefix_group_filter(
    expr: &Expr,
    window: &contextdb_parser::RowNumberWindow,
    key: &[Value],
) -> Result<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => Ok(Expr::BinaryOp {
            left: Box::new(ranked_prefix_group_filter(left, window, key)?),
            op: BinOp::And,
            right: Box::new(ranked_prefix_group_filter(right, window, key)?),
        }),
        original @ Expr::InList {
            expr,
            negated: false,
            ..
        } => {
            if let Expr::Column(column) = expr.as_ref() {
                if let Some(position) = window
                    .partition_by
                    .iter()
                    .position(|partition| partition.column == column.column)
                {
                    return Ok(Expr::BinaryOp {
                        left: Box::new(Expr::Column(column.clone())),
                        op: BinOp::Eq,
                        right: Box::new(super::value_to_literal(key[position].clone())?),
                    });
                }
            }
            Ok(original.clone())
        }
        _ => Ok(expr.clone()),
    }
}

fn build_ranked_prefix(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    predicate: &Expr,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<Box<PullNode>>, BoundedExecutionError> {
    let Some((scan, columns, window, binding)) = ranked_prefix_shape(input) else {
        return Ok(None);
    };
    if !db.ordered_prefix_snapshot_is_current(
        snapshot,
        context.source_read_memory(BoundedWorkSource::IndexRange),
    )? {
        return Ok(None);
    }
    let PhysicalPlan::Scan { table, filter, .. } = scan else {
        return Ok(None);
    };
    let Some(inner_filter) = filter.as_ref() else {
        return Ok(None);
    };
    // Selector normalization can produce an empty answer without constructing
    // a scan. It still reads this table and must pass the scan's access gate.
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;
    if window.partition_by.iter().any(|partition| {
        !meta.columns.iter().any(|column| {
            column.name == partition.column
                && matches!(
                    column.column_type,
                    ColumnType::Uuid
                        | ColumnType::Integer
                        | ColumnType::Timestamp
                        | ColumnType::TxId
                        | ColumnType::Boolean
                        | ColumnType::Text
                )
        })
    }) {
        return Ok(None);
    }
    // The admitted scratch ceiling covers every syntactic branch, typed
    // literal clone and temporary plan. No store population affects it.
    let atoms = expr_nodes(predicate)?.saturating_add(expr_nodes(inner_filter)?);
    let scratch = checked_size_add(
        checked_size_mul(
            atoms,
            checked_size_add(
                std::mem::size_of::<PullNode>(),
                checked_size_mul(
                    window.partition_by.len().saturating_add(1),
                    std::mem::size_of::<Value>() * 4,
                    "ranked selectors",
                )?,
                "ranked selectors",
            )?,
            "ranked selectors",
        )?,
        checked_size_mul(
            checked_size_add(
                expr_capacity_bytes(predicate)?,
                physical_plan_capacity_bytes(input)?,
                "ranked selector payload",
            )?,
            atoms.saturating_add(1),
            "ranked selector payload",
        )?,
        "ranked selectors",
    )?;
    fn parameter_clone_bytes(
        expr: &Expr,
        params: &HashMap<String, Value>,
    ) -> std::result::Result<usize, BoundedExecutionError> {
        match expr {
            Expr::Parameter(name) => params.get(name).map_or(Ok(0), planned_value_clone_bytes),
            Expr::BinaryOp { left, right, .. } => checked_size_add(
                parameter_clone_bytes(left, params)?,
                parameter_clone_bytes(right, params)?,
                "selector parameters",
            ),
            Expr::InList { expr, list, .. } => {
                list.iter()
                    .try_fold(parameter_clone_bytes(expr, params)?, |bytes, item| {
                        checked_size_add(
                            bytes,
                            parameter_clone_bytes(item, params)?,
                            "selector parameters",
                        )
                    })
            }
            Expr::IsNull { expr, .. } | Expr::UnaryOp { operand: expr, .. } => {
                parameter_clone_bytes(expr, params)
            }
            _ => Ok(0),
        }
    }
    let scratch = checked_size_add(
        scratch,
        checked_size_mul(
            checked_size_add(
                parameter_clone_bytes(predicate, &params)?,
                parameter_clone_bytes(inner_filter, &params)?,
                "selector parameters",
            )?,
            atoms
                .checked_add(1)
                .ok_or_else(|| bounded_size_error("selector count"))?,
            "selector parameters",
        )?,
        "selector scratch",
    )?;
    context.reserve(BoundedWorkSource::IndexRange, scratch)?;
    let mut scratch_charge = ReservedBytes::new(context, scratch);
    let mut groups = Vec::new();
    if !ranked_prefix_branches(
        predicate,
        columns,
        window,
        binding,
        &meta,
        &params,
        &mut groups,
    )? {
        return Ok(None);
    }
    groups.sort_by(|left, right| {
        left.key
            .iter()
            .zip(&right.key)
            .map(|(left, right)| compare_sort_values(left, right, SortDirection::Asc))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    let keys: Vec<SortKey> = window
        .order_by
        .iter()
        .map(|key| SortKey {
            expr: Expr::Column(key.column.clone()),
            direction: key.direction,
        })
        .collect();
    let mut prepared = Vec::new();
    for group in groups {
        let Some(in_scope) = ranked_prefix_inner_scope(inner_filter, window, &group.key, &params)?
        else {
            return Ok(None);
        };
        if !in_scope {
            continue;
        }
        let mut group_filter = ranked_prefix_group_filter(inner_filter, window, &group.key)?;
        for (column, value) in window.partition_by.iter().zip(&group.key) {
            group_filter = Expr::BinaryOp {
                left: Box::new(group_filter),
                op: BinOp::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column(column.clone())),
                    op: BinOp::Eq,
                    right: Box::new(super::value_to_literal(value.clone())?),
                }),
            };
        }
        let mut group_scan = scan.clone();
        if let PhysicalPlan::Scan { filter, .. } = &mut group_scan {
            *filter = Some(group_filter);
        }
        let Some((_, filter, ordered)) = ordered_scan(db, &group_scan, &keys, &params) else {
            return Ok(None);
        };
        if ordered.reverse {
            return Ok(None);
        }
        let Some(declaration) = meta
            .indexes
            .iter()
            .find(|index| index.name == ordered.index)
        else {
            return Ok(None);
        };
        if !window
            .partition_by
            .iter()
            .zip(&declaration.columns)
            .all(|(partition, (column, _))| partition.column == *column)
        {
            return Ok(None);
        }
        // Every source restriction must be represented by the selected key
        // range. Reject row-body residuals before configuring prefix demand.
        let Some(pick) = ordered.predicate_pick.as_ref() else {
            return Ok(None);
        };
        let pick = db.coerce_pick_shape_to_column_type(table, pick)?;
        if pick.pushed_columns.len() < window.partition_by.len() {
            return Ok(None);
        }
        let null_restriction = ranked_prefix_null_restriction(inner_filter);
        if let Some((column, _)) = null_restriction {
            if declaration
                .columns
                .get(group.key.len())
                .is_none_or(|(indexed, _)| *indexed != column.column)
                || declaration.columns[group.key.len() + 1..]
                    .iter()
                    .any(|(_, direction)| *direction != contextdb_core::SortDirection::Asc)
            {
                return Ok(None);
            }
        }
        fn represented(
            expr: &Expr,
            pushed: &[String],
            null_restriction: Option<(&contextdb_parser::ColumnRef, bool)>,
        ) -> bool {
            match expr {
                Expr::BinaryOp {
                    left,
                    op: BinOp::And,
                    right,
                } => {
                    represented(left, pushed, null_restriction)
                        && represented(right, pushed, null_restriction)
                }
                Expr::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } => {
                    matches!(left.as_ref(),Expr::Column(column) if pushed.contains(&column.column))
                        || matches!(right.as_ref(),Expr::Column(column) if pushed.contains(&column.column))
                }
                Expr::InList {
                    expr,
                    negated: false,
                    ..
                } => {
                    matches!(expr.as_ref(),Expr::Column(column) if pushed.contains(&column.column))
                }
                Expr::IsNull { expr, negated } => {
                    matches!((expr.as_ref(),null_restriction), (Expr::Column(column),Some((restricted,expected))) if column.column == restricted.column && *negated == expected)
                }
                _ => false,
            }
        }
        if !filter.is_some_and(|filter| represented(filter, &pick.pushed_columns, null_restriction))
        {
            return Ok(None);
        }
        let mut source = Box::new(build_scan(
            db,
            table,
            filter,
            Arc::clone(&params),
            snapshot,
            Some(ordered),
            None,
            context,
        )?);
        let PullKind::Scan(state) = &mut source.kind else {
            return Ok(None);
        };
        if let Some(transaction) = &state.transaction {
            if !transaction
                .overlay
                .staged_rows
                .as_ref()
                .is_none_or(|rows| rows.has_index_for(&declaration.name, &meta))
            {
                return Ok(None);
            }
        }
        let ScanMode::Ordered { cursor, .. } = &mut state.mode else {
            return Ok(None);
        };
        if let Some((_, non_null)) = null_restriction {
            let mut prefix: IndexKey = group
                .key
                .iter()
                .zip(&declaration.columns)
                .map(|(value, (_, direction))| match direction {
                    contextdb_core::SortDirection::Asc => {
                        DirectedValue::Asc(TotalOrdAsc(value.clone()))
                    }
                    contextdb_core::SortDirection::Desc => {
                        DirectedValue::Desc(TotalOrdDesc(value.clone()))
                    }
                })
                .collect();
            let group_prefix = prefix.clone();
            let null_direction = declaration.columns[group.key.len()].1;
            prefix.push(match null_direction {
                contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(Value::Null)),
                contextdb_core::SortDirection::Desc => {
                    DirectedValue::Desc(TotalOrdDesc(Value::Null))
                }
            });
            let endpoint_len = prefix.len();
            let (start, end) = if non_null {
                match null_direction {
                    contextdb_core::SortDirection::Desc => {
                        // Skip the entire NULL timestamp region, including all
                        // requested ascending tie components at their NULL-high edge.
                        prefix.resize_with(declaration.columns.len(), || {
                            DirectedValue::Asc(TotalOrdAsc(Value::Null))
                        });
                        (Bound::Excluded(prefix), Bound::Included(group_prefix))
                    }
                    contextdb_core::SortDirection::Asc => {
                        (Bound::Included(group_prefix), Bound::Excluded(prefix))
                    }
                }
            } else {
                let mut high = prefix.clone();
                high.resize_with(declaration.columns.len(), || {
                    DirectedValue::Asc(TotalOrdAsc(Value::Null))
                });
                (Bound::Included(prefix), Bound::Included(high))
            };
            *cursor = BoundedOrderedRowCursor::for_index(declaration.name.clone(), false)
                .seeking_run_with_prefix_len(start, end, endpoint_len);
        }
        let (start, end, prefix_len) = cursor.source_run_bounds();
        let bound_has_group = |bound: &Bound<IndexKey>| match bound {
            Bound::Included(key) | Bound::Excluded(key) => {
                key.len() >= group.key.len()
                    && key.iter().zip(&group.key).all(|(component, value)| {
                        let component = match component {
                            DirectedValue::Asc(value) => &value.0,
                            DirectedValue::Desc(value) => &value.0,
                        };
                        super::compare_values(component, value) == Some(Ordering::Equal)
                    })
            }
            Bound::Unbounded => false,
        };
        if prefix_len < group.key.len()
            || !bound_has_group(&start.as_ref().map(|key| key.to_vec()))
            || !bound_has_group(&end.as_ref().map(|key| key.to_vec()))
        {
            return Ok(None);
        }
        let (start, _, _) = cursor.source_run_bounds();
        if matches!(start, Bound::Excluded(key) if key.len() < declaration.columns.len() && matches!(key.last(), Some(DirectedValue::Desc(TotalOrdDesc(Value::Null)))) && declaration.columns[key.len()..].iter().any(|(_,direction)| *direction != contextdb_core::SortDirection::Asc))
        {
            return Ok(None);
        }
        cursor.select_identities_only();
        cursor.source_set_prefix_quota(Some(if state.transaction.is_some() {
            1
        } else {
            group.ceiling
        }));
        let positions = window
            .partition_by
            .iter()
            .map(
                |column| match resolve_query_result_column(&source.columns, column) {
                    QueryResultColumn::Found(position) => Ok(position),
                    resolution => Err(query_result_column_error(column, &resolution)),
                },
            )
            .collect::<Result<Vec<_>>>()?;
        prepared.push((source, group, positions));
    }
    // All eligibility checks finish before publishing any observer binding.
    let descriptor = context.new_group_descriptor()?;
    let mut nodes = Vec::new();
    let mut trace = QueryTrace::scan();
    for (source, group, positions) in prepared {
        let group_binding = context.bind_group(
            descriptor,
            &group.key,
            "<redacted>",
            crate::database::GroupRoute::Index,
        )?;
        let partition_bytes = values_retained_bytes(&group.key)?;
        let ranked = Box::new(PullNode {
            columns: source.columns.clone(),
            trace: source.trace.clone(),
            kind: PullKind::Window(WindowState {
                prepaid_construction: 0,
                descriptor,
                group_binding: Some(group_binding),
                ceiling: Some(group.ceiling),
                input: source,
                binding,
                partition_positions: positions,
                partition: Some(group.key),
                partition_bytes,
                rank: 0,
                pending: None,
            }),
        });
        let projected = PullNode {
            columns: project_output_columns(columns),
            trace: ranked.trace.clone(),
            kind: PullKind::Project(ProjectState {
                input: ranked,
                columns: columns.to_vec(),
                params: Arc::clone(&params),
                aggregate_done: false,
                aggregate: false,
                pending: None,
                aggregates: None,
                aggregate_state_bytes: 0,
            }),
        };
        trace = projected.trace.clone();
        nodes.push(projected);
    }
    trace.sort_elided = true;
    let mut node = Box::new(PullNode {
        columns: project_output_columns(columns),
        trace,
        kind: PullKind::Union(UnionState {
            prepaid_construction: 0,
            inputs: nodes,
            input: 0,
            all: true,
            seen: HashSet::new(),
            seen_bytes: 0,
            pending: None,
        }),
    });
    let retained = pull_continuation_bytes(&node)?;
    if retained > scratch_charge.bytes {
        return Err(Error::Other(
            "ranked prefix retained bytes exceed admitted construction".to_string(),
        )
        .into());
    }
    if let PullKind::Union(union) = &mut node.kind {
        union.prepaid_construction = retained;
    }
    scratch_charge.bytes -= retained;
    scratch_charge.release()?;
    Ok(Some(node))
}

/// Build recursive plan nodes behind their eventual owning pointer.
///
/// `PullNode` carries every source state inline and is deliberately large.
/// Returning it by value from each recursive plan layer makes an unoptimised
/// build retain several full-node result slots per layer, so a valid deep CTE
/// query can exhaust an ordinary thread stack before it reads a row. Keeping
/// the recursive result boxed gives each layer a pointer-sized return slot;
/// the finished tree has the same ownership shape its wrapper states already
/// use.
fn build_kernel_with_ctes<'plan>(
    db: &dyn ReadExecutionTarget,
    plan: &'plan PhysicalPlan,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &HashMap<&'plan str, &'plan PhysicalPlan>,
    context: &Arc<RequestContext>,
) -> std::result::Result<Box<PullNode>, BoundedExecutionError> {
    match plan {
        PhysicalPlan::WindowRank {
            input,
            window,
            binding,
        } => {
            let mut built =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            let descriptor = context.new_group_descriptor()?;
            let mut source_group_bytes = 0;
            let mut key_construction_charge = ReservedBytes::new(context, 0);
            let mut keys = Vec::new();
            let key_count = checked_size_add(
                window.partition_by.len(),
                window.order_by.len(),
                "window keys",
            )?;
            let mut references = window
                .partition_by
                .iter()
                .chain(window.order_by.iter().map(|key| &key.column));
            let key_bytes = references.try_fold(
                checked_size_mul(key_count, std::mem::size_of::<SortKey>(), "window keys")?,
                |bytes, column| {
                    checked_size_add(
                        bytes,
                        checked_size_add(
                            column.column.len(),
                            column.table.as_ref().map_or(0, String::len),
                            "window column",
                        )?,
                        "window keys",
                    )
                },
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, key_bytes)?;
            key_construction_charge.bytes = key_bytes;
            keys.try_reserve_exact(key_count)
                .map_err(|_| Error::Other("bounded window key allocation failed".to_string()))?;
            for column in &window.partition_by {
                keys.push(SortKey {
                    expr: Expr::Column(column.clone()),
                    direction: SortDirection::Asc,
                });
            }
            for key in &window.order_by {
                keys.push(SortKey {
                    expr: Expr::Column(key.column.clone()),
                    direction: key.direction,
                });
            }
            let position_bytes = checked_size_mul(
                window.partition_by.len(),
                std::mem::size_of::<usize>(),
                "window positions",
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, position_bytes)?;
            let mut position_construction_charge = ReservedBytes::new(context, position_bytes);
            let mut partition_positions = Vec::new();
            partition_positions
                .try_reserve_exact(window.partition_by.len())
                .map_err(|_| {
                    Error::Other("bounded window binding allocation failed".to_string())
                })?;
            // Resolve all references before execution, including an empty source.
            for (key_number, key) in keys.iter().enumerate() {
                if let Expr::Column(column) = &key.expr {
                    match resolve_query_result_column(&built.columns, column) {
                        QueryResultColumn::Found(position) => {
                            if key_number < window.partition_by.len() {
                                partition_positions.push(position);
                            }
                        }
                        resolution => {
                            return Err(query_result_column_error(column, &resolution).into());
                        }
                    }
                }
            }
            if let PullKind::Scan(scan) = &mut built.kind
                && matches!(
                    scan.mode,
                    ScanMode::Physical { .. } | ScanMode::Ordered { .. } | ScanMode::Index { .. }
                )
                && window.partition_by.iter().all(|partition| {
                    partition.column == "row_id"
                        || scan.meta.columns.iter().any(|column| {
                            column.name == partition.column
                                && matches!(
                                    column.column_type,
                                    ColumnType::Uuid
                                        | ColumnType::Integer
                                        | ColumnType::Timestamp
                                        | ColumnType::TxId
                                        | ColumnType::Boolean
                                        | ColumnType::Text
                                )
                        })
                })
            {
                let source_position_bytes = checked_size_mul(
                    window.partition_by.len(),
                    std::mem::size_of::<PhysicalPartitionColumn>(),
                    "source group column bindings",
                )?;
                context.reserve(BoundedWorkSource::TableScan, source_position_bytes)?;
                let mut source_charge = ReservedBytes::new(context, source_position_bytes);
                let mut positions = Vec::new();
                positions
                    .try_reserve_exact(partition_positions.len())
                    .map_err(|_| {
                        Error::Other("source group positions allocation failed".to_string())
                    })?;
                for partition in &window.partition_by {
                    positions.push(if partition.column == "row_id" {
                        PhysicalPartitionColumn::RowId
                    } else {
                        PhysicalPartitionColumn::Schema(
                            scan.schema_columns
                                .iter()
                                .position(|name| name == &partition.column)
                                .ok_or_else(|| {
                                    Error::PlanError(
                                        "physical group column binding is missing".to_string(),
                                    )
                                })?,
                        )
                    });
                }
                source_group_bytes = checked_size_mul(
                    positions.capacity(),
                    std::mem::size_of::<PhysicalPartitionColumn>(),
                    "source group positions",
                )?;
                if source_group_bytes > source_position_bytes {
                    context.reserve(
                        BoundedWorkSource::TableScan,
                        source_group_bytes - source_position_bytes,
                    )?;
                } else if source_position_bytes > source_group_bytes {
                    context.release(source_position_bytes - source_group_bytes)?;
                }
                source_charge.bytes = 0;
                scan.source_group = Some(SourceInputGroup {
                    descriptor,
                    positions,
                });
            }
            let actual = sort_keys_capacity_bytes(&keys)?;
            if actual > key_bytes {
                context.reserve(BoundedWorkSource::UnindexedSort, actual - key_bytes)?;
            } else if key_bytes > actual {
                context.release(key_bytes - actual)?;
            }
            key_construction_charge.bytes = actual;
            // Each guard precedes its payload so later construction failures
            // destroy the funded allocation before returning its credit.
            let mut column_construction_charge = ReservedBytes::new(context, 0);
            let (columns, column_bytes) =
                clone_strings_with_reservation(&built.columns, context, "window schema")?;
            column_construction_charge.bytes = column_bytes;
            let mut sort_column_construction_charge = ReservedBytes::new(context, 0);
            let (sort_columns, sort_column_bytes) =
                clone_strings_with_reservation(&columns, context, "window sort schema")?;
            sort_column_construction_charge.bytes = sort_column_bytes;
            let trace = built.trace.clone();
            let node_bytes =
                checked_size_mul(2, std::mem::size_of::<PullNode>(), "window operators")?;
            context.reserve(BoundedWorkSource::UnindexedSort, node_bytes)?;
            let mut node_construction_charge = ReservedBytes::new(context, node_bytes);
            let sorted = Box::new(PullNode {
                columns: sort_columns,
                trace: trace.clone(),
                kind: PullKind::Sort(SortState {
                    input: built,
                    keys,
                    params,
                    rows: Vec::new(),
                    prepared: false,
                    pending: None,
                    buffered_bytes: 0,
                    container_bytes: 0,
                    rank_limit: None,
                }),
            });
            let node = Box::new(PullNode {
                columns,
                trace,
                kind: PullKind::Window(WindowState {
                    input: sorted,
                    prepaid_construction: checked_size_add(
                        checked_size_add(
                            checked_size_add(actual, column_bytes, "window construction")?,
                            source_group_bytes,
                            "window source observation",
                        )?,
                        checked_size_add(
                            checked_size_add(
                                sort_column_bytes,
                                position_bytes,
                                "window construction",
                            )?,
                            node_bytes,
                            "window construction",
                        )?,
                        "window construction",
                    )?,
                    descriptor,
                    group_binding: None,
                    ceiling: None,
                    binding: *binding,
                    partition_positions,
                    partition: None,
                    partition_bytes: 0,
                    rank: 0,
                    pending: None,
                }),
            });
            key_construction_charge.bytes = 0;
            column_construction_charge.bytes = 0;
            sort_column_construction_charge.bytes = 0;
            position_construction_charge.bytes = 0;
            node_construction_charge.bytes = 0;
            Ok(node)
        }
        PhysicalPlan::Scan { table, filter, .. } => build_scan(
            db,
            table,
            filter.as_ref(),
            params,
            snapshot,
            None,
            None,
            context,
        )
        .map(Box::new),
        PhysicalPlan::GroupedCount {
            input,
            group_by,
            columns,
        } => grouped_count::build_grouped_count(
            db, input, group_by, columns, params, snapshot, context,
        ),
        // Read-classified statements about the store itself. They walk no
        // source, so the whole answer arrives at once -- and every row of it
        // is charged on the way in, so a store with a great many vector
        // indexes is bounded by the same ceilings as a store with a great
        // many rows.
        PhysicalPlan::ShowMemoryLimit
        | PhysicalPlan::ShowDiskLimit
        | PhysicalPlan::ShowContentSize
        | PhysicalPlan::ShowMaintenancePollInterval
        | PhysicalPlan::ShowVectorServingCache
        | PhysicalPlan::ShowSyncConflictPolicy
        | PhysicalPlan::ShowVectorIndexes
        | PhysicalPlan::ShowTenantTablePolicy { .. }
        | PhysicalPlan::ShowSyncBindings
        | PhysicalPlan::ShowDeliveryOutcomes(_)
        | PhysicalPlan::ShowVectorPartitions { .. } => {
            build_store_state(db, plan, context).map(Box::new)
        }
        PhysicalPlan::Project { input, columns } => {
            let mut maintained_count = None;
            if let Some(count) = build_authenticated_index_count(
                db,
                input,
                columns,
                &params,
                context,
                &mut maintained_count,
            )? {
                return Ok(count);
            }
            let input =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            let mut trace = input.trace.clone();
            // The count this aggregate is about to compute exactly is the one
            // a declaration would have served in O(1); say so on the trace the
            // caller reads.
            trace.maintained_count = maintained_count.map(Box::new);
            let aggregate = projection_aggregates(columns);
            if aggregate
                && columns
                    .iter()
                    .any(|column| column_aggregate(column).is_none())
            {
                return Err(mixed_aggregate_error().into());
            }
            Ok(Box::new(PullNode {
                columns: project_output_columns(columns),
                trace,
                kind: PullKind::Project(ProjectState {
                    input,
                    columns: columns.clone(),
                    params,
                    aggregate_done: false,
                    aggregate,
                    pending: None,
                    aggregates: None,
                    aggregate_state_bytes: 0,
                }),
            }))
        }
        PhysicalPlan::Limit { input, count } => {
            let input = build_kernel_with_ctes(db, input, params, snapshot, ctes, context)?;
            Ok(Box::new(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Limit(LimitState {
                    input,
                    count: *count,
                    emitted: 0,
                }),
            }))
        }
        PhysicalPlan::Sort { input, keys } => {
            if let PhysicalPlan::Filter {
                input: selected,
                predicate,
            } = input.as_ref()
            {
                if let Some((_, columns, window, binding)) = ranked_prefix_shape(selected) {
                    let matches = keys.len() == window.partition_by.len() + 1
                        && keys.iter().enumerate().all(|(position, key)| {
                            if key.direction != SortDirection::Asc {
                                return false;
                            }
                            let Expr::Column(reference) = &key.expr else {
                                return false;
                            };
                            match ranked_prefix_project_expr(reference, columns) {
                                Some(Expr::Column(column))
                                    if position < window.partition_by.len() =>
                                {
                                    *column == window.partition_by[position]
                                }
                                Some(Expr::WindowRank(found))
                                    if position == window.partition_by.len() =>
                                {
                                    *found == binding
                                }
                                _ => false,
                            }
                        });
                    if matches {
                        if let Some(prefix) = build_ranked_prefix(
                            db,
                            selected,
                            predicate,
                            Arc::clone(&params),
                            snapshot,
                            context,
                        )? {
                            return Ok(prefix);
                        }
                    }
                }
            }
            // An ordered index run is the committed rows in order and nothing
            // else, so with a transaction open the rows it staged have to be
            // put where they belong. The scan does that as it walks, which is
            // why the run below is taken with a transaction open too; the
            // shortcut after it has no such fold, so it still stands down.
            // Asked once, so the two places below cannot disagree about
            // whether a transaction is open.
            let inside_transaction = context.transaction().is_some();
            if let Some((table, filter, ordered)) = ordered_scan(db, input, keys, &params) {
                return build_scan(
                    db,
                    table,
                    filter,
                    params,
                    snapshot,
                    Some(ordered),
                    None,
                    context,
                )
                .map(Box::new);
            }
            let built =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            // The scan may already be reading one run of an index, and that
            // run may already be in the order asked for -- in which case there
            // is nothing to sort. Whether it is, is the executor's rule again.
            if !inside_transaction
                && built.trace.physical_plan == "IndexScan"
                && let Some(index) = built.trace.index_used.as_deref()
                && crate::executor::sort_keys_match_index_prefix(db, input, index, keys)
            {
                let mut built = built;
                built.trace.sort_elided = true;
                return Ok(built);
            }
            let input = built;
            Ok(Box::new(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Sort(SortState {
                    input,
                    keys: keys.clone(),
                    params,
                    rows: Vec::new(),
                    prepared: false,
                    pending: None,
                    buffered_bytes: 0,
                    container_bytes: 0,
                    rank_limit: None,
                }),
            }))
        }
        PhysicalPlan::Filter { input, predicate } => {
            if let Some(prefix) =
                build_ranked_prefix(db, input, predicate, Arc::clone(&params), snapshot, context)?
            {
                return Ok(prefix);
            }
            let mut built =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            limit_ranked_sort(input, predicate, &params, &mut built);
            Ok(Box::new(PullNode {
                columns: built.columns.clone(),
                trace: built.trace.clone(),
                kind: PullKind::Filter(FilterState {
                    input: built,
                    predicate: predicate.clone(),
                    params,
                    pending: None,
                }),
            }))
        }
        PhysicalPlan::Distinct { input } | PhysicalPlan::MaterializeCte { input, .. } => {
            let input = build_kernel_with_ctes(db, input, params, snapshot, ctes, context)?;
            if matches!(plan, PhysicalPlan::MaterializeCte { .. }) {
                return Ok(input);
            }
            Ok(Box::new(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Distinct(DistinctState {
                    input,
                    seen: HashSet::new(),
                    seen_bytes: 0,
                    pending: None,
                }),
            }))
        }
        PhysicalPlan::Union { inputs, all } => {
            let mut built = Vec::with_capacity(inputs.len());
            for input in inputs {
                built.push(*build_kernel_with_ctes(
                    db,
                    input,
                    Arc::clone(&params),
                    snapshot,
                    ctes,
                    context,
                )?);
            }
            let columns = built
                .first()
                .map_or_else(Vec::new, |input| input.columns.clone());
            let trace = built
                .first()
                .map_or_else(QueryTrace::scan, |input| input.trace.clone());
            Ok(Box::new(PullNode {
                columns,
                trace,
                kind: PullKind::Union(UnionState {
                    prepaid_construction: 0,
                    inputs: built,
                    input: 0,
                    all: *all,
                    seen: HashSet::new(),
                    seen_bytes: 0,
                    pending: None,
                }),
            }))
        }
        PhysicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            left_alias,
            right_alias,
        } => {
            let right_prefix = right_alias
                .clone()
                .unwrap_or_else(|| right_table_name(right));
            let left =
                build_kernel_with_ctes(db, left, Arc::clone(&params), snapshot, ctes, context)?;
            let right =
                build_kernel_with_ctes(db, right, Arc::clone(&params), snapshot, ctes, context)?;
            let right_column_count = right.columns.len();
            let right_duplicate_names = duplicate_column_names(&left.columns, &right.columns);
            let mut condition_columns = left.columns.clone();
            condition_columns.extend(right.columns.iter().map(|column| {
                if right_duplicate_names.contains(column) {
                    format!("{right_prefix}.{column}")
                } else {
                    column.clone()
                }
            }));
            let output_columns = qualify_join_columns(
                &condition_columns,
                &left.columns,
                &right.columns,
                left_alias,
                &right_prefix,
            );
            let trace = left.trace.clone();
            Ok(Box::new(PullNode {
                columns: output_columns,
                trace,
                kind: PullKind::Join(JoinState {
                    left,
                    right,
                    condition: condition.clone(),
                    condition_columns,
                    right_column_count,
                    join_type: *join_type,
                    params,
                    right_rows: Vec::new(),
                    right_pending: None,
                    right_prepared: false,
                    left_row: None,
                    right_position: 0,
                    matched: false,
                }),
            }))
        }
        PhysicalPlan::GraphBfs {
            start_alias,
            start_expr,
            start_candidates,
            filter_ctes,
            steps,
            filter,
            ..
        } => {
            let Some(step) = steps.first() else {
                return Err(
                    Error::Other("bounded graph plan has no traversal step".to_string()).into(),
                );
            };
            let resolved_filter = filter
                .as_ref()
                .map(|filter| {
                    bounded_resolve_in_subqueries(
                        db,
                        filter,
                        &params,
                        snapshot,
                        filter_ctes,
                        context,
                    )
                })
                .transpose()?;
            let mut predicates_pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]> =
                smallvec::SmallVec::new();
            let mut candidate_root = None;
            let mut unpinned_start = false;
            let starts = match resolve_uuid(start_expr, &params) {
                Ok(start) => {
                    predicates_pushed.push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                    VecDeque::from([start])
                }
                Err(Error::PlanError(_))
                    if matches!(
                        start_expr,
                        Expr::Column(contextdb_parser::ast::ColumnRef { table: None, .. })
                    ) =>
                {
                    if let Some(candidates) = start_candidates.as_deref() {
                        predicates_pushed
                            .push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                        candidate_root = Some(build_kernel_with_ctes(
                            db,
                            candidates,
                            Arc::clone(&params),
                            snapshot,
                            ctes,
                            context,
                        )?);
                        VecDeque::new()
                    } else if let Some(filter) = resolved_filter.as_ref() {
                        match bounded_graph_static_starts(filter, &params, start_alias)? {
                            Some(starts) => {
                                // A traversal that PINS its start by id is
                                // asking about a node it names, so a reader
                                // who may not see that node is refused rather
                                // than handed an empty answer -- being told
                                // "no edges" about a node that has them says
                                // the store does not hold what it holds. The
                                // executor draws this line for the same
                                // statement; the kernel asks it the same way.
                                if let Some(database) = db.as_database_for_subquery_plan() {
                                    let mut access_memory = ReservedBytes::new(context, 0);
                                    database.bounded_assert_graph_anchor_nodes_readable(
                                        context.transaction(),
                                        &starts,
                                        snapshot,
                                        |bytes| access_memory.charge_access_row(bytes),
                                        context
                                            .source_read_memory(BoundedWorkSource::GraphTraversal),
                                    )?;
                                }
                                predicates_pushed
                                    .push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                                starts.into()
                            }
                            None => {
                                // A start named by something other than its id
                                // is still a PINNED start -- the executor
                                // resolves it against the store and then
                                // probes. Calling it unpinned walks every edge
                                // in the store to answer a two-entry probe, and
                                // tells the operator it did. Resolved here by
                                // the executor's own resolver, in the caller's
                                // own transaction.
                                let resolution = bounded_resolve_graph_start_nodes(
                                    db,
                                    filter,
                                    &params,
                                    snapshot,
                                    start_alias,
                                    context,
                                )?;
                                if resolution.as_ref().is_some_and(|r| r.pinned) {
                                    // Every row the resolution read was charged
                                    // as it was read, inside the bounded source
                                    // that read it, so `rows_examined` already
                                    // holds them. Charging again here -- which
                                    // is what this did while the resolution was
                                    // eager -- pays for work after it is done,
                                    // which is not a ceiling.
                                    let resolution = resolution.expect("pinned resolution");
                                    predicates_pushed.extend(resolution.predicates_pushed);
                                    VecDeque::from(resolution.ids)
                                } else {
                                    unpinned_start = true;
                                    VecDeque::new()
                                }
                            }
                        }
                    } else {
                        unpinned_start = true;
                        VecDeque::new()
                    }
                }
                Err(error) => return Err(error.into()),
            };
            let single_step = steps.len() == 1 && step.min_depth == 1 && step.max_depth == 1;
            // Both ends of a single hop can be pinned, and when they are, both
            // reached the source: the start picks the adjacency list and the
            // target decides which of its entries is even looked at. Reporting
            // only the start tells an operator the read filtered a whole
            // neighbourhood in the query when it filtered it at the source.
            // Resolved from the same residual the executor resolves.
            let start_id_predicate = format!("{start_alias}.id");
            let target_residual = if single_step {
                resolved_filter
                    .as_ref()
                    .map(|filter| {
                        super::resolve_graph_target_id_residual(filter, &params, &step.target_alias)
                    })
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            if target_residual.is_some()
                && predicates_pushed
                    .iter()
                    .any(|predicate| predicate.as_ref() == start_id_predicate)
            {
                predicates_pushed
                    .push(std::borrow::Cow::Owned(format!("{}.id", step.target_alias)));
            }
            // A hop pinned only where it ENDS names one node, and that node's
            // adjacency is the only list worth opening -- reached by reversing
            // the hop. Scanning every edge of the type to find the ones that
            // arrive there reads the whole graph to answer a question about
            // one node, and reports itself as a full edge scan while doing it.
            let mut starts = starts;
            let backwards_target = match target_residual {
                Some(target) if unpinned_start => {
                    predicates_pushed
                        .push(std::borrow::Cow::Owned(format!("{}.id", step.target_alias)));
                    unpinned_start = false;
                    starts = VecDeque::from([target]);
                    Some(super::reverse_graph_probe_direction(step.direction))
                }
                _ => None,
            };
            let edge_source = if unpinned_start {
                Some(db.bounded_graph_edge_cursor(
                    step.direction,
                    snapshot,
                    &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                    &mut |bytes| context.release(bytes),
                )?)
            } else {
                None
            };
            let trace = graph_query_trace(
                match backwards_target {
                    Some(direction) => super::GraphTraceShape::AdjacencyProbe {
                        index: super::graph_adjacency_index_label(direction),
                    },
                    None => graph_trace_shape(single_step, unpinned_start, Some(step.direction)),
                },
                predicates_pushed,
            );
            let mut columns = Vec::with_capacity(
                steps
                    .len()
                    .checked_add(3)
                    .ok_or_else(|| bounded_size_error("graph output columns"))?,
            );
            columns.push(format!("{start_alias}.id"));
            columns.extend(steps.iter().map(|step| format!("{}.id", step.target_alias)));
            columns.push("id".to_string());
            columns.push("depth".to_string());
            Ok(Box::new(PullNode {
                columns,
                trace,
                kind: PullKind::Graph(GraphState {
                    start_alias: start_alias.clone(),
                    steps: steps.clone(),
                    filter: resolved_filter,
                    params,
                    snapshot,
                    starts,
                    start_candidates: candidate_root,
                    edge_source,
                    visited_cap: db.bounded_legacy_visited_cap(context.declares_no_ceilings()),
                    single_step_unpinned: single_step && unpinned_start,
                    probes_backwards_from_target: backwards_target.is_some(),
                    single_step_probe: single_step,
                    probe_loops: Vec::new(),
                    probe_seen_container_bytes: 0,
                    seen_starts: Vec::new(),
                    seen_start_container_bytes: 0,
                    seen_paths: Vec::new(),
                    seen_path_container_bytes: 0,
                    frontier: Vec::new(),
                    frontier_container_bytes: 0,
                    row_memory: AmortizedRowMemory::default(),
                    active: None,
                    start_walks: None,
                    start_walk_container_bytes: 0,
                    chosen: None,
                    active_rank: None,
                }),
            }))
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            search_mode,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            search_mode,
            ..
        } => build_vector_kernel(
            db,
            plan,
            table,
            column,
            query_expr,
            *k,
            candidates.as_deref(),
            sort_key.as_deref(),
            *search_mode,
            params,
            snapshot,
            ctes,
            context,
        )
        .map(Box::new),
        PhysicalPlan::Pipeline(plans) => {
            let mut visible_ctes = ctes.clone();
            let mut last = None;
            for pipeline_plan in plans {
                if let PhysicalPlan::MaterializeCte { name, input } = pipeline_plan {
                    visible_ctes.insert(name.as_str(), input.as_ref());
                } else {
                    last = Some(pipeline_plan);
                }
            }
            let Some(last) = last else {
                return Ok(Box::new(empty_node()));
            };
            build_kernel_with_ctes(db, last, params, snapshot, &visible_ctes, context)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
        } => build_direct_index_scan(db, table, index, range, params, snapshot, context)
            .map(Box::new),
        PhysicalPlan::CteRef { name } => {
            let input = ctes.get(name.as_str()).copied().ok_or_else(|| {
                Error::Other(format!(
                    "bounded CTE reference `{name}` has no materialized input"
                ))
            })?;
            build_kernel_with_ctes(db, input, params, snapshot, ctes, context)
        }
        _ => Err(
            Error::PlanError("bounded execution accepts read-only SELECT plans".to_string()).into(),
        ),
    }
}

/// Resolve value-based staged deletes by their complete indexed keys. This
/// costs the staged predicate set, not every candidate row in the search.
fn resolve_candidate_delete_membership(
    db: &dyn ReadExecutionTarget,
    table: &str,
    snapshot: SnapshotId,
    tx: &mut ScanTransactionOverlay,
    context: &Arc<RequestContext>,
) -> std::result::Result<bool, BoundedExecutionError> {
    let store = db.relational_membership_store();
    let indexes = store.indexes.read();
    let Some(indexes) = indexes.get(table) else {
        return Ok(false);
    };
    // Establish complete support before allocating or reading any postings.
    let storage_for = |predicate: &contextdb_tx::RelationalDeletePredicate| {
        indexes.values().find(|storage| {
            storage.columns.len() == predicate.predicates.len()
                && !storage.columns.is_empty()
                && storage
                    .columns
                    .iter()
                    .all(|(column, _)| predicate.predicates.iter().any(|(name, _)| name == column))
        })
    };
    if tx
        .overlay
        .delete_predicates
        .iter()
        .any(|predicate| storage_for(predicate).is_none())
    {
        return Ok(false);
    }
    let mut deleted = Vec::new();
    let mut held = 0usize;
    for predicate in &tx.overlay.delete_predicates {
        let storage = storage_for(predicate).expect("complete delete key checked");
        let key_bytes = storage
            .columns
            .len()
            .saturating_mul(std::mem::size_of::<DirectedValue>())
            .saturating_add(
                predicate
                    .predicates
                    .iter()
                    .map(|(_, value)| value.estimated_bytes())
                    .sum::<usize>(),
            );
        context.reserve(BoundedWorkSource::IndexRange, key_bytes)?;
        let key: IndexKey = storage
            .columns
            .iter()
            .map(|(column, direction)| {
                let value = &predicate
                    .predicates
                    .iter()
                    .find(|(name, _)| name == column)
                    .expect("key component checked")
                    .1;
                match direction {
                    contextdb_core::SortDirection::Asc => {
                        DirectedValue::Asc(TotalOrdAsc(value.clone()))
                    }
                    contextdb_core::SortDirection::Desc => {
                        DirectedValue::Desc(TotalOrdDesc(value.clone()))
                    }
                }
            })
            .collect();
        let mut collect = |entry: &contextdb_relational::IndexEntry| -> std::result::Result<(), BoundedExecutionError> {
            context.charge(BoundedWorkSource::IndexRange, BoundedSourceTouch::IndexEntry)?;
            store.bump_index_entries_touched(1);
            if entry.visible_at(snapshot) {
                held += reserve_vector_slot(context, BoundedWorkSource::IndexRange, &mut deleted, "staged delete identities")?;
                deleted.push(entry.row_id);
            }
            Ok(())
        };
        let result = (|| {
            if storage.exact_only() {
                for entry in storage.exact_postings(&key).into_iter().flatten() {
                    collect(entry)?;
                }
            } else {
                for (_, entries) in storage.read_range(
                    snapshot,
                    Bound::Included(key.as_slice()),
                    Bound::Included(key.as_slice()),
                ) {
                    for entry in entries {
                        collect(entry)?;
                    }
                }
            }
            Ok::<(), BoundedExecutionError>(())
        })();
        drop(key);
        context.release(key_bytes)?;
        result?;
    }
    deleted.sort_unstable();
    deleted.dedup();
    tx.predicate_deleted = deleted;
    tx.overlay.bytes += held;
    tx.committed_predicates_resolved = true;
    Ok(true)
}

// Proof-only: why one vector candidate source did or did not become
// identity-only. Production never reads it.
#[cfg(feature = "test-seams")]
std::thread_local! {
    static VECTOR_CANDIDATE_IDENTITY_DECISION: std::cell::RefCell<Vec<&'static str>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Whether every term of a predicate can be settled by one row's own values.
/// A subquery reads another table, so it cannot.
fn predicate_is_answerable_from_one_row(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } => false,
        Expr::BinaryOp { left, right, .. } => {
            predicate_is_answerable_from_one_row(left)
                && predicate_is_answerable_from_one_row(right)
        }
        Expr::UnaryOp { operand, .. } => predicate_is_answerable_from_one_row(operand),
        Expr::IsNull { expr, .. } => predicate_is_answerable_from_one_row(expr),
        Expr::Like { expr, .. } => predicate_is_answerable_from_one_row(expr),
        Expr::InList { expr, list, .. } => {
            predicate_is_answerable_from_one_row(expr)
                && list.iter().all(predicate_is_answerable_from_one_row)
        }
        Expr::FunctionCall { args, .. } => args.iter().all(predicate_is_answerable_from_one_row),
        _ => true,
    }
}

fn note_vector_candidate_identity(
    reason: &'static str,
) -> std::result::Result<(), BoundedExecutionError> {
    #[cfg(feature = "test-seams")]
    VECTOR_CANDIDATE_IDENTITY_DECISION.with(|slot| slot.borrow_mut().push(reason));
    let _ = reason;
    Ok(())
}

/// Take and clear what this thread's vector candidate sources decided.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_vector_candidate_identity_decisions_for_test() -> Vec<&'static str> {
    VECTOR_CANDIDATE_IDENTITY_DECISION.with(|slot| std::mem::take(&mut *slot.borrow_mut()))
}

/// Index membership can supply identities directly when every conjunct is
/// enforced by the selected key. Residual predicates and authorization keep
/// their normal charged row reads.
fn select_vector_candidate_identity_source(
    db: &dyn ReadExecutionTarget,
    root: &mut PullNode,
    access_filter: bool,
    context: &Arc<RequestContext>,
) -> std::result::Result<(), BoundedExecutionError> {
    let PullKind::Scan(state) = &mut root.kind else {
        return note_vector_candidate_identity("candidate source is not a scan");
    };
    state.vector_candidate_source = true;
    if state.ordered_merge.is_some() {
        return note_vector_candidate_identity("candidate source merges ordered runs");
    }
    // A read gate the candidate index already answers is decided by the index
    // range itself: the reader's Context and scope-label restrictions were
    // folded into this scan's filter before it was planned, and `covered`
    // below refuses this route unless every term of that filter is served from
    // the index key. What an index cannot answer is a grant lookup, which
    // reads another table once per row, so an ACL-gated table keeps its row
    // route. Every identity this route delivers still passes the real row gate
    // where it is published; selecting a tuple never grants access.
    if access_filter
        && db
            .table_meta(&state.table)
            .is_some_and(|meta| meta.columns.iter().any(|column| column.acl_ref.is_some()))
    {
        return note_vector_candidate_identity("table entitlement is a grant lookup");
    }
    let Some(filter) = state.filter.as_ref() else {
        return note_vector_candidate_identity("candidate source carries no predicate");
    };
    let pick = match &state.mode {
        ScanMode::Index { pick, .. } => pick.as_ref(),
        ScanMode::Exact { .. } => match state.residual_pick.as_deref() {
            Some(pick) => pick,
            None => return note_vector_candidate_identity("exact source has no residual pick"),
        },
        _ => return note_vector_candidate_identity("candidate source is not an index source"),
    };
    // One driving shape does not prove every predicate on that column. Keep
    // repeated terms on the row route, where the complete expression runs.
    fn column_terms(expr: &Expr, column: &str) -> usize {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => column_terms(left, column) + column_terms(right, column),
            Expr::BinaryOp { left, .. } | Expr::InList { expr: left, .. } => match left.as_ref() {
                Expr::Row(elements) => elements
                    .iter()
                    .filter(|element| matches!(element, Expr::Column(c) if c.column == column))
                    .count(),
                left => usize::from(matches!(left, Expr::Column(c) if c.column == column)),
            },
            _ => 0,
        }
    }
    // The declared type a bound value arrives with, when it is one the index
    // compares without coercion.
    fn value_type(value: &Expr, state: &ScanState) -> Option<ColumnType> {
        match value {
            Expr::Literal(Literal::Text(_)) => Some(ColumnType::Text),
            Expr::Literal(Literal::Integer(_)) => Some(ColumnType::Integer),
            Expr::Literal(Literal::Bool(_)) => Some(ColumnType::Boolean),
            Expr::Literal(Literal::Real(value)) if value.is_finite() => Some(ColumnType::Real),
            Expr::Parameter(name) => match state.params.get(name) {
                Some(Value::Text(_)) => Some(ColumnType::Text),
                Some(Value::Int64(_)) => Some(ColumnType::Integer),
                Some(Value::Bool(_)) => Some(ColumnType::Boolean),
                Some(Value::Float64(value)) if value.is_finite() => Some(ColumnType::Real),
                Some(Value::Uuid(_)) => Some(ColumnType::Uuid),
                Some(Value::Timestamp(_)) => Some(ColumnType::Timestamp),
                Some(Value::TxId(_)) => Some(ColumnType::TxId),
                _ => None,
            },
            _ => None,
        }
    }
    // A row-value list is served by the index only as the pick's own tuple
    // prefix: its columns are exactly the leading pushed columns, each named
    // by no other term, and every listed value has its column's type.
    fn row_list_covered(
        columns: &[Expr],
        list: &[Expr],
        state: &ScanState,
        pick: &IndexPick,
    ) -> bool {
        let Some(tuples) = pick.tuple_prefix() else {
            return false;
        };
        let arity = tuples.first().map_or(columns.len(), Vec::len);
        if arity != columns.len() || pick.pushed_columns.len() < arity {
            return false;
        }
        let mut types = Vec::with_capacity(columns.len());
        for element in columns {
            let Expr::Column(column) = element else {
                return false;
            };
            if !pick.pushed_columns[..arity]
                .iter()
                .any(|name| name == &column.column)
                || state
                    .filter
                    .as_ref()
                    .is_none_or(|filter| column_terms(filter, &column.column) != 1)
            {
                return false;
            }
            let Some(meta) = state
                .meta
                .columns
                .iter()
                .find(|meta| meta.name == column.column)
            else {
                return false;
            };
            if meta.column_type == ColumnType::Real {
                return false;
            }
            types.push(meta.column_type.clone());
        }
        list.iter().all(|item| {
            let Expr::Row(values) = item else {
                return false;
            };
            values.len() == types.len()
                && values.iter().zip(&types).all(|(value, column_type)| {
                    value_type(value, state).as_ref() == Some(column_type)
                })
        })
    }
    // SQL comparisons that need coercion, NULL/NaN exclusion or residual
    // expression evaluation retain their charged row reads.
    fn covered(expr: &Expr, state: &ScanState, pick: &IndexPick) -> bool {
        if let Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } = expr
        {
            return covered(left, state, pick) && covered(right, state, pick);
        }
        let (column, values): (&str, &[Expr]) = match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::Eq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte,
                right,
            } => {
                let Expr::Column(column) = left.as_ref() else {
                    return false;
                };
                (column.column.as_str(), std::slice::from_ref(right.as_ref()))
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => match expr.as_ref() {
                Expr::Column(column) => (column.column.as_str(), list.as_slice()),
                Expr::Row(columns) => return row_list_covered(columns, list, state, pick),
                _ => return false,
            },
            _ => return false,
        };
        if !pick.pushed_columns.iter().any(|name| name == column) {
            return false;
        }
        let Some(meta) = state.meta.columns.iter().find(|meta| meta.name == column) else {
            return false;
        };
        if state
            .filter
            .as_ref()
            .is_none_or(|filter| column_terms(filter, column) != 1)
        {
            return false;
        }
        if let Expr::BinaryOp {
            op: op @ (BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte),
            ..
        } = expr
        {
            // The total index order includes NULL and NaN, which SQL ordered
            // comparisons exclude. NULL is the physical tail in ascending
            // order and the physical head in descending order, so a lower
            // SQL bound (`>` / `>=`) reaches it in either direction. Keep
            // nullable lower-bound ranges on the row-checking route.
            if meta.column_type == ColumnType::Real
                || (meta.nullable && matches!(op, BinOp::Gt | BinOp::Gte))
            {
                return false;
            }
        }
        values
            .iter()
            .all(|value| value_type(value, state).as_ref() == Some(&meta.column_type))
    }
    if !covered(filter, state, pick) {
        return note_vector_candidate_identity("a predicate is not served from the index key");
    }
    // A finite equality/IN tuple is expanded into exact physical runs. A
    // range on a composite prefix has only a partial-key seek edge, so it
    // must retain the ordinary per-row predicate check.
    let finite_composite_run = pick.columns.len() == 1
        || std::iter::once(&pick.shape)
            .chain(&pick.suffix_shapes)
            .all(|shape| {
                matches!(
                    shape,
                    IndexPredicateShape::Equality(_)
                        | IndexPredicateShape::InList(_)
                        | IndexPredicateShape::IsNull
                )
            });
    if !finite_composite_run {
        return note_vector_candidate_identity("the index run is not a finite composite");
    }
    // An identities-only candidate IS the answer on this route: no row body is
    // read, so the posting itself has to have been authenticated against the
    // committed content root before it is delivered. The exact source walks the
    // physical posting table alone, which on the file route means a posting
    // deleted behind that root silently removes an allowed candidate and one
    // added behind it silently joins the candidate set. So the same finite keys
    // are re-planned onto the authenticated ordered route: one closed run per
    // probed key, over the same physical index in the same order, delivered
    // through the bounded window proof and its per-window physical
    // reconciliation -- the route every other authenticated range already
    // takes. In-process reads keep the exact source: they have no committed
    // content root to authenticate against, and a single-column automatic index
    // is stored there as an exact hash with no ordered tree to walk.
    let replanned_exact_pick = matches!(state.mode, ScanMode::Exact { .. })
        .then(|| pick.clone())
        .filter(|_| db.retained_relational_membership_source());
    if let Some(tx) = state.transaction.as_mut()
        && !resolve_candidate_delete_membership(db, &state.table, state.snapshot, tx, context)?
    {
        return Ok(());
    }
    if let Some(pick) = replanned_exact_pick {
        let ScanMode::Exact { index, .. } = &state.mode else {
            unreachable!("the exact pick was taken from an exact source");
        };
        let index = index.clone();
        let (column, direction) = pick.columns.first().cloned().ok_or_else(|| {
            Error::Other(format!(
                "exact automatic index {index} has no leading column"
            ))
        })?;
        // Admitted before the runs are built, and handed back as soon as they
        // exist: what the cursor ends up holding is charged by the continuation
        // this kernel is reconciled to, exactly as the declared-index route
        // charges it.
        let planned = crate::executor::planned_index_run_construction_bytes(&pick)?;
        context.reserve(BoundedWorkSource::TableScan, planned)?;
        let runs = match crate::executor::expanded_index_runs(&pick) {
            Ok(runs) => runs,
            Err(error) => {
                context.release(planned)?;
                return Err(error.into());
            }
        };
        let cursor = BoundedOrderedRowCursor::for_index(index.clone(), false).seeking_runs(runs);
        context.release(planned)?;
        // The pick moves into the mode, so the continuation counts it once.
        state.residual_pick = None;
        state.authenticated_exact_index = Some(index);
        state.mode = ScanMode::Index {
            pick: Box::new(pick),
            column,
            direction,
            cursor,
        };
    }
    match &mut state.mode {
        ScanMode::Exact { cursor, .. } => cursor.select_identities_only(),
        ScanMode::Index { cursor, .. } => cursor.select_identities_only(),
        _ => return note_vector_candidate_identity("candidate source has no identity cursor"),
    }
    note_vector_candidate_identity("identities only")?;
    // Everything above has proved this range is inside what the reader may
    // see. Either the reader's own restrictions were folded into this scan's
    // filter and `covered` just proved every term of it is served from the
    // index key, or they were left out because the statement's own finite
    // terms on those columns were already a subset of them -- and those terms
    // are covered too. A grant column, the one restriction an index cannot
    // answer, took the row route long before here. So a candidate drawn from
    // this range needs no row of its own to be judged by; the row that is
    // published is still judged, where it is published.
    state.gate_answered_by_candidate_range = access_filter;
    state.identities_only = true;
    state.schema_columns.clear();
    root.columns.truncate(1);
    Ok(())
}

/// The partitions a candidate scan's runs name, when the scan would admit
/// every live vector of exactly those partitions and nothing else.
///
/// The scan is already proved to be served entirely from one index key: every
/// term of its filter is a finite set over a pushed key column (see
/// `select_vector_candidate_identity_source`). When its leading pushed columns
/// are exactly the vector column's partition key, the runs name whole
/// partitions, and every further pushed column is boolean, then what the
/// filter rejects inside a named partition is a finite set of key prefixes --
/// the complement of each column's admitted values. If the committed index
/// proves every one of those prefixes empty, the filter admits the whole
/// partition. The search can then walk those partitions directly, and the scan
/// never enumerates a posting: its work no longer grows with how many rows the
/// reader may see.
///
/// `None` whenever any of that is not proved -- a non-boolean residual, a
/// probe that did not fit, a partition holding a row the filter rejects. The
/// caller then keeps the scan, which proves its candidates window by window.
fn candidate_runs_admit_whole_partitions(
    db: &dyn ReadExecutionTarget,
    root: &PullNode,
    meta: &TableMeta,
    partition_keys: &[String],
    scope: Option<&[VectorPartitionKey]>,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<Vec<VectorPartitionKey>>, BoundedExecutionError> {
    let PullKind::Scan(state) = &root.kind else {
        return Ok(None);
    };
    if !state.identities_only || !db.retained_relational_membership_source() {
        return Ok(None);
    }
    let ScanMode::Index { pick, .. } = &state.mode else {
        return Ok(None);
    };
    let leading = partition_keys.len();
    if leading == 0
        || pick.pushed_columns.len() < leading
        || !pick.pushed_columns[..leading]
            .iter()
            .all(|column| partition_keys.contains(column))
        || !matches!(pick.tuple_arity(), 0) && pick.tuple_arity() != leading
    {
        return Ok(None);
    }
    let shapes = std::iter::once(&pick.shape)
        .chain(&pick.suffix_shapes)
        .collect::<Vec<_>>();
    // What each boolean suffix column admits, and what it rejects.
    let mut admitted = Vec::with_capacity(shapes.len() - leading);
    let mut rejected = Vec::with_capacity(shapes.len() - leading);
    for (shape, (column, _)) in shapes.iter().zip(&pick.columns).skip(leading) {
        let Some(values) = super::finite_shape_values(shape) else {
            return Ok(None);
        };
        let Some(declared) = meta
            .columns
            .iter()
            .find(|candidate| &candidate.name == column)
        else {
            return Ok(None);
        };
        if declared.column_type != ColumnType::Boolean {
            return Ok(None);
        }
        let mut domain = vec![Value::Bool(false), Value::Bool(true)];
        if declared.nullable {
            domain.push(Value::Null);
        }
        let admits = |candidate: &Value| values.iter().any(|value| value == candidate);
        rejected.push(
            domain
                .iter()
                .filter(|candidate| !admits(candidate))
                .cloned()
                .collect::<Vec<_>>(),
        );
        admitted.push(
            domain
                .into_iter()
                .filter(|candidate| admits(candidate))
                .collect::<Vec<_>>(),
        );
    }
    let directed = |value: &Value, direction: contextdb_core::SortDirection| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
    };
    let mut partitions = BTreeSet::<IndexKey>::new();
    for (start, _) in super::expanded_index_runs(pick)? {
        let Bound::Included(key) = start else {
            return Ok(None);
        };
        partitions.insert(key[..leading].to_vec());
    }
    // Every prefix the filter rejects inside a named partition: an admitted
    // value on each earlier suffix column, then a rejected value on this one.
    let mut probes = Vec::new();
    for position in 0..rejected.len() {
        let mut heads = vec![Vec::<DirectedValue>::new()];
        for (earlier, values) in admitted.iter().enumerate().take(position) {
            let direction = pick.columns[leading + earlier].1;
            heads = heads
                .into_iter()
                .flat_map(|head| {
                    values.iter().map(move |value| {
                        let mut next = head.clone();
                        next.push(directed(value, direction));
                        next
                    })
                })
                .collect();
        }
        let direction = pick.columns[leading + position].1;
        for head in heads {
            for value in &rejected[position] {
                let mut probe = head.clone();
                probe.push(directed(value, direction));
                probes.push(probe);
            }
        }
    }
    // Every rejected prefix of every named partition, proved empty together:
    // one pool and one batch of authenticated pages for the statement.
    let mut prefixes = Vec::with_capacity(partitions.len().saturating_mul(probes.len()));
    for partition in &partitions {
        for probe in &probes {
            let mut prefix = partition.clone();
            prefix.extend(probe.iter().cloned());
            prefixes.push(prefix);
        }
    }
    if !prefixes.is_empty() {
        let empty = db.bounded_verified_index_prefixes_are_empty(
            &state.table,
            &pick.name,
            &prefixes,
            &mut || context.checkpoint(),
            &mut || {
                context.charge(
                    BoundedWorkSource::IndexRange,
                    BoundedSourceTouch::IndexEntry,
                )
            },
            context.source_read_memory(BoundedWorkSource::IndexRange),
        )?;
        if empty != Some(true) {
            return Ok(None);
        }
    }
    let positions = partition_keys
        .iter()
        .map(|key| {
            pick.pushed_columns[..leading]
                .iter()
                .position(|column| column == key)
        })
        .collect::<Option<Vec<_>>>();
    let Some(positions) = positions else {
        return Ok(None);
    };
    let mut keys = Vec::with_capacity(partitions.len());
    for partition in &partitions {
        let values = positions
            .iter()
            .map(|position| match &partition[*position] {
                DirectedValue::Asc(TotalOrdAsc(value))
                | DirectedValue::Desc(TotalOrdDesc(value)) => value.clone(),
            })
            .collect::<Vec<_>>();
        let Some(key) = VectorPartitionKey::from_values(&values) else {
            return Ok(None);
        };
        // The partitions searched are never wider than the statement's own
        // partition scope.
        if scope.is_some_and(|scope| !scope.contains(&key)) {
            return Ok(None);
        }
        keys.push(key);
    }
    keys.sort();
    keys.dedup();
    Ok(Some(keys))
}

/// Where one cold vector graph load's credit move stands. Test observation
/// only.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphCreditTransferPhase {
    /// The request still holds the load credit; nothing is admitted yet.
    BeforeDestination,
    /// The destination has admitted the bytes; the request's credit is not
    /// split yet.
    DestinationAdmitted,
    /// The request's credit has been split off and given back.
    SourceSplit,
}

/// One observation of a graph credit move: the bytes moving, what this
/// request's credit holds, and what the destination owner has in use.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphCreditTransferCut {
    pub phase: GraphCreditTransferPhase,
    pub bytes: usize,
    pub request_credit: usize,
    pub destination_used: usize,
}

/// The callback a cold vector graph load uses to hand its load credit to the
/// graph's owner. It owns `funding`, the charge for its own allocation, until
/// its last clone drops. The prototype and the funded callback are both built
/// here, so they share one closure type and one layout.
fn graph_credit_transfer(
    context: Arc<RequestContext>,
    funding: SourcePageCharge,
) -> impl Fn(&crate::memory_accounting::MemoryAccountant, usize) -> contextdb_core::Result<()>
+ Send
+ Sync
+ 'static {
    move |destination: &crate::memory_accounting::MemoryAccountant, bytes: usize| {
        let _funding = &funding;
        context.transfer_graph_credit(destination, bytes)
    }
}

/// Whether a refusal is this reader's own memory limit. An AUTO search that
/// cannot afford its graph answers with the exact scan only for this refusal;
/// a work, time, cancellation or any other failure still ends the read.
fn reader_memory_refusal(error: &BoundedExecutionError) -> bool {
    matches!(
        error,
        BoundedExecutionError::Refused(failure)
            if matches!(
                failure.detail(),
                ReadFailureDetail::OwnerLimitExceeded(detail)
                    if detail.limit == ReadFailureLimit::Memory
            )
    )
}

/// The transfer callback, charged to this request before it is allocated. A
/// stack prototype with an empty charge gives the closure's exact layout; the
/// allocation is that layout behind the `Arc` header. The callback carries a
/// live charge for exactly those bytes, so this read's ceiling keeps counting
/// them for as long as the callback lives.
fn funded_graph_credit_transfer(
    context: &Arc<RequestContext>,
) -> std::result::Result<super::VectorLoadReservationTransfer, BoundedExecutionError> {
    let prototype = graph_credit_transfer(
        Arc::clone(context),
        SourcePageCharge {
            _credit: ReadCredit::new(Arc::clone(&context.reader_budget)),
            live: Arc::clone(&context.live_source_pages),
            bytes: 0,
        },
    );
    let bytes = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::for_value(&prototype))
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| Error::Other("vector load transfer layout overflows".to_string()))?;
    drop(prototype);
    let funding = context.admit_live_charge(
        BoundedWorkSource::VectorCandidates,
        bytes,
        "fund_vector_load_transfer",
    )?;
    let transfer: super::VectorLoadReservationTransfer =
        Arc::new(graph_credit_transfer(Arc::clone(context), funding));
    Ok(transfer)
}

#[allow(clippy::too_many_arguments)]
fn build_vector_kernel<'plan>(
    db: &dyn ReadExecutionTarget,
    plan: &'plan PhysicalPlan,
    table: &str,
    column: &str,
    query_expr: &Expr,
    requested_k: u64,
    candidates: Option<&'plan PhysicalPlan>,
    sort_key: Option<&str>,
    requested_search_mode: Option<contextdb_parser::ast::VectorSearchMode>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &HashMap<&'plan str, &'plan PhysicalPlan>,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    db.assert_table_read_allowed(table)?;
    let index = VectorIndexRef::new(table.to_string(), column.to_string());
    let mut schema_indexes = vec![index.clone()];
    if let Some(source) = row_vector_source_ref(query_expr) {
        schema_indexes.push(source);
    }
    let schema = db.bounded_vector_schema_read_many(schema_indexes);
    db.assert_vector_index_exists_under_schema_read(&index)?;
    let (query, query_vector_source, query_bytes) =
        bounded_query_vector(db, query_expr, &params, snapshot, context)?;
    let k = usize::try_from(requested_k).map_err(|_| {
        Error::Other("bounded vector LIMIT exceeds the native address space".to_string())
    })?;
    let candidate_k = k;
    let access_filter = db.bounded_read_requires_candidate_filter(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let vector_column = meta
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| Error::UnknownVectorIndex {
            index: index.clone(),
        })?;
    let search_mode = requested_search_mode
        .map(super::map_vector_search_mode)
        .unwrap_or(vector_column.search_mode);
    super::bind_vector_search_parameters(query_expr, candidates, &params)?;
    let route = super::vector_candidate_route(db, &index, candidates, &params)?;
    let partition_scope = route.scope;
    let access_candidates = route.candidates;
    let complete_relational_route = route.complete;
    // Refuse before the candidate subtree gets to run.  A bounded read has a
    // ceiling, but spending that ceiling walking every row is still not an
    // INDEXED answer.  The shared check uses the same relational index choice
    // this kernel will use after the caller adds a supporting index.
    if search_mode == contextdb_core::VectorSearchMode::Indexed && !complete_relational_route {
        return Err(Error::VectorFilteredRouteUnavailable {
            index: index.clone(),
            predicate_columns: super::vector_candidate_refusal_columns(
                db,
                &index,
                access_candidates.as_ref(),
                access_filter,
            ),
        }
        .into());
    }
    if search_mode == contextdb_core::VectorSearchMode::Auto
        && !complete_relational_route
        && ReadCredit::try_new(
            Arc::clone(&context.reader_budget),
            context
                .with_vector_copies(|owner| db.bounded_vector_entry_count(&index, snapshot, owner))?
                .saturating_mul(std::mem::size_of::<(RowId, f32)>()),
            "vector_search",
            "preflight_filtered_exact_scores",
            "Add a relational index for the filter, reduce vector volume, or raise MEMORY_LIMIT.",
        )
        .is_err()
    {
        return Err(Error::VectorFilteredRouteUnavailable {
            index: index.clone(),
            predicate_columns: super::vector_candidate_refusal_columns(
                db,
                &index,
                access_candidates.as_ref(),
                access_filter,
            ),
        }
        .into());
    }
    let directory_candidates = access_filter && route.directory_route;
    let unrestricted = !access_filter
        && (partition_scope
            .as_ref()
            .is_some_and(super::VectorPartitionScope::covers_filter_for_execution)
            || candidates
                .is_some_and(|candidate| is_unrestricted_scan_for_table(candidate, table)));
    let candidate_plan = if unrestricted || directory_candidates {
        None
    } else {
        access_candidates.as_ref()
    };
    let mut candidate_root = candidate_plan
        .map(|candidate| {
            build_kernel_with_ctes(db, candidate, Arc::clone(&params), snapshot, ctes, context)
        })
        .transpose()?;
    if let Some(root) = candidate_root.as_mut() {
        select_vector_candidate_identity_source(db, root, access_filter, context)?;
    }
    let identity_source_index = match candidate_root.as_ref().map(|root| &root.kind) {
        Some(PullKind::Scan(state)) if state.identities_only => match &state.mode {
            ScanMode::Index { pick, .. } => Some(pick.name.clone()),
            _ => state.authenticated_exact_index.clone(),
        },
        _ => None,
    };
    // A candidate scan that would admit every live vector of the partitions
    // it names is not run at all: the search walks those partitions whole.
    // The row gate and the statement's own predicate still judge every row
    // that is published, and the scan's index still names any row that
    // contradicts it (`materialize_scored_row`).
    let whole_partitions = match (
        candidate_root.as_ref(),
        vector_column.partition_key_columns.as_deref(),
    ) {
        (Some(root), Some(keys)) if context.transaction().is_none() => {
            candidate_runs_admit_whole_partitions(
                db,
                root,
                &meta,
                keys,
                partition_scope
                    .as_ref()
                    .and_then(super::VectorPartitionScope::resolved_prefixes),
                context,
            )?
        }
        _ => None,
    };
    let candidate_root_was_kept = whole_partitions.is_none();
    if !candidate_root_was_kept {
        note_vector_candidate_identity("candidate runs admit whole partitions")?;
        candidate_root = None;
    }
    // What this handle's own transaction has done to these vectors, decided
    // once here rather than asked per entry. Charged before it is held, like
    // the row overlay a scan takes.
    let mut overlay_bytes = 0;
    let vector_overlay = match context.transaction() {
        Some(tx) => {
            let overlay = db.transaction_vector_overlay(tx, &index, &mut |bytes| {
                context.reserve(BoundedWorkSource::VectorCandidates, bytes)?;
                overlay_bytes = bytes;
                Ok(())
            });
            match overlay {
                Ok(overlay) => overlay.into_option_if_not_empty(),
                Err(error) => {
                    context.release(overlay_bytes)?;
                    return Err(error);
                }
            }
        }
        None => None,
    };
    if vector_overlay.is_none() {
        context.release(overlay_bytes)?;
        overlay_bytes = 0;
    }
    let mut candidate_ids = Vec::new();
    let mut candidate_bytes = 0usize;
    if directory_candidates {
        let sql_filter = candidates.and_then(|mut candidate| {
            while let PhysicalPlan::Project { input, .. } = candidate {
                candidate = input;
            }
            match candidate {
                PhysicalPlan::Scan { filter, .. } => filter.as_ref(),
                _ => None,
            }
        });
        let mut admit = |row_id| {
            note_vector_candidate_identity("directory admit")?;
            let mut retained = 0;
            let row =
                context.with_source_page_admission(BoundedWorkSource::VectorCandidates, || {
                    db.bounded_row_by_identity(
                        context.source_read_memory(BoundedWorkSource::VectorCandidates),
                        context.transaction(),
                        table,
                        row_id,
                        snapshot,
                        &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                        &mut |bytes| {
                            retained = context
                                .admit_source_row(BoundedWorkSource::VectorCandidates, bytes)?;
                            Ok(())
                        },
                    )
                })?;
            let result = (|| {
                let Some(row) = row.as_ref() else {
                    return Ok(());
                };
                if let Some(filter) = sql_filter
                    && super::eval_bool_expr(filter, &|inner| {
                        super::eval_expr_value(row, inner, &params)
                    })? != Some(true)
                {
                    return Ok(());
                }
                if db.bounded_read_allowed_for_row(
                    context.transaction(),
                    table,
                    &meta,
                    row,
                    snapshot,
                    &mut |_| {
                        context.charge(
                            BoundedWorkSource::AccessControl,
                            BoundedSourceTouch::AccessRow,
                        )?;
                        context.attribute_live_memory(BoundedWorkSource::AccessControl);
                        Ok(())
                    },
                    context.source_read_memory(BoundedWorkSource::AccessControl),
                )? {
                    candidate_bytes += reserve_vector_slot(
                        context,
                        BoundedWorkSource::VectorCandidates,
                        &mut candidate_ids,
                        "authorized vector identities",
                    )?;
                    candidate_ids.push(row_id.0);
                }
                Ok::<(), BoundedExecutionError>(())
            })();
            drop(row);
            context.release(retained)?;
            result
        };
        let mut collect = || {
            context.with_vector_copies(|vector_copy_owner| {
                db.bounded_vector_visit_visible_ids(
                    &index,
                    snapshot,
                    &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                    &mut |bytes| context.reserve(BoundedWorkSource::VectorCandidates, bytes),
                    &mut |bytes| context.release_parked(bytes),
                    &mut admit,
                    vector_copy_owner,
                    context.reader_budget.clone(),
                )
            })
        };
        if let Some(prefixes) = partition_scope
            .as_ref()
            .and_then(super::VectorPartitionScope::resolved_prefixes)
        {
            contextdb_vector::mem::with_selected_partition_prefixes(
                &index,
                prefixes.to_vec(),
                collect,
            )?;
        } else {
            collect()?;
        }
        // Staged rows have no committed directory entry. Admit their metadata
        // under the same predicate and authorization before scoring the delta.
        if let Some(overlay) = vector_overlay.as_ref() {
            for row_id in overlay
                .staged_identities
                .iter()
                .chain(overlay.moved.values())
            {
                admit(*row_id)?;
            }
        }
        candidate_ids.sort_unstable();
        candidate_ids.dedup();
    }
    // A description describes the plan, not the shortcut through it. The
    // candidate source above is skipped only when it is an unrestricted scan
    // of the whole table -- the vector index already covers every row, so
    // reading them first would be work for nothing -- but the statement still
    // named that scan, and the source it names is what an operator is asking
    // about. So the elided scan is described exactly as a scan is described,
    // and the label composes through the same function either way.
    let candidate_trace = match candidate_root.as_ref() {
        Some(root) => Some(root.trace.clone()),
        None if unrestricted => Some(QueryTrace::scan()),
        None => None,
    };
    // The overlay owns its real store-wide container admission. Vector
    // payload clones are admitted when touched by the existing kernel.
    // With an overlay in play the source must hand up enough extra committed
    // candidates for every removed, moved, or staged delta to have its say.
    // Stopping at the statement's LIMIT can lose the replacement for a row
    // the transaction removed; treating the delta count as a total does the
    // same whenever LIMIT is larger than the delta. The established door adds
    // this exact overfetch before final truncation too.
    let candidate_k = match vector_overlay.as_ref() {
        Some(overlay) => candidate_k.saturating_add(overlay.searchable_entry_count),
        None => candidate_k,
    };
    let schema_columns: Vec<String> = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    columns.push("score".to_string());
    let operator = if matches!(plan, PhysicalPlan::HnswSearch { .. }) {
        "HNSWSearch"
    } else {
        "VectorSearch"
    };
    let restricted_candidates =
        candidates.is_some_and(|candidate| !is_unrestricted_scan_for_table(candidate, table));
    let trace = vector_search_trace_with_source(operator, candidate_trace, query_vector_source);
    context.reserve(BoundedWorkSource::VectorCandidates, size_of::<VectorState>())?;
    Ok(PullNode {
        columns,
        trace,
        kind: PullKind::Vector(Box::new(VectorState {
            #[cfg(feature = "test-seams")]
            receipt: vector_search_receipt::SearchReceiptState::Unstarted,
            supplement_columns: Vec::new(),
            table: table.to_string(),
            index,
            query,
            _query_bytes: query_bytes,
            k,
            candidate_k,
            partition_prefixes: match whole_partitions {
                Some(partitions) => Some(partitions),
                None => partition_scope
                    .as_ref()
                    .and_then(super::VectorPartitionScope::resolved_prefixes)
                    .map(<[VectorPartitionKey]>::to_vec),
            },
            search_mode,
            force_exact_for_relational_fallback: search_mode
                == contextdb_core::VectorSearchMode::Auto
                && !complete_relational_route,
            sort_key: sort_key.map(str::to_string),
            params,
            requested_search_mode,
            query_expr: query_expr.clone(),
            candidate_plan: candidates.cloned().map(Box::new),
            restricted_candidates,
            snapshot,
            _schema: schema,
            identity_source_index,
            preparation: VectorPreparation::Candidates(candidate_root),
            candidate_filter: (candidate_plan.is_some() && candidate_root_was_kept)
                || directory_candidates,
            candidate_ids,
            candidate_bytes,
            source: None,
            scores: Vec::new(),
            score_bytes: 0,
            schema_columns,
            materialized: 0,
            ranked_rows: Vec::new(),
            ranked_container_bytes: 0,
            output: VecDeque::new(),
            output_container_bytes: 0,
            used_hnsw: false,
            disclosure: None,
            vector_overlay,
            overlay_bytes,
        })),
    })
}

fn reserve_query_vector_clone(
    source: &[f32],
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    let planned = checked_size_mul(source.len(), std::mem::size_of::<f32>(), "query vector")?;
    context.reserve(BoundedWorkSource::VectorCandidates, planned)?;
    let mut vector = Vec::new();
    if vector.try_reserve_exact(source.len()).is_err() {
        context.release(planned)?;
        return Err(Error::Other("bounded query-vector allocation failed".to_string()).into());
    }
    vector.extend_from_slice(source);
    let actual = match checked_size_mul(
        vector.capacity(),
        std::mem::size_of::<f32>(),
        "query vector",
    ) {
        Ok(actual) => actual,
        Err(error) => {
            context.release(planned)?;
            return Err(error);
        }
    };
    reconcile_new_reservation(
        context,
        BoundedWorkSource::VectorCandidates,
        planned,
        actual,
    )?;
    Ok((vector, actual))
}

fn bounded_text_query_vector(
    text: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| Error::Other(format!("invalid VECTOR literal '{text}'")))?;
    let component_count = if inner.trim().is_empty() {
        0
    } else {
        inner.split(',').count()
    };
    let planned = checked_size_mul(component_count, std::mem::size_of::<f32>(), "query vector")?;
    context.reserve(BoundedWorkSource::VectorCandidates, planned)?;
    let mut vector = Vec::new();
    if vector.try_reserve_exact(component_count).is_err() {
        context.release(planned)?;
        return Err(Error::Other("bounded query-vector allocation failed".to_string()).into());
    }
    if component_count != 0 {
        for part in inner.split(',') {
            let value = match part.trim().parse::<f32>() {
                Ok(value) => value,
                Err(error) => {
                    context.release(planned)?;
                    return Err(Error::Other(format!(
                        "invalid VECTOR component '{}': {error}",
                        part.trim()
                    ))
                    .into());
                }
            };
            vector.push(value);
        }
    }
    let actual = checked_size_mul(
        vector.capacity(),
        std::mem::size_of::<f32>(),
        "query vector",
    )?;
    reconcile_new_reservation(
        context,
        BoundedWorkSource::VectorCandidates,
        planned,
        actual,
    )?;
    Ok((vector, actual))
}

fn bounded_query_vector_from_value(
    value: &Value,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    bounded_query_vector_from_scalar(BorrowedScalar::from_value(value), params, context)
}

fn bounded_query_vector_from_scalar(
    value: BorrowedScalar<'_>,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    match value {
        BorrowedScalar::Vector(vector) => reserve_query_vector_clone(vector, context),
        BorrowedScalar::Text(text) if text.trim_start().starts_with('[') => {
            bounded_text_query_vector(text, context)
        }
        BorrowedScalar::Text(name) => match params.get(name) {
            Some(Value::Vector(vector)) => reserve_query_vector_clone(vector, context),
            _ => Err(Error::PlanError("vector parameter missing".to_string()).into()),
        },
        _ => Err(Error::PlanError("invalid vector query expression".to_string()).into()),
    }
}

fn bounded_query_vector_from_text(
    text: &str,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    if text.trim_start().starts_with('[') {
        return bounded_text_query_vector(text, context);
    }
    match params.get(text) {
        Some(Value::Vector(vector)) => reserve_query_vector_clone(vector, context),
        _ => Err(Error::PlanError("vector parameter missing".to_string()).into()),
    }
}

fn bounded_query_vector(
    db: &dyn ReadExecutionTarget,
    expr: &Expr,
    params: &HashMap<String, Value>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, Option<VectorIndexRef>, usize), BoundedExecutionError> {
    match expr {
        Expr::RowVectorSource { table, column, key } => {
            let source_index = VectorIndexRef::new(table.clone(), column.clone());
            let meta = db
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            db.assert_table_read_allowed(table)?;
            if !meta.columns.iter().any(|candidate| {
                candidate.name == *column && matches!(candidate.column_type, ColumnType::Vector(_))
            }) {
                return Err(Error::UnknownVectorIndex {
                    index: source_index.clone(),
                }
                .into());
            }
            db.assert_vector_index_exists_under_schema_read(&source_index)?;

            let raw_key = bounded_resolve_scalar(key, params)?;
            if raw_key.is_null() {
                return Err(Error::PlanError("ROW_VECTOR key cannot be NULL".to_string()).into());
            }
            if matches!(raw_key, BorrowedScalar::Vector(_)) {
                return Err(
                    Error::PlanError("ROW_VECTOR key cannot be a vector".to_string()).into(),
                );
            }
            let key_column = db.natural_key_column_for_table(table)?;
            let key = db.coerce_into_column(
                table,
                &key_column,
                raw_key.into_owned(),
                None,
                None,
            )
            .map_err(|error| {
                Error::PlanError(format!(
                    "ROW_VECTOR argument 3 key cannot be coerced to `{table}`.`{key_column}` natural key: {error}"
                ))
            })?;
            if matches!(key, Value::Null) {
                return Err(Error::PlanError("ROW_VECTOR key cannot be NULL".to_string()).into());
            }
            let key_label = row_vector_key_label(&key);
            // The row the key names is resolved inside the caller's own
            // transaction, exactly as the executor resolves it, so a row this
            // session staged is the row this read is about.
            let tx = context.transaction();
            let row_id = db
                .row_id_for_natural_key_in_tx(
                    tx,
                    table,
                    &key_column,
                    &key,
                    snapshot,
                    context.source_read_memory(BoundedWorkSource::VectorCandidates),
                )?
                .ok_or_else(|| Error::PersistedRowVectorRowMissing {
                    index: source_index.clone(),
                    key: key_label.clone(),
                })?;
            let mut vector_copy_owner =
                crate::vector_read_memory::admission(Arc::new(SourceDecodeBudget {
                    context: context.clone(),
                    source: BoundedWorkSource::VectorCandidates,
                }));
            let resolved = db.with_bounded_row_vector(
                &source_index,
                row_id,
                snapshot,
                tx,
                &mut |_candidate_bytes| {
                    context.charge(
                        BoundedWorkSource::AccessControl,
                        BoundedSourceTouch::AccessRow,
                    )?;
                    context.attribute_live_memory(BoundedWorkSource::AccessControl);
                    Ok::<(), BoundedExecutionError>(())
                },
                &mut |vector| {
                    db.validate_vector_under_schema_read(&source_index, vector.len())?;
                    reserve_query_vector_clone(vector, context)
                },
                &mut vector_copy_owner,
                Arc::new(SourceDecodeBudget {
                    context: context.clone(),
                    source: BoundedWorkSource::VectorCandidates,
                }),
            )?;
            // The helper's temporary source body is gone; the returned clone
            // has its separate, existing query-vector reservation.
            drop(vector_copy_owner);
            let (vector, retained_bytes) =
                resolved.ok_or_else(|| Error::PersistedRowVectorCellNull {
                    index: source_index.clone(),
                    key: key_label,
                })?;
            Ok((vector, Some(source_index), retained_bytes))
        }
        Expr::Literal(Literal::Vector(vector)) => {
            let (vector, bytes) = reserve_query_vector_clone(vector, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Literal(Literal::Text(text)) => {
            let (vector, bytes) = bounded_query_vector_from_text(text, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Parameter(name) => {
            let value = params
                .get(name)
                .ok_or_else(|| Error::NotFound(format!("missing parameter: {name}")))?;
            let (vector, bytes) = bounded_query_vector_from_value(value, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Column(column) => {
            let value = params
                .get(&column.column)
                .ok_or_else(|| Error::PlanError("vector parameter missing".to_string()))?;
            let (vector, bytes) = bounded_query_vector_from_value(value, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::CosineDistance { right, .. } => {
            bounded_query_vector(db, right, params, snapshot, context)
        }
        _ => {
            let resolved = bounded_resolve_scalar(expr, params)?;
            let (vector, bytes) = bounded_query_vector_from_scalar(resolved, params, context)?;
            Ok((vector, None, bytes))
        }
    }
}

fn empty_node() -> PullNode {
    PullNode {
        columns: Vec::new(),
        trace: QueryTrace::scan(),
        kind: PullKind::Static(StaticState {
            rows: VecDeque::new(),
        }),
    }
}

/// Capture what an open transaction changes about this table, and charge what
/// holding it costs.
///
/// An index and an ordered run describe the committed rows only -- neither
/// knows a staged row exists -- so a scan that has to see a transaction reads
/// the table itself and re-applies the predicate per row. That is slower and
/// it is the only way the answer can be right; it applies to the writing
/// session's own reads and to nothing else.
fn scan_transaction_overlay(
    db: &dyn ReadExecutionTarget,
    table: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<ScanTransactionOverlay>, BoundedExecutionError> {
    let Some(tx) = context.transaction() else {
        return Ok(None);
    };
    let overlay = db.transaction_table_overlay(
        tx,
        table,
        context.source_read_memory(BoundedWorkSource::TableScan),
    )?;
    if overlay.is_empty() {
        return Ok(None);
    }
    context.reserve(BoundedWorkSource::TableScan, overlay.bytes)?;
    Ok(Some(ScanTransactionOverlay {
        predicate_deleted: Vec::new(),
        committed_predicates_resolved: false,
        overlay,
        restaged_over_committed: std::collections::HashSet::new(),
        published: None,
        ordered_staged: None,
        staged_key_bytes: 0,
        committed_exhausted: false,
    }))
}

#[allow(clippy::too_many_arguments)]
fn build_scan(
    db: &dyn ReadExecutionTarget,
    table: &str,
    filter: Option<&Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ordered: Option<OrderedIndexScan>,
    // The index the CALLER already chose for this table. A caller that has
    // asked the planner which index answers its predicate hands the answer in
    // rather than making the scan ask again -- and a scan that is given no
    // filter has nothing to ask with, so without this it would walk the whole
    // table where the executor reads one run of an index.
    chosen_pick: Option<IndexPick>,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    // The name `dual` answers as a one-row, zero-column constant source only
    // while no table carries it. An operator who creates a table called `dual`
    // gets that table: its rows, its columns, and its vector index.
    if table == "dual" && db.table_meta(table).is_none() {
        return Ok(PullNode {
            columns: Vec::new(),
            trace: QueryTrace::scan(),
            kind: PullKind::Static(StaticState {
                rows: VecDeque::from([PulledRow {
                    window: None,
                    values: Vec::new(),
                    retained_bytes: 0,
                    group_binding: None,
                }]),
            }),
        });
    }
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let schema_columns = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    // A filter written as another query is resolved before the scan plans
    // anything, by the executor's own resolver and inside the caller's own
    // transaction -- the same call the executor makes. Left unresolved, the
    // inner query reaches the evaluator intact and the read refuses a
    // statement the store answers perfectly well through its other door.
    let resolved_filter = filter
        .map(|filter| bounded_resolve_in_subqueries(db, filter, &params, snapshot, &[], context))
        .transpose()?;
    let mut considered = smallvec::SmallVec::new();
    // The index choice is made from the filter AS WRITTEN. Resolving an inner
    // query first turns `a IN (SELECT ...)` into a list of literals, and a
    // shape the writer never wrote then looks indexable -- so the two doors
    // disagree about whether the statement uses an index at all.
    let filter_analysis =
        filter.map(|filter| analyze_filter_for_index(table, &meta, filter, &params));
    // A predicate that can match nothing -- contradictory equalities on the
    // same column, a NULL or NaN bound where a value is required, a literal
    // that cannot even be read as the column's type -- answers zero rows
    // having read none. The executor decides this before it touches the store
    // (`executor.rs:6912`); a read that instead drops the unusable pick walks
    // the whole table, charges the reader for every row of it, and for NaN
    // hands back a row the other door never returns.
    let coerced_filter_pick = filter_analysis
        .as_ref()
        .and_then(|analysis| analysis.pick.clone())
        .map(|pick| db.coerce_pick_shape_to_column_type(table, &pick));
    let filter_matches_nothing = match coerced_filter_pick.as_ref() {
        Some(Ok(pick)) => crate::executor::pick_matches_nothing(pick),
        Some(Err(_)) => true,
        None => false,
    };
    let filter_pick = match coerced_filter_pick {
        Some(Ok(pick)) => Some(pick),
        Some(Err(_)) | None => None,
    };
    let transaction = scan_transaction_overlay(db, table, context)?;
    let mut ordered_pick = None;
    let mut ordered_merge_keys = None;
    let mut exact_pick: Option<IndexPick> = None;
    let mut empty_pick: Option<IndexPick> = None;
    let empty_ordered = ordered.is_some();
    let mut mode = if filter_matches_nothing {
        considered.extend(
            filter_analysis
                .as_ref()
                .map(|analysis| analysis.considered.clone())
                .unwrap_or_default(),
        );
        // The index was still chosen and still answered -- with nothing. The
        // statement reports the index it used, exactly as it does when the
        // same index returns rows.
        empty_pick = filter_pick.clone();
        ScanMode::Empty
    } else if let Some(OrderedIndexScan {
        column,
        direction,
        keys: order_keys,
        index,
        reverse,
        predicate_pick,
        predicate_considered,
    }) = ordered
    {
        // The index that serves the order often serves the predicate too.
        // Walking it end to end and rejecting rows afterwards reads the whole
        // index where the executor reads the run the predicate names, so the
        // same statement reports a different examined count through this door.
        let pick = predicate_pick
            .or_else(|| filter_pick.clone().filter(|pick| pick.name == index))
            .map(|pick| db.coerce_pick_shape_to_column_type(table, &pick))
            .transpose()?;
        let cursor = BoundedOrderedRowCursor::for_index(index, reverse);
        let cursor = match pick.as_ref() {
            Some(pick) => {
                considered.extend(predicate_considered);
                // The run is expressed in the INDEX's physical order, which a
                // descending leading column reverses -- so the bounds are read
                // off the index's own declared direction, never off the
                // direction the ORDER BY asked for.
                let index_direction = pick
                    .columns
                    .first()
                    .map(|(_, index_direction)| *index_direction)
                    .unwrap_or(direction);
                if matches!(
                    pick.shape,
                    IndexPredicateShape::Equality(_)
                        | IndexPredicateShape::InList(_)
                        | IndexPredicateShape::IsNull
                ) {
                    let planned = crate::executor::planned_index_run_construction_bytes(pick)?;
                    context.reserve(BoundedWorkSource::TableScan, planned)?;
                    let mut runs = match crate::executor::expanded_index_runs(pick) {
                        Ok(runs) => runs,
                        Err(error) => {
                            context.release(planned)?;
                            return Err(error.into());
                        }
                    };
                    if reverse {
                        runs.reverse();
                    }
                    let cursor = cursor.seeking_runs(runs);
                    context.release(planned)?;
                    cursor
                } else {
                    let (run_start, run_end, prefix_len) =
                        declared_index_run(pick, index_direction, reverse);
                    cursor.seeking_run_with_prefix_len(run_start, run_end, prefix_len)
                }
            }
            None => cursor,
        };
        ordered_pick = pick;
        ordered_merge_keys = Some(order_keys);
        ScanMode::Ordered {
            column,
            direction,
            cursor,
        }
    } else if let Some(pick) = match chosen_pick {
        Some(pick) => Some(db.coerce_pick_shape_to_column_type(table, &pick)?),
        None => match filter_analysis.as_ref() {
            Some(analysis) => {
                considered.extend(analysis.considered.clone());
                analysis
                    .pick
                    .clone()
                    .map(|pick| db.coerce_pick_shape_to_column_type(table, &pick))
                    .transpose()?
            }
            None => None,
        },
    } {
        let pick = Some(pick);
        let declaration = pick.as_ref().and_then(|pick| {
            meta.indexes
                .iter()
                .find(|index| index.name == pick.name)
                .cloned()
        });
        if let (Some(pick), Some(declaration)) = (pick.as_ref(), declaration.as_ref())
            && declaration.kind == contextdb_core::IndexKind::Auto
            && declaration.columns.len() == 1
        {
            // Single-column automatic indexes deliberately retain their exact
            // hash representation. Admit and build their complete key list
            // before the cursor can retain it.
            let planned = crate::executor::planned_index_run_construction_bytes(pick)?;
            context.reserve(BoundedWorkSource::TableScan, planned)?;
            let Some(keys) = crate::executor::exact_probe_keys(declaration.columns.len(), pick)
            else {
                context.release(planned)?;
                return Err(
                    Error::Other("exact automatic index lost its finite key set".into()).into(),
                );
            };
            let cursor = contextdb_relational::mem::BoundedExactIndexCursor::for_keys(keys);
            context.release(planned)?;
            exact_pick = Some(pick.clone());
            ScanMode::Exact {
                index: pick.name.clone(),
                cursor,
            }
        } else if let Some(pick) = pick.filter(|pick| {
            meta.indexes.iter().any(|index| {
                index.name == pick.name
                    && (index.kind == contextdb_core::IndexKind::UserDeclared
                        || index.columns.len() > 1)
            })
        }) {
            let (column, direction) = pick.columns.first().cloned().ok_or_else(|| {
                Error::Other(format!("bounded index {} has no leading column", pick.name))
            })?;
            // Admitted before the runs are built, so a list too long for the
            // ceiling is refused rather than allocated and then discovered.
            // What the cursor ends up holding is charged by the continuation
            // this kernel is reconciled to, so the admission is handed back
            // as soon as the runs exist and is never counted twice.
            let cursor = BoundedOrderedRowCursor::for_index(pick.name.clone(), false);
            let cursor = if matches!(
                pick.shape,
                IndexPredicateShape::Equality(_)
                    | IndexPredicateShape::InList(_)
                    | IndexPredicateShape::IsNull
            ) {
                let planned_pending = crate::executor::planned_pending_run_bytes(&pick)?;
                let planned_construction =
                    crate::executor::planned_index_run_construction_bytes(&pick)?;
                context.reserve(BoundedWorkSource::TableScan, planned_construction)?;
                let runs = match crate::executor::expanded_index_runs(&pick) {
                    Ok(runs) => runs,
                    Err(error) => {
                        context.release(planned_construction)?;
                        return Err(error.into());
                    }
                };
                let cursor = cursor.seeking_runs(runs);
                debug_assert_eq!(cursor.pending_run_bytes(), planned_pending);
                context.release(planned_construction)?;
                cursor
            } else {
                let (run_start, run_end, prefix_len) = declared_index_run(&pick, direction, false);
                cursor.seeking_run_with_prefix_len(run_start, run_end, prefix_len)
            };
            ScanMode::Index {
                cursor,
                pick: Box::new(pick),
                column,
                direction,
            }
        } else {
            ScanMode::Physical {
                cursor: db.bounded_physical_table_cursor(table)?,
            }
        }
    } else {
        ScanMode::Physical {
            cursor: db.bounded_physical_table_cursor(table)?,
        }
    };
    let selected_index = match &mode {
        ScanMode::Index { pick, .. } => Some(pick.name.as_str()),
        ScanMode::Ordered { cursor, .. } => cursor.index_name(),
        ScanMode::Exact { index, .. } => Some(index.as_str()),
        ScanMode::Empty => empty_pick.as_ref().map(|pick| pick.name.as_str()),
        ScanMode::Physical { .. } => None,
    };
    if let Some(index) = selected_index {
        db.preflight_historical_index(table, index, snapshot,
            context.source_read_memory(BoundedWorkSource::IndexRange),
            &mut || context.charge_operator(BoundedWorkSource::IndexRange))?;
    }
    capture_scan_membership(db, table, snapshot, &mut mode, context)?;
    let offers_every_committed_row = matches!(mode, ScanMode::Physical { .. });
    // An ordered walk with a transaction open folds the transaction's own
    // rows into the run rather than giving the run up for a sort of the
    // whole table.
    let ordered_merge = match (ordered_merge_keys, transaction.is_some()) {
        (Some(keys), true) => Some(OrderedMerge {
            keys,
            staged: None,
            deferred: None,
            held_bytes: 0,
        }),
        _ => None,
    };
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    Ok(PullNode {
        columns,
        trace: match &mode {
            ScanMode::Ordered { cursor, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: cursor.index_name().map(str::to_string),
                sort_elided: true,
                predicates_pushed: ordered_pick
                    .as_ref()
                    .map(|pick| {
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                indexes_considered: considered.clone(),
                ..Default::default()
            },
            ScanMode::Index { pick, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: Some(pick.name.clone()),
                predicates_pushed: pick
                    .pushed_columns
                    .iter()
                    .cloned()
                    .map(std::borrow::Cow::Owned)
                    .collect(),
                indexes_considered: considered,
                ..Default::default()
            },
            ScanMode::Exact { index, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: Some(index.clone()),
                predicates_pushed: exact_pick
                    .as_ref()
                    .map(|pick| {
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                indexes_considered: considered,
                ..Default::default()
            },
            ScanMode::Empty if empty_pick.is_some() => {
                let pick = empty_pick.as_ref().expect("empty scan pick");
                QueryTrace {
                    physical_plan: "IndexScan",
                    sort_elided: empty_ordered,
                    index_used: Some(pick.name.clone()),
                    predicates_pushed: pick
                        .pushed_columns
                        .iter()
                        .cloned()
                        .map(std::borrow::Cow::Owned)
                        .collect(),
                    indexes_considered: considered,
                    ..Default::default()
                }
            }
            ScanMode::Physical { .. } | ScanMode::Empty => QueryTrace {
                indexes_considered: considered,
                ..QueryTrace::scan()
            },
        },
        kind: PullKind::Scan(ScanState {
            source_group: None,
            identities_only: false,
            gate_answered_by_candidate_range: false,
            vector_candidate_source: false,
            table: table.to_string(),
            meta: Arc::new(meta),
            schema_columns,
            filter: resolved_filter,
            params,
            snapshot,
            mode,
            cursor_key_bytes: 0,
            residual_pick: ordered_pick.or(exact_pick).map(Box::new),
            authenticated_exact_index: None,
            committed_source_offers_every_row: offers_every_committed_row,
            pending_row: None,
            pending_vector_copy: None,
            supplement_columns: Vec::new(),
            pending_from_committed: true,
            ordered_merge,
            transaction,
        }),
    })
}

/// Tell every source in this kernel which quantized vector columns the answer
/// names, so a scan fills those and leaves the rest in the store.
///
/// Demand is not only the answer's column list: a predicate, a join key or an
/// ordering expression evaluated against the ROW needs the value too, and a
/// column that reaches such an expression as `Null` answers wrong. Each node
/// therefore adds what its own expressions name before handing the set down.
/// A vector-distance ordering is not among them -- that comparison is scored
/// from the index, never from the row.
fn declare_supplemented_columns(
    db: &dyn ReadExecutionTarget,
    node: &mut PullNode,
    wanted: &std::collections::BTreeSet<String>,
) {
    let mut wanted = wanted.clone();
    match &node.kind {
        PullKind::Project(state) => {
            for column in &state.columns {
                collect_expr_columns(&column.expr, &mut wanted);
            }
        }
        PullKind::Filter(state) => collect_expr_columns(&state.predicate, &mut wanted),
        PullKind::Sort(state) => {
            for key in &state.keys {
                collect_expr_columns(&key.expr, &mut wanted);
            }
        }
        PullKind::Join(state) => collect_expr_columns(&state.condition, &mut wanted),
        _ => {}
    }
    if let PullKind::Scan(state) = &mut node.kind {
        if let Some(filter) = state.filter.as_ref() {
            collect_expr_columns(filter, &mut wanted);
        }
        state.supplement_columns = db
            .quantized_vector_columns(&state.table)
            .into_iter()
            .filter(|column| wanted.contains(column))
            .collect();
        return;
    }
    // A search fetches and projects its own rows, so it carries the same
    // demand the scan does.
    if let PullKind::Vector(state) = &mut node.kind {
        state.supplement_columns = db
            .quantized_vector_columns(&state.table)
            .into_iter()
            .filter(|column| wanted.contains(column))
            .collect();
        // Candidate derivation needs identity and its own predicates. The
        // answer's projection is hydrated only after the global top-k.
        wanted.clear();
    }
    for input in pull_node_inputs_mut(&mut node.kind) {
        declare_supplemented_columns(db, input, &wanted);
    }
}

fn adopt_kernel_construction(
    node: &mut PullNode,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut credit = match &mut node.kind {
        PullKind::Window(window) => std::mem::take(&mut window.prepaid_construction),
        PullKind::Union(union) => std::mem::take(&mut union.prepaid_construction),
        _ => 0,
    };
    for child in pull_node_inputs_mut(&mut node.kind) {
        credit = checked_size_add(
            credit,
            adopt_kernel_construction(child)?,
            "kernel construction credit",
        )?;
    }
    Ok(credit)
}

/// Every column name an expression reads from a row. The vector-distance
/// operator is skipped: it is answered by the index, not by the row's value.
fn collect_expr_columns(expr: &Expr, into: &mut std::collections::BTreeSet<String>) {
    match expr {
        Expr::Column(reference) => {
            into.insert(reference.column.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, into);
            collect_expr_columns(right, into);
        }
        // Answered by the index, never by the row's own value, so a column
        // that appears only here is not demand.
        Expr::CosineDistance { .. } => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, into);
            }
        }
        Expr::UnaryOp { operand, .. } => collect_expr_columns(operand, into),
        Expr::IsNull { expr, .. } => collect_expr_columns(expr, into),
        Expr::Like { expr, .. } => collect_expr_columns(expr, into),
        Expr::InList { expr, .. } => collect_expr_columns(expr, into),
        Expr::InSubquery { expr, .. } => collect_expr_columns(expr, into),
        Expr::Row(elements) => {
            for element in elements {
                collect_expr_columns(element, into);
            }
        }
        _ => {}
    }
}

/// The nodes this one reads from, borrowed for change.
fn pull_node_inputs_mut(kind: &mut PullKind) -> Vec<&mut PullNode> {
    match kind {
        PullKind::Graph(state) => state.start_candidates.as_deref_mut().into_iter().collect(),
        PullKind::Vector(state) => match &mut state.preparation {
            VectorPreparation::Candidates(Some(candidates)) => vec![candidates.as_mut()],
            _ => Vec::new(),
        },
        PullKind::Project(state) => vec![state.input.as_mut()],
        PullKind::Sort(state) => vec![state.input.as_mut()],
        PullKind::Window(state) => vec![state.input.as_mut()],
        PullKind::Limit(state) => vec![state.input.as_mut()],
        PullKind::Filter(state) => vec![state.input.as_mut()],
        PullKind::Distinct(state) => vec![state.input.as_mut()],
        PullKind::Join(state) => vec![state.left.as_mut(), state.right.as_mut()],
        PullKind::Union(state) => state.inputs.iter_mut().collect(),
        PullKind::Scan(_) | PullKind::Static(_) => Vec::new(),
    }
}

/// One complete answer about the store itself, admitted row by row.
fn build_store_state(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    // `SHOW MEMORY_LIMIT` / `SHOW DISK_LIMIT` are the operator's one way to see
    // and then raise a budget that is already exhausted -- the same recovery
    // path `redb_writer_cache_bytes_for_limit`'s own floor exists to protect
    // ("a store declaring 1 KiB ... could not serve even the read that would
    // raise the limit again"). Their answer is one row of a few fixed-width
    // integers, never a table's worth of data, so it is exempt from the
    // ordinary per-row budget charge that every other store-state answer
    // still pays: an operator locked out of seeing their own limit has no way
    // left to raise it.
    let is_self_recovery_answer = matches!(
        plan,
        PhysicalPlan::ShowMemoryLimit | PhysicalPlan::ShowDiskLimit
    );
    let answered = if matches!(plan, PhysicalPlan::ShowContentSize) {
        let read_memory: Arc<dyn ReadMemoryBudget> = Arc::new(SourceDecodeBudget {
            context: context.clone(),
            source: BoundedWorkSource::TableScan,
        });
        let mut before_table = || {
            context
                .charge(BoundedWorkSource::TableScan, BoundedSourceTouch::IndexEntry)
                .map_err(|error| match error {
                    BoundedExecutionError::Refused(failure) => Error::ReadFailure(failure),
                    BoundedExecutionError::Cancelled => Error::ReadCancelled,
                    BoundedExecutionError::Engine(error) => error,
                    BoundedExecutionError::Unimplemented => {
                        Error::Other("unsupported content-size source work".into())
                    }
                })
        };
        db.content_size_answer(read_memory, &mut before_table)?
    } else {
        context.with_vector_copies(|vector_copy_owner| {
            db.store_state_answer(
                plan,
                vector_copy_owner,
                Arc::new(SourceDecodeBudget {
                    context: context.clone(),
                    source: BoundedWorkSource::TableScan,
                }),
            )
        })?
    };
    // The payload moves into this node; the credits that fund it stay held
    // until the node owns every row, exactly as long as the answer lived.
    let crate::database::OwnedQueryResult {
        result: answered,
        answer_charge: _answer_charge,
        vector_copy_charge: _vector_copy_charge,
        retained_read_memory: _retained_read_memory,
    } = answered;
    let mut rows = VecDeque::new();
    for values in answered.rows {
        // A row this function never reserved must never be released either:
        // `PulledRow::release` unconditionally hands `retained_bytes` back to
        // `context` when the row is consumed, so a self-recovery answer's row
        // is stamped with a zero charge here, not merely an unreserved one.
        let retained_bytes = if is_self_recovery_answer {
            0
        } else {
            let retained_bytes = value_refs_retained_bytes(values.iter(), values.len())?;
            context.charge(BoundedWorkSource::TableScan, BoundedSourceTouch::TableRow)?;
            context.reserve(BoundedWorkSource::TableScan, retained_bytes)?;
            retained_bytes
        };
        rows.push_back(PulledRow {
            window: None,
            values,
            retained_bytes,
            group_binding: None,
        });
    }
    Ok(PullNode {
        columns: answered.columns,
        trace: answered.trace,
        kind: PullKind::Static(StaticState { rows }),
    })
}

fn build_direct_index_scan(
    db: &dyn ReadExecutionTarget,
    table: &str,
    index: &str,
    range: &contextdb_planner::ScanRange,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let declaration = meta
        .indexes
        .iter()
        .find(|candidate| candidate.name == index)
        .ok_or_else(|| Error::IndexNotFound {
            table: table.to_string(),
            index: index.to_string(),
        })?;
    let (leading_column, direction) = declaration
        .columns
        .first()
        .cloned()
        .ok_or_else(|| Error::Other(format!("bounded index {index} has no leading column")))?;
    let shape = if let Some(value) = range.equality.as_ref() {
        IndexPredicateShape::Equality(value.clone())
    } else {
        IndexPredicateShape::Range {
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        }
    };
    let pick = db.coerce_pick_shape_to_column_type(
        table,
        &IndexPick {
            name: index.to_string(),
            columns: declaration.columns.clone(),
            shape,
            pushed_column: leading_column.clone(),
            pushed_columns: vec![leading_column.clone()],
            suffix_shapes: Vec::new(),
            trailing_range: None,
            tuple_prefix: None,
            prefix_empty: false,
            probe_values_normalized: false,
        },
    )?;
    let schema_columns = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let transaction = scan_transaction_overlay(db, table, context)?;
    // A declared index is walked as the run the predicate names, with an open
    // transaction as much as without one: the run answers for the committed
    // rows, and the same predicate is applied per row as the staged ones
    // merge in, so an open transaction no longer costs the reader a walk of
    // the whole table for a query the trace calls an index scan. An index the
    // engine synthesized is not a run this scan can seek, so its predicate
    // stays a residual the physical walk applies to every row it sees.
    let ordered = declaration.kind == contextdb_core::IndexKind::UserDeclared
        || declaration.columns.len() > 1;
    let residual_pick = (!ordered).then(|| Box::new(pick.clone()));
    let mut mode = if ordered {
        let (run_start, run_end, prefix_len) = declared_index_run(&pick, direction, false);
        ScanMode::Index {
            pick: Box::new(pick),
            column: leading_column,
            direction,
            cursor: BoundedOrderedRowCursor::for_index(index.to_string(), false)
                .seeking_run_with_prefix_len(run_start, run_end, prefix_len),
        }
    } else {
        ScanMode::Physical {
            cursor: db.bounded_physical_table_cursor(table)?,
        }
    };
    db.preflight_historical_index(table, index, snapshot,
        context.source_read_memory(BoundedWorkSource::IndexRange),
        &mut || context.charge_operator(BoundedWorkSource::IndexRange))?;
    capture_scan_membership(db, table, snapshot, &mut mode, context)?;
    let offers_every_committed_row = matches!(mode, ScanMode::Physical { .. });
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    Ok(PullNode {
        columns,
        trace: QueryTrace {
            physical_plan: "IndexScan",
            index_used: Some(index.to_string()),
            ..Default::default()
        },
        kind: PullKind::Scan(ScanState {
            source_group: None,
            identities_only: false,
            gate_answered_by_candidate_range: false,
            vector_candidate_source: false,
            table: table.to_string(),
            meta: Arc::new(meta),
            schema_columns,
            filter: None,
            params,
            snapshot,
            mode,
            cursor_key_bytes: 0,
            residual_pick,
            authenticated_exact_index: None,
            committed_source_offers_every_row: offers_every_committed_row,
            pending_row: None,
            pending_vector_copy: None,
            supplement_columns: Vec::new(),
            pending_from_committed: true,
            ordered_merge: None,
            transaction,
        }),
    })
}

fn ordered_scan<'a>(
    db: &dyn ReadExecutionTarget,
    input: &'a PhysicalPlan,
    keys: &'a [SortKey],
    params: &HashMap<String, Value>,
) -> Option<(&'a str, Option<&'a Expr>, OrderedIndexScan)> {
    let PhysicalPlan::Scan { table, filter, .. } = input else {
        return None;
    };
    // Which index answers an ORDER BY without sorting is the executor's own
    // decision, asked here rather than decided again. A WHERE that pins the
    // leading equality prefix leaves the following index column ordered, so
    // it asks the same prefix rule as the eager executor instead of falling
    // through to a buffered sort.
    let automatic_ordered = db.automatic_indexes_are_ordered();
    let decl = crate::executor::bounded_sort_elision_index_decl(db, input, keys).or_else(|| {
        let filter = filter.as_ref()?;
        let meta = db.table_meta(table)?;
        let pick = crate::executor::analyze_filter_for_index_with_capability(
            table,
            &meta,
            filter,
            params,
            Some(keys),
            true,
            automatic_ordered,
        )
        .pick?;
        meta.indexes
            .iter()
            .find(|decl| {
                decl.name == pick.name
                    && crate::executor::sort_keys_match_index_prefix_in_either_direction(
                        db, input, &decl.name, keys,
                    )
            })
            .cloned()
    })?;
    let (column, leading_direction) = decl.columns.first()?.clone();
    let order_keys = keys
        .iter()
        .map(|key| {
            let direction = match key.direction {
                SortDirection::Asc => contextdb_core::SortDirection::Asc,
                SortDirection::Desc => contextdb_core::SortDirection::Desc,
                SortDirection::CosineDistance => return None,
            };
            let column = match &key.expr {
                Expr::Column(reference) => reference.column.clone(),
                _ => return None,
            };
            Some((column, direction))
        })
        .collect::<Option<Vec<_>>>()?;
    let (order_column, order_direction) = order_keys.first()?;
    let order_index_direction = decl
        .columns
        .iter()
        .find(|(candidate, _)| candidate == order_column)
        .map(|(_, direction)| *direction)?;
    let reverse = order_index_direction != *order_direction;
    // The ordered route must bind the predicate against the index it will
    // actually walk. A predicate-only winner can be a different index, which
    // otherwise turns a selective ordered query into a full ordered scan.
    let sort_analysis = filter.as_ref().and_then(|filter| {
        let meta = db.table_meta(table)?;
        Some(crate::executor::analyze_filter_for_index_with_capability(
            table,
            &meta,
            filter,
            params,
            Some(keys),
            true,
            automatic_ordered,
        ))
    });
    let predicate_pick = sort_analysis
        .as_ref()
        .and_then(|analysis| analysis.pick.clone())
        .filter(|pick| pick.name == decl.name);
    let predicate_considered = sort_analysis.map_or_else(Vec::new, |analysis| analysis.considered);
    let direction = if reverse {
        match leading_direction {
            contextdb_core::SortDirection::Asc => contextdb_core::SortDirection::Desc,
            contextdb_core::SortDirection::Desc => contextdb_core::SortDirection::Asc,
        }
    } else {
        leading_direction
    };
    Some((
        table.as_str(),
        filter.as_ref(),
        OrderedIndexScan {
            column,
            direction,
            keys: order_keys,
            index: decl.name.clone(),
            reverse,
            predicate_pick,
            predicate_considered,
        },
    ))
}

/// Where in index order the run this predicate names begins and ends, stated
/// on the leading indexed component. A shape that names no run — an exclusion,
/// or a prefix already known to be empty — declares none, and the source walks
/// as it would with no predicate at all.
pub(super) fn declared_index_run(
    pick: &IndexPick,
    direction: contextdb_core::SortDirection,
    reverse: bool,
) -> (Bound<IndexKey>, Bound<IndexKey>, usize) {
    let directed = |value: &Value, direction| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
    };
    let ordered = |lower: &Bound<Value>, upper: &Bound<Value>| {
        let low = match lower {
            Bound::Included(value) => Bound::Included(vec![directed(value, direction)]),
            Bound::Excluded(value) => Bound::Excluded(vec![directed(value, direction)]),
            Bound::Unbounded => Bound::Unbounded,
        };
        let high = match upper {
            Bound::Included(value) => Bound::Included(vec![directed(value, direction)]),
            Bound::Excluded(value) => Bound::Excluded(vec![directed(value, direction)]),
            Bound::Unbounded => Bound::Unbounded,
        };
        // A descending component stores the largest value first, so the run
        // the predicate names starts at the value order's upper edge.
        match direction {
            contextdb_core::SortDirection::Asc => (low, high),
            contextdb_core::SortDirection::Desc => (high, low),
        }
    };
    if pick.prefix_empty {
        return (Bound::Unbounded, Bound::Unbounded, 0);
    }
    let (start, mut end, prefix_len) = match &pick.shape {
        IndexPredicateShape::Equality(_) | IndexPredicateShape::IsNull => {
            let mut runs = crate::executor::expanded_index_runs(pick).unwrap_or_default();
            if let Some((start, end)) = runs.pop() {
                let prefix_len = if pick.trailing_range.is_some() {
                    let len = |bound: &Bound<IndexKey>| match bound {
                        Bound::Included(key) | Bound::Excluded(key) => key.len(),
                        Bound::Unbounded => 0,
                    };
                    len(&start).min(len(&end))
                } else {
                    1 + pick.suffix_shapes.len()
                };
                (start, end, prefix_len)
            } else {
                (Bound::Unbounded, Bound::Unbounded, 0)
            }
        }
        IndexPredicateShape::Range { lower, upper } => {
            let (start, end) = ordered(lower, upper);
            (start, end, 1)
        }
        IndexPredicateShape::InList(values) => {
            let mut edges = values
                .iter()
                .map(|value| directed(value, direction))
                .collect::<Vec<_>>();
            edges.sort();
            match (edges.first(), edges.last()) {
                (Some(first), Some(last)) => (
                    Bound::Included(vec![first.clone()]),
                    Bound::Included(vec![last.clone()]),
                    1,
                ),
                _ => (Bound::Unbounded, Bound::Unbounded, 0),
            }
        }
        IndexPredicateShape::NotEqual(_) | IndexPredicateShape::IsNotNull => {
            (Bound::Unbounded, Bound::Unbounded, 0)
        }
    };
    if reverse {
        pad_reverse_inclusive_end(pick, &mut end);
    }
    (start, end, prefix_len)
}

/// Complete an inclusive physical upper edge when every missing component
/// has the same NULL-high representation. The logical predicate prefix stays
/// separate, so the sentinel affects only where a reverse walk starts.
fn pad_reverse_inclusive_end(pick: &IndexPick, end: &mut Bound<IndexKey>) {
    let Bound::Included(edge) = end else {
        return;
    };
    let edge_len = edge.len();
    if edge_len >= pick.columns.len()
        || !pick
            .columns
            .iter()
            .skip(edge_len)
            .all(|(_, direction)| *direction == contextdb_core::SortDirection::Asc)
    {
        return;
    }
    edge.extend(
        pick.columns
            .iter()
            .skip(edge_len)
            .map(|_| DirectedValue::Asc(TotalOrdAsc(Value::Null))),
    );
}

fn index_key_matches_pick(key: &IndexKey, pick: &IndexPick) -> bool {
    if pick.prefix_empty || key.is_empty() {
        return false;
    }
    // A row-value list admits its listed tuples only, not every combination
    // of their per-column values.
    if let Some(tuples) = pick.tuple_prefix() {
        let arity = tuples.first().map_or(0, Vec::len);
        if key.len() < arity {
            return false;
        }
        let leading = key[..arity]
            .iter()
            .map(|value| match value {
                DirectedValue::Asc(TotalOrdAsc(value))
                | DirectedValue::Desc(TotalOrdDesc(value)) => value,
            })
            .collect::<Vec<_>>();
        if !tuples.iter().any(|tuple| {
            tuple
                .iter()
                .zip(&leading)
                .all(|(expected, actual)| values_equal(expected, actual))
        }) {
            return false;
        }
    }
    let mut values = key.iter().map(|value| match value {
        DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value,
    });
    let Some(value) = values.next() else {
        return false;
    };
    for expected in &pick.suffix_shapes {
        let Some(actual) = values.next() else {
            return false;
        };
        let matches = match expected {
            IndexPredicateShape::Equality(expected) => values_equal(expected, actual),
            IndexPredicateShape::InList(expected) => expected
                .iter()
                .any(|candidate| values_equal(candidate, actual)),
            IndexPredicateShape::IsNull => actual == &Value::Null,
            _ => false,
        };
        if !matches {
            return false;
        }
    }
    match &pick.shape {
        IndexPredicateShape::Equality(expected) => values_equal(value, expected),
        IndexPredicateShape::NotEqual(expected) => !values_equal(value, expected),
        IndexPredicateShape::Range { lower, upper } => {
            bound_allows_lower(value, lower) && bound_allows_upper(value, upper)
        }
        IndexPredicateShape::InList(expected) => expected
            .iter()
            .any(|candidate| values_equal(value, candidate)),
        IndexPredicateShape::IsNull => value == &Value::Null,
        IndexPredicateShape::IsNotNull => value != &Value::Null,
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    left == right || compare_values(left, right) == Some(Ordering::Equal)
}

fn bound_allows_lower(value: &Value, bound: &std::ops::Bound<Value>) -> bool {
    match bound {
        std::ops::Bound::Included(lower) => matches!(
            compare_values(value, lower),
            Some(Ordering::Equal | Ordering::Greater)
        ),
        std::ops::Bound::Excluded(lower) => compare_values(value, lower) == Some(Ordering::Greater),
        std::ops::Bound::Unbounded => true,
    }
}

fn bound_allows_upper(value: &Value, bound: &std::ops::Bound<Value>) -> bool {
    match bound {
        std::ops::Bound::Included(upper) => matches!(
            compare_values(value, upper),
            Some(Ordering::Equal | Ordering::Less)
        ),
        std::ops::Bound::Excluded(upper) => compare_values(value, upper) == Some(Ordering::Less),
        std::ops::Bound::Unbounded => true,
    }
}

fn pull(
    db: &dyn ReadExecutionTarget,
    context: &Arc<RequestContext>,
    root: &mut PullNode,
) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
    root.next(db, context)
}

fn planned_value_clone_bytes(value: &Value) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        Value::Text(value) => Ok(value.len()),
        Value::Vector(values) => checked_size_mul(
            values.len(),
            std::mem::size_of::<f32>(),
            "canonical vector clone",
        ),
        Value::Json(value) => Ok(crate::read_image_memory::json_clone_heap_bytes(value)),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

#[allow(dead_code)]
fn planned_values_clone_bytes(
    values: &[Value],
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        values.len(),
        std::mem::size_of::<Value>(),
        "canonical row clone",
    )?;
    for value in values {
        bytes = checked_size_add(
            bytes,
            planned_value_clone_bytes(value)?,
            "canonical row clone",
        )?;
    }
    Ok(bytes)
}

#[allow(dead_code)]
fn planned_query_clone_bytes(
    result: &QueryResult,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = std::mem::size_of::<CanonicalQueryResult>();
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.columns.len(),
            std::mem::size_of::<String>(),
            "canonical columns",
        )?,
        "canonical columns",
    )?;
    for column in &result.columns {
        bytes = checked_size_add(bytes, column.len(), "canonical column")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.rows.len(),
            std::mem::size_of::<Vec<Value>>(),
            "canonical rows",
        )?,
        "canonical rows",
    )?;
    for row in &result.rows {
        bytes = checked_size_add(bytes, planned_values_clone_bytes(row)?, "canonical row")?;
    }
    bytes = checked_size_add(bytes, result.trace.physical_plan.len(), "canonical trace")?;
    if let Some(index) = result.trace.index_used.as_ref() {
        bytes = checked_size_add(bytes, index.len(), "canonical trace index")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.predicates_pushed.len(),
            std::mem::size_of::<String>(),
            "canonical trace predicates",
        )?,
        "canonical trace",
    )?;
    for predicate in &result.trace.predicates_pushed {
        bytes = checked_size_add(bytes, predicate.len(), "canonical trace predicate")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.indexes_considered.len(),
            std::mem::size_of::<crate::read_contract::CanonicalIndexCandidate>(),
            "canonical considered indexes",
        )?,
        "canonical trace",
    )?;
    for index in &result.trace.indexes_considered {
        bytes = checked_size_add(bytes, index.name.len(), "canonical considered index")?;
        bytes = checked_size_add(
            bytes,
            index.rejected_reason.len(),
            "canonical considered-index reason",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.groups.len(),
            std::mem::size_of::<crate::database::GroupDisclosure>(),
            "canonical group slots",
        )?,
        "canonical trace",
    )?;
    for group in &result.trace.groups {
        bytes = checked_size_add(bytes, group.partition_key.len(), "canonical group key")?;
    }
    if let Some(source) = result.trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, source.table.len(), "canonical vector source")?;
        bytes = checked_size_add(bytes, source.column.len(), "canonical vector source")?;
    }
    if let Some(cascade) = result.cascade.as_ref() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                cascade.dropped_indexes.len(),
                std::mem::size_of::<String>(),
                "canonical cascade",
            )?,
            "canonical cascade",
        )?;
        for index in &cascade.dropped_indexes {
            bytes = checked_size_add(bytes, index.len(), "canonical cascade")?;
        }
    }
    Ok(bytes)
}

#[allow(dead_code)]
pub(crate) fn canonical_query_capacity_bytes(
    result: &CanonicalQueryResult,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<CanonicalQueryResult>(),
        strings_capacity_bytes(&result.columns, "canonical columns")?,
        "canonical query",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.rows.capacity(),
            std::mem::size_of::<Vec<Value>>(),
            "canonical rows",
        )?,
        "canonical query",
    )?;
    for row in &result.rows {
        bytes = checked_size_add(
            bytes,
            values_capacity_allocation_bytes(row)?,
            "canonical row",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        result.trace.physical_plan.capacity(),
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&result.trace.index_used),
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        strings_capacity_bytes(
            &result.trace.predicates_pushed,
            "canonical trace predicates",
        )?,
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.indexes_considered.capacity(),
            std::mem::size_of::<crate::read_contract::CanonicalIndexCandidate>(),
            "canonical considered indexes",
        )?,
        "canonical trace",
    )?;
    for candidate in &result.trace.indexes_considered {
        bytes = checked_size_add(bytes, candidate.name.capacity(), "canonical trace index")?;
        bytes = checked_size_add(
            bytes,
            candidate.rejected_reason.capacity(),
            "canonical trace rejection",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.groups.len(),
            std::mem::size_of::<crate::database::GroupDisclosure>(),
            "canonical group slots",
        )?,
        "canonical trace",
    )?;
    for group in &result.trace.groups {
        bytes = checked_size_add(bytes, group.partition_key.capacity(), "canonical group key")?;
    }
    if let Some(source) = result.trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, source.table.capacity(), "canonical vector source")?;
        bytes = checked_size_add(bytes, source.column.capacity(), "canonical vector source")?;
    }
    if let Some(cascade) = result.cascade.as_ref() {
        bytes = checked_size_add(
            bytes,
            strings_capacity_bytes(&cascade.dropped_indexes, "canonical cascade")?,
            "canonical cascade",
        )?;
    }
    Ok(bytes)
}

fn encoded_query_size(
    context: &Arc<RequestContext>,
    result: &QueryResult,
) -> std::result::Result<u64, BoundedExecutionError> {
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(Error::Other("canonical query row arity is invalid".to_string()).into());
        }
    }
    // Measured, not copied. The encoder writes into a counter that keeps
    // nothing, so a copy of the whole result would be a second live copy of
    // every row it holds -- remade after each row appended -- taken purely to
    // be looked at. Borrowed, this costs the rows nothing and the number is
    // the same number, because a borrowed slice and a `Vec` write identically.
    let measured = serialized_size(&crate::read_contract::CanonicalQueryResultView::from(
        result,
    ))?;
    u64::try_from(measured).map_err(|_| {
        RequestContext::limit_failure(ReadFailureLimit::ResultBytes, context.limits.result_bytes)
    })
}

fn encoded_page_size(
    context: &Arc<RequestContext>,
    page: &CursorPage,
    observation: &CursorObservation,
) -> std::result::Result<u64, BoundedExecutionError> {
    page.validate()
        .map_err(|error| Error::Other(error.to_string()))?;
    let measured = serialized_size(
        &crate::read_contract::cursor_observation::BorrowedObservedPage::new(page, observation),
    )?;
    u64::try_from(measured).map_err(|_| {
        RequestContext::limit_failure(
            ReadFailureLimit::CursorPageBytes,
            context.limits.cursor_page_bytes,
        )
    })
}

#[allow(dead_code)]
fn strip_internal_row_id(columns: &mut Vec<String>, rows: &mut [Vec<Value>]) {
    if let Some(position) = columns.iter().position(|column| column == "row_id") {
        columns.remove(position);
        for row in rows {
            if position < row.len() {
                row.remove(position);
            }
        }
    }
}

fn bounded_error_text(error: &BoundedExecutionError) -> String {
    match error {
        BoundedExecutionError::Engine(error) => error.to_string(),
        BoundedExecutionError::Refused(failure) => format!("{failure:?}"),
        BoundedExecutionError::Cancelled => "bounded read cancelled".to_string(),
        BoundedExecutionError::Unimplemented => {
            "bounded read implementation is unavailable".to_string()
        }
    }
}

fn deadline_clock_duration(clock: &dyn DeadlineClock, started_ms: u64) -> Duration {
    let elapsed_ms = clock.now_ms().saturating_sub(started_ms);
    Duration::from_millis(elapsed_ms)
}

fn bounded_query_outcome(
    result: &std::result::Result<BoundedExecutionResult, BoundedExecutionError>,
) -> QueryOutcome {
    match result {
        Ok(result) => QueryOutcome::Success {
            row_count: result.result.rows.len(),
        },
        Err(error) => QueryOutcome::Error {
            error: bounded_error_text(error),
        },
    }
}

/// The bytes two same-length result containers occupy at these capacities.
fn result_container_bytes(
    rows: usize,
    charges: usize,
) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        checked_size_mul(
            rows,
            std::mem::size_of::<Vec<Value>>(),
            "bounded result rows",
        )?,
        checked_size_mul(
            charges,
            std::mem::size_of::<usize>(),
            "bounded result row charges",
        )?,
        "bounded result containers",
    )
}

/// Make room for one more result row, paying for the room first.
///
/// The charge follows the answer as it is assembled rather than the ceiling
/// the caller permitted, so a read costs what it does. Growth is doubled so
/// the charge is re-stated a logarithmic number of times over a long result
/// rather than once per row.
fn grow_result_containers(
    context: &Arc<RequestContext>,
    rows: &mut Vec<Vec<Value>>,
    charges: &mut Vec<usize>,
    held_bytes: &mut usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if rows.len() < rows.capacity() && charges.len() < charges.capacity() {
        return Ok(());
    }
    let target = rows
        .capacity()
        .max(charges.capacity())
        .max(4)
        .saturating_mul(2);
    let planned = result_container_bytes(target, target)?;
    let growth = planned.saturating_sub(*held_bytes);
    context.reserve(BoundedWorkSource::TableScan, growth)?;
    if rows
        .try_reserve_exact(target.saturating_sub(rows.len()))
        .is_err()
    {
        context.release(growth)?;
        return Err(Error::Other("bounded result-row allocation failed".to_string()).into());
    }
    // The rows allocation has already grown in the caller. Keep its admitted
    // credit and record it even if the parallel charge ledger cannot grow.
    // The caller can destroy both containers before request cleanup, or retry
    // without losing existing rows, payloads or their charge bookkeeping.
    *held_bytes = planned;
    if charges
        .try_reserve_exact(target.saturating_sub(charges.len()))
        .is_err()
    {
        return Err(Error::Other("bounded result-charge allocation failed".to_string()).into());
    }
    let actual = result_container_bytes(rows.capacity(), charges.capacity())?;
    match actual.cmp(&planned) {
        std::cmp::Ordering::Greater => {
            context.reserve(BoundedWorkSource::TableScan, actual - planned)?;
        }
        std::cmp::Ordering::Less => context.release(planned - actual)?,
        std::cmp::Ordering::Equal => {}
    }
    *held_bytes = actual;
    Ok(())
}

/// Ceilings that refuse nothing.
///
/// The established embedding door promises an uncapped answer, so the read it
/// runs is not given limits to stop it. What it shares with a bounded read is
/// the KERNEL: one statement is planned once and drained once, whichever door
/// asked for it, so the two can never answer differently or count differently.
fn uncapped_ceilings() -> ReadLimits {
    ReadLimits {
        result_rows: u64::MAX,
        result_bytes: u64::MAX,
        work: u64::MAX,
        active_ms: u64::MAX,
        memory: u64::MAX,
        cursor_page_rows: u64::MAX,
        cursor_page_bytes: u64::MAX,
        cursor_idle_ms: u64::MAX,
        cursor_lifetime_ms: u64::MAX,
    }
}

/// Whether the shared kernel can answer this plan at all.
///
/// The kernel reads; a plan that writes, or that names a shape it has no
/// source for, goes to the executor as it always did. Asked before anything
/// is built, so a plan the kernel cannot take costs nothing to refuse.
pub(crate) fn kernel_can_answer(plan: &PhysicalPlan) -> bool {
    ensure_supported_plan(plan).is_ok()
}

struct StatementProofReuse<'a> {
    db: &'a dyn ReadExecutionTarget,
}

impl Drop for StatementProofReuse<'_> {
    fn drop(&mut self) {
        self.db.release_statement_proof_reuse();
    }
}

/// Drain a plan the CALLER already chose, with no ceilings.
///
/// This is the established embedding door's half of the shared kernel: the
/// statement was planned by the caller, so nothing is planned again here, and
/// the answer is drained once from the same sources a bounded read draws from.
/// One planning, one drain -- which is what stops a public read from being
/// answered twice, once for the rows and once for the trace.
pub(crate) fn drain_chosen_plan(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    clock: Arc<dyn DeadlineClock>,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    read_memory: Arc<dyn ReadMemoryBudget>,
) -> std::result::Result<crate::database::OwnedQueryResult, BoundedExecutionError> {
    // These fixed control answers remain available when the operator has
    // exhausted the store budget, including the kernel's scaffolding. They
    // share the same recovery exemption as their uncharged static row.
    if matches!(
        plan,
        PhysicalPlan::ShowMemoryLimit | PhysicalPlan::ShowDiskLimit
    ) {
        return db
            .store_state_answer(
                plan,
                &mut crate::vector_read_memory::admission(read_memory.clone()),
                read_memory.clone(),
            )
            .map_err(BoundedExecutionError::from);
    }
    let _release_proofs = StatementProofReuse { db };
    let context = RequestContext::new(
        Arc::clone(&read_memory),
        uncapped_ceilings(),
        ReadResumption::OneShot,
        clock,
        OwnerReadCancellation::new(),
        tx,
        #[cfg(feature = "test-seams")]
        probe,
    )?;
    let _vector_read = enter_vector_read(db, snapshot, &context, plan_needs_vector_read(plan))?;
    let params = Arc::new(params.clone());
    let mut root = build_kernel(db, plan, Arc::clone(&params), snapshot, &context)?;
    let prepaid = adopt_kernel_construction(&mut root)?;
    let root_bytes = boxed_pull_bytes(&root)?;
    // The adopted credit funds the live root until it is destroyed, including
    // a failed admission of the remaining continuation bytes.
    if root_bytes > prepaid {
        context.reserve(BoundedWorkSource::TableScan, root_bytes - prepaid)?;
    } else if prepaid > root_bytes {
        context.release(prepaid - root_bytes)?;
    }

    // The answer's own column list is the demand every source below is filled
    // against; each node adds what its expressions name on the way down.
    {
        let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
        declare_supplemented_columns(db, &mut root, &named);
    }
    #[cfg(feature = "test-seams")]
    crate::database::Database::observe_pull_kernel_entered_for_test();
    let (result_columns, result_column_bytes) =
        clone_strings_with_reservation(&root.columns, &context, "result columns")?;
    let planned_trace_bytes = query_trace_capacity_bytes(&root.trace)?;
    context.reserve(BoundedWorkSource::TableScan, planned_trace_bytes)?;
    let result_trace = root.trace.clone();
    {
        let admitted_bytes = planned_trace_bytes;
        let retained_bytes = query_trace_capacity_bytes(&result_trace)?;
        if retained_bytes > admitted_bytes {
            context.reserve(
                BoundedWorkSource::TableScan,
                retained_bytes - admitted_bytes,
            )?;
        } else if admitted_bytes > retained_bytes {
            context.release(admitted_bytes - retained_bytes)?;
        }
    }
    let mut result = crate::database::OwnedQueryResult::new(QueryResult {
        columns: result_columns,
        rows: Vec::new(),
        rows_affected: 0,
        trace: result_trace,
        cascade: None,
    });
    let internal_row_id = result.columns.iter().position(|column| column == "row_id");
    if let Some(position) = internal_row_id {
        result.columns.remove(position);
    }
    {
        let admitted_bytes = result_column_bytes;
        let retained_bytes = strings_capacity_bytes(&result.columns, "result columns")?;
        if retained_bytes > admitted_bytes {
            context.reserve(
                BoundedWorkSource::TableScan,
                retained_bytes - admitted_bytes,
            )?;
        } else if admitted_bytes > retained_bytes {
            context.release(admitted_bytes - retained_bytes)?;
        }
    }
    let mut retained_rows = Vec::<usize>::new();
    let mut held_container_bytes = 0usize;
    while let Some(mut row) = pull(db, &context, &mut root)? {
        if let Some(position) = internal_row_id
            && position < row.values.len()
        {
            row.values.remove(position);
        }
        if let Some(binding) = row.group_binding {
            context.stage_group_return(binding)?;
        }
        grow_result_containers(
            &context,
            &mut result.rows,
            &mut retained_rows,
            &mut held_container_bytes,
        )?;
        retained_rows.push(row.retained_bytes);
        result.rows.push(row.values);
    }
    #[cfg(feature = "test-seams")]
    crate::database::Database::observe_pull_kernel_drained_for_test();
    result.trace.physical_plan = root.published_plan();
    result.trace.vector_search = root.published_vector_search().cloned();
    result.trace.rows_examined = context.rows_examined()?;
    context.refresh_group_trace(&mut result.trace)?;
    context.check_final_boundary()?;
    context.transfer_answer(&mut result)?;
    context.publish_group_returns()?;
    // The ledger is scratch. Destroy it before the request refunds its
    // remaining scratch credit; answer payload credit now has its own owner.
    drop(retained_rows);
    drop(root);
    Ok(result)
}

// These answers contain committed metadata and atomically copied progress,
// never a query's row bodies or an in-flight native graph.
fn inspection_reads_committed_state(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::ShowMemoryLimit
            | PhysicalPlan::ShowVectorIndexes
            | PhysicalPlan::ShowVectorPartitions { .. }
    )
}

fn publication_tables(plan: &PhysicalPlan) -> BTreeSet<String> {
    fn collect(plan: &PhysicalPlan, tables: &mut BTreeSet<String>) {
        match plan {
            PhysicalPlan::Scan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
                tables.insert(table.clone());
            }
            PhysicalPlan::VectorSearch {
                table,
                query_expr,
                candidates,
                ..
            }
            | PhysicalPlan::HnswSearch {
                table,
                query_expr,
                candidates,
                ..
            } => {
                tables.insert(table.clone());
                if let Expr::RowVectorSource { table, .. } = query_expr {
                    tables.insert(table.clone());
                }
                if let Some(candidates) = candidates {
                    collect(candidates, tables);
                }
            }
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::GroupedCount { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::WindowRank { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::MaterializeCte { input, .. } => collect(input, tables),
            PhysicalPlan::Join { left, right, .. } => {
                collect(left, tables);
                collect(right, tables);
            }
            PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => {
                for input in inputs {
                    collect(input, tables);
                }
            }
            // These nodes can resolve sources through aliases no longer
            // carried by the final physical node.
            PhysicalPlan::GraphBfs {
                start_candidates, ..
            } => {
                tables.insert("*".to_owned());
                if let Some(candidates) = start_candidates {
                    collect(candidates, tables);
                }
            }
            PhysicalPlan::CteRef { .. } => {
                tables.insert("*".to_owned());
            }
            // Store-state reads can expose table-bound inventories.
            _ => {
                tables.insert("*".to_owned());
            }
        }
    }

    let mut tables = BTreeSet::new();
    collect(plan, &mut tables);
    tables
}

/// Scalar DATA reads need no mutable vector graph. Vector ORDER BY may use
/// an indexed graph unless EXACT was selected explicitly, including in CTEs
/// and scalar subqueries. This follows the planner's vector-order predicate.
fn statement_needs_vector_graph(statement: &contextdb_parser::ast::Statement) -> bool {
    use contextdb_parser::ast::{
        Cte, Expr, SelectBody, SortDirection, Statement, VectorSearchMode,
    };
    fn expr(value: &Expr) -> bool {
        match value {
            Expr::InSubquery {
                expr: input,
                subquery,
                ..
            } => expr(input) || body(subquery),
            Expr::BinaryOp { left, right, .. }
            | Expr::CosineDistance { left, right }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            } => expr(left) || expr(right),
            Expr::UnaryOp { operand, .. }
            | Expr::IsNull { expr: operand, .. }
            | Expr::RowVectorSource { key: operand, .. } => expr(operand),
            Expr::FunctionCall { args, .. } | Expr::Row(args) => args.iter().any(expr),
            Expr::InList {
                expr: input, list, ..
            } => expr(input) || list.iter().any(expr),
            _ => false,
        }
    }
    fn body(select: &SelectBody) -> bool {
        (select
            .order_by
            .first()
            .is_some_and(|item| item.direction == SortDirection::CosineDistance)
            && !matches!(select.use_vector, Some(VectorSearchMode::Exact)))
            || select.columns.iter().any(|column| expr(&column.expr))
            || select.where_clause.as_ref().is_some_and(expr)
            || select.order_by.iter().any(|item| expr(&item.expr))
            || select.joins.iter().any(|join| expr(&join.on))
    }
    match statement {
        Statement::Select(select) => {
            body(&select.body)
                || select.ctes.iter().any(|cte| match cte {
                    Cte::SqlCte { query, .. } => body(query),
                    Cte::MatchCte { match_clause, .. } => {
                        match_clause.where_clause.as_ref().is_some_and(expr)
                            || match_clause
                                .return_cols
                                .iter()
                                .any(|column| expr(&column.expr))
                    }
                })
        }
        _ => false,
    }
}

fn statement_needs_vector_read(statement: &contextdb_parser::ast::Statement) -> bool {
    use contextdb_parser::ast::{Cte, Expr, SelectBody, SortDirection, Statement};
    fn expr(value: &Expr) -> bool {
        match value {
            Expr::CosineDistance { .. } | Expr::RowVectorSource { .. } => true,
            Expr::InSubquery { expr: input, subquery, .. } => expr(input) || body(subquery),
            Expr::BinaryOp { left, right, .. } | Expr::Like { expr: left, pattern: right, .. } => expr(left) || expr(right),
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => expr(operand),
            Expr::FunctionCall { args, .. } | Expr::Row(args) => args.iter().any(expr),
            Expr::InList { expr: input, list, .. } => expr(input) || list.iter().any(expr),
            _ => false,
        }
    }
    fn body(select: &SelectBody) -> bool {
        if select.limit == Some(0) { return false; }
        select.order_by.iter().any(|item| item.direction == SortDirection::CosineDistance || expr(&item.expr))
            || select.columns.iter().any(|column| expr(&column.expr))
            || select.where_clause.as_ref().is_some_and(expr)
            || select.joins.iter().any(|join| expr(&join.on))
    }
    match statement {
        Statement::Select(select) => body(&select.body) || select.ctes.iter().any(|cte| match cte {
            Cte::SqlCte { query, .. } => body(query),
            Cte::MatchCte { match_clause, .. } => match_clause.where_clause.as_ref().is_some_and(expr)
                || match_clause.return_cols.iter().any(|column| expr(&column.expr)),
        }),
        _ => false,
    }
}

pub(super) fn execute(
    db: &dyn ReadExecutionTarget,
    sql: &str,
    params: &HashMap<String, Value>,
    read_memory: Arc<dyn ReadMemoryBudget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedExecutionResult, BoundedExecutionError> {
    let _release_proofs = StatementProofReuse { db };
    let statement = contextdb_parser::parse(sql)?;
    db.plugin().on_query(sql)?;
    let plugin_started_ms = clock.now_ms();
    let plugin_clock = Arc::clone(&clock);
    let withdrawn = cancellation.clone();
    let execution = (|| {
        let context = RequestContext::new(
            Arc::clone(&read_memory),
            limits,
            ReadResumption::OneShot,
            clock,
            cancellation,
            db.active_read_transaction(),
            #[cfg(feature = "test-seams")]
            probe,
        )?;
        let schema_capture = db.capture_read_schema();
        let membership_capture = (!db.retained_relational_membership_source())
            .then(|| db.relational_membership_store().begin_membership_capture());
        let Some((snapshot, publication_lsn, _snapshot_registration, writer_root)) = db
            .bounded_read_snapshot_registration(
                statement_needs_vector_graph(&statement),
                &context.registration_budget(),
                &withdrawn,
                context.source_read_memory(BoundedWorkSource::TableScan),
            )?
        else {
            return Err(context.withdrawn_failure());
        };
        let _writer_read = db.enter_writer_read(
            writer_root,
            context.source_read_memory(BoundedWorkSource::TableScan),
        );
        let _vector_read = enter_vector_read(db, snapshot, &context,
            statement_needs_vector_read(&statement))?;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = context.probe() {
            probe.registered_snapshot_for_test(snapshot);
            probe.after_snapshot_registration();
        }
        let statement_bytes = statement_capacity_bytes(&statement)?;
        context.reserve(BoundedWorkSource::TableScan, statement_bytes)?;
        // The resolved statement and the physical plan are built out of this
        // statement, so the statement's own size is what the request is
        // admitted for before either exists; the reservation is trued up to
        // what they hold as soon as they do. A request whose ceiling cannot
        // cover the shape it asked for is refused before the engine builds
        // that shape, not after.
        let planned_prepared_bytes = checked_size_mul(statement_bytes, 2, "bounded read plan")?;
        context.reserve(BoundedWorkSource::TableScan, planned_prepared_bytes)?;
        let (resolved, plan) = context.with_vector_copies(|vector_copy_owner| {
            db.prepare_bounded_read_plan(
                &statement,
                params,
                vector_copy_owner,
                Arc::new(SourceDecodeBudget {
                    context: context.clone(),
                    source: BoundedWorkSource::TableScan,
                }),
            )
        })?;
        let _vector_plan_read = enter_vector_read(db, snapshot, &context,
            plan_needs_vector_read(&plan))?;
        let publication = context.publication_scope(
            publication_lsn,
            publication_tables(&plan),
            inspection_reads_committed_state(&plan),
        );
        let resolved_bytes = statement_capacity_bytes(&resolved)?;
        ensure_supported_plan(&plan)?;
        let plan_bytes = physical_plan_capacity_bytes(&plan)?;
        {
            let admitted_bytes = planned_prepared_bytes;
            let retained_bytes = checked_size_add(resolved_bytes, plan_bytes, "bounded read plan")?;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        drop(statement);
        context.release(statement_bytes)?;
        drop(resolved);
        context.release(resolved_bytes)?;

        let planned_params_bytes = params_retained_bytes(params)?;
        context.reserve(BoundedWorkSource::TableScan, planned_params_bytes)?;
        let params = Arc::new(params.clone());
        let params_bytes = params_retained_bytes(&params)?;
        {
            let admitted_bytes = planned_params_bytes;
            let retained_bytes = params_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        // The kernel is built out of the plan and holds a copy of every table
        // shape it reads, so the plan's own size is the floor the request is
        // admitted for before the kernel is materialized; the reservation is
        // trued up to what the kernel holds once it exists.
        context.reserve(BoundedWorkSource::TableScan, plan_bytes)?;
        let mut root = build_kernel(db, &plan, Arc::clone(&params), snapshot, &context)?;
        // The answer's own column list is the demand every source below is filled
        // against; each node adds what its expressions name on the way down.
        {
            let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
            declare_supplemented_columns(db, &mut root, &named);
        }
        drop(membership_capture);
        drop(schema_capture);
        #[cfg(feature = "test-seams")]
        if let Some(probe) = context.probe() {
            probe.after_source_capture();
        }
        let root_bytes = pull_continuation_bytes(&root)?;
        let prepaid = adopt_kernel_construction(&mut root)?;
        let construction_bytes =
            checked_size_add(plan_bytes, prepaid, "kernel construction adoption")?;
        // Keep adopted credit while the root is alive if admission refuses.
        if root_bytes > construction_bytes {
            context.reserve(
                BoundedWorkSource::TableScan,
                root_bytes - construction_bytes,
            )?;
        } else if construction_bytes > root_bytes {
            context.release(construction_bytes - root_bytes)?;
        }
        drop(plan);
        context.release(plan_bytes)?;

        let (result_columns, result_column_bytes) =
            clone_strings_with_reservation(&root.columns, &context, "bounded result columns")?;
        let planned_result_trace_bytes = query_trace_capacity_bytes(&root.trace)?;
        context.reserve(BoundedWorkSource::TableScan, planned_result_trace_bytes)?;
        let result_trace = root.trace.clone();
        let result_trace_bytes = query_trace_capacity_bytes(&result_trace)?;
        {
            let admitted_bytes = planned_result_trace_bytes;
            let retained_bytes = result_trace_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        let mut result = crate::database::OwnedQueryResult::new(QueryResult {
            columns: result_columns,
            rows: Vec::new(),
            rows_affected: 0,
            trace: result_trace,
            cascade: None,
        });
        let internal_row_id = result.columns.iter().position(|column| column == "row_id");
        if let Some(position) = internal_row_id {
            result.columns.remove(position);
        }
        let retained_result_column_bytes =
            strings_capacity_bytes(&result.columns, "bounded result columns")?;
        {
            let admitted_bytes = result_column_bytes;
            let retained_bytes = retained_result_column_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        // The read is charged for the answer it is building, not for the
        // ceiling the caller allowed it. Sizing these containers to
        // `result_rows` up front would refuse a caller who only said "do not
        // truncate my answer" -- a generous row ceiling would be billed as
        // memory before a single row existed, and a one-row answer would
        // never come back. So the containers start empty and are charged
        // again each time they actually have to grow; a growth the request
        // cannot pay for is still refused before the allocation happens.
        let mut held_container_bytes = 0usize;
        let mut retained_rows = Vec::<usize>::new();

        loop {
            let result_rows = u64::try_from(result.rows.len()).map_err(|_| {
                RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
            })?;
            if result_rows >= limits.result_rows {
                match root.has_more_hint() {
                    Some(false) => break,
                    Some(true) => {
                        return Err(RequestContext::limit_failure(
                            ReadFailureLimit::ResultRows,
                            limits.result_rows,
                        ));
                    }
                    None => {
                        if pull(db, &context, &mut root)?.is_some() {
                            return Err(RequestContext::limit_failure(
                                ReadFailureLimit::ResultRows,
                                limits.result_rows,
                            ));
                        }
                        break;
                    }
                }
            }

            let Some(mut row) = pull(db, &context, &mut root)? else {
                break;
            };
            if let Some(position) = internal_row_id
                && position < row.values.len()
            {
                row.values.remove(position);
            }
            grow_result_containers(
                &context,
                &mut result.rows,
                &mut retained_rows,
                &mut held_container_bytes,
            )?;
            retained_rows.push(row.retained_bytes);
            result.rows.push(row.values);
            result.trace.physical_plan = root.published_plan();
            result.trace.vector_search = root.published_vector_search().cloned();
            result.trace.rows_examined = context.rows_examined()?;
            context.refresh_group_trace(&mut result.trace)?;
            let encoded = encoded_query_size(&context, &result)?;
            if encoded > limits.result_bytes {
                return Err(RequestContext::limit_failure(
                    ReadFailureLimit::ResultBytes,
                    limits.result_bytes,
                ));
            }
            context.note_result_shape(result.rows.len(), encoded);
        }

        result.trace.physical_plan = root.published_plan();
        result.trace.vector_search = root.published_vector_search().cloned();
        result.trace.rows_examined = context.rows_examined()?;
        context.refresh_group_trace(&mut result.trace)?;
        let encoded = encoded_query_size(&context, &result)?;
        if encoded > limits.result_bytes {
            return Err(RequestContext::limit_failure(
                ReadFailureLimit::ResultBytes,
                limits.result_bytes,
            ));
        }
        context.set_encoded_bytes(encoded);
        context.check_final_boundary()?;
        // Every read says what it did at least once, however short it was.
        context.flush_final_progress();
        context.transfer_answer(&mut result)?;
        context.publish_group_returns()?;
        drop(retained_rows);
        drop(root);
        Ok(BoundedExecutionResult {
            result,
            publication,
            telemetry: context.telemetry(),
        })
    })();
    let execution = execution.map_err(|error| work_refusal_naming_the_statement(sql, error));
    let plugin_outcome = bounded_query_outcome(&execution);
    db.plugin().post_query(
        sql,
        deadline_clock_duration(plugin_clock.as_ref(), plugin_started_ms),
        &plugin_outcome,
    );
    execution
}

pub(super) struct CursorState {
    database: Arc<dyn ReadExecutionTarget>,
    _snapshot: SnapshotId,
    _snapshot_registration: Box<dyn Send + Sync>,
    storage_snapshot: Option<crate::persistence::OwnerCursorStoragePin>,
    writer_root: Option<Arc<crate::persistence::WriterSnapshot>>,
    vector_read: bool,
    publication: ReadPublicationScope,
    /// The idle window this cursor's registration is judged by. Stamped
    /// beside `last_used_ms` on every page handed back, so removal reads the
    /// same "still entitled to another page" answer `check_expiry` gives.
    idle_window: Arc<CursorIdleWindow>,
    root: PullNode,
    replay: VecDeque<PulledRow>,
    visible_columns: Vec<String>,
    internal_row_id: Option<usize>,
    sql: String,
    created_ms: u64,
    last_used_ms: u64,
    plugin_started_ms: u64,
    plugin_finished: bool,
    published_rows: usize,
    /// The page scaffolding a live cursor holds between fetches.
    ///
    /// A cursor costs its scaffolding plus whatever rows it is still holding.
    /// The scaffolding is the flat part -- a page's columns and row capacity
    /// are the same size for equally-shaped pages, so the previous page's share
    /// is given back as this page's is taken, and an interrupted build keeps it.
    /// `held_page_base` is that share. The replay container belongs to the
    /// cursor and keeps its credit across fetches until its capacity changes.
    ///
    /// The rows are NOT flat, and pretending they were is what made the quoted
    /// cost untrue: a row that did not fit a page, or that a cancelled build
    /// handed back, stays alive in the replay until some later page publishes
    /// it, so it is charged for exactly as long as this cursor owns it. Every
    /// terminal path settles the request to scaffolding plus the rows still in
    /// the replay, which returns the rows that left with the answer and keeps
    /// the ones that did not.
    held_page_base: usize,
    observation: Option<CursorObservation>,
    // Drop retained kernel/replay payloads before their request credit.
    context: Arc<RequestContext>,
}

/// What a page build must leave unchanged, measured before it changes anything.
#[derive(Clone, Copy)]
struct CursorPageAnchor {
    /// The request's standing reservations plus the page scaffolding it holds
    /// right now -- everything except the two parts that move.
    fixed_bytes: usize,
    /// The scaffolding this build is about to give back.
    released_base: usize,
}

impl CursorState {
    fn check_expiry(&self) -> std::result::Result<(), BoundedExecutionError> {
        let now = self.context.clock.now_ms();
        let lifetime = now
            .checked_sub(self.created_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if lifetime > self.context.limits.cursor_lifetime_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Lifetime,
            )));
        }
        let idle = now
            .checked_sub(self.last_used_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if idle > self.context.limits.cursor_idle_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Idle,
            )));
        }
        Ok(())
    }

    fn check_lifetime(&self) -> std::result::Result<(), BoundedExecutionError> {
        let now = self.context.clock.now_ms();
        let lifetime = now
            .checked_sub(self.created_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if lifetime > self.context.limits.cursor_lifetime_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Lifetime,
            )));
        }
        Ok(())
    }

    fn finish_plugin_success(&mut self) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Success {
                row_count: self.published_rows,
            },
        );
        self.plugin_finished = true;
    }

    fn finish_plugin_error(&mut self, error: &BoundedExecutionError) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Error {
                error: bounded_error_text(error),
            },
        );
        self.plugin_finished = true;
    }

    fn page_bytes_refusal(
        &self,
        required: u64,
    ) -> std::result::Result<BoundedExecutionError, BoundedExecutionError> {
        const SETTING_PREFIX: &str = "effective cursor_page_bytes >= ";
        const REMEDY_COMMAND: &str = "select fewer columns";
        let required_setting_len = checked_size_add(
            SETTING_PREFIX.len(),
            decimal_digits_u64(required),
            "cursor-page remedy",
        )?;
        let planned_bytes = checked_size_add(
            checked_size_add(required_setting_len, self.sql.len(), "cursor-page remedy")?,
            REMEDY_COMMAND.len(),
            "cursor-page remedy",
        )?;
        self.context
            .reserve(BoundedWorkSource::TableScan, planned_bytes)?;
        let mut required_setting = String::new();
        let mut statement = String::new();
        let mut remedy_command = String::new();
        if required_setting
            .try_reserve_exact(required_setting_len)
            .is_err()
            || statement.try_reserve_exact(self.sql.len()).is_err()
            || remedy_command
                .try_reserve_exact(REMEDY_COMMAND.len())
                .is_err()
        {
            self.context.release(planned_bytes)?;
            return Err(
                Error::Other("bounded cursor-page remedy allocation failed".to_string()).into(),
            );
        }
        required_setting.push_str(SETTING_PREFIX);
        if write!(&mut required_setting, "{required}").is_err() {
            self.context.release(planned_bytes)?;
            return Err(
                Error::Other("bounded cursor-page remedy formatting failed".to_string()).into(),
            );
        }
        statement.push_str(&self.sql);
        remedy_command.push_str(REMEDY_COMMAND);
        let actual_bytes = match (|| {
            checked_size_add(
                checked_size_add(
                    required_setting.capacity(),
                    statement.capacity(),
                    "cursor-page remedy",
                )?,
                remedy_command.capacity(),
                "cursor-page remedy",
            )
        })() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.context.release(planned_bytes)?;
                return Err(error);
            }
        };
        reconcile_new_reservation(
            &self.context,
            BoundedWorkSource::TableScan,
            planned_bytes,
            actual_bytes,
        )?;
        let refusal = BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::CursorPageBytes,
                value: self.context.limits.cursor_page_bytes,
                required: Some(RequiredBytesSetting {
                    required_bytes: required,
                    required_setting,
                }),
                statement: Some(StatementRemedy {
                    statement,
                    remedy_command,
                }),
            },
        ));
        self.context.release(actual_bytes)?;
        Ok(refusal)
    }

    fn reserve_replay_slot(&mut self) -> std::result::Result<(), BoundedExecutionError> {
        if self.replay.len() < self.replay.capacity() {
            return Ok(());
        }
        Err(Error::Other(
            "bounded cursor replay exceeded the capacity admitted for this page".to_string(),
        )
        .into())
    }

    fn replay_push_front(
        &mut self,
        row: PulledRow,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.reserve_replay_slot()?;
        self.replay.push_front(row);
        Ok(())
    }

    fn replay_push_back(
        &mut self,
        row: PulledRow,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.reserve_replay_slot()?;
        self.replay.push_back(row);
        Ok(())
    }

    fn restore_page_rows(
        &mut self,
        page: &mut CursorPage,
        retained: &mut Vec<(usize, Option<GroupBinding>)>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        while !page.rows.is_empty() && !retained.is_empty() {
            if self.replay.len() >= self.replay.capacity() {
                return Err(Error::Other(
                    "bounded cursor replay capacity cannot restore its cancelled page".to_string(),
                )
                .into());
            }
            let values = page.rows.pop().ok_or_else(|| {
                Error::Other("bounded cursor lost a cancelled page row".to_string())
            })?;
            let (retained_bytes, group_binding) = retained.pop().ok_or_else(|| {
                Error::Other("bounded cursor lost a cancelled page charge".to_string())
            })?;
            self.replay.push_front(PulledRow {
                window: None,
                values,
                retained_bytes,
                group_binding,
            });
        }
        if !page.rows.is_empty() || !retained.is_empty() {
            return Err(
                Error::Other("bounded cursor page rows and charges diverged".to_string()).into(),
            );
        }
        Ok(())
    }

    fn pull_visible_row(
        &mut self,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if let Some(row) = self.replay.pop_front() {
            return Ok(Some(row));
        }
        let Some(mut row) = pull(self.database.as_ref(), &self.context, &mut self.root)? else {
            return Ok(None);
        };
        if let Some(position) = self.internal_row_id
            && position < row.values.len()
        {
            row.values.remove(position);
        }
        Ok(Some(row))
    }

    fn has_more_after_page(&mut self) -> std::result::Result<bool, BoundedExecutionError> {
        if !self.replay.is_empty() {
            return Ok(true);
        }
        match self.root.has_more_hint() {
            Some(has_more) => Ok(has_more),
            None if self.context.can_charge_more_work() => {
                let pulled = match self.pull_visible_row() {
                    Err(error)
                        if !cursor_replay_work_available()
                            && self.context.refusal_pauses_this_fetch(&error) =>
                    {
                        return Ok(true);
                    }
                    pulled => pulled?,
                };
                let Some(row) = pulled else {
                    return Ok(false);
                };
                self.replay_push_back(row)?;
                Ok(true)
            }
            None => Ok(true),
        }
    }

    /// Settle this cursor to what it is genuinely still holding.
    ///
    /// A page build moves the charge in every direction at once. The cursor
    /// exchanges one page's scaffolding for the next. The rows that leave with
    /// the answer stop being its cost -- but WHO gave them back differs by
    /// source: a sorted read hands its rows over already un-charged, because
    /// the sort released them as it drained, while a graph walk's rows are
    /// still charged when they are published. And the SOURCE ITSELF grows as
    /// it reads: a traversal's frontier, visited set and adjacency cursor all
    /// expand deeper into a walk, through this same request.
    ///
    /// So neither "release the rows that left" nor "come home to the figure
    /// you started from" is right on its own -- the first double-releases a
    /// source that already gave its rows back, the second hands back a growing
    /// source's own bytes and leaves it to over-release when it finally runs
    /// dry. What holds for every source is this: the parts that do not move
    /// -- the request's standing reservations and this page's scaffolding --
    /// are anchored, and the parts that do move are measured where they live,
    /// the continuation through `pull_continuation_bytes` and the deferred
    /// rows through the replay itself.
    /// What this cursor is holding right now, part by part -- the same walk
    /// the settlement above is computed from.
    #[cfg(feature = "test-seams")]
    pub(super) fn continuation_bytes(
        &self,
    ) -> std::result::Result<ContinuationBytes, BoundedExecutionError> {
        continuation_bytes_breakdown(&self.root)
    }

    fn settle_retained_charge(
        &mut self,
        anchor: CursorPageAnchor,
        page_base_bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let continuation_now = pull_continuation_bytes(&self.root)?;
        let carried_now = self.carried_replay_bytes()?;
        let fixed = anchor
            .fixed_bytes
            .saturating_sub(anchor.released_base)
            .saturating_add(page_base_bytes);
        let observed_membership = {
            let state = self.context.state();
            checked_size_add(
                state.membership_bytes,
                state.group_observer_bytes,
                "cursor observed membership",
            )?
        };
        let target = checked_size_add(
            checked_size_add(fixed, continuation_now, "cursor retained charge")?,
            checked_size_add(carried_now, observed_membership, "cursor membership")?,
            "cursor retained charge",
        )?;
        let held = self.context.held_bytes();
        match held.cmp(&target) {
            std::cmp::Ordering::Greater => self.context.release(held - target),
            std::cmp::Ordering::Less => self
                .context
                .reserve(BoundedWorkSource::TableScan, target - held),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }

    /// Measure the parts of this cursor's charge that a page build must leave
    /// where it found them, before the build touches any of them.
    fn page_anchor(&self) -> std::result::Result<CursorPageAnchor, BoundedExecutionError> {
        let continuation = pull_continuation_bytes(&self.root)?;
        let carried = self.carried_replay_bytes()?;
        let observed_membership = {
            let state = self.context.state();
            checked_size_add(
                state.membership_bytes,
                state.group_observer_bytes,
                "cursor observed membership",
            )?
        };
        Ok(CursorPageAnchor {
            fixed_bytes: self
                .context
                .held_bytes()
                .saturating_sub(continuation)
                .saturating_sub(carried)
                .saturating_sub(observed_membership),
            released_base: self.held_page_base,
        })
    }

    fn carried_replay_bytes(&self) -> std::result::Result<usize, BoundedExecutionError> {
        self.replay.iter().try_fold(0usize, |sum, row| {
            checked_size_add(sum, row.retained_bytes, "cursor replay rows")
        })
    }

    fn released(&mut self, reason: BoundedCursorTerminalReason) -> BoundedCursorLifecycle {
        let (mut observation, overflow) = self.context.take_final_observation();
        if let Some(observation) = observation.as_mut() {
            Arc::get_mut(&mut observation.trace)
                .expect("terminal receipt is private until release")
                .physical_plan = self.root.published_plan();
        }
        BoundedCursorLifecycle::Released(reason, observation, overflow)
    }

    fn observation(&mut self) -> std::result::Result<CursorObservation, BoundedExecutionError> {
        self.observation_with_boundary(true)
    }

    fn observation_with_boundary(
        &mut self,
        check_boundary: bool,
    ) -> std::result::Result<CursorObservation, BoundedExecutionError> {
        let plan = self.root.published_plan();
        let vector = self.root.published_vector_search();
        let groups_changed = match self.observation.as_ref() {
            Some(prior) => !self
                .context
                .groups_match(&prior.trace.groups, check_boundary)?,
            None => true,
        };
        let changed = groups_changed
            || self.observation.as_ref().is_none_or(|prior| {
                prior.trace.physical_plan != plan || prior.trace.vector_search.as_ref() != vector
            });
        if changed && !check_boundary {
            let mut observation = self.context.final_observation_snapshot()?;
            Arc::get_mut(&mut observation.trace)
                .expect("new observation snapshot")
                .physical_plan = plan;
            self.observation = Some(observation);
        } else if changed {
            // Reserve both the shared metadata and the public handle's single
            // owned copy before either is allocated. Subsequent pages share
            // metadata; only their touch-derived scalar changes.
            let mut bytes = checked_size_add(
                std::mem::size_of::<QueryTrace>(),
                query_trace_capacity_bytes(&self.root.trace)?,
                "cursor trace",
            )?;
            bytes = checked_size_add(bytes, self.context.group_snapshot_bytes()?, "cursor groups")?;
            if let Some(vector) = vector {
                bytes = checked_size_add(
                    bytes,
                    vector_search_disclosure_capacity_bytes(vector)?,
                    "cursor vector trace",
                )?;
            }
            let bytes = checked_size_add(
                checked_size_mul(bytes, 2, "cursor trace copies")?,
                std::mem::size_of::<SourcePageCharge>() + 4 * std::mem::size_of::<usize>(),
                "cursor trace owners",
            )?;
            if check_boundary {
                self.context.reserve(BoundedWorkSource::TableScan, bytes)?;
            } else {
                self.context
                    .reserve_metadata(BoundedWorkSource::TableScan, bytes)?;
            }
            let credit = self.context.state().memory.try_split_off(bytes)?;
            self.context
                .live_source_pages
                .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
            let charge = Arc::new(SourcePageCharge {
                _credit: credit,
                live: Arc::clone(&self.context.live_source_pages),
                bytes,
            });
            let mut trace = self.root.trace.clone();
            trace.physical_plan = plan;
            trace.vector_search = vector.cloned();
            trace.groups = self.context.group_snapshot(check_boundary)?;
            self.observation = Some(CursorObservation {
                trace: Arc::new(trace),
                rows_examined: 0,
                charge: Some(charge),
                group_page_returns: None,
            });
        }
        let mut observation = self.observation.as_ref().unwrap().clone();
        observation.rows_examined = self.context.cumulative_rows_examined()?;
        Ok(observation)
    }

    fn fetch_page(
        &mut self,
        rows: NonZeroUsize,
    ) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
        self.context.prepare_final_observation_work()?;
        let publication_context = Arc::clone(&self.context);
        let _group_attempt = GroupPublicationAttempt(&publication_context);
        let _writer_read = self.database.enter_writer_read(
            self.writer_root.clone(),
            self.context
                .source_read_memory(BoundedWorkSource::TableScan),
        );
        let _vector_read = enter_vector_read(self.database.as_ref(), self._snapshot,
            &self.context, self.vector_read)?;
        let row_limit = u64::try_from(rows.get()).map_err(|_| {
            RequestContext::limit_failure(
                ReadFailureLimit::ResultRows,
                self.context.limits.result_rows,
            )
        })?;
        if row_limit > self.context.limits.result_rows {
            return Err(RequestContext::limit_failure(
                ReadFailureLimit::ResultRows,
                self.context.limits.result_rows,
            ));
        }
        let row_capacity = rows.get();
        // What this cursor holds before it touches anything. A fetch that is
        // INTERRUPTED publishes nothing, so nothing about the cursor may have
        // changed when it returns -- every row it had assembled goes back into
        // the replay and every byte it took for the attempt goes back here.
        // The figure is taken before the page's own scaffolding is exchanged,
        // so "as it was" means exactly that.
        let mut anchor = self.page_anchor()?;
        // This page's scaffolding replaces the last page's rather than adding
        // to it: a cursor that is handing back equally-shaped pages costs the
        // same while it builds one as it does while it holds one.
        let released_base = anchor.released_base;
        let _ = std::mem::take(&mut self.held_page_base);
        if released_base > 0 {
            self.context.release(released_base)?;
        }
        // Construction guards precede the funded payloads and stay armed
        // through every fallible scaffolding step before page ownership.
        let mut page_column_charge = ReservedBytes::new(&publication_context, 0);
        let (page_columns, page_column_bytes) = clone_strings_with_reservation(
            &self.visible_columns,
            &self.context,
            "cursor-page columns",
        )?;
        page_column_charge.bytes = page_column_bytes;
        let planned_row_state_bytes = checked_size_add(
            checked_size_mul(
                row_capacity,
                std::mem::size_of::<Vec<Value>>(),
                "cursor-page row capacity",
            )?,
            checked_size_mul(
                row_capacity,
                std::mem::size_of::<(usize, Option<GroupBinding>)>(),
                "cursor-page retained capacity",
            )?,
            "cursor-page row state",
        )?;
        let planned_row_state_bytes = checked_size_add(
            planned_row_state_bytes,
            crate::owned_cursor_page::SharedRows::allocation_header_bytes(),
            "immutable page root",
        )?;
        self.context
            .reserve(BoundedWorkSource::TableScan, planned_row_state_bytes)?;
        let mut row_state_charge =
            ReservedBytes::new(&publication_context, planned_row_state_bytes);
        let mut page_rows = Vec::new();
        if page_rows.try_reserve_exact(row_capacity).is_err() {
            return Err(
                Error::Other("bounded cursor-page row allocation failed".to_string()).into(),
            );
        }
        let mut retained = Vec::<(usize, Option<GroupBinding>)>::new();
        if retained.try_reserve_exact(row_capacity).is_err() {
            return Err(
                Error::Other("bounded cursor-page charge allocation failed".to_string()).into(),
            );
        }
        let actual_row_state_bytes = checked_size_add(
            checked_size_mul(
                page_rows.capacity(),
                std::mem::size_of::<Vec<Value>>(),
                "cursor-page row capacity",
            )?,
            checked_size_mul(
                retained.capacity(),
                std::mem::size_of::<(usize, Option<GroupBinding>)>(),
                "cursor-page retained capacity",
            )?,
            "cursor-page row state",
        )?;
        let actual_row_state_bytes = checked_size_add(
            actual_row_state_bytes,
            crate::owned_cursor_page::SharedRows::allocation_header_bytes(),
            "immutable page root",
        )?;
        if actual_row_state_bytes > planned_row_state_bytes {
            self.context.reserve(
                BoundedWorkSource::TableScan,
                actual_row_state_bytes - planned_row_state_bytes,
            )?;
        } else if planned_row_state_bytes > actual_row_state_bytes {
            self.context
                .release(planned_row_state_bytes - actual_row_state_bytes)?;
        }
        row_state_charge.bytes = actual_row_state_bytes;
        // Replay allocation belongs to the cursor across fetches. Reserve only
        // growth, and return capacity credit only after shrinking the allocation.
        let replay_capacity = row_capacity
            .checked_add(1)
            .ok_or_else(|| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })?
            .max(self.replay.len());
        let previous_replay_bytes = checked_size_mul(
            self.replay.capacity(),
            std::mem::size_of::<PulledRow>(),
            "cursor replay capacity",
        )?;
        let planned_replay_bytes = checked_size_mul(
            replay_capacity,
            std::mem::size_of::<PulledRow>(),
            "cursor replay capacity",
        )?;
        let growth_bytes = planned_replay_bytes.saturating_sub(previous_replay_bytes);
        if growth_bytes > 0 {
            self.context
                .reserve(BoundedWorkSource::TableScan, growth_bytes)?;
        }
        if self.replay.capacity() < replay_capacity {
            let additional = replay_capacity.saturating_sub(self.replay.len());
            if self.replay.try_reserve_exact(additional).is_err() {
                self.context.release(growth_bytes)?;
                return Err(
                    Error::Other("bounded cursor replay allocation failed".to_string()).into(),
                );
            }
        } else if self.replay.capacity() > replay_capacity {
            self.replay.shrink_to(replay_capacity);
        }
        let replay_bytes = checked_size_mul(
            self.replay.capacity(),
            std::mem::size_of::<PulledRow>(),
            "cursor replay capacity",
        )?;
        let admitted_replay_bytes = previous_replay_bytes.saturating_add(growth_bytes);
        if replay_bytes > admitted_replay_bytes {
            // The allocation already exists and survives cancellation. Finish
            // its credit reconciliation before the next cancellation boundary.
            self.context.reserve_metadata(
                BoundedWorkSource::TableScan,
                replay_bytes - admitted_replay_bytes,
            )?;
        } else if admitted_replay_bytes > replay_bytes {
            self.context.release(admitted_replay_bytes - replay_bytes)?;
        }
        // Settlement preserves the updated replay allocation in the fixed
        // cursor credit, separate from replaceable page scaffolding.
        anchor.fixed_bytes = anchor
            .fixed_bytes
            .saturating_sub(previous_replay_bytes)
            .saturating_add(replay_bytes);
        let page_base_bytes = checked_size_add(
            page_column_bytes,
            actual_row_state_bytes,
            "cursor-page state",
        )?;
        let mut page = CursorPage::from_parts(page_columns, page_rows, true);
        page_column_charge.bytes = 0;
        row_state_charge.bytes = 0;
        let assembled = (|| {
            while u64::try_from(page.rows.len()).map_err(|_| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })? < row_limit
            {
                if !cursor_replay_work_available() {
                    page.has_more = true;
                    break;
                }
                let pulled = match self.pull_visible_row() {
                    Err(error)
                        if !cursor_replay_work_available()
                            && self.context.refusal_pauses_this_fetch(&error) =>
                    {
                        page.has_more = true;
                        break;
                    }
                    pulled => pulled?,
                };
                let Some(row) = pulled else {
                    // Out of rows for THIS page is not always out of input:
                    // an aggregate that has spent this fetch's work budget
                    // keeps its partial answer and says it still has more,
                    // so the page goes back empty rather than carrying a
                    // total that has not finished adding up.
                    page.has_more = matches!(self.root.has_more_hint(), Some(true));
                    break;
                };
                if let Some(binding) = row.group_binding {
                    if let Err(error) = self.context.stage_group_return(binding) {
                        self.replay_push_front(row)?;
                        return Err(error);
                    }
                }
                retained.push((row.retained_bytes, row.group_binding));
                page.rows.push(row.values);
                page.has_more =
                    !self.replay.is_empty() || !matches!(self.root.has_more_hint(), Some(false));
                let observation = self.observation()?;
                let encoded = encoded_page_size(&self.context, &page, &observation)?;
                self.context.note_result_shape(page.rows.len(), encoded);
                if encoded > self.context.limits.cursor_page_bytes {
                    let values = page.rows.pop().ok_or_else(|| {
                        Error::Other("bounded cursor lost its byte-stopped row".to_string())
                    })?;
                    let (retained_bytes, group_binding) = retained.pop().ok_or_else(|| {
                        Error::Other("bounded cursor lost its byte-stopped charge".to_string())
                    })?;
                    if let Some(binding) = group_binding {
                        self.context.unstage_group_return(binding)?;
                    }
                    self.replay_push_front(PulledRow {
                        window: None,
                        values,
                        retained_bytes,
                        group_binding,
                    })?;
                    if page.rows.is_empty() {
                        return Err(self.page_bytes_refusal(encoded)?);
                    }
                    page.has_more = true;
                    break;
                }
                if !page.has_more {
                    break;
                }
            }
            if u64::try_from(page.rows.len()).map_err(|_| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })? == row_limit
            {
                page.has_more = self.has_more_after_page()?;
            }
            let observation = self.observation()?;
            let encoded = encoded_page_size(&self.context, &page, &observation)?;
            if encoded > self.context.limits.cursor_page_bytes {
                return Err(self.page_bytes_refusal(encoded)?);
            }
            let page_bytes = u64::try_from(serialized_size(&page)?)
                .map_err(|_| bounded_size_error("cursor page telemetry"))?;
            let trace_bytes = encoded
                .checked_sub(page_bytes)
                .ok_or_else(|| bounded_size_error("cursor trace telemetry"))?;
            let mut state = self.context.state();
            state.telemetry.encoded_bytes = page_bytes;
            state.telemetry.encoded_trace_bytes = trace_bytes;
            Ok(encoded)
        })();
        let encoded = match assembled {
            Ok(encoded) => encoded,
            Err(BoundedExecutionError::Cancelled) => {
                // Interrupted while the page was still being assembled. The
                // cursor lives on, still holds a page's worth of scaffolding,
                // and has just taken every assembled row back into the replay,
                // so all of those rows stay charged.
                self.restore_page_rows(&mut page, &mut retained)?;
                self.held_page_base = page_base_bytes;
                self.settle_retained_charge(anchor, page_base_bytes)?;
                return Err(BoundedExecutionError::Cancelled);
            }
            Err(error) => return Err(error),
        };
        let publication_boundary = (|| {
            self.check_lifetime()?;
            self.context.check_final_boundary()?;
            let telemetry = self.context.telemetry();
            if telemetry
                .encoded_bytes
                .checked_add(telemetry.encoded_trace_bytes)
                != Some(encoded)
            {
                return Err(Error::Other(
                    "bounded cursor encoded-byte telemetry changed before publication".to_string(),
                )
                .into());
            }
            let mut observation = self.observation()?;
            observation.group_page_returns = self.context.prepare_group_page_returns(&retained)?;
            self.check_lifetime()?;
            self.context.check_final_boundary()?;
            Ok((telemetry, observation))
        })();
        let (telemetry, observation) = match publication_boundary {
            Ok(prepared) => prepared,
            Err(BoundedExecutionError::Cancelled) => {
                // Interrupted at the publication boundary. Nothing was
                // published, every assembled row is back in the replay, and
                // the cursor comes home to exactly what it held before this
                // fetch touched anything.
                self.restore_page_rows(&mut page, &mut retained)?;
                self.held_page_base = page_base_bytes;
                self.settle_retained_charge(anchor, page_base_bytes)?;
                return Err(BoundedExecutionError::Cancelled);
            }
            Err(error) => return Err(error),
        };
        let published = page.rows.len();
        self.context.publish_group_returns()?;
        // Transfer the existing request credit with the published allocation.
        // The row membership scratch drops here; it never travels with the DTO.
        let row_bytes = retained.iter().try_fold(0usize, |bytes, (held, _)| {
            checked_size_add(bytes, *held, "published cursor rows")
        })?;
        let page_bytes = checked_size_add(
            page_column_bytes,
            checked_size_mul(
                page.rows.capacity(),
                std::mem::size_of::<Vec<Value>>(),
                "published cursor row slots",
            )?,
            "published cursor page",
        )?;
        let page_bytes = checked_size_add(page_bytes, row_bytes, "published cursor page")?;
        let page_bytes = checked_size_add(
            page_bytes,
            crate::owned_cursor_page::SharedRows::allocation_header_bytes(),
            "published immutable page root",
        )?;
        let owner_bytes = 0; // Credit moves inline; no credit Arc allocation.
        let credit = self.context.state().memory.try_split_off(checked_size_add(
            page_bytes,
            owner_bytes,
            "published cursor credit owner",
        )?)?;
        page = page.with_credit(credit);
        drop(retained);
        self.held_page_base = 0;
        self.settle_retained_charge(anchor, 0)?;
        self.published_rows = self
            .published_rows
            .checked_add(published)
            .ok_or_else(|| Error::Other("bounded cursor row counter overflow".to_string()))?;
        self.last_used_ms = self.context.clock.now_ms();
        self.idle_window.touch(self.last_used_ms);
        // The page is published, so this fetch is over: whatever it examined
        // and assembled is told now, even if it never crossed an interval.
        self.context.flush_final_progress();
        Ok(BoundedCursorFetch {
            page,
            observation,
            publication: self.context.publication_scope(
                self.publication.snapshot_lsn,
                self.publication.tables.clone(),
                self.publication.inspection,
            ),
            telemetry,
        })
    }
}

impl Drop for CursorState {
    fn drop(&mut self) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Error {
                error: "bounded cursor dropped before completion".to_string(),
            },
        );
        self.plugin_finished = true;
    }
}

fn default_page_rows(
    limits: ReadLimits,
) -> std::result::Result<NonZeroUsize, BoundedExecutionError> {
    let rows = usize::try_from(limits.cursor_page_rows).map_err(|_| {
        RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
    })?;
    NonZeroUsize::new(rows).ok_or_else(|| {
        RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
    })
}

pub(super) fn open_cursor(
    db: Arc<dyn ReadExecutionTarget>,
    sql: &str,
    params: &HashMap<String, Value>,
    read_memory: Arc<dyn ReadMemoryBudget>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorOpen, BoundedExecutionError> {
    let statement = contextdb_parser::parse(sql)?;
    db.plugin().on_query(sql)?;
    let plugin_started_ms = clock.now_ms();
    let withdrawn = cancellation.clone();
    let prepared = (|| {
        let context = RequestContext::new(
            Arc::clone(&read_memory),
            limits,
            ReadResumption::AcrossFetches,
            Arc::clone(&clock),
            cancellation,
            db.active_read_transaction(),
            #[cfg(feature = "test-seams")]
            probe,
        )?;
        if CAPTURE_CURSOR_OPEN_CONTEXT.with(Cell::get) {
            CURSOR_OPEN_CONTEXT.with(|slot| *slot.borrow_mut() = Some(Arc::clone(&context)));
        }
        let schema_capture = db.capture_read_schema();
        let membership_capture = (!db.retained_relational_membership_source())
            .then(|| db.relational_membership_store().begin_membership_capture());
        let Some((
            snapshot,
            publication_lsn,
            idle_window,
            storage_snapshot,
            snapshot_registration,
            writer_root,
        )) = db.bounded_cursor_snapshot_registration(
            statement_needs_vector_graph(&statement),
            Arc::clone(&clock),
            limits.cursor_idle_ms,
            &context.registration_budget(),
            &withdrawn,
            context.source_read_memory(BoundedWorkSource::TableScan),
        )?
        else {
            return Err(context.withdrawn_failure());
        };
        let _writer_read = db.enter_writer_read(
            writer_root.clone(),
            context.source_read_memory(BoundedWorkSource::TableScan),
        );
        let vector_read = statement_needs_vector_read(&statement);
        let _vector_read = enter_vector_read(db.as_ref(), snapshot, &context, vector_read)?;
        #[cfg(feature = "test-seams")]
        if let Some(probe) = context.probe() {
            probe.registered_snapshot_for_test(snapshot);
            probe.after_snapshot_registration();
        }
        let statement_bytes = statement_capacity_bytes(&statement)?;
        context.reserve(BoundedWorkSource::TableScan, statement_bytes)?;
        // The resolved statement and the physical plan are built out of this
        // statement, so the statement's own size is what the request is
        // admitted for before either exists; the reservation is trued up to
        // what they hold as soon as they do. A request whose ceiling cannot
        // cover the shape it asked for is refused before the engine builds
        // that shape, not after.
        let planned_prepared_bytes = checked_size_mul(statement_bytes, 2, "bounded read plan")?;
        context.reserve(BoundedWorkSource::TableScan, planned_prepared_bytes)?;
        let (resolved, plan) = context.with_vector_copies(|vector_copy_owner| {
            db.prepare_bounded_read_plan(
                &statement,
                params,
                vector_copy_owner,
                Arc::new(SourceDecodeBudget {
                    context: context.clone(),
                    source: BoundedWorkSource::TableScan,
                }),
            )
        })?;
        let vector_read = vector_read || plan_needs_vector_read(&plan);
        let _vector_plan_read = enter_vector_read(db.as_ref(), snapshot, &context, vector_read)?;
        let publication = context.publication_scope(
            publication_lsn,
            publication_tables(&plan),
            inspection_reads_committed_state(&plan),
        );
        let resolved_bytes = statement_capacity_bytes(&resolved)?;
        ensure_supported_plan(&plan)?;
        let plan_bytes = physical_plan_capacity_bytes(&plan)?;
        {
            let admitted_bytes = planned_prepared_bytes;
            let retained_bytes = checked_size_add(resolved_bytes, plan_bytes, "bounded read plan")?;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        drop(statement);
        context.release(statement_bytes)?;
        drop(resolved);
        context.release(resolved_bytes)?;
        context.reserve(
            BoundedWorkSource::TableScan,
            std::mem::size_of::<BoundedCursorExecutionState>(),
        )?;
        let planned_params_bytes = params_retained_bytes(params)?;
        context.reserve(BoundedWorkSource::TableScan, planned_params_bytes)?;
        let params = Arc::new(params.clone());
        let params_bytes = params_retained_bytes(&params)?;
        {
            let admitted_bytes = planned_params_bytes;
            let retained_bytes = params_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        // The kernel is built out of the plan and holds a copy of every table
        // shape it reads, so the plan's own size is the floor the request is
        // admitted for before the kernel is materialized; the reservation is
        // trued up to what the kernel holds once it exists.
        context.reserve(BoundedWorkSource::TableScan, plan_bytes)?;
        let mut root = build_kernel(db.as_ref(), &plan, params, snapshot, &context)?;
        // Every door that builds a kernel declares the answer's demand, or the
        // sources below it fill nothing and a column the answer names comes
        // back empty on that door alone.
        {
            let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
            declare_supplemented_columns(db.as_ref(), &mut root, &named);
        }
        drop(membership_capture);
        drop(schema_capture);
        #[cfg(feature = "test-seams")]
        if let Some(probe) = context.probe() {
            probe.after_source_capture();
        }
        let root_bytes = pull_continuation_bytes(&root)?;
        let prepaid = adopt_kernel_construction(&mut root)?;
        let construction_bytes =
            checked_size_add(plan_bytes, prepaid, "kernel construction adoption")?;
        // Keep adopted credit while the root is alive if admission refuses.
        if root_bytes > construction_bytes {
            context.reserve(
                BoundedWorkSource::TableScan,
                root_bytes - construction_bytes,
            )?;
        } else if construction_bytes > root_bytes {
            context.release(construction_bytes - root_bytes)?;
        }
        drop(plan);
        context.release(plan_bytes)?;
        context.bind_final_trace(&root.trace)?;
        let internal_row_id = root.columns.iter().position(|column| column == "row_id");
        let (mut visible_columns, visible_column_bytes) =
            clone_strings_with_reservation(&root.columns, &context, "cursor columns")?;
        if let Some(position) = internal_row_id {
            visible_columns.remove(position);
        }
        let retained_visible_column_bytes =
            strings_capacity_bytes(&visible_columns, "cursor columns")?;
        {
            let admitted_bytes = visible_column_bytes;
            let retained_bytes = retained_visible_column_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        // The replay carries rows a page did not publish into the next page,
        // so it is shaped by the page being built, not by the largest result
        // the limits would ever allow. Its capacity stays charged across
        // `fetch_page` calls until resized or the cursor is destroyed; a cursor
        // that has not built a page yet holds no replay at all.
        let replay = VecDeque::new();
        let planned_sql_bytes = sql.len();
        context.reserve(BoundedWorkSource::TableScan, planned_sql_bytes)?;
        let mut retained_sql = String::new();
        if retained_sql.try_reserve_exact(sql.len()).is_err() {
            context.release(planned_sql_bytes)?;
            return Err(Error::Other("bounded cursor SQL allocation failed".to_string()).into());
        }
        retained_sql.push_str(sql);
        let sql_bytes = retained_sql.capacity();
        {
            let admitted_bytes = planned_sql_bytes;
            let retained_bytes = sql_bytes;
            if retained_bytes > admitted_bytes {
                context.reserve(
                    BoundedWorkSource::TableScan,
                    retained_bytes - admitted_bytes,
                )?;
            } else if admitted_bytes > retained_bytes {
                context.release(admitted_bytes - retained_bytes)?;
            }
        }
        let now = clock.now_ms();
        Ok(CursorState {
            database: Arc::clone(&db),
            _snapshot: snapshot,
            _snapshot_registration: snapshot_registration,
            storage_snapshot,
            writer_root,
            vector_read,
            publication,
            idle_window,
            context,
            root,
            replay,
            visible_columns,
            internal_row_id,
            sql: retained_sql,
            created_ms: now,
            last_used_ms: now,
            plugin_started_ms,
            plugin_finished: false,
            published_rows: 0,
            held_page_base: 0,
            observation: None,
        })
    })();
    let mut state = match prepared {
        Ok(state) => state,
        Err(error) => {
            let error = work_refusal_naming_the_statement(sql, error);
            db.plugin().post_query(
                sql,
                deadline_clock_duration(clock.as_ref(), plugin_started_ms),
                &QueryOutcome::Error {
                    error: bounded_error_text(&error),
                },
            );
            return Err(error);
        }
    };
    let first_page_rows = match default_page_rows(limits) {
        Ok(rows) => rows,
        Err(error) => {
            state.finish_plugin_error(&error);
            return Err(error);
        }
    };
    let first = match state.fetch_page(first_page_rows) {
        Ok(first) => first,
        Err(error) => {
            let error = work_refusal_naming_the_statement(sql, error);
            state.finish_plugin_error(&error);
            return Err(error);
        }
    };
    let publication = first.publication.clone();
    let storage_snapshot = first
        .page
        .has_more
        .then(|| {
            state
                .storage_snapshot
                .as_ref()
                .map(|pin| pin.storage_identity())
                .or_else(|| {
                    state
                        .writer_root
                        .as_ref()
                        .map(|root| root.storage_identity())
                })
        })
        .flatten();
    let lifecycle = if first.page.has_more {
        BoundedCursorLifecycle::Open(state)
    } else {
        let columns = CursorPage::try_empty(&first.page.columns, read_memory.clone())?;
        state.finish_plugin_success();
        BoundedCursorLifecycle::Exhausted(columns, first.observation.clone())
    };
    let execution = Arc::new(BoundedCursorExecutionState {
        read_memory: read_memory.clone(),
        lifecycle: Mutex::new(lifecycle),
        publication: publication.clone(),
        erased: std::sync::atomic::AtomicBool::new(false),
        active_cancellation: Mutex::new(OwnerReadCancellation::new()),
        expiry_watch: Mutex::new(None),
    });
    db.register_writer_cursor(&execution)?;
    start_cursor_expiry_watch(&execution, Arc::clone(&read_memory))?;
    Ok(BoundedCursorOpen {
        cursor: BoundedCursorHandle { _state: execution },
        first_page: first.page,
        observation: first.observation,
        storage_snapshot,
        publication,
        telemetry: first.telemetry,
    })
}

/// The existing deadline future wakes this admitted control owner directly.
/// No separate native thread or strong execution cycle is created.
pub(super) struct CursorExpiryWatch {
    execution: std::sync::Weak<BoundedCursorExecutionState>,
    clock: Arc<dyn DeadlineClock>,
    wait: Mutex<Option<(u64, contextdb_core::read_contract::DeadlineWait<'static>)>>,
    _read_memory: Arc<dyn ReadMemoryBudget>,
}

impl CursorExpiryWatch {
    pub(super) fn retire(&self) {
        let wait = self.wait.lock().unwrap_or_else(|p| p.into_inner()).take();
        drop(wait);
    }

    pub(super) fn drive(self: &Arc<Self>) {
        let Some(execution) = self.execution.upgrade() else {
            self.retire();
            return;
        };
        // A wake may come from inside the active fetch. That fetch drives us
        // again after releasing its lifecycle borrow and updating its deadline.
        let Ok(mut lifecycle) = execution.lifecycle.try_lock() else {
            return;
        };
        let BoundedCursorLifecycle::Open(state) = &mut *lifecycle else {
            drop(lifecycle);
            execution.retire_expiry();
            return;
        };
        if let Err(error) = state.check_expiry() {
            state.finish_plugin_error(&error);
            let reason = match &error {
                BoundedExecutionError::Refused(failure) => match failure.detail() {
                    ReadFailureDetail::CursorExpired { expiry } => {
                        BoundedCursorTerminalReason::Expired(*expiry)
                    }
                    _ => BoundedCursorTerminalReason::Refused,
                },
                _ => BoundedCursorTerminalReason::Refused,
            };
            release_cursor_lifecycle(&mut lifecycle, reason);
            drop(lifecycle);
            execution.retire_expiry();
            return;
        }
        let deadline = cursor_deadline(state);
        drop(lifecycle);
        let Ok(mut wait) = self.wait.try_lock() else {
            return;
        };
        if wait.as_ref().is_none_or(|(armed, _)| *armed != deadline) {
            let clock = self.clock.clone();
            *wait = Some((
                deadline,
                Box::pin(async move { clock.wait_until(deadline).await }),
            ));
        }
        let waker = std::task::Waker::from(self.clone());
        let mut context = std::task::Context::from_waker(&waker);
        let ready = wait
            .as_mut()
            .is_some_and(|(_, wait)| wait.as_mut().poll(&mut context).is_ready());
        if ready {
            // Latch completion: a failed timer arm is also a one-shot. Reuse
            // the established monotonic timer for the remaining interval.
            let now = self.clock.now_ms();
            if now < deadline {
                let remaining = deadline.saturating_sub(now);
                *wait = Some((
                    deadline,
                    Box::pin(async move {
                        let fallback = crate::local_transport::MonotonicDeadlineClock::new();
                        fallback.wait_until(remaining).await;
                    }),
                ));
                let fallback_ready = wait
                    .as_mut()
                    .is_some_and(|(_, fallback)| fallback.as_mut().poll(&mut context).is_ready());
                if fallback_ready {
                    let completed = wait.take();
                    drop(wait);
                    drop(completed);
                    // No independently armed wake exists. Retire the parked
                    // resources now rather than leaving a completed future
                    // in the observer and retaining the snapshot forever.
                    let mut lifecycle = execution
                        .lifecycle
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let BoundedCursorLifecycle::Open(state) = &mut *lifecycle {
                        let error = BoundedExecutionError::Engine(Error::Other(
                            "cursor deadline timer could not be armed".into(),
                        ));
                        state.finish_plugin_error(&error);
                        release_cursor_lifecycle(
                            &mut lifecycle,
                            BoundedCursorTerminalReason::Refused,
                        );
                    }
                    drop(lifecycle);
                    execution.retire_expiry();
                }
            } else {
                let completed = wait.take();
                drop(wait);
                drop(completed);
                self.drive();
            }
        }
    }
}
impl std::task::Wake for CursorExpiryWatch {
    fn wake(self: Arc<Self>) {
        self.drive();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.drive();
    }
}

fn start_cursor_expiry_watch(
    execution: &Arc<BoundedCursorExecutionState>,
    read_memory: Arc<dyn ReadMemoryBudget>,
) -> std::result::Result<(), BoundedExecutionError> {
    let clock = {
        let lifecycle = execution
            .lifecycle
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let BoundedCursorLifecycle::Open(state) = &*lifecycle else {
            return Ok(());
        };
        state.context.clock.clone()
    };
    let watch = Arc::new(CursorExpiryWatch {
        execution: Arc::downgrade(execution),
        clock,
        wait: Mutex::new(None),
        _read_memory: read_memory,
    });
    *execution
        .expiry_watch
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(watch.clone());
    watch.drive();
    Ok(())
}

fn cursor_deadline(state: &CursorState) -> u64 {
    state
        .created_ms
        .saturating_add(state.context.limits.cursor_lifetime_ms)
        .min(
            state
                .last_used_ms
                .saturating_add(state.context.limits.cursor_idle_ms),
        )
        .saturating_add(1)
}

/// A work refusal names the statement it stopped and the one move that
/// answers it inside the same ceiling.
///
/// A caller told only "the work ceiling is one thousand" learns nothing: the
/// number is theirs already. What they can act on is which statement reached
/// it and that the same statement, opened as a cursor, answers inside the
/// ceiling they declared. Attached where the statement text is known; a
/// refusal that already names one is left exactly as it is.
fn work_refusal_naming_the_statement(
    sql: &str,
    error: BoundedExecutionError,
) -> BoundedExecutionError {
    const REMEDY_COMMAND: &str =
        "open this statement as a cursor and fetch until it reports no more";
    let BoundedExecutionError::Refused(failure) = &error else {
        return error;
    };
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        return error;
    };
    if detail.limit != ReadFailureLimit::Work || detail.statement.is_some() {
        return error;
    }
    BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
        OwnerLimitExceededDetail {
            limit: detail.limit,
            value: detail.value,
            required: detail.required.clone(),
            statement: Some(StatementRemedy {
                statement: sql.to_string(),
                remedy_command: REMEDY_COMMAND.to_string(),
            }),
        },
    ))
}

fn cursor_not_found() -> BoundedExecutionError {
    match ReadFailure::new(ReadFailureKind::CursorNotFound, ReadFailureDetail::None) {
        Ok(failure) => BoundedExecutionError::Refused(failure),
        Err(error) => BoundedExecutionError::Engine(Error::Other(error.to_string())),
    }
}

impl BoundedCursorHandle {
    /// Observe credits already held by this read, without charging or advancing it.
    pub(crate) fn retained_read_bytes(&self) -> usize {
        self.retained_read_bytes_with_observations(None, None)
    }

    /// Carrier-held observations can outlive the kernel's released lifecycle.
    /// Count each surviving shared live counter once, including when the open
    /// kernel and the carrier still share the same observation credits.
    pub(crate) fn retained_read_bytes_with_observations(
        &self,
        pending: Option<&CursorObservation>,
        cancelled: Option<&CursorObservation>,
    ) -> usize {
        let lifecycle = cursor_lifecycle(self);
        let (reserved, kernel_live, kernel_returns) = match &*lifecycle {
            BoundedCursorLifecycle::Open(state) => (
                state.context.state().memory.bytes(),
                Some(&state.context.live_source_pages),
                None,
            ),
            BoundedCursorLifecycle::Exhausted(page, observation) => (
                page.credit_bytes(),
                observation.charge.as_ref().map(|charge| &charge.live),
                observation
                    .group_page_returns
                    .as_ref()
                    .map(|receipt| &receipt._charge.live),
            ),
            BoundedCursorLifecycle::Released(_, observation, _) => (
                0,
                observation
                    .as_ref()
                    .and_then(|observation| observation.charge.as_ref().map(|charge| &charge.live)),
                None,
            ),
        };
        let counters = [
            kernel_live,
            kernel_returns,
            pending.and_then(|observation| observation.charge.as_ref().map(|charge| &charge.live)),
            pending.and_then(|observation| {
                observation
                    .group_page_returns
                    .as_ref()
                    .map(|receipt| &receipt._charge.live)
            }),
            cancelled
                .and_then(|observation| observation.charge.as_ref().map(|charge| &charge.live)),
            cancelled.and_then(|observation| {
                observation
                    .group_page_returns
                    .as_ref()
                    .map(|receipt| &receipt._charge.live)
            }),
        ];
        counters
            .iter()
            .enumerate()
            .fold(reserved, |bytes, (index, counter)| {
                let Some(counter) = counter else {
                    return bytes;
                };
                if counters[..index]
                    .iter()
                    .flatten()
                    .any(|prior| Arc::ptr_eq(prior, counter))
                {
                    return bytes;
                }
                bytes.saturating_add(counter.load(std::sync::atomic::Ordering::SeqCst))
            })
    }
}

/// Whether this cursor is still one a caller can ask anything of.
///
/// A drained cursor counts: it answers one empty page and closes clean. A
/// RELEASED one does not -- the read behind it is over, whether a close ended
/// it, a refusal did, or it expired.
pub(super) fn cursor_is_live(cursor: &BoundedCursorHandle) -> bool {
    !matches!(
        *cursor_lifecycle(cursor),
        BoundedCursorLifecycle::Released(_, _, _)
    )
}

/// Whether this cursor still holds the suspended read it was opened with.
///
/// A refusal the kernel answered without touching the continuation leaves the
/// cursor open, and the layers above ask here rather than assuming every
/// refusal ended the read.
pub(super) fn cursor_retains_continuation(cursor: &BoundedCursorHandle) -> bool {
    matches!(*cursor_lifecycle(cursor), BoundedCursorLifecycle::Open(_))
}

fn cursor_lifecycle(cursor: &BoundedCursorHandle) -> MutexGuard<'_, BoundedCursorLifecycle> {
    match cursor._state.lifecycle.lock() {
        Ok(lifecycle) => lifecycle,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn exhausted_fetch(
    page: CursorPage,
    observation: CursorObservation,
    publication: ReadPublicationScope,
    cancellation: OwnerReadCancellation,
) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
    let encoded = serialized_size(
        &crate::read_contract::cursor_observation::BorrowedObservedPage::new(&page, &observation),
    )?;
    let encoded = u64::try_from(encoded)
        .map_err(|_| Error::Other("empty cursor page exceeds u64".to_string()))?;
    let page_bytes = u64::try_from(serialized_size(&page)?)
        .map_err(|_| Error::Other("empty cursor page exceeds u64".to_string()))?;
    let trace_bytes = encoded
        .checked_sub(page_bytes)
        .ok_or_else(|| Error::Other("empty cursor trace byte accounting underflow".to_string()))?;
    Ok(BoundedCursorFetch {
        page,
        observation,
        publication: publication.refreshed(cancellation),
        telemetry: BoundedExecutionTelemetry {
            encoded_bytes: page_bytes,
            encoded_trace_bytes: trace_bytes,
            ..BoundedExecutionTelemetry::default()
        },
    })
}

/// A cancellation receipt excludes only this page's unpublished group returns.
/// The retained page keeps its original observation for eventual publication.
/// Clearing the copied receipt makes repeated withdrawal of the copy a no-op;
/// it cannot subtract a previous page or roll back any examined-entry work.
pub(super) fn unpublished_groups_withdrawn(
    page: &CursorObservation,
) -> std::result::Result<CursorObservation, BoundedExecutionError> {
    groups_published_through(page, 0)
}

pub(super) fn groups_published_through(
    page: &CursorObservation,
    offset: usize,
) -> std::result::Result<CursorObservation, BoundedExecutionError> {
    let mut published = page.clone();
    let Some(receipt) = published.group_page_returns.take() else {
        return Ok(published);
    };
    if receipt.generation == 0 || offset > receipt.rows.len() {
        return Err(Error::Other("invalid group page publication offset".to_string()).into());
    }
    let suffix = &receipt.rows[offset..];
    if suffix.iter().all(Option::is_none) {
        return Ok(published);
    }
    for binding in suffix.iter().flatten() {
        if binding.0 >= published.trace.groups.len() {
            return Err(Error::Other("invalid group page return binding".to_string()).into());
        }
    }
    // No temporary count container: these are the actual remaining row bindings.
    for (index, group) in published.trace.groups.iter().enumerate() {
        let returned = suffix
            .iter()
            .filter(|binding| **binding == Some(GroupBinding(index)))
            .count();
        let returned = u64::try_from(returned)
            .map_err(|_| Error::Other("group page suffix exceeds u64".to_string()))?;
        group
            .returned_rows
            .checked_sub(returned)
            .ok_or_else(|| Error::Other("group page return withdrawal underflow".to_string()))?;
    }
    // The original page trace admission funds its retained outward copy.
    for (index, group) in Arc::make_mut(&mut published.trace)
        .groups
        .iter_mut()
        .enumerate()
    {
        let returned = suffix
            .iter()
            .filter(|binding| **binding == Some(GroupBinding(index)))
            .count();
        group.returned_rows -= u64::try_from(returned)
            .map_err(|_| Error::Other("group page suffix exceeds u64".to_string()))?;
    }
    Ok(published)
}

pub(super) fn release_cursor_lifecycle(
    lifecycle: &mut BoundedCursorLifecycle,
    reason: BoundedCursorTerminalReason,
) {
    let previous = std::mem::replace(
        lifecycle,
        BoundedCursorLifecycle::Released(reason, None, false),
    );
    *lifecycle = match previous {
        BoundedCursorLifecycle::Open(mut state) => state.released(reason),
        BoundedCursorLifecycle::Exhausted(_, observation) => {
            BoundedCursorLifecycle::Released(reason, Some(observation), false)
        }
        BoundedCursorLifecycle::Released(_, observation, overflow) => {
            BoundedCursorLifecycle::Released(reason, observation, overflow)
        }
    };
}

pub(super) fn cursor_observation(
    cursor: &mut BoundedCursorHandle,
) -> std::result::Result<Option<CursorObservation>, BoundedExecutionError> {
    let mut lifecycle = cursor_lifecycle(cursor);
    match &mut *lifecycle {
        BoundedCursorLifecycle::Open(state) => state.observation_with_boundary(false).map(Some),
        BoundedCursorLifecycle::Exhausted(_, observation) => Ok(Some(observation.clone())),
        BoundedCursorLifecycle::Released(_, observation, overflow) => {
            if *overflow {
                return Err(
                    Error::Other("bounded rows-examined counter overflow".to_string()).into(),
                );
            }
            Ok(observation.take())
        }
    }
}

pub(super) fn fetch_cursor(
    cursor: &mut BoundedCursorHandle,
    rows: Option<NonZeroUsize>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
    // A request cancelled before it starts consumes no continuation state.
    // Do not attach its already-fired listener to the parked cursor: that
    // listener terminally withdraws a read, rather than cancelling this page.
    if cancellation.is_cancelled()
        && !cursor
            ._state
            .erased
            .load(std::sync::atomic::Ordering::SeqCst)
        && matches!(*cursor_lifecycle(cursor), BoundedCursorLifecycle::Open(_))
    {
        return Err(BoundedExecutionError::Cancelled);
    }
    *cursor
        ._state
        .active_cancellation
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = cancellation.clone();
    struct DriveAfterFetch(Arc<BoundedCursorExecutionState>);
    impl Drop for DriveAfterFetch {
        fn drop(&mut self) {
            // The token belongs to this request. A later cancellation must
            // not affect the continuation parked between fetches.
            *self
                .0
                .active_cancellation
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = OwnerReadCancellation::new();
            self.0.wake_expiry();
        }
    }
    let _drive_after_fetch = DriveAfterFetch(cursor._state.clone());
    let mut lifecycle = cursor_lifecycle(cursor);
    if cursor
        ._state
        .erased
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        release_cursor_lifecycle(&mut lifecycle, BoundedCursorTerminalReason::Erased);
        return Err(cursor_not_found());
    }
    let current = std::mem::replace(
        &mut *lifecycle,
        BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::ExplicitClose, None, false),
    );
    let mut state = match current {
        BoundedCursorLifecycle::Released(
            BoundedCursorTerminalReason::Expired(expiry),
            observation,
            overflow,
        ) => {
            *lifecycle = BoundedCursorLifecycle::Released(
                BoundedCursorTerminalReason::Refused,
                observation,
                overflow,
            );
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                expiry,
            )));
        }
        BoundedCursorLifecycle::Released(reason, observation, overflow) => {
            *lifecycle = BoundedCursorLifecycle::Released(reason, observation, overflow);
            return Err(cursor_not_found());
        }
        BoundedCursorLifecycle::Exhausted(columns, observation) => {
            let fetch = exhausted_fetch(
                columns.try_clone(cursor._state.read_memory.clone())?,
                observation.clone(),
                cursor._state.publication.clone(),
                cancellation,
            );
            *lifecycle = BoundedCursorLifecycle::Exhausted(columns, observation);
            return fetch;
        }
        BoundedCursorLifecycle::Open(state) => state,
    };
    if let Err(error) = state.check_expiry() {
        state.finish_plugin_error(&error);
        *lifecycle = state.released(BoundedCursorTerminalReason::Refused);
        return Err(error);
    }
    if let Err(error) = state.context.begin_fetch(
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    ) {
        if matches!(error, BoundedExecutionError::Cancelled) {
            *lifecycle = BoundedCursorLifecycle::Open(state);
        } else {
            state.finish_plugin_error(&error);
            *lifecycle = state.released(BoundedCursorTerminalReason::Refused);
        }
        return Err(error);
    }
    let rows = match rows {
        Some(rows) => rows,
        None => match default_page_rows(state.context.limits) {
            Ok(rows) => rows,
            Err(error) => {
                state.finish_plugin_error(&error);
                *lifecycle = state.released(BoundedCursorTerminalReason::Refused);
                return Err(error);
            }
        },
    };
    // An explicit count above the effective row ceiling refuses this REQUEST,
    // not the read. Nothing has been pulled, no continuation position has been
    // consumed, and no charge has been taken, so the cursor stays exactly where
    // the caller left it -- which is what makes the refusal's own escape (ask
    // for a smaller page, or close and reopen) executable rather than advice
    // about a cursor that is already gone.
    if u64::try_from(rows.get()).unwrap_or(u64::MAX) > state.context.limits.result_rows {
        let error = RequestContext::limit_failure(
            ReadFailureLimit::ResultRows,
            state.context.limits.result_rows,
        );
        *lifecycle = BoundedCursorLifecycle::Open(state);
        return Err(error);
    }
    let outcome = state.fetch_page(rows);
    match outcome {
        Ok(fetch) => {
            if fetch.page.has_more {
                *lifecycle = BoundedCursorLifecycle::Open(state);
            } else {
                let columns = match CursorPage::try_empty(
                    &fetch.page.columns,
                    cursor._state.read_memory.clone(),
                ) {
                    Ok(columns) => columns,
                    Err(error) => {
                        let error = BoundedExecutionError::from(error);
                        state.finish_plugin_error(&error);
                        *lifecycle = state.released(BoundedCursorTerminalReason::Refused);
                        return Err(error);
                    }
                };
                state.finish_plugin_success();
                *lifecycle = BoundedCursorLifecycle::Exhausted(columns, fetch.observation.clone());
            }
            Ok(fetch)
        }
        Err(BoundedExecutionError::Cancelled) => {
            // fetch_page restored every unpublished row to the replay. The
            // same accounted continuation and captured root remain usable.
            *lifecycle = BoundedCursorLifecycle::Open(state);
            Err(BoundedExecutionError::Cancelled)
        }
        Err(error) => {
            let error = work_refusal_naming_the_statement(&state.sql, error);
            state.finish_plugin_error(&error);
            *lifecycle = state.released(BoundedCursorTerminalReason::Refused);
            Err(error)
        }
    }
}

pub(super) fn close_cursor(
    cursor: &mut BoundedCursorHandle,
) -> std::result::Result<(), BoundedExecutionError> {
    let mut lifecycle = cursor_lifecycle(cursor);
    let current = std::mem::replace(
        &mut *lifecycle,
        BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::ExplicitClose, None, false),
    );
    match current {
        BoundedCursorLifecycle::Open(mut state) => {
            state.finish_plugin_success();
            *lifecycle = state.released(BoundedCursorTerminalReason::ExplicitClose);
        }
        BoundedCursorLifecycle::Exhausted(columns, observation) => {
            drop(columns);
            *lifecycle = BoundedCursorLifecycle::Released(
                BoundedCursorTerminalReason::ExplicitClose,
                Some(observation),
                false,
            );
        }
        BoundedCursorLifecycle::Released(reason, observation, overflow) => {
            *lifecycle = BoundedCursorLifecycle::Released(reason, observation, overflow);
        }
    }
    drop(lifecycle);
    cursor._state.retire_expiry();
    Ok(())
}

/// What a paged vector cursor holds, measured against the accountant it really
/// charges. The public paged-cursor contract lives in
/// `tests/read/verified_ordered_run_work_resume_contract.rs`; this is the same
/// fault seen from the inside, in exact bytes, on success, on refusal and on
/// close.
#[cfg(all(test, feature = "test-seams"))]
mod paged_vector_accounting_tests {
    use super::*;
    use crate::Database;
    use contextdb_core::read_contract::DeadlineWait;

    struct FrozenClock;

    impl DeadlineClock for FrozenClock {
        fn now_ms(&self) -> u64 {
            0
        }

        fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    const LABEL_BYTES: usize = 4 * 1024;

    fn limits() -> ReadLimits {
        ReadLimits {
            result_rows: 256,
            result_bytes: 64 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 600_000,
            memory: 64 * 1024 * 1024,
            cursor_page_rows: 8,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 600_000,
            cursor_lifetime_ms: 600_000,
        }
    }

    fn params() -> HashMap<String, Value> {
        HashMap::from([
            ("query".to_owned(), Value::Vector(vec![1.0, 0.0])),
            ("label".to_owned(), Value::Text("w".repeat(LABEL_BYTES))),
        ])
    }

    fn statement(rows: usize) -> String {
        format!(
            "SELECT id FROM paged_rows WHERE label = $label ORDER BY embedding <=> $query LIMIT {rows}"
        )
    }

    fn fixture(path: &std::path::Path, rows: i64) -> Arc<Database> {
        let db = Database::open(path).expect("open the paged-cursor fixture");
        db.execute(
            "CREATE TABLE paged_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
            &HashMap::new(),
        )
        .expect("create the paged-cursor table");
        db.execute(
            "CREATE INDEX paged_rows_label ON paged_rows (label)",
            &HashMap::new(),
        )
        .expect("create the candidate index");
        let label = "w".repeat(LABEL_BYTES);
        for id in 0..rows {
            db.execute(
                "INSERT INTO paged_rows VALUES ($id, $label, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("label".to_owned(), Value::Text(label.clone())),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vec![1.0 - id as f32 / 512.0, id as f32 / 512.0]),
                    ),
                ]),
            )
            .expect("insert a paged-cursor row");
        }
        Arc::new(db)
    }

    fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
        rows.iter()
            .map(|row| match row.as_slice() {
                [Value::Int64(id)] => *id,
                other => panic!("expected one integer id, got {other:?}"),
            })
            .collect()
    }

    fn observation_credit_bytes(observation: &CursorObservation) -> usize {
        let live = observation.charge.as_ref().map(|charge| &charge.live);
        let returns = observation
            .group_page_returns
            .as_ref()
            .map(|returns| &returns._charge.live);
        live.map_or(0, |bytes| bytes.load(std::sync::atomic::Ordering::SeqCst))
            + returns
                .filter(|bytes| live.is_none_or(|prior| !Arc::ptr_eq(prior, bytes)))
                .map_or(0, |bytes| bytes.load(std::sync::atomic::Ordering::SeqCst))
    }

    fn release_retained_cursor_receipts(
        cursor: BoundedCursorHandle,
        observation: CursorObservation,
        reader: &crate::memory_accounting::MemoryAccountant,
        baseline: usize,
        trace_credit: usize,
    ) {
        let observation_credits =
            cursor.retained_read_bytes_with_observations(Some(&observation), None);
        assert!(
            observation_credits > 0,
            "the released cursor and its caller still hold admitted observations"
        );
        assert_eq!(
            reader.usage().used,
            baseline + observation_credits + trace_credit,
            "close releases kernel state and retains exactly the surviving observations and trace"
        );
        drop(cursor);
        let final_observation_credit = observation_credit_bytes(&observation);
        assert!(
            final_observation_credit > 0,
            "the caller's open observation keeps its original credit through the last holder"
        );
        assert_eq!(
            reader.usage().used,
            baseline + final_observation_credit + trace_credit,
            "dropping the cursor leaves only the caller observation and Database trace"
        );
        drop(observation);
        assert_eq!(
            reader.usage().used,
            baseline + trace_credit,
            "the final observation releases its credit while the Database trace remains alive"
        );
    }

    /// A paged vector cursor takes memory for its candidates, its scores and
    /// its output queue, holds them between fetches, and must give every byte
    /// back exactly once — against the reservation that took it. When the
    /// per-page settlement could not see that state it gave those bytes away
    /// underneath the search, and the fetch that released them under-ran the
    /// request: `bounded_read/release requested N bytes, M available of M`.
    #[test]
    fn a_drained_paged_vector_cursor_returns_every_byte_it_took() {
        let rows = 64usize;
        let root = tempfile::tempdir().expect("task-scoped fixture");
        let db = fixture(&root.path().join("paged.db"), rows as i64);
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            usize::try_from(limits().memory)
                .expect("the original declared reader limit fits usize"),
        ));
        let baseline = accountant.usage().used;

        let mut opened = open_cursor(
            Arc::clone(&db) as Arc<dyn ReadExecutionTarget>,
            &statement(rows),
            &params(),
            accountant.clone(),
            limits(),
            Arc::new(FrozenClock),
            OwnerReadCancellation::new(),
            None,
        )
        .expect("open a paged vector cursor");
        let first_page = opened.first_page.into_public();

        let held_open = accountant.usage().used;
        assert!(
            held_open > baseline,
            "an open cursor holds its continuation: baseline={baseline} open={held_open}"
        );

        let mut delivered = ids(&first_page.rows);
        let mut more = first_page.has_more;
        let page = NonZeroUsize::new(8).expect("a non-zero page");
        while more {
            let fetched = fetch_cursor(
                &mut opened.cursor,
                Some(page),
                OwnerReadCancellation::new(),
                None,
            )
            .expect("a paged vector cursor fetches its next page");
            let fetched_page = fetched.page.into_public();
            delivered.extend(ids(&fetched_page.rows));
            more = fetched_page.has_more;
        }
        assert_eq!(delivered.len(), rows, "every row, once: {delivered:?}");
        let mut sorted = delivered.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..rows as i64).collect::<Vec<_>>());

        let peak = accountant.high_water_for_test();
        close_cursor(&mut opened.cursor).expect("the drained cursor closes");
        let original_trace_credit = db.last_query_vector_trace_credit_bytes_for_test();
        assert!(
            original_trace_credit > 0,
            "the Database retains its last query trace"
        );
        release_retained_cursor_receipts(
            opened.cursor,
            opened.observation,
            &accountant,
            baseline,
            original_trace_credit,
        );

        let mut replacement = open_cursor(
            Arc::clone(&db) as Arc<dyn ReadExecutionTarget>,
            &statement(rows),
            &params(),
            accountant.clone(),
            limits(),
            Arc::new(FrozenClock),
            OwnerReadCancellation::new(),
            None,
        )
        .expect("a second query replaces the retained last-query trace");
        let replacement_first_page = replacement.first_page.into_public();
        assert!(
            !replacement_first_page.rows.is_empty(),
            "the second query performs real vector work"
        );
        drop(replacement_first_page);
        close_cursor(&mut replacement.cursor).expect("close the replacement query cursor");
        let replacement_trace_credit = db.last_query_vector_trace_credit_bytes_for_test();
        assert!(
            replacement_trace_credit > 0,
            "the replacement trace remains admitted"
        );
        release_retained_cursor_receipts(
            replacement.cursor,
            replacement.observation,
            &accountant,
            baseline,
            replacement_trace_credit,
        );
        assert!(
            accountant.usage().used
                <= baseline + original_trace_credit.max(replacement_trace_credit),
            "the second query retains at most one trace's credit"
        );
        assert!(
            accountant.usage().used < baseline + original_trace_credit + replacement_trace_credit,
            "replacing the last trace never retains the sum of both traces"
        );
        db.__reset_last_query_vector_trace_for_test();
        let after_close = accountant.usage().used;
        eprintln!(
            "paged vector cursor accounting: baseline={baseline} open={held_open} \
             peak={peak} after_close={after_close}"
        );
        assert_eq!(
            after_close, baseline,
            "closing returns exactly what the cursor took: baseline={baseline} \
             after_close={after_close}"
        );
        assert!(
            peak >= held_open,
            "the peak covers what the cursor held: peak={peak} open={held_open}"
        );
    }

    /// A cursor refused part way through is held to the same rule: nothing it
    /// took stays behind, and the refusal is the typed one the caller declared.
    #[test]
    fn a_refused_paged_vector_cursor_leaves_nothing_admitted() {
        let rows = 64usize;
        let root = tempfile::tempdir().expect("task-scoped fixture");
        let db = fixture(&root.path().join("refused.db"), rows as i64);
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            usize::try_from(limits().memory)
                .expect("the original declared reader limit fits usize"),
        ));
        let baseline = accountant.usage().used;

        let mut tight = limits();
        tight.work = 1;
        let refused = open_cursor(
            Arc::clone(&db) as Arc<dyn ReadExecutionTarget>,
            &statement(rows),
            &params(),
            accountant.clone(),
            tight,
            Arc::new(FrozenClock),
            OwnerReadCancellation::new(),
            None,
        );
        let error = match refused {
            Ok(_) => panic!("one work unit cannot open this cursor"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, BoundedExecutionError::Refused(failure)
                if matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail)
                    if detail.limit == ReadFailureLimit::Work)),
            "the refusal is the declared work ceiling: {error:?}"
        );
        let after_refusal = accountant.usage().used;
        eprintln!("refused paged vector cursor: baseline={baseline} after_refusal={after_refusal}");
        assert_eq!(
            after_refusal, baseline,
            "a refused open leaves nothing admitted: baseline={baseline} \
             after_refusal={after_refusal}"
        );
    }

    /// Closing releases kernel state. Surviving observations and the last
    /// query trace keep their own credit until their final holders release it.
    #[test]
    fn closing_a_part_drained_paged_vector_cursor_returns_every_byte() {
        let rows = 64usize;
        let root = tempfile::tempdir().expect("task-scoped fixture");
        let db = fixture(&root.path().join("partial.db"), rows as i64);
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            usize::try_from(limits().memory)
                .expect("the original declared reader limit fits usize"),
        ));
        let baseline = accountant.usage().used;

        let mut opened = open_cursor(
            Arc::clone(&db) as Arc<dyn ReadExecutionTarget>,
            &statement(rows),
            &params(),
            accountant.clone(),
            limits(),
            Arc::new(FrozenClock),
            OwnerReadCancellation::new(),
            None,
        )
        .expect("open a paged vector cursor");
        let first_page = opened.first_page.into_public();
        assert!(first_page.has_more, "this cursor has more pages to give");
        let page = NonZeroUsize::new(8).expect("a non-zero page");
        let _ = fetch_cursor(
            &mut opened.cursor,
            Some(page),
            OwnerReadCancellation::new(),
            None,
        )
        .expect("one more page");
        let held = accountant.usage().used;
        assert!(held > baseline, "the part-drained cursor is holding state");
        close_cursor(&mut opened.cursor).expect("a part-drained cursor closes");
        let trace_credit = db.last_query_vector_trace_credit_bytes_for_test();
        assert!(
            trace_credit > 0,
            "the Database retains the part-drained query trace"
        );
        release_retained_cursor_receipts(
            opened.cursor,
            opened.observation,
            &accountant,
            baseline,
            trace_credit,
        );
        db.__reset_last_query_vector_trace_for_test();
        let after_close = accountant.usage().used;
        eprintln!(
            "part-drained paged vector cursor: baseline={baseline} held={held} \
             after_close={after_close}"
        );
        assert_eq!(after_close, baseline, "closing returns the whole charge");
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod cursor_replay_budget_tests {
    use super::*;
    use crate::local_transport::ManualDeadlineClock;

    #[test]
    fn replay_work_does_not_reset_at_internal_page_boundaries() {
        let db = Database::open(":memory:").unwrap();
        let clock = Arc::new(ManualDeadlineClock::default());
        let limits = ReadLimits {
            work: 2,
            ..ReadLimits::default()
        };
        let context = RequestContext::new(
            db.declared_reader_memory()
                .expect("admit the fixture request reader"),
            limits,
            ReadResumption::AcrossFetches,
            clock.clone(),
            OwnerReadCancellation::new(),
            None,
            None,
        )
        .unwrap();
        with_cursor_replay_budget(limits, clock, 0, 0, 0, || {
            context
                .charge_operator(BoundedWorkSource::UnindexedSort)
                .unwrap();
            context
                .begin_fetch(OwnerReadCancellation::new(), None)
                .unwrap();
            context
                .charge_operator(BoundedWorkSource::UnindexedSort)
                .unwrap();
            context
                .begin_fetch(OwnerReadCancellation::new(), None)
                .unwrap();
            assert!(
                matches!(context.charge_operator(BoundedWorkSource::UnindexedSort), Err(BoundedExecutionError::Refused(failure)) if matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail) if detail.limit == ReadFailureLimit::Work))
            );
            assert!(!cursor_replay_work_available());
        });
        assert!(cursor_replay_work_available(), "request scope is released");
    }

    #[test]
    fn replay_checks_original_active_idle_and_lifetime_deadlines_inside_the_kernel() {
        let db = Database::open(":memory:").unwrap();
        for (active_ms, idle_ms, lifetime_ms, expected) in [
            (10, 100, 100, None),
            (100, 10, 100, Some(CursorExpiryKind::Idle)),
            (100, 10, 10, Some(CursorExpiryKind::Lifetime)),
        ] {
            let clock = Arc::new(ManualDeadlineClock::default());
            let limits = ReadLimits {
                active_ms,
                cursor_idle_ms: idle_ms,
                cursor_lifetime_ms: lifetime_ms,
                ..ReadLimits::default()
            };
            let context = RequestContext::new(
                db.declared_reader_memory()
                    .expect("admit the fixture request reader"),
                limits,
                ReadResumption::AcrossFetches,
                clock.clone(),
                OwnerReadCancellation::new(),
                None,
                None,
            )
            .unwrap();
            with_cursor_replay_budget(limits, clock.clone(), 0, 0, 0, || {
                context
                    .charge_operator(BoundedWorkSource::UnindexedSort)
                    .unwrap();
                clock.advance_to(11);
                context
                    .begin_fetch(OwnerReadCancellation::new(), None)
                    .unwrap();
                let Err(BoundedExecutionError::Refused(failure)) = context.checkpoint() else {
                    panic!("the original request deadline must survive an internal page reset");
                };
                match expected {
                    Some(kind) => assert_eq!(failure, ReadFailure::cursor_expired(kind)),
                    None => assert!(
                        matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail) if detail.limit == ReadFailureLimit::ActiveMs)
                    ),
                }
            });
        }
    }
}

/// Dynamic capacities owned by the typed specification; the enclosing enum's
/// inline fields are already counted by its AST/plan container.
fn window_capacity_bytes(
    window: &contextdb_parser::ast::RowNumberWindow,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        window.partition_by.capacity(),
        std::mem::size_of::<contextdb_parser::ast::ColumnRef>(),
        "window partitions",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            window.order_by.capacity(),
            std::mem::size_of::<contextdb_parser::ast::WindowOrderColumn>(),
            "window orders",
        )?,
        "window specification",
    )?;
    for column in window
        .partition_by
        .iter()
        .chain(window.order_by.iter().map(|order| &order.column))
    {
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.table),
            "window qualifier",
        )?;
        bytes = checked_size_add(bytes, column.column.capacity(), "window column")?;
    }
    Ok(bytes)
}
