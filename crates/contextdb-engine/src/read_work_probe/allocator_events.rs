//! Passive, fixed-storage bridge for the existing integration global allocator.
//! No allocator, credits, heap log, or policy is introduced here. All hooks are
//! nonallocating and never panic inside GlobalAlloc. Qualification is fail-closed.
use std::alloc::Layout;
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

pub const OVERFLOW: u64 = 1;
pub const REENTRANT: u64 = 2;
pub const FOREIGN_THREAD: u64 = 4;
pub const STALE: u64 = 8;
pub const MISSING: u64 = 16;
pub const MISMATCH: u64 = 32;
pub const REQUEST_CAPACITY: usize = 65_536;
const WATCH_CAPACITY: usize = 16;
const FILTER_CAPACITY: usize = 16;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static ERRORS: AtomicU64 = AtomicU64::new(0);
static WINDOW_THREAD: AtomicUsize = AtomicUsize::new(0);
static START_NS: AtomicU64 = AtomicU64::new(0);
static FIRST_REASON: AtomicU64 = AtomicU64::new(0);
static FIRST_THREAD: AtomicUsize = AtomicUsize::new(0);
static FIRST_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);
static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
fn monotonic_ns() -> u64 {
    // Initialized before ACTIVE; sampling never initializes, allocates or locks.
    EPOCH.get().map_or(0, |epoch| {
        epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirstFailure {
    pub reason: u64,
    pub caller_is_window_thread: bool,
    pub elapsed_ns: u64,
}
/// Read only after the recording window has closed; this is diagnostic evidence.
pub fn first_failure() -> Option<FirstFailure> {
    let reason = FIRST_REASON.load(Ordering::SeqCst);
    (reason != 0).then(|| FirstFailure {
        reason,
        caller_is_window_thread: FIRST_THREAD.load(Ordering::SeqCst) == 1,
        elapsed_ns: FIRST_ELAPSED_NS.load(Ordering::SeqCst),
    })
}
thread_local! { static THREAD: Cell<u8> = const { Cell::new(0) }; }
fn thread() -> usize {
    THREAD.try_with(|key| key as *const _ as usize).unwrap_or(0)
}
pub(crate) fn fail(reason: u64) {
    if FIRST_REASON
        .compare_exchange(0, reason, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let caller = thread();
        FIRST_THREAD.store(
            usize::from(caller != 0 && caller == WINDOW_THREAD.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );
        FIRST_ELAPSED_NS.store(
            monotonic_ns().saturating_sub(START_NS.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );
    }
    ERRORS.fetch_or(reason, Ordering::SeqCst);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Request {
    pub pointer: usize,
    pub bytes: usize,
    pub align: usize,
    /// Every allocation attempt advances this ordinal, including filtered ones.
    pub ordinal: usize,
    /// Recording-window generation; (generation, ordinal) identifies a request,
    /// independently of its address or reusable storage slot.
    pub generation: usize,
    pub event_ordinal: usize,
    /// A realloc is the original allocator's alloc/copy/free implementation.
    pub previous_pointer: usize,
    pub previous_bytes: usize,
    /// Counter address is historical identity only. The value is loaded by
    /// the independent allocator immediately BEFORE System, never afterward.
    pub funding_before_system: Option<(usize, u64)>,
}
impl Request {
    pub fn contains(self, payload: usize) -> bool {
        self.pointer != 0
            && self.pointer <= payload
            && self
                .pointer
                .checked_add(self.bytes)
                .is_some_and(|end| payload < end)
    }
}
#[derive(Clone, Copy)]
struct Entry {
    request: Request,
    state: u8,
    free_event: usize,
    protected: bool,
}
const EMPTY_REQUEST: Request = Request {
    pointer: 0,
    bytes: 0,
    align: 0,
    ordinal: 0,
    generation: 0,
    event_ordinal: 0,
    previous_pointer: 0,
    previous_bytes: 0,
    funding_before_system: None,
};
const EMPTY_ENTRY: Entry = Entry {
    request: EMPTY_REQUEST,
    state: 0,
    free_event: 0,
    protected: false,
};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FreeReceipt {
    pub request: Request,
    pub counter: usize,
    pub before_system: u64,
    pub after_system: u64,
    pub physical_frees: usize,
    pub event_ordinal: usize,
}
#[derive(Clone, Copy)]
struct Watch {
    receipt: FreeReceipt,
    active: bool,
    pending: bool,
    serial: usize,
}
const EMPTY_WATCH: Watch = Watch {
    receipt: FreeReceipt {
        request: EMPTY_REQUEST,
        counter: 0,
        before_system: 0,
        after_system: 0,
        physical_frees: 0,
        event_ordinal: 0,
    },
    active: false,
    pending: false,
    serial: 0,
};
struct State {
    generation: usize,
    thread: usize,
    ordinal: usize,
    event: usize,
    last: Request,
    pending_constructor: bool,
    pending_counter: usize,
    pending_sample: Option<(usize, u64)>,
    requests: [Entry; REQUEST_CAPACITY],
    count: usize,
    filters: [(usize, usize); FILTER_CAPACITY],
    filter_count: usize,
    watches: [Watch; WATCH_CAPACITY],
    serial: usize,
}
// The only large storage is STATIC. Never copy it onto TLS or a caller stack.
static STATE: Mutex<State> = Mutex::new(State {
    generation: 0,
    thread: 0,
    ordinal: 0,
    event: 0,
    last: EMPTY_REQUEST,
    pending_constructor: false,
    pending_counter: 0,
    pending_sample: None,
    requests: [EMPTY_ENTRY; REQUEST_CAPACITY],
    count: 0,
    filters: [(0, 0); FILTER_CAPACITY],
    filter_count: 0,
    watches: [EMPTY_WATCH; WATCH_CAPACITY],
    serial: 0,
});
fn access<T>(operation: impl FnOnce(&mut State) -> T) -> Option<T> {
    if !ACTIVE.load(Ordering::SeqCst) {
        return None;
    }
    let caller = thread();
    if caller == 0 || caller != WINDOW_THREAD.load(Ordering::SeqCst) {
        if ACTIVE.load(Ordering::SeqCst) {
            fail(FOREIGN_THREAD);
        }
        return None;
    }
    let Ok(mut state) = STATE.try_lock() else {
        fail(REENTRANT);
        return None;
    };
    // Cleanup can revoke recording without this lock. A caller that observed
    // ACTIVE before revocation must not enter with an abandoned watch afterward.
    if !ACTIVE.load(Ordering::SeqCst) {
        return None;
    }
    if state.thread == 0 || state.thread != thread() {
        fail(FOREIGN_THREAD);
        return None;
    }
    Some(operation(&mut state))
}
fn disarm_failed_cleanup() {
    // Never wait for a guard held by a reentrant callback or a foreign thread.
    // Only the window's thread may sample counters; it cannot sample concurrently
    // with its own cleanup. Foreign holders reject access before any raw load.
    ACTIVE.store(false, Ordering::SeqCst);
    fail(MISSING);
    WINDOW_THREAD.store(0, Ordering::SeqCst);
}
fn advance(value: &mut usize) -> usize {
    match value.checked_add(1) {
        Some(next) => {
            *value = next;
            next
        }
        None => {
            fail(OVERFLOW);
            0
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Unqualified {
    pub reasons: u64,
}
impl std::fmt::Debug for Unqualified {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Unqualified")
            .field("reasons", &self.reasons)
            .field("first_failure", &first_failure())
            .finish()
    }
}
fn qualified<T>(value: T) -> Result<T, Unqualified> {
    let reasons = ERRORS.load(Ordering::SeqCst);
    if reasons == 0 {
        Ok(value)
    } else {
        Err(Unqualified { reasons })
    }
}
fn missing<T>() -> Result<T, Unqualified> {
    fail(MISSING);
    Err(Unqualified {
        reasons: ERRORS.load(Ordering::SeqCst),
    })
}

/// One exclusive same-thread recording window. Empty filters capture all
/// requests; otherwise capture matching actual Layouts plus native constructors.
/// Filters select observations only; they never affect allocation or accounting.
pub struct Window {
    generation: usize,
    finished: bool,
    _thread: PhantomData<Rc<()>>,
}
impl Window {
    pub fn begin(layouts: &[Layout]) -> Result<Self, Unqualified> {
        let Ok(mut state) = STATE.try_lock() else {
            fail(REENTRANT);
            return Err(Unqualified { reasons: REENTRANT });
        };
        if ACTIVE.load(Ordering::SeqCst) {
            fail(REENTRANT);
            return Err(Unqualified { reasons: REENTRANT });
        }
        ERRORS.store(0, Ordering::SeqCst);
        FIRST_REASON.store(0, Ordering::SeqCst);
        FIRST_THREAD.store(0, Ordering::SeqCst);
        FIRST_ELAPSED_NS.store(0, Ordering::SeqCst);
        EPOCH.get_or_init(std::time::Instant::now);
        START_NS.store(monotonic_ns(), Ordering::SeqCst);
        if layouts.len() > FILTER_CAPACITY {
            fail(OVERFLOW);
            return Err(Unqualified { reasons: OVERFLOW });
        }
        state.generation = match state.generation.checked_add(1) {
            Some(next) => next,
            None => {
                fail(OVERFLOW);
                return Err(Unqualified { reasons: OVERFLOW });
            }
        };
        state.thread = thread();
        state.ordinal = 0;
        state.event = 0;
        state.last = EMPTY_REQUEST;
        state.pending_constructor = false;
        state.count = 0;
        state.pending_counter = 0;
        state.pending_sample = None;
        state.filter_count = layouts.len();
        for (index, layout) in layouts.iter().enumerate() {
            state.filters[index] = (layout.size(), layout.align());
        }
        for watch in &mut state.watches {
            *watch = EMPTY_WATCH;
        }
        if state.thread == 0 {
            fail(FOREIGN_THREAD);
            return Err(Unqualified {
                reasons: FOREIGN_THREAD,
            });
        }
        WINDOW_THREAD.store(state.thread, Ordering::SeqCst);
        ACTIVE.store(true, Ordering::SeqCst);
        qualified(Self {
            generation: state.generation,
            finished: false,
            _thread: PhantomData,
        })
    }
    /// Call after every free watch has been checked and dropped. Any earlier
    /// missing/foreign/stale/overflow event remains sticky through this result.
    pub fn finish(mut self) -> Result<(), Unqualified> {
        let ended = access(|state| {
            if !self.check(state) {
                return false;
            }
            if (state.pending_constructor
                || state.pending_counter != 0
                || state.pending_sample.is_some())
                || state.watches.iter().any(|watch| watch.active)
            {
                fail(MISSING);
            }
            ACTIVE.store(false, Ordering::SeqCst);
            WINDOW_THREAD.store(0, Ordering::SeqCst);
            true
        })
        .unwrap_or(false);
        if !ended {
            disarm_failed_cleanup();
        }
        self.finished = true;
        if ended { qualified(()) } else { missing() }
    }
    fn check(&self, state: &State) -> bool {
        if state.generation == self.generation {
            true
        } else {
            fail(STALE);
            false
        }
    }
    pub fn status(&self) -> Result<(), Unqualified> {
        if access(|state| self.check(state)).unwrap_or(false) {
            qualified(())
        } else {
            missing()
        }
    }
    /// Number of occupied slots, not cumulative selected requests or attempts.
    /// Slot indices are snapshots: an unprotected freed slot can be overwritten
    /// by a later allocator hook. Use request identity for delayed free lookup.
    pub fn request_count(&self) -> Result<usize, Unqualified> {
        match access(|state| self.check(state).then_some(state.count)).flatten() {
            Some(count) => qualified(count),
            None => missing(),
        }
    }
    /// Current occupant of a slot; enumeration alone does not retain it.
    pub fn request(&self, index: usize) -> Result<Request, Unqualified> {
        match access(|state| {
            if !self.check(state) || index >= state.count {
                return None;
            }
            Some(state.requests[index].request)
        })
        .flatten()
        {
            Some(request) => qualified(request),
            None => missing(),
        }
    }
    /// All allocator attempts, including filtered and failed attempts, in this
    /// window. This ordinal never decreases when a record slot is reused.
    pub fn allocation_attempts(&self) -> Result<usize, Unqualified> {
        match access(|state| self.check(state).then_some(state.ordinal)).flatten() {
            Some(count) => qualified(count),
            None => missing(),
        }
    }
    /// Retains the current slot occupant's evidence until finish. An index saved
    /// across allocator hooks is not an identity; use the identity lookup below.
    pub fn physical_free_event(&self, index: usize) -> Result<Option<usize>, Unqualified> {
        match access(|state| {
            if !self.check(state) || index >= state.count {
                return None;
            }
            state.requests[index].protected = true;
            let entry = state.requests[index];
            if entry.state == 2 {
                fail(MISSING);
            }
            Some((entry.state == 3).then_some(entry.free_event))
        })
        .flatten()
        {
            Some(event) => qualified(event),
            None => missing(),
        }
    }
    /// Exact identity lookup fails closed if an unprotected freed record has
    /// already been recycled. Expected Layout or a reused address cannot match.
    pub fn physical_free_event_for_request(
        &self,
        request: Request,
    ) -> Result<Option<usize>, Unqualified> {
        match access(|state| {
            if !self.check(state) {
                return None;
            }
            let Some(entry) = state.requests[..state.count]
                .iter_mut()
                .find(|entry| entry.request == request)
            else {
                fail(STALE);
                return None;
            };
            entry.protected = true;
            if entry.state == 2 {
                fail(MISSING);
            }
            Some((entry.state == 3).then_some(entry.free_event))
        })
        .flatten()
        {
            Some(event) => qualified(event),
            None => missing(),
        }
    }
    /// Matches the actual request containing a live payload. Address reuse or
    /// ambiguity cannot qualify; neither expected Layout nor residual bytes do.
    pub fn live_request(&self, payload: usize) -> Result<Request, Unqualified> {
        let result = access(|state| {
            if !self.check(state) {
                return None;
            }
            let mut result = None;
            for entry in &mut state.requests[..state.count] {
                if entry.state == 1 && entry.request.contains(payload) {
                    if result.is_some() {
                        fail(MISMATCH);
                        return None;
                    }
                    entry.protected = true;
                    result = Some(entry.request);
                }
            }
            result
        })
        .flatten();
        match result {
            Some(request) => qualified(request),
            None => missing(),
        }
    }
    /// Each counter owns a strong pin of its original funding allocation.
    /// Forgetting a watch leaks that pin, keeping its raw counter live.
    /// Watch N with the separate original parent pin, never N's inline counter.
    pub fn watch<'w, 'c>(
        &'w self,
        request: Request,
        counter: FundingCounter<'c>,
    ) -> Result<FreeWatch<'w, 'c>, Unqualified> {
        let slot = access(|state| {
            if !self.check(state) {
                return None;
            }
            let Some(entry) = state.requests[..state.count]
                .iter_mut()
                .find(|entry| entry.state == 1 && entry.request == request)
            else {
                fail(STALE);
                return None;
            };
            // Keep watched evidence even after its watch has been checked/dropped.
            entry.protected = true;
            if state
                .watches
                .iter()
                .any(|watch| watch.active && watch.receipt.request.pointer == request.pointer)
            {
                fail(MISMATCH);
                return None;
            }
            let Some(index) = state.watches.iter().position(|watch| !watch.active) else {
                fail(OVERFLOW);
                return None;
            };
            let serial = advance(&mut state.serial);
            state.watches[index] = Watch {
                active: true,
                serial,
                receipt: FreeReceipt {
                    request,
                    counter: counter.pointer,
                    ..FreeReceipt::default()
                },
                ..EMPTY_WATCH
            };
            Some((index, serial))
        })
        .flatten();
        match slot {
            Some((index, serial)) => qualified(FreeWatch {
                window: self,
                index,
                serial,
                _counter: counter,
            }),
            None => missing(),
        }
    }
}
impl Drop for Window {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let ended = access(|state| {
            if !self.check(state) {
                return false;
            }
            if (state.pending_constructor
                || state.pending_counter != 0
                || state.pending_sample.is_some())
                || state.watches.iter().any(|watch| watch.active)
            {
                fail(MISSING);
            }
            ACTIVE.store(false, Ordering::SeqCst);
            WINDOW_THREAD.store(0, Ordering::SeqCst);
            true
        })
        .unwrap_or(false);
        if !ended {
            disarm_failed_cleanup();
        }
    }
}

// These are existing original owners, never replacement allocations or invoices.
#[derive(Clone)]
enum FundingPin {
    Native {
        _owner: contextdb_core::read_memory::ReadMemoryOwner,
    },
    Parent {
        _owner: crate::vector_read_memory::DeclaredReadRoot,
    },
}

/// Constructed only by genuine original-owner pins in vector_read_memory.
/// Cloning retains the same allocation without allocating or charging credits.
/// Drop all counter clones before asserting the original allocation's final free.
#[derive(Clone)]
pub struct FundingCounter<'a> {
    pointer: usize,
    _pin: FundingPin,
    _borrow: PhantomData<&'a AtomicU64>,
}
impl<'a> FundingCounter<'a> {
    /// SAFETY: pointer must address a live AtomicU64 within this original owner.
    pub(crate) unsafe fn from_native_pinned(
        pointer: usize,
        owner: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Self {
        Self {
            pointer,
            _pin: FundingPin::Native { _owner: owner },
            _borrow: PhantomData,
        }
    }
    /// SAFETY: pointer must address a live AtomicU64 within this original parent.
    pub(crate) unsafe fn from_parent_pinned(
        pointer: usize,
        owner: crate::vector_read_memory::DeclaredReadRoot,
    ) -> Self {
        Self {
            pointer,
            _pin: FundingPin::Parent { _owner: owner },
            _borrow: PhantomData,
        }
    }
    pub fn value(&self) -> u64 {
        // SAFETY: the owned original pin keeps the counter live through this load.
        unsafe { (&*(self.pointer as *const AtomicU64)).load(Ordering::SeqCst) }
    }
    pub fn address(&self) -> usize {
        self.pointer
    }
}
pub struct FreeWatch<'w, 'c> {
    window: &'w Window,
    index: usize,
    serial: usize,
    _counter: FundingCounter<'c>,
}
impl FreeWatch<'_, '_> {
    pub fn receipt(&self) -> Result<FreeReceipt, Unqualified> {
        let result = access(|state| {
            if !self.window.check(state) {
                return None;
            }
            let watch = state.watches[self.index];
            if !watch.active || watch.serial != self.serial {
                fail(STALE);
                return None;
            }
            if watch.pending || watch.receipt.physical_frees != 1 {
                fail(MISSING);
                return None;
            }
            Some(watch.receipt)
        })
        .flatten();
        match result {
            Some(receipt) => qualified(receipt),
            None => missing(),
        }
    }
}
impl Drop for FreeWatch<'_, '_> {
    fn drop(&mut self) {
        let disarmed = access(|state| {
            if !self.window.check(state) {
                return false;
            }
            let watch = &mut state.watches[self.index];
            if watch.serial != self.serial || !watch.active {
                fail(STALE);
                return false;
            }
            if watch.pending || watch.receipt.physical_frees != 1 {
                fail(MISSING);
            }
            // Disarm even during unwinding, before the owned counter pin drops.
            watch.active = false;
            true
        })
        .unwrap_or(false);
        if !disarmed {
            disarm_failed_cleanup();
        }
    }
}

/// The caller borrows the actual constructor owner/invoice through the next
/// System request. No watch can retain or dereference this address afterward.
pub(crate) fn funding_next(counter: usize) {
    access(|state| {
        if counter == 0 || state.pending_counter != 0 || state.pending_sample.is_some() {
            fail(MISSING);
            return;
        }
        state.pending_counter = counter;
    });
}
/// Called by the existing allocator before System.alloc/alloc_zeroed, not by
/// the producer that supplies the expected inventory or reservation receipt.
pub fn before_alloc(_layout: Layout) {
    crate::vector_read_memory::root_observations::install_physical_observer();
    access(|state| {
        if state.pending_sample.is_some() {
            fail(MISSING);
            return;
        }
        if state.pending_counter != 0 {
            let counter = state.pending_counter;
            // SAFETY: the immediately enclosing real producer/invoice keeps
            // this original counter live until System returns to it.
            let bytes = unsafe { &*(counter as *const AtomicU64) }.load(Ordering::SeqCst);
            state.pending_sample = Some((counter, bytes));
        }
    });
}

/// Called after the real System request, including null results.
pub fn allocated(pointer: *mut u8, layout: Layout) {
    crate::vector_read_memory::root_observations::install_physical_observer();
    record_request(pointer, layout, 0, 0);
}
pub fn reallocated(old: *mut u8, old_layout: Layout, pointer: *mut u8, layout: Layout) {
    crate::vector_read_memory::root_observations::install_physical_observer();
    record_request(pointer, layout, old as usize, old_layout.size());
}
fn record_request(
    pointer: *mut u8,
    layout: Layout,
    previous_pointer: usize,
    previous_bytes: usize,
) {
    access(|state| {
        let ordinal = advance(&mut state.ordinal);
        let event_ordinal = advance(&mut state.event);
        let request = Request {
            pointer: pointer as usize,
            bytes: layout.size(),
            align: layout.align(),
            ordinal,
            generation: state.generation,
            event_ordinal,
            previous_pointer,
            previous_bytes,
            funding_before_system: state.pending_sample.take(),
        };
        state.last = request;
        if state.pending_counter != 0 && request.funding_before_system.is_none() {
            fail(MISSING);
        }
        let funded = state.pending_counter != 0;
        state.pending_counter = 0;
        let constructor = state.pending_constructor;
        let selected = constructor
            || funded
            || state.filter_count == 0
            || state.filters[..state.filter_count].contains(&(layout.size(), layout.align()));
        state.pending_constructor = false;
        if !selected {
            return;
        }
        if pointer.is_null() {
            fail(MISSING);
            return;
        }
        // Only after_dealloc can make a slot reusable. Never evict live,
        // pending-free, constructor, watched, or explicitly retained evidence.
        let slot = if state.count < REQUEST_CAPACITY {
            let slot = state.count;
            state.count += 1;
            slot
        } else if let Some(slot) = state.requests[..state.count]
            .iter()
            .position(|entry| entry.state == 3 && !entry.protected)
        {
            slot
        } else {
            fail(OVERFLOW);
            return;
        };
        state.requests[slot] = Entry {
            request,
            state: 1,
            free_event: 0,
            protected: constructor,
        };
    });
}

/// Scalar token carries no pointer dereference after the actual System free.
/// It only identifies a still-borrowed funding counter and original watch slot.
#[must_use]
pub struct FreeToken {
    generation: usize,
    event: usize,
    pointer: usize,
    watches: u16,
    request: Option<(usize, usize)>,
    producer: u64,
}
pub fn before_dealloc(pointer: *mut u8, layout: Layout) -> FreeToken {
    crate::vector_read_memory::root_observations::install_physical_observer();
    let producer = crate::vector_read_memory::root_observations::before_physical_free(
        pointer as usize,
        layout,
    );
    access(|state| {
        let event = advance(&mut state.event);
        let address = pointer as usize;
        let mut request = None;
        for (slot, entry) in state.requests[..state.count].iter_mut().enumerate() {
            if entry.request.pointer == address && entry.state == 1 {
                if request.is_some() {
                    fail(MISMATCH);
                }
                request = Some((slot, entry.request.ordinal));
                if (entry.request.bytes, entry.request.align) != (layout.size(), layout.align()) {
                    fail(MISMATCH);
                }
                entry.state = 2;
                entry.free_event = event;
            }
        }
        let mut watches = 0;
        for (index, watch) in state.watches.iter_mut().enumerate() {
            if !watch.active || watch.receipt.request.pointer != address {
                continue;
            }
            if watch.pending || watch.receipt.physical_frees != 0 {
                fail(STALE);
                continue;
            }
            if (watch.receipt.request.bytes, watch.receipt.request.align)
                != (layout.size(), layout.align())
            {
                fail(MISMATCH);
            }
            watch.receipt.before_system =
                unsafe { (&*(watch.receipt.counter as *const AtomicU64)).load(Ordering::SeqCst) };
            watch.receipt.event_ordinal = event;
            watch.pending = true;
            watches |= 1 << index;
        }
        FreeToken {
            generation: state.generation,
            event,
            pointer: address,
            watches,
            request,
            producer,
        }
    })
    .unwrap_or(FreeToken {
        generation: 0,
        event: 0,
        pointer: 0,
        watches: 0,
        request: None,
        producer,
    })
}
pub fn after_dealloc(token: FreeToken) {
    crate::vector_read_memory::root_observations::after_physical_free(token.producer);
    if token.generation == 0 {
        return;
    }
    access(|state| {
        if token.generation != state.generation {
            fail(STALE);
            return;
        }
        if let Some((slot, ordinal)) = token.request {
            let entry = &mut state.requests[slot];
            if entry.request.generation != token.generation
                || entry.request.ordinal != ordinal
                || entry.request.pointer != token.pointer
                || entry.state != 2
                || entry.free_event != token.event
            {
                fail(STALE);
                return;
            }
            // The real System.dealloc has returned. Only now may a subsequent
            // allocation reuse this slot, and only if its evidence is unprotected.
            entry.state = 3;
        }
        for (index, watch) in state.watches.iter_mut().enumerate() {
            if token.watches & (1 << index) == 0 {
                continue;
            }
            if !watch.active || !watch.pending || watch.receipt.event_ordinal != token.event {
                fail(STALE);
                continue;
            }
            watch.receipt.after_system =
                unsafe { (&*(watch.receipt.counter as *const AtomicU64)).load(Ordering::SeqCst) };
            watch.receipt.physical_frees += 1;
            watch.pending = false;
        }
    });
}

pub(crate) fn constructor_ordinal() -> usize {
    access(|state| {
        if state.pending_constructor {
            fail(MISSING);
        }
        state.pending_constructor = true;
        state.ordinal
    })
    .unwrap_or(0)
}
pub(crate) fn constructor_request(
    before: usize,
    payload: usize,
    layout: Layout,
) -> Option<Request> {
    access(|state| {
        let request = state.last;
        if state.pending_constructor || before.checked_add(1) != Some(request.ordinal) {
            fail(MISSING);
            return None;
        }
        if !request.contains(payload)
            || (request.bytes, request.align) != (layout.size(), layout.align())
        {
            fail(MISMATCH);
            return None;
        }
        if ERRORS.load(Ordering::SeqCst) != 0 {
            return None;
        }
        Some(request)
    })
    .flatten()
}

#[cfg(test)]
mod observer_tests {
    use super::*;
    #[test]
    fn foreign_contention_and_same_thread_reentry_remain_distinct() {
        let window = Window::begin(&[]).unwrap();
        access(|_| {
            // Join while holding STATE: a foreign caller must never try its lock.
            std::thread::spawn(|| {
                access(|_| panic!("foreign caller entered state"));
            })
            .join()
            .unwrap();
        });
        assert_eq!(window.finish().unwrap_err().reasons, FOREIGN_THREAD);
        let failure = first_failure().unwrap();
        assert_eq!(failure.reason, FOREIGN_THREAD);
        assert!(!failure.caller_is_window_thread);
        let window = Window::begin(&[]).unwrap();
        access(|_| {
            access(|_| panic!("reentrant caller entered state"));
        });
        assert_eq!(window.finish().unwrap_err().reasons, REENTRANT);
        let failure = first_failure().unwrap();
        assert_eq!(failure.reason, REENTRANT);
        assert!(failure.caller_is_window_thread);
        let window = Window::begin(&[]).unwrap();
        window.finish().unwrap();
        assert_eq!(first_failure(), None);
    }
}
