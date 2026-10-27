//! Fixed test receipts. No user callbacks, acquisition, admission, formatting or allocation.
//! Fixture capacities are observation limits: exhaustion invalidates the run, never production policy.
use crate::read_memory::{ReadCredit, ReadMemoryBudget, ReadMemoryOwner};
use std::alloc::Layout;
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

const RECORDS: usize = 2048;
const WATCHES: usize = 256;
const LOANS: usize = 32;
const DEPTH: usize = 16;
static ERRORS: AtomicU64 = AtomicU64::new(0);
// Error bits: recursive=1, TLS=2, capacity=4, identity/state=8, layout=16,
// missing constructor/allocator=32, unavailable funding=64, unfinished lifetime=128, underfunded physical cut=256.
fn error(bit: u64) {
    ERRORS.fetch_or(bit, Ordering::SeqCst);
}

/// The original provider constructs this borrow from a REAL typed atomic, never an identity cast.
pub enum FundingCounter<'a> {
    Usize(&'a AtomicUsize),
    U64(&'a AtomicU64),
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum CounterAddress {
    Usize(*const AtomicUsize),
    U64(*const AtomicU64),
}
impl FundingCounter<'_> {
    pub(crate) fn address(self) -> CounterAddress {
        match self {
            Self::Usize(v) => CounterAddress::Usize(v),
            Self::U64(v) => CounterAddress::U64(v),
        }
    }
}
impl CounterAddress {
    /// Caller pins the exact original provider from which this typed borrow was made.
    pub(crate) unsafe fn borrowed<'a>(self) -> FundingCounter<'a> {
        match self {
            Self::Usize(p) => FundingCounter::Usize(unsafe { &*p }),
            Self::U64(p) => FundingCounter::U64(unsafe { &*p }),
        }
    }
    unsafe fn sample(self) -> FundingSample {
        match self {
            Self::Usize(p) => FundingSample::Usize(unsafe { &*p }.load(Ordering::SeqCst)),
            Self::U64(p) => FundingSample::U64(unsafe { &*p }.load(Ordering::SeqCst)),
        }
    }
}
struct FundingPin {
    owner: ReadMemoryOwner,
    counter: CounterAddress,
    provider_identity: usize,
}
// SAFETY: the real original provider's strong owner retains the typed atomic through all reads.
// Only funding_loan creates this pair from its lifetime-bounded provider borrow. Removal/drop
// occurs outside STATE and only after watched physical cuts and constructor threads finish.
unsafe impl Send for FundingPin {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingSample {
    Unavailable,
    Usize(usize),
    U64(u64),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllocationClass {
    HelperPlan,
    WakePipe,
    CancellationCallback,
    CancellationListenerNode,
    HelperStack,
    AnchorCell,
    TaskStart,
    RegistrationTransaction,
}
#[derive(Clone, Copy, Debug)]
pub struct Record {
    pub kind: u8,
    pub window: u64,
    pub sequence: usize,
    pub request: u64,
    pub operation: u64,
    pub operation_generation: u64,
    pub thread: u64,
    pub constructor: usize,
    pub class: AllocationClass,
    pub allocator_ordinal: u64,
    pub allocator_window: usize,
    pub allocator_generation: u64,
    pub system_called: bool,
    pub address: usize,
    pub bytes: usize,
    pub align: usize,
    pub result: i32,
    pub errno: Option<i32>,
    pub funding: FundingSample,
    pub physical_live_at_cut: u64,
    pub invoice_bytes: usize,
    pub payer_identity: usize,
    pub provider_identity: usize,
}
impl Record {
    const EMPTY: Self = Self {
        kind: 0,
        window: 0,
        sequence: 0,
        request: 0,
        operation: 0,
        operation_generation: 0,
        thread: 0,
        constructor: 0,
        class: AllocationClass::HelperPlan,
        allocator_ordinal: 0,
        allocator_window: 0,
        allocator_generation: 0,
        system_called: false,
        address: 0,
        bytes: 0,
        align: 0,
        result: 0,
        errno: None,
        funding: FundingSample::Unavailable,
        physical_live_at_cut: 0,
        invoice_bytes: 0,
        payer_identity: 0,
        provider_identity: 0,
    };
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Window(u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchId {
    window: u64,
    index: usize,
}
#[derive(Clone, Copy)]
struct Watch {
    state: u8,
    record: Record,
    loan: usize,
}
impl Watch {
    const EMPTY: Self = Self {
        state: 0,
        record: Record::EMPTY,
        loan: 0,
    };
}
// states: 0 unused, 1 allocator/map pending, 2 live, 3 physical free pending, 4 freed, 5 allocation failed.
struct State {
    window: u64,
    active: bool,
    threads: usize,
    next_thread: u64,
    count: usize,
    used: usize,
    loan_used: usize,
    allocator_sequence: u64,
    records: [Record; RECORDS],
    watches: [Watch; WATCHES],
    loans: [Option<FundingPin>; LOANS],
}
static STATE: Mutex<State> = Mutex::new(State {
    window: 0,
    active: false,
    threads: 0,
    next_thread: 0,
    count: 0,
    used: 0,
    loan_used: 0,
    allocator_sequence: 0,
    records: [Record::EMPTY; RECORDS],
    watches: [Watch::EMPTY; WATCHES],
    loans: [const { None }; LOANS],
});
#[derive(Clone, Copy)]
struct Frame {
    watch: Option<WatchId>,
    claimed: bool,
}
impl Frame {
    const EMPTY: Self = Self {
        watch: None,
        claimed: false,
    };
}
#[derive(Clone, Copy)]
struct Thread {
    window: u64,
    request: u64,
    operation: u64,
    operation_generation: u64,
    thread: u64,
    depth: usize,
    frames: [Frame; DEPTH],
}
impl Thread {
    const EMPTY: Self = Self {
        window: 0,
        request: 0,
        operation: 0,
        operation_generation: 0,
        thread: 0,
        depth: 0,
        frames: [Frame::EMPTY; DEPTH],
    };
}
thread_local! {
    static HERE: Cell<Thread> = const { Cell::new(Thread::EMPTY) };
    static INSIDE: Cell<bool> = const { Cell::new(false) };
}
struct Entry;
impl Drop for Entry {
    fn drop(&mut self) {
        let _ = INSIDE.try_with(|v| v.set(false));
    }
}
fn state<R>(f: impl FnOnce(&mut State) -> R) -> Option<R> {
    let entered = INSIDE.try_with(|v| !v.replace(true));
    if entered.is_err() {
        return None;
    } // Unrelated TLS teardown is not a fixture error.
    if entered != Ok(true) {
        error(1);
        return None;
    }
    let _entry = Entry;
    let mut lock = STATE.lock().unwrap_or_else(|p| p.into_inner());
    Some(f(&mut lock))
}
fn emit(s: &mut State, mut record: Record) {
    if s.count >= RECORDS {
        error(4);
        return;
    }
    record.sequence = s.count + 1;
    s.records[s.count] = record;
    s.count += 1;
}
fn sample(s: &State, loan: usize) -> FundingSample {
    s.loans
        .get(loan)
        .and_then(Option::as_ref)
        .map(|p| unsafe { p.counter.sample() })
        .unwrap_or(FundingSample::Unavailable)
}
fn cut(s: &State, loan: usize, record: &mut Record, extra: usize) {
    record.funding = sample(s, loan);
    let Some(pin) = s.loans.get(loan).and_then(Option::as_ref) else {
        error(64);
        return;
    };
    let mut live = extra as u64;
    for w in &s.watches[..s.used] {
        if matches!(w.state, 2 | 3)
            && s.loans[w.loan]
                .as_ref()
                .is_some_and(|other| other.counter == pin.counter)
        {
            let Some(next) = live.checked_add(w.record.bytes as u64) else {
                error(4);
                return;
            };
            live = next;
        }
    }
    record.physical_live_at_cut = live;
    let used = match record.funding {
        FundingSample::Usize(v) => v as u64,
        FundingSample::U64(v) => v,
        FundingSample::Unavailable => {
            error(64);
            return;
        }
    };
    if used < live {
        error(256);
    }
}
fn loan_for(s: &State, invoice: &ReadCredit) -> Option<usize> {
    s.loans.iter().position(|v| {
        v.as_ref()
            .is_some_and(|o| o.owner.same_owner(invoice.original_payer_for_test()))
    })
}
pub fn begin() -> Option<Window> {
    state(|s| {
        if s.active || s.threads != 0 || s.loans.iter().any(Option::is_some) {
            error(128);
            return None;
        }
        let Some(next) = s.window.checked_add(1) else {
            error(8);
            return None;
        };
        s.window = next;
        s.active = true;
        s.count = 0;
        s.used = 0;
        s.next_thread = 0;
        s.allocator_sequence = 0;
        s.loan_used = 0;
        s.records.fill(Record::EMPTY);
        s.watches.fill(Watch::EMPTY);
        ERRORS.store(0, Ordering::SeqCst);
        Some(Window(next))
    })
    .flatten()
}
/// Setup only: pins the ORIGINAL accounting provider, never the allocation under test.
pub struct FundingLoan {
    window: Window,
    index: usize,
    finished: bool,
}
pub fn funding_loan(window: Window, owner: &ReadMemoryOwner) -> Option<FundingLoan> {
    let original = owner.original_native_owner_for_test().unwrap_or(owner);
    let Some(counter) = original
        .coordination_counter_for_test()
        .map(FundingCounter::address)
    else {
        error(64);
        return None;
    };
    let provider_identity = original.accounting_identity();
    let owner = owner.clone(); // retain exact payer alias AND its original provider, before lock
    let mut owner = Some(FundingPin {
        owner,
        counter,
        provider_identity,
    });
    let index = state(|s| {
        if !s.active || s.window != window.0 {
            error(8);
            return None;
        }
        if s.loan_used >= LOANS {
            error(4);
            return None;
        }
        let index = s.loan_used;
        s.loan_used += 1; // never reuse a loan ID within its window
        s.loans[index] = owner.take();
        Some(index)
    })
    .flatten();
    // Any unused owner drops after releasing the registry lock.
    drop(owner);
    index.map(|index| FundingLoan {
        window,
        index,
        finished: false,
    })
}
impl FundingLoan {
    /// Call after all constructing threads detach and all watched payloads physically free.
    pub fn finish(mut self) -> bool {
        let owner = state(|s| {
            if s.window != self.window.0
                || s.threads != 0
                || s.watches[..s.used]
                    .iter()
                    .any(|w| w.loan == self.index && matches!(w.state, 1..=3))
            {
                error(128);
                return None;
            }
            let funding = sample(s, self.index);
            let record = Record {
                kind: 10,
                window: s.window,
                funding,
                ..Record::EMPTY
            };
            emit(s, record);
            s.loans[self.index].take()
        })
        .flatten();
        self.finished = owner.is_some();
        drop(owner); // outside recorder lock; may free accounting provider
        self.finished
    }
}
impl Drop for FundingLoan {
    fn drop(&mut self) {
        if !self.finished {
            error(128);
        } // retain registry loan on incomplete finish, never a dangling counter
    }
}
pub struct ThreadGuard {
    window: Window,
    _thread: PhantomData<Rc<()>>,
}
pub fn attach(window: Window, request: u64, operation: u64) -> Option<ThreadGuard> {
    let old = HERE.try_with(Cell::get).ok()?;
    if old.window != 0 {
        error(8);
        return None;
    }
    let thread = state(|s| {
        if !s.active || s.window != window.0 {
            error(8);
            return None;
        }
        let Some(next) = s.next_thread.checked_add(1) else {
            error(8);
            return None;
        };
        s.next_thread = next;
        s.threads += 1;
        Some(next)
    })
    .flatten()?;
    HERE.with(|h| {
        h.set(Thread {
            window: window.0,
            request,
            operation,
            thread,
            ..Thread::EMPTY
        })
    });
    Some(ThreadGuard {
        window,
        _thread: PhantomData,
    })
}
impl Drop for ThreadGuard {
    fn drop(&mut self) {
        let old = HERE
            .try_with(|h| h.replace(Thread::EMPTY))
            .unwrap_or(Thread::EMPTY);
        let _ = state(|s| {
            if old.window != self.window.0
                || s.window != old.window
                || old.depth != 0
                || s.threads == 0
            {
                error(128);
                return;
            }
            s.threads -= 1;
        });
    }
}
pub struct AllocationScope {
    window: u64,
    depth: usize,
    _thread: PhantomData<Rc<()>>,
}
/// Scope surrounds exactly one real constructor, including its failure path. Supports nested constructors.
pub fn allocation(class: AllocationClass, layout: Layout, invoice: &ReadCredit) -> AllocationScope {
    let here = HERE.try_with(Cell::get).unwrap_or(Thread::EMPTY);
    let mut scope = AllocationScope {
        window: 0,
        depth: 0,
        _thread: PhantomData,
    };
    if here.window == 0 {
        return scope;
    } // inactive constructor; no recovery admission
    if here.depth >= DEPTH {
        error(4);
        return scope;
    }
    let id = state(|s| {
        if !s.active || s.window != here.window {
            error(8);
            return None;
        }
        let Some(loan) = loan_for(s, invoice) else {
            error(64);
            return None;
        };
        if s.used >= WATCHES {
            error(4);
            return None;
        }
        let index = s.used;
        s.used += 1;
        let mut record = Record {
            kind: 1,
            window: s.window,
            request: here.request,
            operation: here.operation,
            operation_generation: here.operation_generation,
            thread: here.thread,
            constructor: index + 1,
            class,
            bytes: layout.size(),
            align: layout.align(),
            invoice_bytes: invoice.bytes(),
            funding: sample(s, loan),
            payer_identity: invoice.original_payer_identity_for_test(),
            provider_identity: s.loans[loan].as_ref().map_or(0, |p| p.provider_identity),
            ..Record::EMPTY
        };
        if invoice.bytes() != layout.size() {
            error(16);
        }
        cut(s, loan, &mut record, 0);
        s.watches[index] = Watch {
            state: 1,
            record,
            loan,
        };
        emit(s, record);
        Some(WatchId {
            window: s.window,
            index,
        })
    })
    .flatten();
    // Even unavailable funding creates an invalid frame, so an unrelated nested allocation cannot steal a parent slot.
    HERE.with(|h| {
        let mut v = h.get();
        v.frames[v.depth] = Frame {
            watch: id,
            claimed: false,
        };
        v.depth += 1;
        h.set(v);
    });
    scope.window = here.window;
    scope.depth = here.depth + 1;
    scope
}
impl Drop for AllocationScope {
    fn drop(&mut self) {
        if self.window == 0 {
            return;
        }
        let _ = HERE.try_with(|h| {
            let mut v = h.get();
            if v.window != self.window || v.depth != self.depth || v.depth == 0 {
                error(8);
                return;
            }
            let frame = v.frames[v.depth - 1];
            if !frame.claimed {
                error(32);
            }
            v.depth -= 1;
            v.frames[v.depth] = Frame::EMPTY;
            h.set(v);
        });
    }
}
/// Called by the real unit allocator before physical allocation, never by a publish callback.
pub fn allocation_enter(layout: Layout) -> Option<WatchId> {
    if INSIDE.try_with(Cell::get).unwrap_or(false) {
        error(1);
        return None;
    }
    let frame = HERE
        .try_with(|h| {
            let mut v = h.get();
            if v.depth == 0 {
                return None;
            }
            let i = v.depth - 1;
            if v.frames[i].claimed {
                error(32);
                return None;
            }
            v.frames[i].claimed = true;
            let frame = v.frames[i];
            h.set(v);
            Some(frame)
        })
        .ok()
        .flatten()?;
    let id = frame.watch?;
    state(|s| {
        let Some(w) = s.watches.get(id.index) else {
            error(8);
            return None;
        };
        if id.window != s.window || w.state != 1 {
            error(8);
            return None;
        }
        if w.record.bytes != layout.size() || w.record.align != layout.align() {
            error(16);
            return None;
        }
        let Some(next) = s.allocator_sequence.checked_add(1) else {
            error(8);
            return None;
        };
        s.allocator_sequence = next;
        let mut w = s.watches[id.index];
        w.record.allocator_ordinal = next;
        w.record.kind = 12;
        cut(s, w.loan, &mut w.record, 0);
        s.watches[id.index] = w;
        emit(s, w.record);
        Some(id)
    })
    .flatten()
}
pub fn allocation_returned(
    id: Option<WatchId>,
    address: usize,
    allocator_window: usize,
    allocator_generation: u64,
    system_called: bool,
) {
    let Some(id) = id else {
        return;
    };
    let _ = state(|s| {
        if id.window != s.window || id.index >= s.used || s.watches[id.index].state != 1 {
            error(8);
            return;
        }
        let mut w = s.watches[id.index];
        w.state = if address == 0 { 5 } else { 2 };
        w.record.kind = 2;
        w.record.address = address;
        w.record.allocator_window = allocator_window;
        w.record.allocator_generation = allocator_generation;
        if address != 0 && allocator_generation != id.window {
            error(8);
        }
        w.record.system_called = system_called;
        w.record.result = if address == 0 { -1 } else { 0 };
        let extra = if address == 0 { 0 } else { w.record.bytes };
        cut(s, w.loan, &mut w.record, extra);
        s.watches[id.index] = w;
        emit(s, w.record);
    });
}
/// Physical-free watch survives constructor thread exit; only scalar IDs/pointer identity are read.
pub fn before_free(
    address: usize,
    layout: Layout,
    allocator_window: usize,
    allocator_generation: u64,
) -> Option<WatchId> {
    if allocator_generation == 0 {
        return None;
    }
    state(|s| {
        let mut found = None;
        for i in 0..s.used {
            if s.watches[i].state == 2
                && s.watches[i].record.class != AllocationClass::HelperStack
                && s.watches[i].record.address == address
            {
                if found.is_some() {
                    error(8);
                    return None;
                }
                found = Some(i);
            }
        }
        let i = found?;
        let mut w = s.watches[i];
        if w.record.bytes != layout.size()
            || w.record.align != layout.align()
            || w.record.allocator_window != allocator_window
            || w.record.allocator_generation != allocator_generation
            || allocator_generation != s.window
        {
            error(16);
            return None;
        }
        w.state = 3;
        w.record.kind = 3;
        cut(s, w.loan, &mut w.record, 0);
        s.watches[i] = w;
        emit(s, w.record);
        Some(WatchId {
            window: s.window,
            index: i,
        })
    })
    .flatten()
}
pub fn after_free(id: Option<WatchId>) {
    let Some(id) = id else {
        return;
    };
    let _ = state(|s| {
        if id.window != s.window || id.index >= s.used || s.watches[id.index].state != 3 {
            error(8);
            return;
        }
        let mut w = s.watches[id.index];
        w.state = 4;
        w.record.kind = 4;
        cut(s, w.loan, &mut w.record, 0);
        s.watches[id.index] = w;
        emit(s, w.record);
    });
}
// Mapping events use the same stable watch generation, with actual syscall result and funding.
pub fn map_before(length: usize, invoice: &ReadCredit) -> Option<WatchId> {
    let here = HERE.try_with(Cell::get).unwrap_or(Thread::EMPTY);
    if here.window == 0 {
        return None;
    }
    state(|s| {
        if !s.active || s.window != here.window {
            error(8);
            return None;
        }
        let loan = loan_for(s, invoice).or_else(|| {
            error(64);
            None
        })?;
        if s.used >= WATCHES {
            error(4);
            return None;
        }
        let i = s.used;
        s.used += 1;
        let mut record = Record {
            kind: 5,
            window: s.window,
            request: here.request,
            operation: here.operation,
            operation_generation: here.operation_generation,
            thread: here.thread,
            constructor: i + 1,
            class: AllocationClass::HelperStack,
            bytes: length,
            invoice_bytes: invoice.bytes(),
            funding: sample(s, loan),
            payer_identity: invoice.original_payer_identity_for_test(),
            provider_identity: s.loans[loan].as_ref().map_or(0, |p| p.provider_identity),
            ..Record::EMPTY
        };
        if invoice.bytes() != length {
            error(16);
        }
        cut(s, loan, &mut record, 0);
        s.watches[i] = Watch {
            state: 1,
            record,
            loan,
        };
        emit(s, record);
        Some(WatchId {
            window: s.window,
            index: i,
        })
    })
    .flatten()
}
pub fn map_returned(id: Option<WatchId>, address: usize, errno: Option<i32>) {
    let Some(id) = id else {
        return;
    };
    let _ = state(|s| {
        if id.window != s.window || id.index >= s.used || s.watches[id.index].state != 1 {
            error(8);
            return;
        }
        let mut w = s.watches[id.index];
        if w.record.class != AllocationClass::HelperStack {
            error(8);
            return;
        }
        if (address == usize::MAX) != errno.is_some() {
            error(8);
        }
        w.state = if address == usize::MAX || errno.is_some() {
            5
        } else {
            2
        };
        w.record.kind = 6;
        w.record.address = address;
        w.record.errno = errno;
        w.record.result = if errno.is_some() { -1 } else { 0 };
        let extra = if w.state == 2 { w.record.bytes } else { 0 };
        cut(s, w.loan, &mut w.record, extra);
        s.watches[id.index] = w;
        emit(s, w.record);
    });
}
pub fn unmap_before(id: Option<WatchId>, address: usize, length: usize, invoice: &ReadCredit) {
    let Some(id) = id else {
        return;
    };
    let _ = state(|s| {
        if id.window != s.window || id.index >= s.used {
            error(8);
            return;
        }
        let mut w = s.watches[id.index];
        if w.record.class != AllocationClass::HelperStack
            || w.state != 2
            || w.record.address != address
            || w.record.bytes != length
            || loan_for(s, invoice) != Some(w.loan)
        {
            error(8);
            return;
        }
        w.state = 3;
        w.record.kind = 7;
        cut(s, w.loan, &mut w.record, 0);
        s.watches[id.index] = w;
        emit(s, w.record);
    });
}
pub fn unmap_returned(id: Option<WatchId>, result: i32, errno: Option<i32>) {
    let Some(id) = id else {
        return;
    };
    let _ = state(|s| {
        if id.window != s.window || id.index >= s.used || s.watches[id.index].state != 3 {
            error(8);
            return;
        }
        let mut w = s.watches[id.index];
        if w.record.class != AllocationClass::HelperStack {
            error(8);
            return;
        }
        if (result == -1) != errno.is_some() || !matches!(result, -1 | 0) {
            error(8);
        }
        if result != 0 {
            error(128);
            w.state = 2;
        } else {
            w.state = 4;
        }
        w.record.kind = 8;
        w.record.result = result;
        w.record.errno = errno;
        cut(s, w.loan, &mut w.record, 0);
        s.watches[id.index] = w;
        emit(s, w.record);
    });
}
pub fn record(window: Window, index: usize) -> Option<Record> {
    state(|s| {
        if s.window != window.0 {
            error(8);
            return None;
        }
        if index < s.count {
            Some(s.records[index])
        } else {
            None
        }
    })
    .flatten()
}
pub fn status(window: Window) -> Option<(usize, usize, u64)> {
    state(|s| {
        if s.window != window.0 {
            return None;
        }
        Some((s.count, s.used, ERRORS.load(Ordering::SeqCst)))
    })
    .flatten()
}
pub fn finish(window: Window) -> bool {
    state(|s| {
        if s.window != window.0
            || s.threads != 0
            || s.loans.iter().any(Option::is_some)
            || s.watches[..s.used].iter().any(|w| matches!(w.state, 1..=3))
        {
            error(128);
            return false;
        }
        s.active = false;
        ERRORS.load(Ordering::SeqCst) == 0
    })
    .unwrap_or(false)
}

/// Stop observing after fixture abort. This is NOT physical-free/credit success.
/// Invalidates all tokens under the lock before dropping accounting pins outside it.
/// Owned actor/FD/helper cleanup remains the fixture/controller's responsibility.
pub fn abort(window: Window) -> bool {
    let pins = state(|s| {
        if s.window != window.0 || s.threads != 0 {
            error(128);
            return None;
        }
        error(128);
        for w in &mut s.watches[..s.used] {
            if matches!(w.state, 1..=3) {
                w.state = 6;
            }
        }
        s.active = false;
        Some(std::mem::replace(&mut s.loans, [const { None }; LOANS]))
    })
    .flatten();
    let stopped = pins.is_some();
    drop(pins); // no live recorder pointer can sample these after invalidation
    stopped
}

#[cfg(test)]
mod fixed_storage_tests {
    use super::*;
    static SERIAL: Mutex<()> = Mutex::new(());
    #[test]
    fn recursive_recorder_entry_is_rejected_before_locking() {
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        let window = begin().unwrap();
        assert_eq!(state(|_| state(|_| ())), Some(None));
        assert_ne!(status(window).unwrap().2 & 1, 0);
        assert!(!finish(window));
    }
    #[test]
    fn fixed_record_overflow_invalidates_without_writing_past_storage() {
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        let window = begin().unwrap();
        state(|s| {
            for _ in 0..=RECORDS {
                emit(s, Record::EMPTY);
            }
        });
        let (count, _, errors) = status(window).unwrap();
        assert_eq!(count, RECORDS);
        assert_ne!(errors & 4, 0);
        assert!(record(window, RECORDS).is_none());
        assert!(!finish(window));
    }
}

/// Copy identity of a fixture-owned ORIGINAL-accounting pin. It is never an anchor/safety owner.
#[derive(Clone, Copy, Debug)]
pub struct PayerKey {
    window: Window,
    index: usize,
}
/// SETUP boundary only, before the request's first coordination constructor. No-op without attach.
/// Provider projection is invoked here once; mmap/unmap/free recorders never call provider code.
pub fn register_current_payer(owner: &ReadMemoryOwner) -> Option<PayerKey> {
    let here = HERE.try_with(Cell::get).unwrap_or(Thread::EMPTY);
    if here.window == 0 {
        return None;
    }
    let old = state(|s| {
        if !s.active || s.window != here.window {
            error(8);
            return None;
        }
        s.loans
            .iter()
            .position(|v| v.as_ref().is_some_and(|p| p.owner.same_owner(owner)))
    })
    .flatten();
    if let Some(index) = old {
        return Some(PayerKey {
            window: Window(here.window),
            index,
        });
    }
    let pin = funding_loan(Window(here.window), owner)?;
    let key = PayerKey {
        window: pin.window,
        index: pin.index,
    };
    // The registry deliberately owns the accounting count. This guard contains only numeric IDs.
    std::mem::forget(pin);
    let _ = state(|s| {
        let record = Record {
            kind: 9,
            window: here.window,
            request: here.request,
            operation: here.operation,
            operation_generation: here.operation_generation,
            thread: here.thread,
            constructor: key.index + 1,
            payer_identity: owner.accounting_identity(),
            provider_identity: s.loans[key.index]
                .as_ref()
                .map_or(0, |p| p.provider_identity),
            funding: sample(s, key.index),
            ..Record::EMPTY
        };
        emit(s, record);
    });
    Some(key)
}
/// One finite receipt enumeration after owned completion/cleanup, never a wait/poll oracle.
pub fn registered_payer(window: Window, index: usize) -> Option<PayerKey> {
    state(|s| {
        if s.window != window.0 {
            error(8);
            return None;
        }
        if s.loans.get(index).is_some_and(Option::is_some) {
            Some(PayerKey { window, index })
        } else {
            None
        }
    })
    .flatten()
}
pub fn finish_registered_payer(key: PayerKey) -> bool {
    FundingLoan {
        window: key.window,
        index: key.index,
        finished: false,
    }
    .finish()
}

/// Stored by the actual unit allocator in its own live header before returning the block.
pub fn allocator_generation(id: Option<WatchId>) -> u64 {
    id.map_or(0, |v| v.window)
}

/// Actual production operation key is supplied by its real constructor, never inferred from a pointer.
pub struct OperationScope {
    window: u64,
    previous: (u64, u64),
    current: (u64, u64),
    _thread: PhantomData<Rc<()>>,
}
pub fn operation(operation: u64, generation: u64) -> Option<OperationScope> {
    HERE.try_with(|h| {
        let mut v = h.get();
        if v.window == 0 {
            return None;
        }
        if v.depth != 0 {
            error(8);
            return None;
        }
        let previous = (v.operation, v.operation_generation);
        v.operation = operation;
        v.operation_generation = generation;
        h.set(v);
        Some(OperationScope {
            window: v.window,
            previous,
            current: (operation, generation),
            _thread: PhantomData,
        })
    })
    .ok()
    .flatten()
}
impl Drop for OperationScope {
    fn drop(&mut self) {
        let _ = HERE.try_with(|h| {
            let mut v = h.get();
            if v.window != self.window
                || v.depth != 0
                || (v.operation, v.operation_generation) != self.current
            {
                error(8);
                return;
            }
            v.operation = self.previous.0;
            v.operation_generation = self.previous.1;
            h.set(v);
        });
    }
}
/// Native-task startup copies only numeric request/window/key values; install on THAT Rust thread.
pub fn attach_operation(
    window: Window,
    request: u64,
    operation: u64,
    generation: u64,
) -> Option<ThreadGuard> {
    let guard = attach(window, request, operation)?;
    let _ = HERE.try_with(|h| {
        let mut v = h.get();
        v.operation_generation = generation;
        h.set(v);
    });
    Some(guard)
}

/// Numeric-only capture for the real paid native-task startup payload; no resource/owner alias.
#[derive(Clone, Copy, Debug)]
pub struct ThreadCapture {
    pub window: Window,
    pub request: u64,
    pub operation: u64,
    pub generation: u64,
}
pub fn capture_thread() -> Option<ThreadCapture> {
    let here = HERE.try_with(Cell::get).ok()?;
    if here.window == 0 {
        return None;
    }
    Some(ThreadCapture {
        window: Window(here.window),
        request: here.request,
        operation: here.operation,
        generation: here.operation_generation,
    })
}
pub fn attach_capture(capture: ThreadCapture) -> Option<ThreadGuard> {
    attach_operation(
        capture.window,
        capture.request,
        capture.operation,
        capture.generation,
    )
}
