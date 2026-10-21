use super::{LocalTransportError, operation_already_completed, owner_disconnected, owner_timeout};
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
#[cfg(any(test, feature = "test-seams"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// How many timer threads this thread has started since it last reset the
    /// count.
    ///
    /// A wait that has to be woken at a particular instant and has nothing else to
    /// wake it starts an operating-system thread to do the waking. That is the
    /// right shape for a wait that genuinely waits and the wrong price for an
    /// operation that never does, so the count is what a proof reads. It is kept
    /// per thread, because the thread that starts one is the thread that asked for
    /// the wait, and per-thread counting stays honest beside other tests sharing
    /// the binary.
    static FAILED_TIMER_ARMS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TIMER_THREADS_STARTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
fn note_timer_thread_started_for_test() {
    TIMER_THREADS_STARTED.with(|started| started.set(started.get() + 1));
}

/// Start counting from zero on this thread.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn reset_timer_threads_started_for_test() {
    TIMER_THREADS_STARTED.with(|started| started.set(0));
}

/// Refuse the next timer arms on this thread, including a fallback clock.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn fail_next_timer_arms_for_test(count: usize) {
    FAILED_TIMER_ARMS.with(|remaining| remaining.set(count));
}

/// Timer threads this thread has started since the reset.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn timer_threads_started_for_test() -> usize {
    TIMER_THREADS_STARTED.with(std::cell::Cell::get)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineStage {
    Connect,
    Handshake,
    RequestWrite,
    Response,
    RoutingRetry,
    StaleProbe,
    Request,
    CursorIdle,
    CursorLifetime,
    ShutdownDrain,
}

#[derive(Default)]
struct ManualDeadlineState {
    now_ms: u64,
    next_registration: u64,
    registrations_created: u64,
    waiters: BTreeMap<u64, RegisteredWaiter>,
    native_parked: NativeParkList,
}

struct RegisteredWaiter {
    deadline_ms: u64,
    waker: Waker,
}

/// Deterministic clock for channel deadline tests. Production callers supply
/// their own `DeadlineClock`; this type is only the hidden test seam.
#[derive(Clone, Default)]
pub struct ManualDeadlineClock {
    state: Arc<Mutex<ManualDeadlineState>>,
}

impl ManualDeadlineClock {
    pub fn at(now_ms: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(ManualDeadlineState {
                now_ms,
                next_registration: 0,
                registrations_created: 0,
                waiters: BTreeMap::new(),
                native_parked: NativeParkList::default(),
            })),
        }
    }

    /// Move the clock forward and wake every waiter the new time has reached.
    /// A caller asking for an earlier time is refused: monotonic time is left
    /// where it was, no waiter is woken, and the clock keeps answering every
    /// later caller.
    pub fn advance_to(&self, now_ms: u64) {
        let mut state = Self::locked(&self.state);
        if now_ms < state.now_ms {
            return;
        }
        state.now_ms = now_ms;
        let ready_ids: Vec<_> = state
            .waiters
            .iter()
            .filter_map(|(registration, waiter)| {
                (waiter.deadline_ms <= now_ms).then_some(*registration)
            })
            .collect();
        let wakers_to_notify: Vec<_> = ready_ids
            .into_iter()
            .filter_map(|registration| state.waiters.remove(&registration))
            .map(|waiter| waiter.waker)
            .collect();
        drop(state);
        // Native callbacks and their final drops also run outside the state lock.
        self.wake_native_due();
        for waker in wakers_to_notify {
            waker.wake();
        }
    }

    fn wake_native_due(&self) {
        loop {
            let wake = {
                let mut state = Self::locked(&self.state);
                // SAFETY: registration and unlink are serialized by this exact
                // mutex. The pinned node's guard cannot leave until it unlinks
                // under the same lock. No stack pointer escapes this scope.
                unsafe {
                    let mut node = state.native_parked.head;
                    let mut due = None;
                    while !node.is_null() {
                        if (*node).deadline_ms <= state.now_ms {
                            // This opaque clone only retains the paid atomic
                            // count; it cannot invoke arbitrary payload code.
                            let wake = (*node).wake.clone();
                            state.native_parked.unlink(node);
                            due = Some(wake);
                            break;
                        }
                        node = (*(*node).links.get()).next;
                    }
                    due
                }
            };
            let Some(wake) = wake else {
                return;
            };
            // Only authentic paid retention, never a stack pointer, survives
            // unlock. A callback/payload/refund panic cannot strand a node.
            wake.wake_read();
            drop(wake);
        }
    }

    #[doc(hidden)]
    pub fn registered_waiter_count(&self) -> usize {
        Self::locked(&self.state).waiters.len()
    }

    #[doc(hidden)]
    pub fn registrations_created(&self) -> u64 {
        Self::locked(&self.state).registrations_created
    }

    /// A caller that failed part-way through an update leaves the state
    /// readable rather than unusable, so every later caller still gets an
    /// answer from the clock.
    fn locked(
        state: &Mutex<ManualDeadlineState>,
    ) -> std::sync::MutexGuard<'_, ManualDeadlineState> {
        state.lock().unwrap_or_else(|held| held.into_inner())
    }
}

impl DeadlineClock for ManualDeadlineClock {
    fn try_owned_wait(
        &self, deadline_ms: u64, owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<Option<contextdb_core::read_memory::ReadOwnedFuture<'_, ()>>> {
        contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner.clone(), || {
            let wake = owned_shared(owner.clone(), || Ok(ManualFutureWake {
                waker: Mutex::new(None), reached: std::sync::atomic::AtomicBool::new(false),
            }))?;
            Ok(OwnedManualWait {
                state: self.state.clone(),
                node: NativeParkNode { deadline_ms, wake: wake.clone().into_native_wake_lease(),
                    links: std::cell::UnsafeCell::new(NativeParkLinks { next: std::ptr::null(), linked: false }),
                    _pin: std::marker::PhantomPinned },
                wake,
            })
        }).map(Some)
    }

    #[doc(hidden)]
    fn try_native_source(
        &self,
        _standing: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<
        Option<
            contextdb_core::read_memory::OwnedReadBox<
                dyn contextdb_core::read_contract::NativeDeadlineSource,
            >,
        >,
    > {
        #[cfg(target_os = "linux")]
        {
            let source = native_source_with(_standing.clone(), || {
                Ok(ManualNativeSource {
                    state: Arc::clone(&self.state),
                })
            })?;
            Ok(Some(source))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(None)
        }
    }

    fn now_ms(&self) -> u64 {
        Self::locked(&self.state).now_ms
    }

    fn wait_until(&self, deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(ManualDeadlineWait {
            state: Arc::clone(&self.state),
            deadline_ms,
            registration: None,
            completed: false,
        })
    }
}

// One fixed list-head slot in the existing engine-created manual clock.
// This changes its private layout. No native registry allocation is created.
struct NativeParkList {
    head: *const NativeParkNode,
}
impl Default for NativeParkList {
    fn default() -> Self {
        Self {
            head: std::ptr::null(),
        }
    }
}
// SAFETY: the head and all node links are accessed only under the owning
// ManualDeadlineState mutex; the pinned caller guard removes every node before
// its stack storage ends. Moving the list head cannot move its pinned nodes.
unsafe impl Send for NativeParkList {}
struct NativeParkLinks {
    next: *const NativeParkNode,
    linked: bool,
}
struct NativeParkNode {
    deadline_ms: u64,
    wake: contextdb_core::read_memory::NativeReadWakeLease,
    links: std::cell::UnsafeCell<NativeParkLinks>,
    _pin: std::marker::PhantomPinned,
}
impl NativeParkList {
    // Caller holds the exact state mutex. No user code or drop occurs here.
    unsafe fn unlink(&mut self, node: *const NativeParkNode) {
        // SAFETY: all nodes in this list have a live pinned registration guard;
        // links are UnsafeCell, never an illicit write through a shared reference.
        unsafe {
            if !(*(*node).links.get()).linked {
                return;
            }
            let mut link = &mut self.head as *mut *const NativeParkNode;
            while !(*link).is_null() {
                let current = *link;
                if current == node {
                    *link = (*(*current).links.get()).next;
                    (*(*node).links.get()).linked = false;
                    (*(*node).links.get()).next = std::ptr::null();
                    return;
                }
                link = &mut (*(*current).links.get()).next;
            }
            unreachable!("linked native clock registration missing from its list");
        }
    }
}
struct NativeParkRegistration<'a> {
    state: &'a Mutex<ManualDeadlineState>,
    node: Pin<&'a NativeParkNode>,
}
impl Drop for NativeParkRegistration<'_> {
    fn drop(&mut self) {
        let mut state = ManualDeadlineClock::locked(self.state);
        // SAFETY: the guard borrows pinned stack storage and the exact mutex;
        // unlink is idempotent if advance_to already removed this registration.
        unsafe { state.native_parked.unlink(self.node.get_ref()) };
        // No lease drop or wake while the lock is held. Node storage outlives
        // this guard and its wake lease is dropped only after this unlock.
    }
}
#[cfg(target_os = "linux")]
struct ManualNativeSource {
    state: Arc<Mutex<ManualDeadlineState>>,
}
#[cfg(target_os = "linux")]
impl contextdb_core::read_contract::NativeDeadlineSource for ManualNativeSource {
    fn park_until(
        &self,
        _now_ms: u64,
        deadline_ms: u64,
        park: &dyn contextdb_core::read_contract::NativeDriverPark,
    ) -> contextdb_core::Result<()> {
        // Arbitrary park/factory code is called only outside the manual mutex.
        let node = std::pin::pin!(NativeParkNode {
            deadline_ms,
            wake: park.wake_lease(),
            links: std::cell::UnsafeCell::new(NativeParkLinks {
                next: std::ptr::null(),
                linked: false,
            }),
            _pin: std::marker::PhantomPinned,
        });
        let registration = NativeParkRegistration {
            state: &self.state,
            node: node.as_ref(),
        };
        {
            let mut state = ManualDeadlineClock::locked(&self.state);
            // Recheck the SAME clock under its mutex to close advance-before-link.
            if state.now_ms >= deadline_ms {
                return Ok(());
            }
            let pointer = node.as_ref().get_ref() as *const NativeParkNode;
            // SAFETY: pin precedes publication, the guard already exists, and
            // this mutex serializes every link access until guard destruction.
            unsafe {
                (*(*pointer).links.get()).next = state.native_parked.head;
                (*(*pointer).links.get()).linked = true;
            }
            state.native_parked.head = pointer;
        }
        // Wake-before-enqueue is retained in the paid word. Err, unwind and
        // cancellation all leave through the same unlinking stack guard.
        let result = park.park();
        drop(registration);
        result
    }
}

struct ManualDeadlineWait {
    state: Arc<Mutex<ManualDeadlineState>>,
    deadline_ms: u64,
    registration: Option<u64>,
    completed: bool,
}

impl Future for ManualDeadlineWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(());
        }
        let mut state = ManualDeadlineClock::locked(&this.state);
        if state.now_ms >= this.deadline_ms {
            if let Some(registration) = this.registration.take() {
                state.waiters.remove(&registration);
            }
            this.completed = true;
            return Poll::Ready(());
        }
        let registration = this.registration.unwrap_or_else(|| {
            let registration = state.next_registration;
            state.next_registration = state.next_registration.wrapping_add(1);
            state.registrations_created = state.registrations_created.saturating_add(1);
            this.registration = Some(registration);
            registration
        });
        state.waiters.insert(
            registration,
            RegisteredWaiter {
                deadline_ms: this.deadline_ms,
                waker: context.waker().clone(),
            },
        );
        Poll::Pending
    }
}

impl Drop for ManualDeadlineWait {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take()
            && let Ok(mut state) = self.state.lock()
        {
            state.waiters.remove(&registration);
        }
    }
}

/// The clock a live owner runs on. Deadlines are measured from one origin
/// taken at construction, so they are unaffected by the wall clock moving.
#[derive(Debug)]
pub struct MonotonicDeadlineClock {
    origin: std::time::Instant,
    owned_waits: Mutex<MonotonicWaitList>,
    native_drivers: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "test-seams")]
    thread_lifecycle: Option<DeadlineThreadLifecycle>,
    #[cfg(any(test, feature = "test-seams"))]
    fail_timer_arm: bool,
    #[cfg(any(test, feature = "test-seams"))]
    timer_arm_failure_waits: AtomicUsize,
    #[cfg(any(test, feature = "test-seams"))]
    timer_arm_failure_polls: AtomicUsize,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone)]
struct DeadlineThreadLifecycle {
    started: std::sync::mpsc::Sender<()>,
    exited: std::sync::mpsc::Sender<()>,
}

#[cfg(feature = "test-seams")]
struct DeadlineThreadExit(Option<std::sync::mpsc::Sender<()>>);

#[cfg(feature = "test-seams")]
impl Drop for DeadlineThreadExit {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            let _ = exited.send(());
        }
    }
}

impl MonotonicDeadlineClock {
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
            owned_waits: Mutex::new(MonotonicWaitList::default()),
            native_drivers: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            thread_lifecycle: None,
            #[cfg(any(test, feature = "test-seams"))]
            fail_timer_arm: false,
            #[cfg(any(test, feature = "test-seams"))]
            timer_arm_failure_waits: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            timer_arm_failure_polls: AtomicUsize::new(0),
        }
    }

    /// Observe the real timer worker entering and leaving its thread body.
    /// These senders own no worker and cannot advance or abandon a deadline.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn with_thread_lifecycle_for_test(
        started: std::sync::mpsc::Sender<()>,
        exited: std::sync::mpsc::Sender<()>,
    ) -> Self {
        Self {
            origin: std::time::Instant::now(),
            owned_waits: Mutex::new(MonotonicWaitList::default()),
            native_drivers: std::sync::atomic::AtomicUsize::new(0),
            thread_lifecycle: Some(DeadlineThreadLifecycle { started, exited }),
            fail_timer_arm: false,
            timer_arm_failure_waits: AtomicUsize::new(0),
            timer_arm_failure_polls: AtomicUsize::new(0),
        }
    }

    /// Deterministically exercise the production timer-thread spawn-failure
    /// result. The returned wait matches production's completed one-shot: it
    /// supplies no wake of its own, so the caller must provide a bounded native
    /// wake fallback and must latch completion rather than poll it twice.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_timer_arm_failure_for_test() -> Self {
        Self {
            origin: std::time::Instant::now(),
            owned_waits: Mutex::new(MonotonicWaitList::default()),
            native_drivers: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            thread_lifecycle: None,
            fail_timer_arm: true,
            timer_arm_failure_waits: AtomicUsize::new(0),
            timer_arm_failure_polls: AtomicUsize::new(0),
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn timer_arm_failure_waits_for_test(&self) -> usize {
        self.timer_arm_failure_waits.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn timer_arm_failure_polls_for_test(&self) -> usize {
        self.timer_arm_failure_polls.load(Ordering::SeqCst)
    }
}

impl Default for MonotonicDeadlineClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "linux")]
struct MonotonicNativeSource;
#[cfg(target_os = "linux")]
impl contextdb_core::read_contract::NativeDeadlineSource for MonotonicNativeSource {
    fn park_until(
        &self,
        now_ms: u64,
        deadline_ms: u64,
        park: &dyn contextdb_core::read_contract::NativeDriverPark,
    ) -> contextdb_core::Result<()> {
        // Linux Instant and relative FUTEX_WAIT use CLOCK_MONOTONIC. This
        // source reads no time and creates no origin; the supplied clock's
        // floor-to-ms now decides every deadline on the following repoll.
        park.park_for_ms(deadline_ms.saturating_sub(now_ms))
    }
}

impl DeadlineClock for MonotonicDeadlineClock {
    fn try_owned_wait(
        &self, deadline_ms: u64, owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<Option<contextdb_core::read_memory::ReadOwnedFuture<'_, ()>>> {
        if self.native_drivers.load(std::sync::atomic::Ordering::Acquire) == 0 {
            return self.legacy_owned_wait(deadline_ms, owner);
        }
        contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner.clone(), || {
            Ok(OwnedMonotonicWait {
                clock: self,
                node: MonotonicWaitNode {
                    deadline_ms,
                    links: std::cell::UnsafeCell::new(MonotonicWaitLinks {
                        next: std::ptr::null(), linked: false, waker: None,
                    }),
                    _pin: std::marker::PhantomPinned,
                },
            })
        }).map(Some)
    }

    fn native_driver_started(&self) { self.native_drivers.fetch_add(1, std::sync::atomic::Ordering::AcqRel); }
    fn native_driver_ended(&self) { self.native_drivers.fetch_sub(1, std::sync::atomic::Ordering::AcqRel); }

    fn native_next_deadline_ms(&self) -> Option<u64> {
        let list = self.owned_waits.lock().unwrap_or_else(|p| p.into_inner());
        let mut next = None;
        let mut node = list.head;
        // Every node remains pinned until unlink under this same mutex.
        unsafe {
            while !node.is_null() {
                let deadline = (*node).deadline_ms;
                next = Some(next.map_or(deadline, |old: u64| old.min(deadline)));
                node = (*(*node).links.get()).next;
            }
        }
        next
    }

    fn drive_native_deadlines(&self) {
        loop {
            let waker = {
                let mut list = self.owned_waits.lock().unwrap_or_else(|p| p.into_inner());
                let now = self.now_ms();
                let mut node = list.head;
                let mut due = None;
                unsafe {
                    while !node.is_null() {
                        if (*node).deadline_ms <= now {
                            list.unlink(node);
                            due = (*(*node).links.get()).waker.take();
                            break;
                        }
                        node = (*(*node).links.get()).next;
                    }
                }
                due
            };
            let Some(waker) = waker else { return; };
            // A callback may re-enter the clock or retire its own wait.
            waker.wake();
        }
    }

    #[doc(hidden)]
    fn try_native_source(
        &self,
        _standing: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<
        Option<
            contextdb_core::read_memory::OwnedReadBox<
                dyn contextdb_core::read_contract::NativeDeadlineSource,
            >,
        >,
    > {
        #[cfg(target_os = "linux")]
        {
            native_source_with(_standing.clone(), || Ok(MonotonicNativeSource)).map(Some)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(None)
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// One waiter, one timer thread, released the moment the deadline is
    /// reached or the wait is abandoned. A wait whose deadline has already
    /// passed never starts one, and a wait that is dropped first ends its
    /// thread before the drop returns, so a deadline that was never needed
    /// leaves nothing running behind the operation it bounded.
    fn wait_until(&self, deadline_ms: u64) -> DeadlineWait<'_> {
        let remaining = deadline_ms.saturating_sub(self.now_ms());
        if remaining == 0 {
            return Box::pin(std::future::ready(()));
        }
        #[cfg(any(test, feature = "test-seams"))]
        if self.fail_timer_arm
            || FAILED_TIMER_ARMS.with(|remaining| {
                let count = remaining.get();
                remaining.set(count.saturating_sub(1));
                count != 0
            })
        {
            self.timer_arm_failure_waits.fetch_add(1, Ordering::SeqCst);
            return Box::pin(InjectedTimerArmFailureWait {
                completed: false,
                polls: &self.timer_arm_failure_polls,
            });
        }
        let state = Arc::new((Mutex::new(TimerState::default()), std::sync::Condvar::new()));
        let timer = Arc::clone(&state);
        #[cfg(feature = "test-seams")]
        let lifecycle = self.thread_lifecycle.clone();
        let spawned = std::thread::Builder::new()
            .name("contextdb-owner-deadline".to_owned())
            .spawn(move || {
                #[cfg(feature = "test-seams")]
                let _exited = lifecycle.as_ref().map(|lifecycle| {
                    let _ = lifecycle.started.send(());
                    DeadlineThreadExit(Some(lifecycle.exited.clone()))
                });
                let (lock, changed) = &*timer;
                let until = std::time::Instant::now() + std::time::Duration::from_millis(remaining);
                let mut state = lock.lock().unwrap_or_else(|held| held.into_inner());
                loop {
                    if state.abandoned {
                        return;
                    }
                    let now = std::time::Instant::now();
                    if now >= until {
                        break;
                    }
                    state = changed
                        .wait_timeout(state, until - now)
                        .map(|(state, _)| state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner().0);
                }
                state.reached = true;
                let waker = state.waker.take();
                drop(state);
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        match spawned {
            Ok(thread) => {
                #[cfg(any(test, feature = "test-seams"))]
                note_timer_thread_started_for_test();
                Box::pin(TimerWait {
                    state,
                    thread: Some(thread),
                })
            }
            Err(_) => Box::pin(std::future::ready(())),
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
struct InjectedTimerArmFailureWait<'a> {
    completed: bool,
    polls: &'a AtomicUsize,
}

#[cfg(any(test, feature = "test-seams"))]
impl Future for InjectedTimerArmFailureWait<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.completed,
            "one-shot timer-arm failure was polled twice"
        );
        self.completed = true;
        self.polls.fetch_add(1, Ordering::SeqCst);
        let _ = context;
        Poll::Ready(())
    }
}

#[derive(Default)]
struct TimerState {
    reached: bool,
    abandoned: bool,
    waker: Option<Waker>,
}

struct TimerWait {
    state: Arc<(Mutex<TimerState>, std::sync::Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Future for TimerWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.0.lock().unwrap_or_else(|held| held.into_inner());
        if state.reached {
            return Poll::Ready(());
        }
        state.waker = Some(context.waker().clone());
        Poll::Pending
    }
}

impl Drop for TimerWait {
    fn drop(&mut self) {
        {
            let mut state = self.state.0.lock().unwrap_or_else(|held| held.into_inner());
            state.abandoned = true;
            state.waker = None;
        }
        self.state.1.notify_all();
        if let Some(thread) = self.thread.take()
            && thread.thread().id() != std::thread::current().id()
        {
            let _ = thread.join();
        }
    }
}

pub type LocalDeadlineOperation<'a, T = ()> =
    Pin<Box<dyn Future<Output = Result<T, LocalTransportError>> + Send + 'a>>;

/// A deadline is attached to a concrete transport or owner operation. Tests
/// can inspect the stage, but callers cannot start a bare stage-only timer.
pub struct DeadlineOperationWait<'a, T = ()> {
    stage: DeadlineStage,
    inner: Option<DeadlineInner<'a, T>>,
    admission_error: Option<LocalTransportError>,
    clock: &'a dyn DeadlineClock,
    deadline_ms: u64,
    deadline: Option<OperationDeadlineWait<'a>>,
}

impl<T> DeadlineOperationWait<'_, T> {
    pub const fn stage(&self) -> DeadlineStage {
        self.stage
    }
}

impl<T> Future for DeadlineOperationWait<'_, T> {
    type Output = Result<T, LocalTransportError>;

    /// An operation that has already produced its outcome keeps no work to
    /// drive. Driving it again reports that the operation is over as a typed
    /// result instead of taking the caller down.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(error) = this.admission_error.take() { return Poll::Ready(Err(error)); }
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(operation_already_completed()));
        };
        // An operation whose outcome is already decided answers with it. A
        // deadline bounds how long this wait may go on; it never overwrites an
        // answer the operation had reached before any waiting began.
        let operation = Pin::new(inner).poll(context);
        if let Poll::Ready(output) = operation {
            this.inner.take();
            this.deadline.take();
            return Poll::Ready(output);
        }
        if this.clock.now_ms() >= this.deadline_ms {
            this.inner.take();
            this.deadline.take();
            return Poll::Ready(Err(owner_timeout()));
        }
        let Some(pending_deadline) = this.deadline.as_mut() else {
            return Poll::Ready(Err(owner_disconnected()));
        };
        match Pin::new(pending_deadline).poll(context) {
            Poll::Ready(()) => {
                this.inner.take();
                this.deadline.take();
                Poll::Ready(Err(owner_timeout()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn deadline_operation<'a, T: Send + 'a>(
    clock: &'a dyn DeadlineClock,
    stage: DeadlineStage,
    deadline_ms: u64,
    operation: LocalDeadlineOperation<'a, T>,
) -> DeadlineOperationWait<'a, T> {
    DeadlineOperationWait {
        stage,
        inner: Some(DeadlineInner::Host(operation)), admission_error: None,
        clock,
        deadline_ms,
        deadline: Some(OperationDeadlineWait::Host(clock.wait_until(deadline_ms))),
    }
}

macro_rules! deadline_operation {
    ($name:ident, $stage:ident) => {
        #[allow(dead_code, reason = "Every declared deadline stage retains the same operation wrapper across feature configurations")]
        pub fn $name<'a, T: Send + 'a>(
            clock: &'a dyn DeadlineClock,
            deadline_ms: u64,
            operation: LocalDeadlineOperation<'a, T>,
        ) -> DeadlineOperationWait<'a, T> {
            deadline_operation(clock, DeadlineStage::$stage, deadline_ms, operation)
        }
    };
}

deadline_operation!(connect_with_deadline, Connect);
deadline_operation!(handshake_with_deadline, Handshake);
deadline_operation!(write_request_with_deadline, RequestWrite);
deadline_operation!(receive_response_with_deadline, Response);
deadline_operation!(retry_route_with_deadline, RoutingRetry);
deadline_operation!(probe_stale_with_deadline, StaleProbe);
deadline_operation!(serve_request_with_deadline, Request);
deadline_operation!(expire_cursor_idle_with_deadline, CursorIdle);
deadline_operation!(expire_cursor_lifetime_with_deadline, CursorLifetime);
deadline_operation!(drain_shutdown_with_deadline, ShutdownDrain);

// Concrete source erasure preserves one allocation and its original invoice.
#[cfg(target_os = "linux")]
fn native_source_with<S: contextdb_core::read_contract::NativeDeadlineSource + 'static>(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    make: impl FnOnce() -> contextdb_core::Result<S>,
) -> contextdb_core::Result<
    contextdb_core::read_memory::OwnedReadBox<
        dyn contextdb_core::read_contract::NativeDeadlineSource,
    >,
> {
    use contextdb_core::read_memory::{OwnedReadBox, ReadCredit};
    let layout = std::alloc::Layout::new::<S>();
    let credit = ReadCredit::try_new_owned(
        owner,
        layout.size(),
        "reader",
        "native deadline source",
        "admit the concrete clock source before its producer",
    )?;
    let block = if layout.size() == 0 {
        std::ptr::NonNull::<S>::dangling()
    } else {
        std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) }.cast::<S>())
            .ok_or(contextdb_core::Error::NativeEngineOther(
                contextdb_core::read_memory::NativeRefusal::AllocatorFailed {
                    site: "native deadline source",
                    layout,
                },
            ))?
    };
    struct Guard<S> {
        block: std::ptr::NonNull<S>,
        layout: std::alloc::Layout,
        credit: contextdb_core::read_memory::ReadCredit,
    }
    impl<S> Drop for Guard<S> {
        fn drop(&mut self) {
            if self.layout.size() != 0 {
                unsafe { std::alloc::dealloc(self.block.as_ptr().cast(), self.layout) };
            }
        }
    }
    let guard = Guard {
        block,
        layout,
        credit,
    };
    let source = make()?;
    unsafe {
        block.as_ptr().write(source);
    }
    let guard = std::mem::ManuallyDrop::new(guard);
    let payload: Box<dyn contextdb_core::read_contract::NativeDeadlineSource> =
        unsafe { Box::from_raw(block.as_ptr()) };
    let credit = unsafe { std::ptr::read(&guard.credit) };
    Ok(OwnedReadBox::new(payload, credit))
}

/// Native funded selection is explicit; a host's already-created future is
/// moved without an engine charge or a substitute owner.
pub(crate) enum OperationDeadlineWait<'a> {
    Engine(contextdb_core::read_memory::ReadOwnedFuture<'a, ()>),
    Host(DeadlineWait<'a>),
}
impl Future for OperationDeadlineWait<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.get_mut() {
            Self::Engine(wait) => Pin::new(wait).poll(cx),
            Self::Host(wait) => wait.as_mut().poll(cx),
        }
    }
}
pub(crate) fn wait_until_owned<'a>(
    clock: &'a dyn DeadlineClock, deadline_ms: u64,
    owner: &contextdb_core::read_memory::ReadMemoryOwner,
) -> contextdb_core::Result<OperationDeadlineWait<'a>> {
    match clock.try_owned_wait(deadline_ms, owner)? {
        Some(wait) if wait.is_funded_by(owner) => Ok(OperationDeadlineWait::Engine(wait)),
        Some(_) => Err(contextdb_core::Error::Other("deadline wait has different memory ownership".into())),
        None => Ok(OperationDeadlineWait::Host(clock.wait_until(deadline_ms))),
    }
}
fn owned_shared<T>(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    make: impl FnOnce() -> contextdb_core::Result<T>,
) -> contextdb_core::Result<contextdb_core::read_memory::ReadShared<T>> {
    use contextdb_core::read_memory::{ReadCredit, ReadShared, NativeProduce};
    let invoice = ReadCredit::try_new_owned(owner, ReadShared::<T>::layout().size(),
        "reader", "deadline_control", "Raise the reader memory limit.")?;
    ReadShared::try_from_prepaid_with_native(invoice, make).map_err(|error| match error {
        NativeProduce::Refused(error) => contextdb_core::Error::NativeEngineOther(error),
        NativeProduce::Producer(error) => error,
    })
}
struct ManualFutureWake {
    waker: Mutex<Option<Waker>>,
    reached: std::sync::atomic::AtomicBool,
}
impl contextdb_core::read_memory::NativeReadWake for ManualFutureWake {
    fn wake_read(&self) {
        self.reached.store(true, std::sync::atomic::Ordering::Release);
        let waker = self.waker.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(waker) = waker { waker.wake(); }
    }
}
struct OwnedManualWait {
    state: Arc<Mutex<ManualDeadlineState>>,
    node: NativeParkNode,
    wake: contextdb_core::read_memory::ReadShared<ManualFutureWake>,
}
// The node is pinned in the admitted future before linking. All link reads
// and writes share the ManualDeadlineState lock; drop unlinks before free.
unsafe impl Send for OwnedManualWait {}
impl Future for OwnedManualWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = unsafe { self.get_unchecked_mut() };
        *this.wake.waker.lock().unwrap_or_else(|p| p.into_inner()) = Some(cx.waker().clone());
        let mut state = ManualDeadlineClock::locked(&this.state);
        if state.now_ms >= this.node.deadline_ms || this.wake.reached.load(std::sync::atomic::Ordering::Acquire) {
            unsafe { state.native_parked.unlink(&this.node); }
            return Poll::Ready(());
        }
        unsafe {
            if !(*this.node.links.get()).linked {
                (*this.node.links.get()).next = state.native_parked.head;
                (*this.node.links.get()).linked = true;
                state.native_parked.head = &this.node;
            }
        }
        Poll::Pending
    }
}
impl Drop for OwnedManualWait {
    fn drop(&mut self) {
        let mut state = ManualDeadlineClock::locked(&self.state);
        unsafe { state.native_parked.unlink(&self.node); }
    }
}
// All links and wakers are protected by the clock mutex. The owning future
// is pinned in an admitted frame before linking and unlinks before free.
#[derive(Debug, Default)]
struct MonotonicWaitList { head: *const MonotonicWaitNode }
unsafe impl Send for MonotonicWaitList {}
struct MonotonicWaitLinks {
    next: *const MonotonicWaitNode,
    linked: bool,
    waker: Option<Waker>,
}
struct MonotonicWaitNode {
    deadline_ms: u64,
    links: std::cell::UnsafeCell<MonotonicWaitLinks>,
    _pin: std::marker::PhantomPinned,
}
impl MonotonicWaitList {
    unsafe fn unlink(&mut self, target: *const MonotonicWaitNode) {
        let mut link = &mut self.head as *mut *const MonotonicWaitNode;
        unsafe {
            while !(*link).is_null() {
                let node = *link;
                if node == target {
                    *link = (*(*node).links.get()).next;
                    (*(*node).links.get()).linked = false;
                    (*(*node).links.get()).next = std::ptr::null();
                    return;
                }
                link = &mut (*(*node).links.get()).next;
            }
        }
    }
}
struct OwnedMonotonicWait<'a> {
    clock: &'a MonotonicDeadlineClock,
    node: MonotonicWaitNode,
}
unsafe impl Send for OwnedMonotonicWait<'_> {}
impl Future for OwnedMonotonicWait<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = unsafe { self.get_unchecked_mut() };
        let next_waker = cx.waker().clone();
        let mut list = this.clock.owned_waits.lock().unwrap_or_else(|p| p.into_inner());
        if this.clock.now_ms() >= this.node.deadline_ms {
            unsafe { list.unlink(&this.node); }
            return Poll::Ready(());
        }
        unsafe {
            let links = &mut *this.node.links.get();
            let previous = links.waker.replace(next_waker);
            if !links.linked {
                links.next = list.head;
                links.linked = true;
                list.head = &this.node;
            }
            drop(list);
            drop(previous);
        }
        Poll::Pending
    }
}
impl Drop for OwnedMonotonicWait<'_> {
    fn drop(&mut self) {
        let waker = {
            let mut list = self.clock.owned_waits.lock().unwrap_or_else(|p| p.into_inner());
            unsafe { list.unlink(&self.node); (*self.node.links.get()).waker.take() }
        };
        drop(waker);
    }
}

pub(crate) fn deadline_operation_owned<'a, T: Send + 'a>(
    clock: &'a dyn DeadlineClock, stage: DeadlineStage, deadline_ms: u64,
    operation: LocalDeadlineOperation<'a, T>,
    owner: &contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<DeadlineOperationWait<'a, T>, LocalTransportError> {
    let deadline = wait_until_owned(clock, deadline_ms, owner)
        .map_err(|error| super::query_funding::transport_error(error, u64::MAX))?;
    Ok(DeadlineOperationWait { stage, inner: Some(DeadlineInner::Host(operation)), admission_error: None, clock, deadline_ms, deadline: Some(deadline) })
}

enum DeadlineInner<'a, T> {
    Host(LocalDeadlineOperation<'a, T>),
    Engine(contextdb_core::read_memory::ReadOwnedFuture<'a, Result<T, LocalTransportError>>),
}
impl<T> Future for DeadlineInner<'_, T> {
    type Output = Result<T, LocalTransportError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Host(future) => future.as_mut().poll(cx),
            Self::Engine(future) => Pin::new(future).poll(cx),
        }
    }
}
pub(crate) fn deadline_operation_funded<'a, T, F>(
    clock: &'a dyn DeadlineClock, stage: DeadlineStage, deadline_ms: u64,
    owner: contextdb_core::read_memory::ReadMemoryOwner, make: impl FnOnce() -> F,
) -> DeadlineOperationWait<'a, T>
where T: Send + 'a, F: Future<Output = Result<T, LocalTransportError>> + Send + 'a {
    let prepared = contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner.clone(), || Ok(make()))
        .and_then(|future| wait_until_owned(clock, deadline_ms, &owner).map(|deadline| (future, deadline)));
    match prepared {
        Ok((future, deadline)) => DeadlineOperationWait { stage, inner: Some(DeadlineInner::Engine(future)),
            admission_error: None, clock, deadline_ms, deadline: Some(deadline) },
        Err(error) => DeadlineOperationWait { stage, inner: None,
            admission_error: Some(super::query_funding::transport_error(error, u64::MAX)),
            clock, deadline_ms, deadline: None },
    }
}

/// Drive an admitted operation on the caller's existing thread. Engine clock
/// waits contribute their earliest deadline; host waits retain their own wake.
#[cfg(target_os = "linux")]
pub(crate) fn drive_local<F, T>(
    future: F, clock: &Arc<dyn DeadlineClock>,
    owner: contextdb_core::read_memory::ReadMemoryOwner,
) -> crate::owner_read::OwnerReadScaffoldResult<T>
where F: Future<Output = crate::owner_read::OwnerReadScaffoldResult<T>> + Send {
    use contextdb_core::read_contract::{NativeClockBinding, NativeDriverPark};
    use crate::owner_read::OwnerReadScaffoldError as E;
    let binding = match NativeClockBinding::try_select(clock, &owner) {
        Ok(binding) => Some(binding),
        Err(contextdb_core::Error::ReadSessionNotImplemented) => None,
        Err(error) => return Err(E::Database(error)),
    };
    let park = crate::transport::in_process::native_serving::NativeInProcessPark::try_new(owner.clone())
        .map_err(E::Database)?;
    let waker = park.waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner, || Ok(future))
        .map_err(E::Database)?;
    loop {
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(result) => return result,
            Poll::Pending => {
                match &binding {
                    Some(binding) => binding.park_until(u64::MAX, &park),
                    None => park.park(),
                }.map_err(E::Database)?;
            }
        }
    }
}

impl MonotonicDeadlineClock {
    fn legacy_owned_wait(
        &self, deadline_ms: u64, owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<Option<contextdb_core::read_memory::ReadOwnedFuture<'_, ()>>> {
        // The complete concrete frame is admitted before state, name, source,
        // or worker creation. Genuine custom clocks use the trait default.
        contextdb_core::read_memory::ReadOwnedFuture::try_new_with(owner.clone(), || {
            let remaining = deadline_ms.saturating_sub(self.now_ms());
            if remaining == 0 { return Ok(LegacyMonotonicWait::Ready); }
            #[cfg(any(test, feature = "test-seams"))]
            if self.fail_timer_arm || FAILED_TIMER_ARMS.with(|remaining| {
                let count = remaining.get(); remaining.set(count.saturating_sub(1)); count != 0
            }) {
                self.timer_arm_failure_waits.fetch_add(1, Ordering::SeqCst);
                return Ok(LegacyMonotonicWait::Injected(InjectedTimerArmFailureWait {
                    completed: false, polls: &self.timer_arm_failure_polls,
                }));
            }
            let state = owned_shared(owner.clone(), || Ok((Mutex::new(TimerState::default()), std::sync::Condvar::new())))?;
            let invoice = contextdb_core::read_memory::ReadCredit::try_new_owned(
                owner.clone(), std::mem::size_of::<OwnedTimerSource>(),
                "reader", "deadline_thread_source", "Raise the reader memory limit.")?;
            let source = contextdb_core::read_memory::OwnedReadBox::try_from_prepaid_with(invoice, || Ok(OwnedTimerSource {
                state: state.clone(), remaining,
                #[cfg(feature = "test-seams")] lifecycle: self.thread_lifecycle.clone(),
            }))?;
            // This internal worker needs no owned debug-name allocation.
            // Public host-created waits keep their existing named path.
            match std::thread::Builder::new().spawn(move || source.run()) {
                Ok(thread) => {
                    #[cfg(any(test, feature = "test-seams"))] note_timer_thread_started_for_test();
                    Ok(LegacyMonotonicWait::Timer(OwnedTimerWait { state, thread: Some(thread) }))
                }
                Err(_) => Ok(LegacyMonotonicWait::Ready),
            }
        }).map(Some)
    }

}
struct OwnedTimerSource {
    state: contextdb_core::read_memory::ReadShared<(Mutex<TimerState>, std::sync::Condvar)>,
    remaining: u64,
    #[cfg(feature = "test-seams")] lifecycle: Option<DeadlineThreadLifecycle>,
}
impl OwnedTimerSource {
    fn run(&self) {
        #[cfg(feature = "test-seams")]
        let _exited = self.lifecycle.as_ref().map(|lifecycle| {
            let _ = lifecycle.started.send(()); DeadlineThreadExit(Some(lifecycle.exited.clone()))
        });
        let (lock, changed) = &*self.state;
        let until = std::time::Instant::now() + std::time::Duration::from_millis(self.remaining);
        let mut state = lock.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if state.abandoned { return; }
            let now = std::time::Instant::now();
            if now >= until { break; }
            state = changed.wait_timeout(state, until - now).map(|(state, _)| state)
                .unwrap_or_else(|p| p.into_inner().0);
        }
        state.reached = true;
        let waker = state.waker.take(); drop(state);
        if let Some(waker) = waker { waker.wake(); }
    }
}
struct OwnedTimerWait {
    state: contextdb_core::read_memory::ReadShared<(Mutex<TimerState>, std::sync::Condvar)>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Future for OwnedTimerWait {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.reached { Poll::Ready(()) }
        else { state.waker = Some(cx.waker().clone()); Poll::Pending }
    }
}
impl Drop for OwnedTimerWait {
    fn drop(&mut self) {
        { let mut state = self.state.0.lock().unwrap_or_else(|p| p.into_inner());
          state.abandoned = true; state.waker = None; }
        self.state.1.notify_all();
        if let Some(thread) = self.thread.take() {
            if thread.thread().id() != std::thread::current().id() { let _ = thread.join(); }
        }
    }
}
enum LegacyMonotonicWait<'a> {
    Ready,
    Timer(OwnedTimerWait),
    #[cfg(any(test, feature = "test-seams"))] Injected(InjectedTimerArmFailureWait<'a>),
    #[cfg(not(any(test, feature = "test-seams")))] Lifetime(std::marker::PhantomData<&'a ()>),
}
impl Future for LegacyMonotonicWait<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.get_mut() {
            Self::Ready => Poll::Ready(()), Self::Timer(wait) => Pin::new(wait).poll(cx),
            #[cfg(any(test, feature = "test-seams"))] Self::Injected(wait) => Pin::new(wait).poll(cx),
            #[cfg(not(any(test, feature = "test-seams")))] Self::Lifetime(_) => Poll::Ready(()),
        }
    }
}
