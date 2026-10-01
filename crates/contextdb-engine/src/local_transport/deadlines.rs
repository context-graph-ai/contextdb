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
        for waker in wakers_to_notify {
            waker.wake();
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

impl DeadlineClock for MonotonicDeadlineClock {
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
    inner: Option<LocalDeadlineOperation<'a, T>>,
    clock: &'a dyn DeadlineClock,
    deadline_ms: u64,
    deadline: Option<DeadlineWait<'a>>,
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
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(operation_already_completed()));
        };
        // An operation whose outcome is already decided answers with it. A
        // deadline bounds how long this wait may go on; it never overwrites an
        // answer the operation had reached before any waiting began.
        let operation = inner.as_mut().poll(context);
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
        match pending_deadline.as_mut().poll(context) {
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
        inner: Some(operation),
        clock,
        deadline_ms,
        deadline: Some(clock.wait_until(deadline_ms)),
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
