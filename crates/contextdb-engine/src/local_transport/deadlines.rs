use super::{LocalTransportError, operation_already_completed, owner_disconnected, owner_timeout};
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

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
}

impl MonotonicDeadlineClock {
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
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
    /// reached. A wait whose deadline has already passed never starts one.
    fn wait_until(&self, deadline_ms: u64) -> DeadlineWait<'_> {
        let remaining = deadline_ms.saturating_sub(self.now_ms());
        if remaining == 0 {
            return Box::pin(std::future::ready(()));
        }
        let state = Arc::new(Mutex::new(TimerState::default()));
        let timer = Arc::clone(&state);
        let spawned = std::thread::Builder::new()
            .name("contextdb-owner-deadline".to_owned())
            .spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(remaining));
                let waker = {
                    let mut timer = timer.lock().unwrap_or_else(|held| held.into_inner());
                    timer.reached = true;
                    timer.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        if spawned.is_err() {
            return Box::pin(std::future::ready(()));
        }
        Box::pin(TimerWait { state })
    }
}

#[derive(Default)]
struct TimerState {
    reached: bool,
    waker: Option<Waker>,
}

struct TimerWait {
    state: Arc<Mutex<TimerState>>,
}

impl Future for TimerWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap_or_else(|held| held.into_inner());
        if state.reached {
            return Poll::Ready(());
        }
        state.waker = Some(context.waker().clone());
        Poll::Pending
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
