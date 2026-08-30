use contextdb_core::read_contract::{
    DeadlineClock, ReadFailure, ReadFailureDetail, ReadFailureKind,
};
use contextdb_engine::local_transport::{
    DeadlineOperationWait, DeadlineStage, LocalDeadlineOperation, LocalTransportError,
    ManualDeadlineClock, connect_with_deadline, drain_shutdown_with_deadline,
    expire_cursor_idle_with_deadline, expire_cursor_lifetime_with_deadline,
    handshake_with_deadline, probe_stale_with_deadline, serve_request_with_deadline,
    write_request_with_deadline,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

struct CountingWake {
    wakes: AtomicUsize,
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWake>, Waker) {
    let counter = Arc::new(CountingWake {
        wakes: AtomicUsize::new(0),
    });
    (Arc::clone(&counter), Waker::from(counter))
}

#[derive(Clone)]
struct PendingOperation {
    polls: Arc<AtomicUsize>,
}

impl Future for PendingOperation {
    type Output = Result<(), LocalTransportError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

type OperationFactory =
    for<'a> fn(&'a dyn DeadlineClock, u64, LocalDeadlineOperation<'a>) -> DeadlineOperationWait<'a>;

fn timeout_refusal() -> LocalTransportError {
    LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
            .expect("owner timeout accepts an empty detail"),
    )
}

#[test]
fn manual_clock_registers_each_future_once_and_wakes_a_cross_thread_waiter_once() {
    let clock = ManualDeadlineClock::at(40);
    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);
    let mut wait = clock.wait_until(47);

    assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
    assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(clock.registered_waiter_count(), 1);
    assert_eq!(clock.registrations_created(), 1);

    let advancing_clock = clock.clone();
    let advancing_thread = std::thread::spawn(move || advancing_clock.advance_to(47));
    advancing_thread
        .join()
        .expect("advance manual time across threads");

    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(wait.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(clock.registered_waiter_count(), 0);
    assert_eq!(clock.registrations_created(), 1);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);
}

#[test]
fn dropped_completed_and_already_past_manual_waiters_leave_no_registration() {
    let clock = ManualDeadlineClock::at(70);
    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);
    {
        let mut dropped_wait = clock.wait_until(77);
        assert!(matches!(
            dropped_wait.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(clock.registered_waiter_count(), 1);
    }
    assert_eq!(clock.registered_waiter_count(), 0);
    clock.advance_to(77);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 0);

    let mut completed_wait = clock.wait_until(80);
    assert!(matches!(
        completed_wait.as_mut().poll(&mut context),
        Poll::Pending
    ));
    clock.advance_to(80);
    assert_eq!(completed_wait.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(clock.registered_waiter_count(), 0);

    let mut already_past = clock.wait_until(79);
    assert_eq!(already_past.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(clock.registered_waiter_count(), 0);
}

fn assert_operation_deadline(stage: DeadlineStage, factory: OperationFactory) {
    let clock = ManualDeadlineClock::at(100);
    let polls = Arc::new(AtomicUsize::new(0));
    let operation: LocalDeadlineOperation<'_> = Box::pin(PendingOperation {
        polls: Arc::clone(&polls),
    });
    let mut wait = factory(&clock, 105, operation);
    assert_eq!(wait.stage(), stage);

    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut wait).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(clock.registered_waiter_count(), 1);
    assert_eq!(clock.registrations_created(), 1);

    assert!(matches!(
        Pin::new(&mut wait).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(clock.registered_waiter_count(), 1);
    assert_eq!(clock.registrations_created(), 1);
    clock.advance_to(104);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        Pin::new(&mut wait).poll(&mut context),
        Poll::Pending
    ));

    clock.advance_to(105);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(
        Pin::new(&mut wait).poll(&mut context),
        Poll::Ready(Err(timeout_refusal()))
    );
    assert_eq!(clock.registered_waiter_count(), 0);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);

    let completion_clock = ManualDeadlineClock::at(200);
    let completed: LocalDeadlineOperation<'_> = Box::pin(std::future::ready(Ok(())));
    let mut completed_wait = factory(&completion_clock, 205, completed);
    assert_eq!(
        Pin::new(&mut completed_wait).poll(&mut context),
        Poll::Ready(Ok(()))
    );
    assert_eq!(completion_clock.registered_waiter_count(), 0);

    let dropped_clock = ManualDeadlineClock::at(300);
    let dropped_operation: LocalDeadlineOperation<'_> = Box::pin(std::future::pending());
    let mut dropped_wait = factory(&dropped_clock, 305, dropped_operation);
    assert!(matches!(
        Pin::new(&mut dropped_wait).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(dropped_clock.registered_waiter_count(), 1);
    drop(dropped_wait);
    assert_eq!(dropped_clock.registered_waiter_count(), 0);

    let past_clock = ManualDeadlineClock::at(401);
    let past_operation: LocalDeadlineOperation<'_> = Box::pin(std::future::pending());
    let mut past_wait = factory(&past_clock, 400, past_operation);
    assert_eq!(
        Pin::new(&mut past_wait).poll(&mut context),
        Poll::Ready(Err(timeout_refusal()))
    );
    assert_eq!(past_clock.registered_waiter_count(), 0);
    assert_eq!(past_clock.registrations_created(), 0);
}

macro_rules! operation_deadline_test {
    ($name:ident, $stage:ident, $factory:ident) => {
        #[test]
        fn $name() {
            fn operation_factory<'a>(
                clock: &'a dyn DeadlineClock,
                deadline_ms: u64,
                operation: LocalDeadlineOperation<'a>,
            ) -> DeadlineOperationWait<'a> {
                $factory(clock, deadline_ms, operation)
            }
            assert_operation_deadline(DeadlineStage::$stage, operation_factory);
        }
    };
}

operation_deadline_test!(
    connect_operation_obeys_its_deadline,
    Connect,
    connect_with_deadline
);
operation_deadline_test!(
    handshake_operation_obeys_its_deadline,
    Handshake,
    handshake_with_deadline
);
operation_deadline_test!(
    request_write_operation_obeys_its_deadline,
    RequestWrite,
    write_request_with_deadline
);
operation_deadline_test!(
    stale_probe_operation_obeys_its_deadline,
    StaleProbe,
    probe_stale_with_deadline
);
operation_deadline_test!(
    owner_request_operation_obeys_its_deadline,
    Request,
    serve_request_with_deadline
);
operation_deadline_test!(
    cursor_idle_operation_obeys_its_deadline,
    CursorIdle,
    expire_cursor_idle_with_deadline
);
operation_deadline_test!(
    cursor_lifetime_operation_obeys_its_deadline,
    CursorLifetime,
    expire_cursor_lifetime_with_deadline
);
operation_deadline_test!(
    shutdown_drain_operation_obeys_its_deadline,
    ShutdownDrain,
    drain_shutdown_with_deadline
);

#[test]
#[cfg(target_os = "linux")]
fn production_connect_waits_on_real_backlog_io_and_expires_without_releasing_the_listener() {
    use contextdb_engine::local_transport::UnixLocalCarrier;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    struct ThreadWake {
        thread: std::thread::Thread,
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    let root = tempfile::tempdir().expect("temporary connect backlog root");
    let path = root.path().join("owner.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("listen through production carrier");
    nix::sys::socket::listen(
        &listener,
        nix::sys::socket::Backlog::new(0).expect("valid zero backlog"),
    )
    .expect("set the audited listener backlog");
    let queued = std::os::unix::net::UnixStream::connect(&path)
        .expect("fill the real Unix listener backlog");

    let clock = ManualDeadlineClock::at(500);
    let connect_clock = clock.clone();
    let connect_path = path.clone();
    let wakes = Arc::new(AtomicUsize::new(0));
    let operation_wakes = Arc::clone(&wakes);
    let operation = std::thread::spawn(move || {
        let operation_carrier = UnixLocalCarrier;
        let waker = Waker::from(Arc::new(ThreadWake {
            thread: std::thread::current(),
            wakes: operation_wakes,
        }));
        let mut context = Context::from_waker(&waker);
        let connect = operation_carrier.connect(&connect_path, &connect_clock, 505);
        assert_eq!(connect.stage(), DeadlineStage::Connect);
        let mut future = Box::pin(connect);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => break output,
                Poll::Pending => std::thread::park(),
            }
        }
    });

    for _ in 0..100_000 {
        if clock.registered_waiter_count() == 1 || operation.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    if clock.registered_waiter_count() != 1 {
        drop(queued);
        drop(listener);
        operation.thread().unpark();
        let early = operation.join().expect("early connect operation completes");
        panic!("production connect did not register its named deadline: {early:?}");
    }
    assert_eq!(clock.registrations_created(), 1);
    clock.advance_to(504);
    assert!(!operation.is_finished());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    clock.advance_to(505);
    let outcome = operation.join().expect("deadline-terminated connect");
    assert!(
        matches!(outcome, Err(error) if error == timeout_refusal()),
        "the stalled production connect must end with the owner-timeout refusal"
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(clock.registered_waiter_count(), 0);
    drop(queued);
    drop(listener);
}

#[test]
#[cfg(unix)]
fn production_response_read_stalls_on_real_carrier_io_until_the_exact_manual_boundary() {
    use contextdb_core::read_contract::ReadLimits;
    use contextdb_engine::local_transport::{
        LocalProtocolBoundary, LocalResponseExpectation, UnixLocalCarrier,
    };

    struct ThreadWake {
        thread: std::thread::Thread,
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    let root = tempfile::tempdir().expect("temporary stalled-response root");
    let path = root.path().join("owner.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("listen through production carrier");
    let client =
        std::os::unix::net::UnixStream::connect(&path).expect("connect real stalled response peer");
    let (mut server, _) = listener.accept().expect("accept stalled response peer");

    let clock = ManualDeadlineClock::at(600);
    let operation_clock = clock.clone();
    let wakes = Arc::new(AtomicUsize::new(0));
    let operation_wakes = Arc::clone(&wakes);
    let operation = std::thread::spawn(move || {
        let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits {
            result_rows: 8,
            result_bytes: 4_096,
            work: 8,
            active_ms: 8,
            memory: 8_192,
            cursor_page_rows: 4,
            cursor_page_bytes: 1_024,
            cursor_idle_ms: 8,
            cursor_lifetime_ms: 16,
        });
        let waker = Waker::from(Arc::new(ThreadWake {
            thread: std::thread::current(),
            wakes: operation_wakes,
        }));
        let mut context = Context::from_waker(&waker);
        let receive = UnixLocalCarrier.receive_response(
            &mut server,
            &boundary,
            LocalResponseExpectation::Custom,
            &operation_clock,
            605,
        );
        assert_eq!(receive.stage(), DeadlineStage::Response);
        let mut future = Box::pin(receive);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => break output,
                Poll::Pending => std::thread::park(),
            }
        }
    });

    for _ in 0..100_000 {
        if clock.registered_waiter_count() == 1 || operation.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(clock.registered_waiter_count(), 1);
    assert_eq!(clock.registrations_created(), 1);
    clock.advance_to(604);
    assert!(!operation.is_finished());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    clock.advance_to(605);
    assert_eq!(
        operation.join().expect("deadline-terminated response read"),
        Err(timeout_refusal())
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(clock.registered_waiter_count(), 0);
    drop(client);
    drop(listener);
}

#[test]
#[cfg(unix)]
fn production_route_retry_uses_real_failed_connects_and_its_named_manual_deadline() {
    use contextdb_engine::local_transport::UnixLocalCarrier;

    let root = tempfile::tempdir().expect("temporary retry root");
    let path = root.path().join("not-listening.sock");
    let clock = ManualDeadlineClock::at(700);
    let retry_clock = clock.clone();
    let retry_path = path.clone();
    let wakes = Arc::new(AtomicUsize::new(0));
    let operation_wakes = Arc::clone(&wakes);

    struct ThreadWake {
        thread: std::thread::Thread,
        wakes: Arc<AtomicUsize>,
    }
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    let operation = std::thread::spawn(move || {
        let waker = Waker::from(Arc::new(ThreadWake {
            thread: std::thread::current(),
            wakes: operation_wakes,
        }));
        let mut context = Context::from_waker(&waker);
        let retry = UnixLocalCarrier.retry_connect(&retry_path, &retry_clock, 705);
        assert_eq!(retry.stage(), DeadlineStage::RoutingRetry);
        let mut future = Box::pin(retry);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => break output,
                Poll::Pending => std::thread::park(),
            }
        }
    });
    for _ in 0..100_000 {
        if clock.registered_waiter_count() == 1 || operation.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(clock.registered_waiter_count(), 1);
    assert_eq!(clock.registrations_created(), 1);
    clock.advance_to(704);
    assert!(!operation.is_finished());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    clock.advance_to(705);
    let outcome = operation.join().expect("deadline-terminated route retry");
    assert!(matches!(outcome, Err(error) if error == timeout_refusal()));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(clock.registered_waiter_count(), 0);
}

#[test]
#[cfg(unix)]
fn production_response_and_retry_handle_already_past_and_drop_cleanup() {
    use contextdb_core::read_contract::ReadLimits;
    use contextdb_engine::local_transport::{
        LocalProtocolBoundary, LocalResponseExpectation, UnixLocalCarrier,
    };

    let (mut response_peer, _held_peer) =
        std::os::unix::net::UnixStream::pair().expect("real response pair");
    let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);

    let past_clock = ManualDeadlineClock::at(801);
    let mut past_response = UnixLocalCarrier.receive_response(
        &mut response_peer,
        &boundary,
        LocalResponseExpectation::Custom,
        &past_clock,
        800,
    );
    assert_eq!(
        Pin::new(&mut past_response).poll(&mut context),
        Poll::Ready(Err(timeout_refusal()))
    );
    assert_eq!(past_clock.registrations_created(), 0);

    let retry_root = tempfile::tempdir().expect("retry cleanup root");
    let retry_path = retry_root.path().join("absent.sock");
    let past_retry_clock = ManualDeadlineClock::at(901);
    let mut past_retry = UnixLocalCarrier.retry_connect(&retry_path, &past_retry_clock, 900);
    assert!(matches!(
        Pin::new(&mut past_retry).poll(&mut context),
        Poll::Ready(Err(error)) if error == timeout_refusal()
    ));
    assert_eq!(past_retry_clock.registrations_created(), 0);

    let drop_clock = ManualDeadlineClock::at(1_000);
    let mut dropped_retry = UnixLocalCarrier.retry_connect(&retry_path, &drop_clock, 1_005);
    assert!(matches!(
        Pin::new(&mut dropped_retry).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(drop_clock.registered_waiter_count(), 1);
    drop(dropped_retry);
    assert_eq!(drop_clock.registered_waiter_count(), 0);
    assert_eq!(counter.wakes.load(Ordering::SeqCst), 0);
}
