use super::*;
use crate::local_transport::MonotonicDeadlineClock;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, OwnerReadCancellation};
use std::future::poll_fn;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

struct ControlledReadyClock {
    now: AtomicU64,
    advance_on_poll: Option<u64>,
    first_poll: Mutex<Option<SyncSender<()>>>,
    release_first_poll: Option<Arc<Barrier>>,
    polls: AtomicUsize,
}

impl DeadlineClock for ControlledReadyClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(poll_fn(move |_| {
            let poll = self.polls.fetch_add(1, Ordering::SeqCst);
            if let Some(now) = self.advance_on_poll {
                self.now.store(now, Ordering::SeqCst);
            }
            if poll == 0 {
                if let Some(started) = self.first_poll.lock().take() {
                    let _ = started.send(());
                }
                if let Some(release) = self.release_first_poll.as_ref() {
                    release.wait();
                }
            }
            std::task::Poll::Ready(())
        }))
    }
}

fn controlled_clock(
    now: u64,
    advance_on_poll: Option<u64>,
    block_first_poll: bool,
) -> (
    Arc<ControlledReadyClock>,
    Receiver<()>,
    Option<Arc<Barrier>>,
) {
    let (sent, received) = sync_channel(1);
    let release = block_first_poll.then(|| Arc::new(Barrier::new(2)));
    (
        Arc::new(ControlledReadyClock {
            now: AtomicU64::new(now),
            advance_on_poll,
            first_poll: Mutex::new(Some(sent)),
            release_first_poll: release.clone(),
            polls: AtomicUsize::new(0),
        }),
        received,
        release,
    )
}

fn scope(
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
) -> crate::executor::ReadPublicationScope {
    crate::executor::ReadPublicationScope {
        snapshot_lsn: Lsn(1),
        tables: crate::executor::PublicationTables::Unfunded(BTreeSet::from(["items".to_owned()])),
        inspection: false,
        budget: crate::executor::ReadPublicationBudget {
            clock,
            cancellation,
            started_ms: 0,
            active_ms: 20,
        },
        read_memory: None,
        publication_funding: None,
    }
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn owner_publication_wait_has_no_false_timer_refusal_repoll_or_lost_release_wake() {
    let gate = Arc::new(ReadPublicationGate::default());
    let writer = gate.acquire_write(&BTreeSet::from(["items".to_owned()]));
    let (clock, polled, release) = controlled_clock(20, None, true);
    let reader_gate = Arc::clone(&gate);
    let reader_scope = scope(clock.clone(), OwnerReadCancellation::new());
    let reader = std::thread::spawn(move || reader_gate.acquire_read(&reader_scope));
    polled.recv().expect("the contended reader polls its timer");
    drop(writer);
    release.expect("first poll is controlled").wait();
    let admission = reader.join().expect("owner reader thread exits");
    assert!(
        admission.is_ok(),
        "timer readiness at the exact active-time boundary cannot refuse admission"
    );
    assert_eq!(
        clock.polls.load(Ordering::SeqCst),
        1,
        "a completed early timer is not repolled after the release wake"
    );
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn owner_publication_wait_wakes_for_cancellation_and_refuses_only_after_elapsed_deadline() {
    let gate = Arc::new(ReadPublicationGate::default());
    let writer = gate.acquire_write(&BTreeSet::from(["items".to_owned()]));
    let cancellation = OwnerReadCancellation::new();
    let (clock, polled, release) = controlled_clock(0, None, true);
    let reader_gate = Arc::clone(&gate);
    let reader_scope = scope(clock.clone(), cancellation.clone());
    let reader = std::thread::spawn(move || reader_gate.acquire_read(&reader_scope));
    polled
        .recv()
        .expect("the cancellable reader polls its timer");
    cancellation.cancel();
    release.expect("first poll is controlled").wait();
    assert!(matches!(
        reader.join().expect("cancelled owner reader exits"),
        Err(Error::ReadCancelled)
    ));
    assert_eq!(clock.polls.load(Ordering::SeqCst), 1);
    drop(writer);

    let deadline_gate = Arc::new(ReadPublicationGate::default());
    let _deadline_writer = deadline_gate.acquire_write(&BTreeSet::from(["items".to_owned()]));
    let (deadline_clock, _, _) = controlled_clock(0, Some(21), false);
    let error =
        match deadline_gate.acquire_read(&scope(deadline_clock, OwnerReadCancellation::new())) {
            Ok(_) => panic!("elapsed contended publication must refuse"),
            Err(error) => error,
        };
    assert!(matches!(
        error,
        Error::ReadFailure(failure)
            if failure.kind()
                == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
    ));
}

#[test]
fn owner_publication_timer_arm_failure_uses_the_real_remaining_budget_once() {
    let gate = Arc::new(ReadPublicationGate::default());
    let writer = gate.acquire_write(&BTreeSet::from(["items".to_owned()]));
    let clock = Arc::new(MonotonicDeadlineClock::with_timer_arm_failure_for_test());
    let reader_gate = Arc::clone(&gate);
    let reader_scope = scope(clock.clone(), OwnerReadCancellation::new());
    let (sent, received) = sync_channel(1);
    let reader = std::thread::spawn(move || {
        let _ = sent.send(reader_gate.acquire_read(&reader_scope));
    });

    let result = received
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("the native remaining-budget wait bounds a failed timer arm");
    let error = match result {
        Ok(_) => panic!("a blocked writer still owns publication after the budget"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::ReadFailure(failure)
            if failure.kind()
                == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
    ));
    assert_eq!(clock.timer_arm_failure_waits_for_test(), 1);
    assert_eq!(clock.timer_arm_failure_polls_for_test(), 1);
    drop(writer);
    reader.join().expect("owner arm-failure reader exits");
}

struct DeadlineCrossingClock {
    now_reads: AtomicUsize,
    polls: AtomicUsize,
}

impl DeadlineClock for DeadlineCrossingClock {
    fn now_ms(&self) -> u64 {
        let read = self.now_reads.fetch_add(1, Ordering::SeqCst);
        if read < 2 { 20 } else { 21 }
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(poll_fn(move |_| {
            self.polls.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready(())
        }))
    }
}

#[test]
fn owner_publication_deadline_crossing_never_becomes_an_unbounded_wait() {
    let gate = Arc::new(ReadPublicationGate::default());
    let writer = gate.acquire_write(&BTreeSet::from(["items".to_owned()]));
    let clock = Arc::new(DeadlineCrossingClock {
        now_reads: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
    });
    let reader_gate = Arc::clone(&gate);
    let reader_scope = scope(clock.clone(), OwnerReadCancellation::new());
    let (sent, received) = sync_channel(1);
    let reader = std::thread::spawn(move || {
        let _ = sent.send(reader_gate.acquire_read(&reader_scope));
    });

    let result = received
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("crossing the deadline must answer without publication release");
    let error = match result {
        Ok(_) => panic!("the elapsed publication budget must refuse"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Error::ReadFailure(failure)
            if failure.kind()
                == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
    ));
    assert_eq!(clock.polls.load(Ordering::SeqCst), 1);
    drop(writer);
    reader.join().expect("deadline-crossing owner reader exits");
}

/// A clock that tells the test when the contended reader first polls its
/// timer (which it does only when it is actually waiting at the erasure
/// gate), and either leaves the timer pending or moves time past the
/// deadline at that poll.
struct GateWaitClock {
    now: AtomicU64,
    expire_on_poll: bool,
    waiting: SyncSender<()>,
    announced: std::sync::atomic::AtomicBool,
}

impl DeadlineClock for GateWaitClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(poll_fn(move |_| {
            if !self.announced.swap(true, Ordering::SeqCst) {
                let _ = self.waiting.send(());
            }
            if self.expire_on_poll {
                self.now.store(1_001, Ordering::SeqCst);
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }))
    }
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn final_read_publication_wait_refuses_cancellation_and_deadline_before_the_purge_releases_the_gate()
 {
    for expire in [false, true] {
        let database = Arc::new(Database::open_memory());
        let purge = database
            .read_publication_gate
            .acquire_write(&BTreeSet::from(["items".to_owned()]));

        let cancellation = OwnerReadCancellation::new();
        let (waiting, waiting_rx) = sync_channel(1);
        let clock = Arc::new(GateWaitClock {
            now: AtomicU64::new(0),
            expire_on_poll: expire,
            waiting,
            announced: std::sync::atomic::AtomicBool::new(false),
        });
        let mut publication_scope = scope(clock.clone(), cancellation.clone());
        publication_scope.budget.active_ms = 1_000;
        let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_database = Arc::clone(&database);
        let reader_published = Arc::clone(&published);
        let (answer, answer_rx) = sync_channel(1);
        let reader = std::thread::spawn(move || {
            let result = reader_database.publish_authoritative_read(
                &publication_scope,
                |error| error,
                || {
                    reader_published.store(true, Ordering::SeqCst);
                    Ok(String::from("computed before the purge finished"))
                },
            );
            answer.send(result).expect("test receives the read outcome");
        });
        waiting_rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("read reached the contended erasure gate");
        if !expire {
            cancellation.cancel();
        }
        let result = answer_rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("read refuses while the purge still holds the gate");
        if expire {
            assert!(matches!(
                result,
                Err(Error::ReadFailure(failure))
                    if failure.kind()
                        == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
            ));
        } else {
            assert!(matches!(result, Err(Error::ReadCancelled)));
        }
        assert!(!published.load(Ordering::SeqCst));
        assert!(
            database
                .read_publication_gate
                .state
                .lock()
                .readers
                .is_empty()
        );
        drop(purge);
        reader.join().expect("reader exits");
    }
}
