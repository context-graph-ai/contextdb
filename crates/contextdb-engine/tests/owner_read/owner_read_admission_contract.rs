use contextdb_core::read_contract::{
    OwnerReadCancellation, OwnerReadLimits, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::owner_read::{OwnerAdmission, OwnerReadScaffoldError};
#[cfg(feature = "test-seams")]
use std::sync::Arc;
#[cfg(feature = "test-seams")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "test-seams")]
use std::task::{Context, Poll, Wake, Waker};

const SHIPPED_OWNER_CONCURRENCY: u64 = 4;

fn shipped_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4 * 1024 * 1024,
        work: 50_000,
        active_ms: 5_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

#[test]
fn shipped_owner_admission_default_is_four() {
    assert_eq!(
        OwnerReadLimits::default().concurrency,
        SHIPPED_OWNER_CONCURRENCY,
        "the shipped owner policy, rather than a test-injected literal, owns the four-slot boundary",
    );
}

#[test]
fn existing_typed_vocabulary_exhaustively_names_only_six_request_limit_kinds() {
    // Core does not yet expose the frozen `ReadFailureLimit::ALL` slice. This
    // exhaustive match remains coupled to the real enum instead of cloning a
    // service-local failure vocabulary; adding a core variant breaks this RED
    // proof until the authority slice and matrix are updated together.
    fn vocabulary_name(limit: ReadFailureLimit) -> &'static str {
        match limit {
            ReadFailureLimit::ResultRows => "result_rows",
            ReadFailureLimit::ResultBytes => "result_bytes",
            ReadFailureLimit::Work => "work",
            ReadFailureLimit::ActiveMs => "active_ms",
            ReadFailureLimit::Memory => "memory",
            ReadFailureLimit::CursorPageBytes => "cursor_page_bytes",
        }
    }

    let vocabulary = [
        ReadFailureLimit::ResultRows,
        ReadFailureLimit::ResultBytes,
        ReadFailureLimit::Work,
        ReadFailureLimit::ActiveMs,
        ReadFailureLimit::Memory,
        ReadFailureLimit::CursorPageBytes,
    ];
    assert_eq!(
        vocabulary.map(vocabulary_name),
        [
            "result_rows",
            "result_bytes",
            "work",
            "active_ms",
            "memory",
            "cursor_page_bytes",
        ],
    );
}

fn prove_immediate_boundary(capacity: u64) {
    let owner = OwnerReadLimits {
        limits: shipped_limits(),
        concurrency: capacity,
    };
    let admission = OwnerAdmission::new(owner)
        .expect("validated immediate owner admission must be constructible");
    let effective = shipped_limits();

    let mut held = Vec::new();
    for admitted in 0..capacity {
        held.push(
            admission
                .try_acquire(effective, OwnerReadCancellation::new())
                .unwrap_or_else(|error| {
                    panic!("slot {admitted} of {capacity} must admit immediately: {error:?}")
                }),
        );
    }
    assert_eq!(admission.counters().capacity, capacity);
    assert_eq!(admission.counters().active_readers, capacity);

    let refused = admission
        .try_acquire(effective, OwnerReadCancellation::new())
        .expect_err("N+1 must refuse synchronously while all N leases remain live");
    assert!(matches!(
        refused,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerAtCapacity
    ));
    assert_eq!(admission.counters().active_readers, capacity);

    drop(held);
    assert_eq!(admission.counters().active_readers, 0);
}

#[test]
fn default_and_raised_capacity_hold_exactly_n_and_refuse_n_plus_one_without_a_queue() {
    let shipped = OwnerReadLimits::default().concurrency;
    prove_immediate_boundary(shipped);
    prove_immediate_boundary(shipped + 3);
}

#[test]
fn closing_admission_before_acquire_never_reserves_a_new_slot() {
    let admission = OwnerAdmission::new(OwnerReadLimits::default())
        .expect("validated owner admission must be constructible");
    admission
        .close_to_new_work()
        .expect("closing owner admission is infallible");

    let refused = admission
        .try_acquire(shipped_limits(), OwnerReadCancellation::new())
        .expect_err("an admission gate closed for shutdown must not reserve one more slot");
    assert!(matches!(
        refused,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerNotServing
    ));
    assert!(!admission.is_accepting());
    assert_eq!(admission.counters().active_readers, 0);
}

#[cfg(feature = "test-seams")]
#[derive(Default)]
struct WakeFlag(AtomicBool);

#[cfg(feature = "test-seams")]
impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[cfg(feature = "test-seams")]
#[test]
fn a_shutdown_racing_after_reservation_rolls_the_slot_back_and_wakes_the_drain() {
    let admission = OwnerAdmission::new(OwnerReadLimits::default())
        .expect("validated owner admission must be constructible");
    let wake_flag = Arc::new(WakeFlag::default());
    let waker = Waker::from(Arc::clone(&wake_flag));
    let mut context = Context::from_waker(&waker);
    let admission_at_reservation = Arc::clone(&admission);

    let refused = admission
        .try_acquire_with_post_reservation_hook_for_test(
            shipped_limits(),
            OwnerReadCancellation::new(),
            || {
                admission_at_reservation
                    .close_to_new_work()
                    .expect("closing owner admission is infallible");
                assert_eq!(admission_at_reservation.counters().active_readers, 1);
                assert_eq!(
                    admission_at_reservation.poll_drained_for_test(&mut context),
                    Poll::Pending,
                    "shutdown must park while the racing reservation is still visible",
                );
            },
        )
        .expect_err("the post-reservation fence must refuse a shutdown-racing request");

    assert!(matches!(
        refused,
        OwnerReadScaffoldError::Refused(failure)
            if failure.kind() == ReadFailureKind::OwnerNotServing
    ));
    assert_eq!(admission.counters().active_readers, 0);
    assert!(
        wake_flag.0.load(Ordering::SeqCst),
        "rolling the last racing slot back must wake the drain already waiting on it",
    );
    assert_eq!(
        admission.poll_drained_for_test(&mut context),
        Poll::Ready(()),
    );
}
