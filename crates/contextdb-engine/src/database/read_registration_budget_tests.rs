use super::*;
use contextdb_core::read_contract::{DeadlineWait, ReadFailureLimit};

struct ManualClock(AtomicU64);

impl DeadlineClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

#[test]
fn active_time_exact_boundary_still_allows_registration() {
    let clock = Arc::new(ManualClock(AtomicU64::new(10)));
    let budget = ReadRegistrationBudget::new(clock.clone(), 0, 10);
    assert_eq!(budget.remaining().unwrap(), Duration::from_millis(1));
    clock.0.store(11, Ordering::SeqCst);
    assert!(matches!(
        budget.remaining(),
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::OwnerLimitExceeded
                && matches!(failure.detail(), ReadFailureDetail::OwnerLimitExceeded(detail)
                    if detail.limit == ReadFailureLimit::ActiveMs)
    ));
}
