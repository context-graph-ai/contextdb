use std::cell::RefCell;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransitionPhase {
    BeforeDurableWrite,
    AfterDurableWrite,
}

type Hook = Arc<dyn Fn(TransitionPhase) + Send + Sync>;

thread_local! {
    /// Armed on the thread that performs the transition, so a test that
    /// suspends one transition can never suspend an unrelated one running
    /// beside it.
    static PAUSE: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from correction_counterexample_tests.rs"
    )
)]
#[allow(
    dead_code,
    reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
)]
pub(crate) fn arm_memory_limit_transition_pause_for_test(hook: Hook) {
    PAUSE.with(|pause| *pause.borrow_mut() = Some(hook));
}

#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from correction_counterexample_tests.rs"
    )
)]
#[allow(
    dead_code,
    reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
)]
pub(crate) fn disarm_memory_limit_transition_pause_for_test() {
    PAUSE.with(|pause| *pause.borrow_mut() = None);
}

pub(super) fn observe(phase: TransitionPhase) {
    let hook = PAUSE.with(|pause| pause.borrow().clone());
    if let Some(hook) = hook {
        hook(phase);
    }
}
