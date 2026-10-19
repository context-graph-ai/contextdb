use std::cell::RefCell;
use std::hash::RandomState;

thread_local! {
    static FIXTURE_RANDOM_STATE: RefCell<Option<RandomState>> = const { RefCell::new(None) };
}

pub(crate) fn fixture_random_state() -> RandomState {
    FIXTURE_RANDOM_STATE.with(|state| state.borrow().clone()).unwrap_or_else(RandomState::new)
}

pub fn with_random_state_for_test<R>(state: RandomState, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<RandomState>);
    impl Drop for Restore {
        fn drop(&mut self) {
            FIXTURE_RANDOM_STATE.with(|state| *state.borrow_mut() = self.0.take());
        }
    }
    let previous = FIXTURE_RANDOM_STATE.with(|current| current.replace(Some(state)));
    let _restore = Restore(previous);
    f()
}
