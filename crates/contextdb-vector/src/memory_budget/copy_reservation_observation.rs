//! Thread-scoped counts at the real vector-copy reservation doors.
use std::cell::Cell;

thread_local! {
    static CALLS: Cell<Option<u64>> = const { Cell::new(None) };
}

pub(super) fn note() {
    CALLS.with(|slot| {
        if let Some(calls) = slot.get() {
            slot.set(Some(calls.saturating_add(1)));
        }
    });
}

/// Count reservation attempts, including zero-byte and refused calls, on this thread.
/// Nested observations and unwinding restore the caller's observation.
#[doc(hidden)]
pub fn observe_copy_reservations_for_test<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CALLS.with(|slot| slot.set(self.0));
        }
    }
    let restore = Restore(CALLS.with(|slot| slot.replace(Some(0))));
    let result = operation();
    let calls = CALLS.with(|slot| slot.get().expect("active copy reservation observation"));
    drop(restore);
    (result, calls)
}
