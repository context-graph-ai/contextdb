//! Passive observations of automatic search planning.

use std::cell::Cell;

thread_local! {
    static COSTED: Cell<Option<u64>> = const { Cell::new(None) };
}

pub fn observe<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COSTED.with(|count| count.set(self.0));
        }
    }
    let restore = Restore(COSTED.with(|count| count.replace(Some(0))));
    let value = operation();
    let count = COSTED.with(|count| count.get().expect("planning observation installed"));
    drop(restore);
    (value, count)
}

// aggregate_auto_threshold_precedes_partition_cost_and_keeps_scope counts
// actual nonempty partition cost calculations, after aggregate admission.
pub(crate) fn note_partition_cost() {
    COSTED.with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}
