//! Counters for the actual per-record billing path, with no alternate read path.
use std::cell::Cell;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BillingWork {
    pub native_child_identities: u64,
    pub metadata_reservations: u64,
    pub copy_reservations: u64,
}

thread_local! {
    static WORK: Cell<Option<BillingWork>> = const { Cell::new(None) };
}

pub(crate) fn note_native_child_identity() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.native_child_identities = work.native_child_identities.saturating_add(1);
            slot.set(Some(work));
        }
    });
}

pub(crate) fn note_metadata_reservation() {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            work.metadata_reservations = work.metadata_reservations.saturating_add(1);
            slot.set(Some(work));
        }
    });
}

/// Count attempts at the executing thread's native identity, metadata and copy doors.
/// Inline thread-local counters require no heap allocation and restore nested/error/unwind scopes.
#[doc(hidden)]
pub fn observe_billing_work_for_test<T>(operation: impl FnOnce() -> T) -> (T, BillingWork) {
    struct Restore(Option<BillingWork>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WORK.with(|slot| slot.set(self.0));
        }
    }
    let restore = Restore(WORK.with(|slot| slot.replace(Some(BillingWork::default()))));
    let (result, copies) = contextdb_vector::observe_copy_reservations_for_test(operation);
    let mut work = WORK.with(|slot| slot.get().expect("active billing observation"));
    work.copy_reservations = copies;
    drop(restore);
    (result, work)
}
