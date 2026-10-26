use std::cell::Cell;

thread_local! {
    static WINDOW_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(super) fn window_bytes() -> Option<usize> {
    WINDOW_BYTES.with(Cell::get)
}

pub(super) fn with_window_bytes<T>(bytes: usize, action: impl FnOnce() -> T) -> T {
    let previous = WINDOW_BYTES.with(|slot| slot.replace(Some(bytes)));
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WINDOW_BYTES.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(previous);
    action()
}

thread_local! {
    /// (every, remaining): evict the run cache after every `every`
    /// delivered cached postings, for as long as the seam is armed. A
    /// single eviction is no longer the interesting case: sustained cache
    /// pressure is what a bounded continuation has to stay linear under.
    static EVICT_AFTER_CACHED_POSTINGS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from retained_read_source.rs"
    )
)]
pub(crate) fn arm_eviction_after_cached_postings_for_test(postings: usize) {
    EVICT_AFTER_CACHED_POSTINGS.with(|armed| {
        armed.set((postings > 0).then_some((postings, postings)));
    });
}

thread_local! {
    static REFUSE_RUN_MEMORY: Cell<bool> = const { Cell::new(false) };
}

/// Make the authenticated run build fail to reserve its memory, the way a
/// database whose budget cannot hold the proof does.
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from correction_counterexample_tests.rs"
    )
)]
#[allow(dead_code, reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it")]
pub(crate) fn arm_verified_run_memory_exhaustion_for_test(armed: bool) {
    REFUSE_RUN_MEMORY.with(|refuse| refuse.set(armed));
}

pub(super) fn verified_run_memory_exhausted_for_test() -> bool {
    REFUSE_RUN_MEMORY.with(Cell::get)
}

pub(super) fn evict_after_cached_posting_for_test() -> bool {
    EVICT_AFTER_CACHED_POSTINGS.with(|armed| match armed.get() {
        Some((every, 1)) => {
            armed.set(Some((every, every)));
            true
        }
        Some((every, remaining)) => {
            armed.set(Some((every, remaining.saturating_sub(1))));
            false
        }
        None => false,
    })
}

thread_local! {
    static REFUSE_CONFIG_WRITE: Cell<bool> = const { Cell::new(false) };
}

/// Make the durable config write of a memory-limit transition fail, the way
/// a full disk or a damaged store does, so the transition's rollback is
/// observable rather than argued.
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from correction_counterexample_tests.rs"
    )
)]
#[allow(dead_code, reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it")]
pub(crate) fn arm_config_write_failure_for_test(armed: bool) {
    REFUSE_CONFIG_WRITE.with(|refuse| refuse.set(armed));
}

pub(super) fn config_write_refused_for_test() -> bool {
    REFUSE_CONFIG_WRITE.with(Cell::get)
}

thread_local! {
    static CAPACITY_DISPLACEMENTS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn note_capacity_displacement_for_test() {
    CAPACITY_DISPLACEMENTS.with(|count| count.set(count.get().saturating_add(1)));
}

pub(crate) fn take_capacity_displacements_for_test() -> usize {
    CAPACITY_DISPLACEMENTS.with(|count| count.replace(0))
}
