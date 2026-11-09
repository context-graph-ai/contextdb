use std::cell::RefCell;
use std::collections::VecDeque;

/// One raw native result, not a validated identity. Ordinals start at one
/// within each scope; every call must match both its ordinal and its PID.
pub(super) struct Observation {
    pub(super) ordinal: usize,
    pub(super) pid: libc::c_int,
    pub(super) info: libc::proc_bsdinfo,
    pub(super) written: libc::c_int,
    pub(super) errno: libc::c_int,
}

struct State {
    pending: VecDeque<Observation>,
    calls: usize,
    consumed: usize,
    rejected: bool,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

struct Restore {
    previous: Option<State>,
    errno: libc::c_int,
}

impl Drop for Restore {
    fn drop(&mut self) {
        let finished = STATE.with(|slot| slot.replace(self.previous.take()));
        // SAFETY: Darwin's __error returns this thread's live errno slot.
        unsafe { *libc::__error() = self.errno };
        // Restore first, even if a queue assertion fails. During unwinding,
        // preserve the original panic instead of causing a double panic.
        if !std::thread::panicking() {
            let finished = finished.expect("native observation scope is absent");
            assert!(
                !finished.rejected,
                "native observation sequence was rejected"
            );
            assert!(
                finished.pending.is_empty(),
                "native observation scope has {} unused results ({} calls, {} consumed)",
                finished.pending.len(),
                finished.calls,
                finished.consumed,
            );
        }
    }
}

/// Runs on this thread only. Nested scopes suspend the outer queue and
/// counters; leaving either scope restores its predecessor and native errno.
/// A caught sequence panic still rejects the scope on normal exit.
pub(super) fn with_observations<R>(observations: Vec<Observation>, run: impl FnOnce() -> R) -> R {
    // SAFETY: Darwin's __error returns this thread's live errno slot.
    let errno = unsafe { *libc::__error() };
    let previous = STATE.with(|slot| {
        slot.replace(Some(State {
            pending: observations.into(),
            calls: 0,
            consumed: 0,
            rejected: false,
        }))
    });
    let _restore = Restore { previous, errno };
    run()
}

/// (raw calls attempted, queued results consumed) in the active scope.
pub(super) fn counts() -> (usize, usize) {
    STATE.with(|slot| {
        let state = slot.borrow();
        let state = state.as_ref().expect("native observation scope is absent");
        (state.calls, state.consumed)
    })
}

/// None selects the unchanged libc call. An active empty/misordered queue
/// panics rather than falling through to a real process observation.
pub(super) fn observe(pid: libc::c_int, info: &mut libc::proc_bsdinfo) -> Option<libc::c_int> {
    STATE.with(|slot| {
        let mut slot = slot.borrow_mut();
        let state = slot.as_mut()?;
        state.calls += 1;
        state.rejected |= state
            .pending
            .front()
            .is_none_or(|observation| observation.ordinal != state.calls || observation.pid != pid);
        assert!(
            !state.rejected,
            "native observation mismatch or exhausted queue at call {} for PID {}",
            state.calls, pid,
        );
        let observation = state.pending.pop_front().expect("checked pending result");
        *info = observation.info;
        state.consumed += 1;
        if observation.written <= 0 {
            // SAFETY: Darwin's __error returns this thread's live errno slot.
            unsafe { *libc::__error() = observation.errno };
        }
        Some(observation.written)
    })
}
