//! Machine operations: the host's clock, bounded lock waits, and the maintenance worker.
//! Scheduling reads time only through `Clock`; a timer wake alone never makes a poll due.

use crate::types::Result;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Where the engine gets the time of day.
///
/// The default is the machine's clock. A host or a test supplies its own through
/// [`crate::DatabaseOpenOptions::clock`] or [`crate::ReadSessionOptions::clock`], and then
/// `NOW()` and `DEFAULT NOW()` answer with its time.
pub trait Clock: Send + Sync + 'static {
    /// The time of day. The engine stores it as whole milliseconds since the first moment of
    /// 1970, UTC.
    fn now(&self) -> SystemTime;
}

struct MachineClock;

impl Clock for MachineClock {
    #[allow(
        clippy::disallowed_methods,
        reason = "the machine's clock is the one place the time of day is read"
    )]
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// The machine's clock.
pub(crate) fn machine_clock() -> Arc<dyn Clock> {
    Arc::new(MachineClock)
}

/// The clock's time of day as a stored timestamp: milliseconds since the first moment of 1970,
/// UTC. `write` stamps `DEFAULT NOW()` with it and `query` answers `NOW()` with it.
pub(crate) fn now_millis(clock: &dyn Clock) -> i64 {
    match clock.now().duration_since(UNIX_EPOCH) {
        Ok(after) => after.as_millis() as i64,
        Err(before) => -(before.duration().as_millis() as i64),
    }
}

/// Pause between lock attempts; the caller owns the deadline and cancellation checks.
#[allow(clippy::disallowed_methods, reason = "caller bounds wait")]
pub(crate) fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(1));
}

/// Completes a possibly partial operation; cancellation wins over an elapsed deadline.
pub(crate) fn wait_for(
    length: usize,
    wait: Option<(SystemTime, &dyn Clock)>,
    cancel: Option<&std::sync::atomic::AtomicBool>,
    mut attempt: impl FnMut(usize) -> std::io::Result<usize>,
) -> std::io::Result<std::result::Result<(), bool>> {
    use std::io::ErrorKind;
    let mut at = 0;
    while at < length {
        let was = at;
        match attempt(at) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(n) if at + n == length => return Ok(Ok(())),
            Ok(n) => at += n,
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
            Err(e) => return Err(e),
        }
        if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Acquire)) {
            return Ok(Err(true));
        }
        if wait.is_some_and(|(deadline, clock)| clock.now() >= deadline) {
            return Ok(Err(false));
        }
        if at == was {
            pause();
        }
    }
    Ok(Ok(()))
}

/// Independent worker wake state; it never owns the writing handles.
#[derive(Default)]
pub(crate) struct Signal {
    state: Mutex<(bool, bool, u64)>,
    woken: Condvar,
}

impl Signal {
    pub(crate) fn wake(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1 = true;
        self.woken.notify_all();
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .2
    }

    pub(crate) fn progress(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .2 += 1;
        self.woken.notify_all();
    }

    pub(crate) fn wait_progress(&self, generation: u64) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !self
            .woken
            .wait_while(state, |state| !state.0 && state.2 == generation)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }

    pub(crate) fn stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0 = true;
        self.woken.notify_all();
    }
}

/// Starts an engine-owned thread; callers join it before releasing their owner.
#[allow(clippy::disallowed_methods, reason = "engine workers start only here")]
pub(crate) fn launch(
    run: impl FnOnce() + Send + 'static,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().spawn(run)
}

/// A timer wakes maintenance; the handle's clock decides when to poll.
pub(crate) fn worker(
    signal: Arc<Signal>,
    clock: Arc<dyn Clock>,
    poll: impl Fn() -> u64 + Send + 'static,
    mut cycle: impl FnMut(bool) -> Result<bool> + Send + 'static,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let mut last = now_millis(&*clock);
    launch(move || {
        let (mut again, mut partial) = (false, false);
        loop {
            // The host clock may block or call back into the engine.
            let elapsed = now_millis(&*clock).saturating_sub(last).max(0) as u64;
            let poll = poll();
            let mut state = signal
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.0 {
                return;
            }
            if elapsed >= poll {
                partial = true;
            } else if !again && !state.1 {
                let delay = std::time::Duration::from_millis(poll - elapsed);
                let state = signal
                    .woken
                    .wait_timeout(state, delay)
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
                if state.0 {
                    return;
                }
                continue;
            }
            state.1 = false;
            drop(state);
            // The cycle keeps its own report; the sealing clock is the last sealing cycle's.
            again = cycle(partial).unwrap_or(false);
            if partial {
                last = now_millis(&*clock);
            }
            partial &= again;
        }
    })
}

/// The memory the process may use: the smallest of the machine's, from /proc or `sysconf`, and
/// every control group limit from its own group up to the root.
pub(crate) fn machine_memory() -> Option<u64> {
    let read = |path: &str| std::fs::read_to_string(path).unwrap_or_default();
    let meminfo = read("/proc/meminfo");
    let total = meminfo.lines().find_map(|line| {
        line.strip_prefix("MemTotal:")?
            .trim_end_matches("kB")
            .trim()
            .parse::<u64>()
            .ok()?
            .checked_mul(1024)
    });
    #[cfg(unix)]
    // SAFETY: `sysconf` reads a system constant and touches no memory of ours.
    let sys = |name| u64::try_from(unsafe { libc::sysconf(name) }).ok();
    #[cfg(unix)]
    let total = total.or_else(|| sys(libc::_SC_PHYS_PAGES)?.checked_mul(sys(libc::_SC_PAGESIZE)?));
    let groups = read("/proc/self/cgroup");
    let group = groups.lines().find_map(|line| line.strip_prefix("0::"));
    let caps = std::path::Path::new(group.unwrap_or(""))
        .ancestors()
        .filter_map(|at| {
            let max = read(&format!("/sys/fs/cgroup{}/memory.max", at.display()));
            max.trim().parse::<u64>().ok()
        });
    total.into_iter().chain(caps).min()
}

/// The writer's undeclared limit.
pub(crate) fn default_memory_limit() -> u64 {
    let floor = crate::memory::account(Some(0), crate::store::cache_under(Some(0), None), Some(0));
    crate::memory::default_limit(machine_memory(), floor.iter().sum())
}
