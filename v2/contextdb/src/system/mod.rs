//! system: the time of day, which is all the engine takes from the machine besides files and
//! memory.
//!
//! It never decides anything. The machine's clock here is the only place in the library that
//! reads a clock. The engine does not sleep, start a thread or draw a random number, and the
//! lint configuration refuses each of those calls, and a clock read, anywhere else. Nothing a
//! user or a test can see may differ between two runs because of this part, except a time the
//! host asked for.

use std::sync::Arc;
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

#[cfg(test)]
mod tests;
