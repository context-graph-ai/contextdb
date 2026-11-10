//! memory: the budget and the reservations taken from it.
//!
//! It never allocates on another part's behalf: it counts what the other parts say they are
//! about to take, and says no before they take it.

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, PoisonError};

use crate::types::{Error, Result};

#[cfg(test)]
mod tests;

/// Bytes every budget sets aside for the small fixed things nobody reserves one by one: a
/// parsed statement and its plan, the table descriptions, a row on its way through. What keeps
/// the figure honest is the test that counts every byte the engine really takes
/// (`tests/memory_ceiling.rs`).
const ALLOWANCE: u64 = 1 << 20;

/// How "no ceiling" is kept: a limit that no count of bytes can pass.
const NO_CEILING: u64 = u64::MAX;

/// The declared memory limit of one open store, and how much of it is spoken for.
///
/// Every figure is in bytes. One budget belongs to one writer or one reading session, and
/// cloning gives another handle to the same budget. The default is no limit: a writer has the
/// limit its store remembers, if one was ever declared, and a reader has one only if its open
/// names one. With no limit there is no ceiling, and what is reserved is still counted.
///
/// Three kinds of memory are counted against it: small fixed things, covered by an allowance
/// of 1 MiB set aside in [`Budget::new`]; the page cache of 8 MiB, reserved once when the store
/// is opened; and everything that grows with data, which must hold a [`Reservation`]. A limit
/// below those 9 MiB therefore cannot open any store: the open is refused with
/// `MemoryBudgetExceeded`.
#[derive(Clone)]
pub(crate) struct Budget {
    counts: Arc<Counts>,
}

/// What the handles of one budget share.
///
/// Every read and write of the two numbers is sequentially consistent, because the limit and
/// the count are judged together: a reservation counts its bytes and then looks at the limit
/// again, a change of the limit publishes it and then looks at the count again, and so one of
/// the two always sees the other.
struct Counts {
    /// The limit in bytes, `NO_CEILING` when none is declared.
    limit: AtomicU64,
    /// Bytes spoken for, to the byte.
    used: AtomicU64,
    /// Held while the limit changes, so that one change is settled before the next begins.
    /// Asking for memory and giving it back never take it.
    changing_limit: Mutex<()>,
}

impl Budget {
    /// A budget with this limit in bytes, or with no ceiling for `None`.
    ///
    /// Refuses with `MemoryBudgetExceeded` a limit too small to hold the fixed allowance.
    pub(crate) fn new(limit: Option<u64>) -> Result<Budget> {
        let budget = Budget {
            counts: Arc::new(Counts {
                limit: AtomicU64::new(limit.unwrap_or(NO_CEILING)),
                used: AtomicU64::new(0),
                changing_limit: Mutex::new(()),
            }),
        };
        budget.take(ALLOWANCE, "open")?;
        Ok(budget)
    }

    /// Asks for `bytes` before they are allocated.
    ///
    /// Refuses with `MemoryBudgetExceeded`, naming `operation`, when the bytes would pass the
    /// limit; nothing is counted on refusal. Never waits and never allocates: it costs a few
    /// atomic operations, so it is cheap enough to call per row but is meant to be called per
    /// batch.
    pub(crate) fn reserve(&self, bytes: u64, operation: &'static str) -> Result<Reservation> {
        self.take(bytes, operation)?;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
            operation,
        })
    }

    /// The declared limit in bytes, `None` when there is no ceiling.
    pub(crate) fn limit(&self) -> Option<u64> {
        let limit = self.counts.limit.load(SeqCst);
        (limit != NO_CEILING).then_some(limit)
    }

    /// Bytes spoken for now: the cache, the allowance and every live reservation.
    pub(crate) fn used(&self) -> u64 {
        self.counts.used.load(SeqCst)
    }

    /// Changes the limit. Refuses a limit below what is already spoken for.
    ///
    /// Once this returns `Ok`, no more than the new limit is spoken for, for as long as that
    /// limit stands. A reservation asked for at the very moment a lower limit is being set may
    /// be refused although it would have fit.
    pub(crate) fn set_limit(&self, limit: Option<u64>) -> Result<()> {
        let counts = &*self.counts;
        let new = limit.unwrap_or(NO_CEILING);
        // The lock guards no data, so a holder that panicked left nothing half done.
        let _one_change_at_a_time = counts
            .changing_limit
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let old = counts.limit.load(SeqCst);
        let mut used = counts.used.load(SeqCst);
        if used <= new {
            counts.limit.store(new, SeqCst);
            // Bytes granted against the old limit since the look above are counted by now;
            // bytes counted from here on are judged against the new limit.
            used = counts.used.load(SeqCst);
            if used <= new {
                return Ok(());
            }
            counts.limit.store(old, SeqCst);
        }
        Err(refused("set_limit", used, new, new))
    }

    /// Counts `bytes` as spoken for, or refuses and counts nothing.
    ///
    /// With no ceiling the only request refused is one that would carry the count past what a
    /// `u64` holds, which no machine has.
    fn take(&self, bytes: u64, operation: &'static str) -> Result<()> {
        let counts = &*self.counts;
        let mut used = counts.used.load(SeqCst);
        loop {
            let limit = counts.limit.load(SeqCst);
            let available = limit.saturating_sub(used);
            if bytes > available {
                return Err(refused(operation, bytes, available, limit));
            }
            match counts
                .used
                .compare_exchange_weak(used, used + bytes, SeqCst, SeqCst)
            {
                Ok(_) if counts.limit.load(SeqCst) == limit => return Ok(()),
                // The limit changed between the look and the count: uncount, and ask again.
                Ok(_) => used = counts.used.fetch_sub(bytes, SeqCst) - bytes,
                Err(now) => used = now,
            }
        }
    }

    fn give_back(&self, bytes: u64) {
        self.counts.used.fetch_sub(bytes, SeqCst);
    }
}

/// The refusal of this part. Its labels are fixed words, so making it allocates nothing.
fn refused(operation: &'static str, requested: u64, available: u64, limit: u64) -> Error {
    Error::MemoryBudgetExceeded {
        subsystem: "memory",
        operation,
        requested_bytes: requested,
        available_bytes: available,
        budget_limit_bytes: limit,
        hint: "select or write less at once, or raise the limit with SET MEMORY_LIMIT or the \
               memory_limit open option",
    }
}

/// Bytes a caller was granted and has not given back.
///
/// The holder of memory that grows with data (a result, a sort, a transaction's changes) keeps
/// one of these beside the container and grows it before the container grows. Dropping it
/// gives everything back.
pub(crate) struct Reservation {
    budget: Budget,
    bytes: u64,
    /// What the holder is doing, as a refusal to grow names it.
    operation: &'static str,
}

impl Reservation {
    /// Asks for `bytes` more. On refusal the reservation keeps what it had.
    pub(crate) fn grow(&mut self, bytes: u64) -> Result<()> {
        self.budget.take(bytes, self.operation)?;
        self.bytes += bytes;
        Ok(())
    }

    /// Gives `bytes` back early, or everything held if that is less.
    pub(crate) fn shrink(&mut self, bytes: u64) {
        let bytes = bytes.min(self.bytes);
        self.budget.give_back(bytes);
        self.bytes -= bytes;
    }

    /// Bytes held now.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.give_back(self.bytes);
    }
}
