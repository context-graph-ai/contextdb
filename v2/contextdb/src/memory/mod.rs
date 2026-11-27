//! memory: the budget and the reservations taken from it.
//!
//! It never allocates on another part's behalf: it counts what the other parts say they are
//! about to take, and says no before they take it.

use std::cell::RefCell;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use crate::types::{Error, Result};

/// The size of the page cache of a store opened under this memory limit, in bytes: a quarter
/// of the limit and never less than 8 MiB, which is also its size when there is no limit. It is
/// reserved from the budget when the store is opened and does not change until the store is
/// closed, because the storage library fixes its cache when the data file is opened.
pub(crate) fn cache_under(limit: Option<u64>, share: Option<u64>) -> u64 {
    let limit = limit.unwrap_or(0);
    share.map_or((limit / 4).max(8 << 20), |p| {
        limit / 100 * p + limit % 100 * p / 100
    })
}

#[cfg(test)]
mod tests;

/// Bytes every budget sets aside for small fixed allocations. Data-sized work holds its own
/// reservation. The figure is guarded by the test that counts every byte the engine takes
/// (`tests/memory_ceiling.rs`).
const ALLOWANCE: u64 = 1 << 20;

/// A reader's room to check one commit: the storage library's longest list, 8 bytes and a bit a
/// page. A writer checks only at open, with its cache empty, and sets none aside.
const CHECK: u64 = 65_536 * 8 + 65_536 / 8;

/// What an open under `limit` sets aside, by name: the allowance, a page cache of `cache` bytes
/// counted with its bookkeeping, the plan cache, and a reader's check or a writer's page maps for
/// a file of `file` bytes. Each door's reservations read it, and its floor is their sum.
pub(crate) fn account(limit: Option<u64>, cache: u64, file: Option<u64>) -> [u64; 4] {
    let plans = limit.map_or(1 << 20, |limit| (limit / 32).clamp(1 << 20, 8 << 20));
    let maps = file.map_or(CHECK, |len| (len / (4 << 30) + 1) * (768 << 10));
    [ALLOWANCE, cache, plans, maps]
}

/// The bytes of pages a page cache of `cache` bytes holds: each 4 KiB page keeps 160 bytes of
/// bookkeeping inside the cache's bytes.
pub(crate) fn pages(cache: u64) -> usize {
    (cache / (4096 + 160) * 4096) as usize
}

/// Refuses a limit below the sum of an open's account, naming that floor.
pub(crate) fn admit(limit: Option<u64>, account: [u64; 4]) -> Result<()> {
    let floor = account.iter().sum();
    match limit {
        Some(limit) if limit < floor => Err(refused("open", floor, limit, limit)),
        _ => Ok(()),
    }
}

/// A writer's undeclared limit: a quarter of the memory the process may use, never below
/// `floor`, and 2 GiB where that is unknown.
pub(crate) fn default_limit(allowed: Option<u64>, floor: u64) -> u64 {
    allowed.map_or(2 << 30, |bytes| (bytes / 4).max(floor))
}

/// How "no ceiling" is kept: a limit that no count of bytes can pass.
const NO_CEILING: u64 = u64::MAX;

thread_local! {
    static STATISTICS_PAYER: RefCell<Option<Budget>> = const { RefCell::new(None) };
}

/// While this is held, a statement-statistics line on this thread is reserved from the budget
/// it was opened with, not from the shared record's budget.
pub(crate) struct StatisticsPayer {
    prior: Option<Budget>,
}

impl Drop for StatisticsPayer {
    fn drop(&mut self) {
        STATISTICS_PAYER.with(|slot| *slot.borrow_mut() = self.prior.take());
    }
}

/// The declared memory limit of one open store, and how much of it is spoken for.
///
/// Every figure is in bytes. One budget belongs to one writer or one reading session, and
/// cloning gives another handle to the same budget. A writer takes its remembered ceiling
/// or the machine default; a reader has one only if its open names one. Internally `None`
/// means no ceiling, and what is reserved is still counted.
///
/// Three kinds of memory are counted against it: small fixed things, covered by an allowance
/// of 1 MiB set aside in [`Budget::new`]; fixed caches reserved once at open; and everything
/// that grows with data, which must hold a [`Reservation`]. The page cache is a quarter of
/// a declared limit, at least 8 MiB; the plan cache is a thirty-second, between 1 and 8 MiB.
/// Without a declared limit both take their minima. A limit below an open's [`account`] is
/// refused with `MemoryBudgetExceeded`, naming that floor.
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
    /// Highest successful total charge since this budget's measurement began.
    peak: AtomicU64,
    /// Bytes held outside this transaction when its statement began; zero for reads.
    outside: AtomicU64,
    /// A statement charges its handle through the same admission gate.
    parent: Option<Budget>,
    /// Held while the limit changes, so that one change is settled before the next begins.
    /// Asking for memory and giving it back never take it.
    changing_limit: Mutex<()>,
    /// The opening allowance as standing memory draws on it, before it asks for fresh room.
    standing: OnceLock<Budget>,
}

impl Budget {
    /// A budget with this limit in bytes, or with no ceiling for `None`.
    ///
    /// Refuses with `MemoryBudgetExceeded` a limit too small to hold the fixed allowance.
    pub(crate) fn new(limit: Option<u64>) -> Result<Budget> {
        let budget = Self::empty(limit, None);
        budget.take(ALLOWANCE, "open")?;
        Ok(budget)
    }

    fn empty(limit: Option<u64>, parent: Option<Budget>) -> Budget {
        Budget {
            counts: Arc::new(Counts {
                limit: AtomicU64::new(limit.unwrap_or(NO_CEILING)),
                used: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                outside: AtomicU64::new(0),
                parent,
                changing_limit: Mutex::new(()),
                standing: OnceLock::new(),
            }),
        }
    }

    pub(crate) fn handle(&self) -> &Budget {
        self.counts.parent.as_ref().map_or(self, Budget::handle)
    }

    /// What standing memory, such as the shared newest view, draws on before fresh room.
    pub(crate) fn standing(&self) -> &Budget {
        (self.counts.standing).get_or_init(|| Self::empty(Some(ALLOWANCE), None))
    }

    /// A statement shares its handle's ceiling, without a second fixed allowance.
    pub(crate) fn statement(&self) -> Budget {
        Self::empty(None, Some(self.clone()))
    }

    pub(crate) fn peak_bytes(&self) -> u64 {
        self.counts.outside.load(SeqCst) + self.counts.peak.load(SeqCst)
    }

    /// A write adds the handle's other reservations at entry to its own high-water.
    pub(crate) fn reset_peak(&self, outside: u64) {
        self.counts.outside.store(outside, SeqCst);
        self.counts.peak.store(self.used(), SeqCst);
    }

    /// While held on this thread, statement statistics reserve from this budget.
    pub(crate) fn pay_for_statistics(&self) -> StatisticsPayer {
        StatisticsPayer {
            prior: STATISTICS_PAYER.with(|slot| slot.replace(Some(self.clone()))),
        }
    }

    /// Asks for `bytes` before they are allocated.
    ///
    /// Refuses with `MemoryBudgetExceeded`, naming `operation`, when the bytes would pass the
    /// limit; nothing is counted on refusal. Never waits and never allocates: it costs a few
    /// atomic operations, so it is cheap enough to call per row but is meant to be called per
    /// batch.
    pub(crate) fn reserve(&self, bytes: u64, operation: &'static str) -> Result<Reservation> {
        let budget = statistics_payer(operation).unwrap_or_else(|| self.clone());
        budget.take(bytes, operation)?;
        Ok(Reservation {
            budget,
            bytes,
            operation,
        })
    }

    /// A high-water cover kept for one pass through values.
    pub(crate) fn passing(&self, operation: &'static str) -> Result<Passing> {
        Ok(Passing(self.reserve(0, operation)?))
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
    /// A charge racing a decrease may be refused even if it fit the previous limit.
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

    /// Restores a stored setting under the writer's commit lock; live charges are retained.
    pub(crate) fn restore_limit(&self, limit: Option<u64>) {
        self.counts.limit.store(limit.unwrap_or(NO_CEILING), SeqCst);
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
                Ok(_) if counts.limit.load(SeqCst) == limit => {
                    if let Some(parent) = &counts.parent
                        && let Err(refused) = parent.take(bytes, operation)
                    {
                        counts.used.fetch_sub(bytes, SeqCst);
                        return Err(refused);
                    }
                    counts.peak.fetch_max(used + bytes, SeqCst);
                    return Ok(());
                }
                // The limit changed between the look and the count: uncount, and ask again.
                Ok(_) => used = counts.used.fetch_sub(bytes, SeqCst) - bytes,
                Err(now) => used = now,
            }
        }
    }

    fn give_back(&self, bytes: u64) {
        self.counts.used.fetch_sub(bytes, SeqCst);
        if let Some(parent) = &self.counts.parent {
            parent.give_back(bytes);
        }
    }
}

/// The refusal of this part. Its labels are fixed words, so making it allocates nothing.
fn statistics_payer(operation: &str) -> Option<Budget> {
    if operation != "statement statistics" {
        return None;
    }
    STATISTICS_PAYER.with(|slot| slot.borrow().clone())
}

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
    pub(crate) fn passing(&self) -> Passing {
        Passing(self.budget.reserve(0, self.operation).unwrap())
    }

    /// Makes room for one more entry in a list that has no place left: the list is made again
    /// with as many more, and never past `most` entries, the bytes of the new one taken from
    /// `self` before it is made and those of the old one given back once it is gone. What an
    /// entry points at is the caller's to take.
    pub(crate) fn room<T>(&mut self, list: &mut impl Grows<T>, most: usize) -> Result<()> {
        if list.len() == list.capacity() {
            let more = list.capacity().max(4).min(most - list.len());
            self.grow(((list.len() + more) * size_of::<T>()) as u64)?;
            list.reserve_exact(more);
            self.shrink((list.len() * size_of::<T>()) as u64);
        }
        Ok(())
    }

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

    /// Transfers an existing charge without changing this budget's used bytes.
    pub(crate) fn split(&mut self, bytes: u64) -> Self {
        assert!(bytes <= self.bytes);
        self.bytes -= bytes;
        Self {
            budget: self.budget.clone(),
            bytes,
            operation: self.operation,
        }
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

/// One pass's largest allocation, kept separately from all other charges.
pub(crate) struct Passing(Reservation);

impl Passing {
    pub(crate) fn passing(&self) -> Passing {
        self.0.passing()
    }

    pub(crate) fn cover(&mut self, bytes: u64) -> Result<()> {
        if bytes > self.0.bytes() {
            self.0.grow(bytes - self.0.bytes())?;
        }
        Ok(())
    }
}

/// An engine answer whose memory stays charged until it is dropped.
pub struct Owned<T> {
    value: T,
    _held: Reservation,
}

impl<T> Owned<T> {
    pub(crate) fn charged(value: T, held: Reservation) -> Self {
        Self { value, _held: held }
    }

    pub(crate) fn bytes(&self) -> u64 {
        self._held.bytes()
    }

    /// The value shared, charged `held` and the allocation that holds it with its counts.
    pub(crate) fn shared(value: T, mut held: Reservation) -> Result<Arc<Self>> {
        held.grow(size_of::<(usize, usize, Self)>() as u64)?;
        Ok(Arc::new(Self::charged(value, held)))
    }
}

impl<T> std::ops::Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

pub(crate) trait Grows<T> {
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn reserve_exact(&mut self, additional: usize);
}
macro_rules! grows {
    ($name:ident $(: $bound:ident)?) => {
        impl<T $(: $bound)?> Grows<T> for $name<T> {
            fn len(&self) -> usize { self.len() }
            fn capacity(&self) -> usize { self.capacity() }
            fn reserve_exact(&mut self, additional: usize) { self.reserve_exact(additional) }
        }
    };
}
grows!(Vec);
grows!(VecDeque);
grows!(BinaryHeap: Ord);

pub(crate) const fn node<T>() -> u64 {
    (4 * size_of::<T>() + 64) as u64
}

impl<T: fmt::Debug> fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}
