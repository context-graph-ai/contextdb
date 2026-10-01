use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

type VectorLoadReservationTransfer = Arc<dyn Fn(usize) -> contextdb_core::Result<()> + Send + Sync>;

struct ActiveVectorLoadReservationTransfer {
    accountant: usize,
    transfer: VectorLoadReservationTransfer,
}

thread_local! {
    static VECTOR_LOAD_RESERVATION_TRANSFERS: RefCell<Vec<ActiveVectorLoadReservationTransfer>> =
        const { RefCell::new(Vec::new()) };
}

/// Give one synchronous vector-load call a way to move its request charge to
/// the resident graph owner. The accountant identity prevents a nested load
/// for another database from adopting the wrong request's bytes.
pub(crate) fn with_vector_load_reservation_transfer<T>(
    accountant: &Arc<MemoryAccountant>,
    transfer: VectorLoadReservationTransfer,
    operation: impl FnOnce() -> T,
) -> T {
    VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
        transfers
            .borrow_mut()
            .push(ActiveVectorLoadReservationTransfer {
                accountant: Arc::as_ptr(accountant) as usize,
                transfer,
            });
    });
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
                transfers.borrow_mut().pop();
            });
        }
    }
    let _restore = Restore;
    operation()
}

fn adopt_vector_load_reservation(
    accountant: &MemoryAccountant,
    bytes: usize,
) -> contextdb_core::Result<bool> {
    VECTOR_LOAD_RESERVATION_TRANSFERS.with(|transfers| {
        let transfers = transfers.borrow();
        let Some(active) = transfers.last() else {
            return Ok(false);
        };
        if active.accountant != accountant as *const MemoryAccountant as usize {
            return Ok(false);
        }
        (active.transfer)(bytes)?;
        Ok(true)
    })
}

/// Test-build only: a one-shot callback slot fired inside `try_allocate` at
/// the TOCTOU window between reading `limit` and re-checking it. It lets a unit
/// test force the exact `set_budget(None)`-races-`try_allocate` interleaving
/// deterministically instead of hoping the scheduler lands it. Production
/// carries neither this field nor its fire site (mirrors the engine's
/// `#[cfg(test)]` `__maintenance_wakes` liveness counter).
#[cfg(test)]
#[derive(Default)]
struct AllocRaceHook(std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>);

#[cfg(test)]
impl std::fmt::Debug for AllocRaceHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AllocRaceHook")
    }
}

thread_local! {
    /// Private, thread-scoped headroom for a limit transition's own control write.
    ///
    /// A limit transition has to write its new value down, and that write is itself
    /// charged to the budget it is moving. Publishing the target budget first so
    /// the write fits hands every concurrent operation capacity the durable value
    /// has not committed to; persisting first under a budget too small to write
    /// under traps the operator. This is the third answer: the transitioning thread
    /// gets the capacity it is moving between, nobody else sees it, every byte it
    /// admits is still counted in `used`, and the grant ends when the scope does.
    /// It is not a privilege a caller can ask for and it is never unaccounted.
    static TRANSITION_HEADROOM: RefCell<(usize, usize)> = const { RefCell::new((0, 0)) };
}

pub(crate) struct TransitionHeadroomScope {
    previous: (usize, usize),
}

impl Drop for TransitionHeadroomScope {
    fn drop(&mut self) {
        TRANSITION_HEADROOM.with(|headroom| *headroom.borrow_mut() = self.previous);
    }
}

/// Budget enforcer for memory-constrained edge devices.
/// All methods are &self — interior mutability via atomics.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemoryAccountant {
    limit: AtomicUsize,
    used: AtomicUsize,
    #[cfg(feature = "test-seams")]
    high_water_for_test: AtomicUsize,
    #[cfg(feature = "test-seams")]
    admitted_bytes_for_test: AtomicUsize,
    /// Bytes taken through `charge_retained` -- the writer's own retention
    /// door, which never refuses. A read session must never pay at it.
    #[cfg(feature = "test-seams")]
    retained_charge_bytes_for_test: AtomicUsize,
    startup_ceiling: AtomicUsize,
    has_ceiling: AtomicBool,
    idle_graph_reclaimer:
        parking_lot::Mutex<Option<std::sync::Weak<contextdb_vector::VectorStore>>>,
    #[cfg(any(test, feature = "test-seams"))]
    underflow_count: AtomicUsize,
    #[cfg(any(test, feature = "test-seams"))]
    tail_admission_failure: AtomicUsize,
    #[cfg(test)]
    alloc_race_hook: AllocRaceHook,
    /// Test-build only: the mirror of `alloc_race_hook` on the SUCCESS path,
    /// fired between the limit read and the `used` CAS. It lets a unit test
    /// land the allocator-reads-old-limit -> narrowing-publish -> CAS
    /// interleaving deterministically, which is the one a narrowing limit has
    /// to survive.
    #[cfg(test)]
    admission_race_hook: AllocRaceHook,
}

/// An ordered record of what one thread charged the budget and what it gave
/// back, so a refusal can be read as a composition rather than as a total.
///
/// Named allocation and reservation-growth entries carry their operation;
/// shrinkage and ordinary releases are untagged releases. `used_after` is the
/// accountant's whole-database total immediately after the change, which is
/// what makes the ledger answer "who is still holding this" for a single-
/// threaded sequence: a name whose allocations are never followed by the total
/// coming back down is the one holding the bytes.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetLedgerEntry {
    /// The `subsystem/operation` that charged, or `"release"`.
    pub what: String,
    pub bytes: usize,
    pub used_after: usize,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static BUDGET_LEDGER: std::cell::RefCell<Option<Vec<BudgetLedgerEntry>>> =
        const { std::cell::RefCell::new(None) };
}

/// Start recording this thread's budget traffic. Recording is off until this is
/// called, so nothing pays for the ledger on an ordinary run.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn watch_budget_ledger_for_test() {
    BUDGET_LEDGER.with(|ledger| *ledger.borrow_mut() = Some(Vec::new()));
}

/// Stop recording and take what was recorded.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_budget_ledger_for_test() -> Vec<BudgetLedgerEntry> {
    BUDGET_LEDGER.with(|ledger| ledger.borrow_mut().take().unwrap_or_default())
}

/// How far past what it was PRICED at an already-admitted received schema unit
/// may still go before the ordinary refusal fires.
///
/// An admitted unit installs whole or not at all, so an allocation its price
/// did not foresee cannot simply be refused half way through. It is charged
/// instead -- but only this far. What the price can miss is decided by what a
/// peer chose to send (an arriving index wider than the ones the table already
/// has is priced at the average of those), so without a ceiling here the amount
/// a declared MEMORY_LIMIT can be overshot by is a peer's choice rather than the
/// operator's. Past this the model was wrong by more than a declared limit
/// should absorb and the unit is refused instead of installed over budget.
///
/// One exact-probe bitmap's worth, which is the largest single structure one
/// arriving index adds.
pub const ADMITTED_SCHEMA_UNIT_EXCESS_BYTES: usize = 64 * 1024;

/// What ONE already-admitted received schema unit may still take past its own
/// price, and which accountant it is entitled to take it from.
///
/// One of these belongs to one unit, pushed while that unit's items install and
/// popped when they are done, so two units never read or spend each other's
/// allowance. The accountant is identified by address, the same way
/// `private_transition_headroom` identifies the accountant a thread's headroom
/// belongs to: an allocation against any OTHER accountant -- another database's
/// statement, or the detached working database an authenticated unit stages
/// through -- keeps the ordinary refusal.
#[derive(Debug)]
struct AdmittedUnitBudget {
    accountant: usize,
    excess_charged: usize,
}

thread_local! {
    /// The schema units installing on THIS thread, innermost last. A unit's
    /// items run on the thread that entered its scope, so the innermost entry
    /// is the unit an allocation belongs to.
    static ADMITTED_UNITS: std::cell::RefCell<Vec<AdmittedUnitBudget>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// The headroom a whole arriving schema unit was priced at, HELD from the
/// preflight that priced it until the unit is installed or refused.
///
/// The preflight used to be a comparison: it read what was available, found the
/// unit fitted, and held nothing -- so any admission between that read and the
/// unit's first item spent the room the unit had just been promised, and the
/// unit installed anyway. The reservation is what makes the preflight's answer
/// still true when the items take it.
pub(crate) struct SchemaUnitReservation {
    accountant: Arc<MemoryAccountant>,
    /// Bytes THIS reservation is holding and has not handed to its own items.
    /// Owned here rather than pooled on the accountant, so a second unit's
    /// reservation can neither be spent by this one nor released by it.
    remaining: std::cell::Cell<usize>,
}

impl Drop for SchemaUnitReservation {
    /// Hands back whatever THIS unit never took. Nothing, if the unit reached
    /// its items -- entering the scope hands them the whole reservation.
    fn drop(&mut self) {
        let unspent = self.remaining.replace(0);
        self.accountant.release(unspent);
    }
}

/// Held for as long as an already-admitted received schema unit is installing
/// its items against THIS accountant.
///
/// Entering it hands the unit the room the preflight reserved for it: the
/// items then take that room through the ordinary allocator, which is what
/// keeps every byte paired with whatever is holding it. Once the room is gone,
/// an allocation the unit's price did not foresee is charged unconditionally up
/// to the bounded excess above -- so the unit cannot be left half installed by
/// a model that was slightly wrong, and cannot push the store arbitrarily past
/// its declared limit by a model that was very wrong.
pub(crate) struct AdmittedSchemaUnit<'a> {
    /// Borrowed for the life of the scope, so the reservation whose room the
    /// items are spending cannot be dropped while they are spending it.
    reservation: &'a SchemaUnitReservation,
}

impl<'a> AdmittedSchemaUnit<'a> {
    /// Hand THIS unit the room its own reservation is holding, and open its own
    /// excess allowance, for as long as its items install on this thread.
    pub(crate) fn enter(reservation: &'a SchemaUnitReservation) -> Self {
        let handed = reservation.remaining.replace(0);
        reservation.accountant.release(handed);
        ADMITTED_UNITS.with(|units| {
            units.borrow_mut().push(AdmittedUnitBudget {
                accountant: std::ptr::from_ref(&*reservation.accountant) as usize,
                excess_charged: 0,
            });
        });
        Self { reservation }
    }
}

impl Drop for AdmittedSchemaUnit<'_> {
    fn drop(&mut self) {
        let _ = self.reservation;
        ADMITTED_UNITS.with(|units| {
            units.borrow_mut().pop();
        });
    }
}

#[cfg(feature = "test-seams")]
fn note_budget_ledger(what: &str, bytes: usize, used_after: usize) {
    BUDGET_LEDGER.with(|ledger| {
        if let Some(entries) = ledger.borrow_mut().as_mut() {
            entries.push(BudgetLedgerEntry {
                what: what.to_owned(),
                bytes,
                used_after,
            });
        }
    });
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub limit: Option<usize>,
    pub used: usize,
    pub available: Option<usize>,
    pub startup_ceiling: Option<usize>,
}

/// An owned database-accountant charge used by suspendable read operations.
///
/// Unlike the short-lived call-site pairs used by the eager executor, this
/// reservation can grow as a pull source retains continuation state and can
/// outlive the stack frame that opened a cursor. Dropping it returns the
/// complete charge exactly once.
#[derive(Debug)]
pub(crate) struct OwnedMemoryReservation {
    accountant: Arc<MemoryAccountant>,
    bytes: usize,
}

/// A stack-scoped database-accountant charge. This is used where an existing
/// execution primitive can unwind from inside its source loop; the charge is
/// then returned even though control never reaches the primitive's ordinary
/// success/error epilogue.
pub(crate) struct ScopedMemoryReservation<'a> {
    accountant: &'a MemoryAccountant,
    bytes: usize,
}

impl<'a> ScopedMemoryReservation<'a> {
    pub(crate) fn try_new_for(
        accountant: &'a MemoryAccountant,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(Self { accountant, bytes })
    }
}

impl Drop for ScopedMemoryReservation<'_> {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

// Ordered staged postings retain the same engine admission owner.
impl contextdb_tx::StagedPostingCharge for OwnedMemoryReservation {}

impl OwnedMemoryReservation {
    pub(crate) fn new(accountant: Arc<MemoryAccountant>) -> Self {
        Self {
            accountant,
            bytes: 0,
        }
    }

    /// Grow a held reservation under a name the store's limit can report.
    ///
    /// The unlabelled `try_grow` below reports a refusal as the bounded read's
    /// own retain; a reservation held on behalf of one named piece of work
    /// grows here instead, so a refusal names that work.
    pub(crate) fn try_grow_for(
        &mut self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        let next = self.bytes.checked_add(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_string(),
                operation: operation.to_string(),
                requested_bytes: bytes,
                available_bytes: usize::MAX - self.bytes,
                budget_limit_bytes: usize::MAX,
                hint: hint.to_string(),
            }
        })?;
        self.accountant
            .try_allocate_for(bytes, subsystem, operation, hint)?;
        self.bytes = next;
        Ok(())
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn try_shrink(&mut self, bytes: usize) -> contextdb_core::Result<()> {
        let remaining = self.bytes.checked_sub(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "bounded_read".to_string(),
                operation: "release".to_string(),
                requested_bytes: bytes,
                available_bytes: self.bytes,
                budget_limit_bytes: self.bytes,
                hint: "The read gave back more memory than it was holding.".to_string(),
            }
        })?;
        self.accountant.release(bytes);
        self.bytes = remaining;
        Ok(())
    }

    /// Transfer an existing charge to a store owner. The shared accountant
    /// total is deliberately unchanged; only this request stops owning it.
    pub(crate) fn try_transfer_to_store(&mut self, bytes: usize) -> contextdb_core::Result<()> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(|| {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "bounded_read".to_string(),
                operation: "transfer_vector_load_reservation".to_string(),
                requested_bytes: bytes,
                available_bytes: self.bytes,
                budget_limit_bytes: self.bytes,
                hint: "The vector load cannot transfer more memory than its request reserved."
                    .to_string(),
            }
        })?;
        Ok(())
    }
}

impl OwnedMemoryReservation {
    /// Take a reservation the store's limit can name, and hold it for as long
    /// as the guard lives.
    ///
    /// The unlabelled `try_grow` above reports a refusal as the bounded read's
    /// own retain, which tells an operator nothing about WHICH piece of work
    /// the store-wide limit stopped. A read that holds a working set on behalf
    /// of one named operation reserves it here instead, so the refusal carries
    /// that operation's name the way the same work reports it on any other
    /// path.
    pub(crate) fn try_new_for(
        accountant: Arc<MemoryAccountant>,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<Self> {
        accountant.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(Self { accountant, bytes })
    }
}

impl Drop for OwnedMemoryReservation {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
        self.bytes = 0;
    }
}

impl MemoryAccountant {
    /// No budget enforcement. All allocations succeed. Default behavior.
    pub fn no_limit() -> Self {
        Self {
            limit: AtomicUsize::new(0),
            used: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
            idle_graph_reclaimer: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            underflow_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            tail_admission_failure: AtomicUsize::new(0),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
            #[cfg(test)]
            admission_race_hook: AllocRaceHook::default(),
        }
    }

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(bytes),
            has_ceiling: AtomicBool::new(true),
            idle_graph_reclaimer: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            underflow_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            tail_admission_failure: AtomicUsize::new(0),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
            #[cfg(test)]
            admission_race_hook: AllocRaceHook::default(),
        }
    }

    /// A persisted live setting, not a process-start ceiling. Reopening a
    /// database must restore enforcement without turning later durable SETs
    /// into forbidden attempts to cross a ceiling the operator never passed.
    pub(crate) fn with_runtime_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            high_water_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            admitted_bytes_for_test: AtomicUsize::new(0),
            #[cfg(feature = "test-seams")]
            retained_charge_bytes_for_test: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
            idle_graph_reclaimer: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            underflow_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            tail_admission_failure: AtomicUsize::new(0),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
            #[cfg(test)]
            admission_race_hook: AllocRaceHook::default(),
        }
    }

    /// Run `operation` on this thread with `bytes` of private headroom above
    /// the published budget. Nothing about the published budget changes, so no
    /// other thread's allocation is admitted by one byte more than before.
    pub(crate) fn with_transition_headroom<T>(
        &self,
        bytes: usize,
        operation: impl FnOnce() -> T,
    ) -> T {
        let token = std::ptr::from_ref(self) as usize;
        let previous = TRANSITION_HEADROOM
            .with(|headroom| std::mem::replace(&mut *headroom.borrow_mut(), (token, bytes)));
        let _scope = TransitionHeadroomScope { previous };
        operation()
    }

    /// What this thread may allocate above the published budget for the limit
    /// transition it is performing, and zero for every other thread and every
    /// other accountant.
    fn private_transition_headroom(&self) -> usize {
        let token = std::ptr::from_ref(self) as usize;
        TRANSITION_HEADROOM
            .try_with(|headroom| {
                let (owner, bytes) = *headroom.borrow();
                if owner == token { bytes } else { 0 }
            })
            .unwrap_or(0)
    }

    /// Attempt to allocate bytes. CAS-based, no TOCTOU.
    pub fn try_allocate(&self, bytes: usize) -> contextdb_core::Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        loop {
            let used = self.used.load(Ordering::SeqCst);
            let published = self.limit.load(Ordering::SeqCst);
            let limit = if published == 0 {
                0
            } else {
                published.saturating_add(self.private_transition_headroom())
            };
            if limit != 0 {
                let available = limit.saturating_sub(used);
                if bytes > available {
                    // TOCTOU window: `limit` was loaded above; a concurrent
                    // `set_budget(None)` may have cleared it since. The re-check
                    // below closes that window. In test builds a hook lets a
                    // unit test land exactly that interleaving deterministically.
                    #[cfg(test)]
                    self.fire_alloc_race_hook();
                    if self.limit.load(Ordering::SeqCst) != published {
                        continue;
                    }
                    return Err(contextdb_core::Error::MemoryBudgetExceeded {
                        subsystem: "memory".to_string(),
                        operation: "allocate".to_string(),
                        requested_bytes: bytes,
                        available_bytes: available,
                        budget_limit_bytes: limit,
                        hint:
                            "Reduce retained data, lower working-set size, or raise MEMORY_LIMIT."
                                .to_string(),
                    });
                }
            }

            let Some(next) = used.checked_add(bytes) else {
                return Err(contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: "memory".to_string(),
                    operation: "allocate".to_string(),
                    requested_bytes: bytes,
                    available_bytes: usize::MAX - used,
                    budget_limit_bytes: usize::MAX,
                    hint: "Reduce retained data or working-set size.".to_string(),
                });
            };
            // The interleaving a narrowing limit has to survive: this
            // allocator has already read `published` and decided the request
            // fits it; a concurrent narrowing publishes a smaller limit right
            // here, and the CAS below still runs. In test builds a hook lands
            // exactly that order deterministically.
            #[cfg(test)]
            self.fire_admission_race_hook();
            if self
                .used
                .compare_exchange(used, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Admission and publication share ONE linearization boundary,
                // and it is this load. An admission is not SUCCESSFUL until it
                // has been validated against a published limit read that
                // happens-after its own CAS. A narrowing that linearizes before
                // this load is therefore always seen: the CAS is undone and the
                // loop produces the ordinary typed refusal under the new limit.
                // A narrowing that linearizes after it is a publication this
                // admission genuinely preceded. Either way no request returns
                // success having been admitted under a limit already superseded,
                // and `used` is never left above the published ceiling by an
                // allocator that read a stale one.
                let current = self.limit.load(Ordering::SeqCst);
                let admissible = if current == 0 {
                    0
                } else {
                    current.saturating_add(self.private_transition_headroom())
                };
                if admissible != 0 && next > admissible {
                    self.release(bytes);
                    continue;
                }
                #[cfg(feature = "test-seams")]
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                #[cfg(feature = "test-seams")]
                self.admitted_bytes_for_test
                    .fetch_add(bytes, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    /// Replace one caller-owned reservation without releasing its protected
    /// capacity or charging both phases. The caller retains `old_bytes` on
    /// refusal and changes its ownership token only after success.
    pub(crate) fn transition_reservation(
        &self,
        old_bytes: usize,
        new_bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let others = used
                .checked_sub(old_bytes)
                .expect("a reservation transition must own its entire old charge");
            let published = self.limit.load(Ordering::SeqCst);
            let limit = if published == 0 {
                0
            } else {
                published.saturating_add(self.private_transition_headroom())
            };
            let available = if limit == 0 {
                usize::MAX - others
            } else {
                limit.saturating_sub(others)
            };
            // A shrinking phase always remains admitted, including after a
            // budget is narrowed below already-retained ownership.
            if new_bytes > old_bytes && new_bytes > available {
                #[cfg(test)]
                self.fire_alloc_race_hook();
                if self.limit.load(Ordering::SeqCst) != published {
                    continue;
                }
                return Err(contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: new_bytes - old_bytes,
                    available_bytes: if limit == 0 {
                        usize::MAX - used
                    } else {
                        limit.saturating_sub(used)
                    },
                    budget_limit_bytes: if limit == 0 { usize::MAX } else { limit },
                    hint: hint.to_owned(),
                });
            }
            let next = others.checked_add(new_bytes).ok_or_else(|| {
                contextdb_core::Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: new_bytes.saturating_sub(old_bytes),
                    available_bytes: usize::MAX - used,
                    budget_limit_bytes: usize::MAX,
                    hint: hint.to_owned(),
                }
            })?;
            #[cfg(test)]
            self.fire_admission_race_hook();
            if self
                .used
                .compare_exchange(used, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            // Match allocation's post-CAS limit linearization. Only growth
            // can invalidate admission; undo exactly its positive delta so
            // concurrent charges stay intact and the old owner stays held.
            let current = self.limit.load(Ordering::SeqCst);
            let admissible = if current == 0 {
                0
            } else {
                current.saturating_add(self.private_transition_headroom())
            };
            if new_bytes > old_bytes && admissible != 0 && next > admissible {
                self.release(new_bytes - old_bytes);
                continue;
            }
            #[cfg(feature = "test-seams")]
            {
                self.high_water_for_test.fetch_max(next, Ordering::SeqCst);
                self.admitted_bytes_for_test
                    .fetch_add(new_bytes.saturating_sub(old_bytes), Ordering::SeqCst);
                if new_bytes < old_bytes {
                    note_budget_ledger("release", old_bytes - new_bytes, next);
                } else {
                    note_budget_ledger(
                        &format!("{subsystem}/{operation}"),
                        new_bytes - old_bytes,
                        next,
                    );
                }
            }
            return Ok(());
        }
    }

    /// Charge bytes the store is holding now, whether or not the budget has
    /// room for them.
    ///
    /// The in-memory change log and the transaction manager's commit index
    /// exist the moment a commit is durable: refusing them at that point
    /// would mean a write that is on disk and not in memory. So the charge is
    /// unconditional, and the ordinary typed refusal fires on the NEXT
    /// admission instead -- which is the whole point, because until these
    /// were charged what stopped an import that outgrew a declared
    /// MEMORY_LIMIT was the machine's out-of-memory kill, not the engine.
    pub fn charge_retained(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        #[cfg(feature = "test-seams")]
        {
            self.high_water_for_test
                .fetch_max(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
            self.admitted_bytes_for_test
                .fetch_add(bytes, Ordering::SeqCst);
            self.retained_charge_bytes_for_test
                .fetch_add(bytes, Ordering::SeqCst);
            note_budget_ledger("charge_retained", bytes, self.used.load(Ordering::SeqCst));
        }
    }

    /// Hold what a whole arriving schema unit was priced at until that unit is
    /// installed or refused.
    ///
    /// The bytes are admitted the ordinary way, so a unit that does not fit is
    /// refused here and nothing of it is applied. What the reservation adds is
    /// that the room it found is still there when the unit's items take it: an
    /// allocation that lands between this call and the unit's first item can no
    /// longer spend the headroom the unit was just promised.
    pub(crate) fn reserve_schema_unit(
        self: &Arc<Self>,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<SchemaUnitReservation> {
        self.try_allocate_for(bytes, subsystem, operation, hint)?;
        Ok(SchemaUnitReservation {
            accountant: Arc::clone(self),
            remaining: std::cell::Cell::new(bytes),
        })
    }

    /// An allocation inside an admitted schema unit that the ordinary budget
    /// has already refused -- so that unit has spent everything it was priced
    /// at and is now asking for more.
    ///
    /// It is charged, unconditionally, up to
    /// [`ADMITTED_SCHEMA_UNIT_EXCESS_BYTES`] FOR THAT UNIT. Past that this
    /// answers `false` and the ordinary typed refusal fires, which is what
    /// keeps the overshoot a peer can cause at that constant rather than at
    /// whatever that peer chose to send. The allowance is read from the unit
    /// installing on this thread, and only if that unit was priced against THIS
    /// accountant, so one unit exhausting its allowance never refuses another's
    /// next allocation.
    fn charge_admitted_unit_excess(&self, bytes: usize) -> bool {
        let token = std::ptr::from_ref(self) as usize;
        let charged = ADMITTED_UNITS
            .try_with(|units| {
                let mut units = units.borrow_mut();
                let Some(budget) = units.last_mut() else {
                    return false;
                };
                if budget.accountant != token
                    || bytes
                        > ADMITTED_SCHEMA_UNIT_EXCESS_BYTES.saturating_sub(budget.excess_charged)
                {
                    return false;
                }
                budget.excess_charged = budget.excess_charged.saturating_add(bytes);
                true
            })
            .unwrap_or(false);
        if charged {
            self.charge_retained(bytes);
        }
        charged
    }

    /// Return freed bytes to the budget.
    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let released = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                Some(used.saturating_sub(bytes))
            });
        #[cfg(any(test, feature = "test-seams"))]
        if bytes > released.expect("release update always supplies a value") {
            self.underflow_count.fetch_add(1, Ordering::SeqCst);
        }
        #[cfg(feature = "test-seams")]
        note_budget_ledger("release", bytes, self.used.load(Ordering::SeqCst));
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = released;
    }

    pub(crate) fn register_idle_graph_reclaimer(
        &self,
        vector: &Arc<contextdb_vector::VectorStore>,
    ) {
        *self.idle_graph_reclaimer.lock() = Some(Arc::downgrade(vector));
    }

    /// Runtime budget adjustment. None removes limit.
    /// Returns Err if new limit exceeds startup ceiling.
    pub fn set_budget(&self, limit: Option<usize>) -> contextdb_core::Result<()> {
        if self.has_ceiling.load(Ordering::SeqCst) {
            let ceiling = self.startup_ceiling.load(Ordering::SeqCst);
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(contextdb_core::Error::Other(format!(
                        "memory limit {bytes} exceeds startup ceiling {ceiling}"
                    )));
                }
                None => {
                    return Err(contextdb_core::Error::Other(
                        "cannot remove memory limit when a startup ceiling is set".to_string(),
                    ));
                }
                _ => {}
            }
        }

        match limit {
            Some(bytes) => {
                self.limit.store(bytes, Ordering::SeqCst);
            }
            None => {
                self.limit.store(0, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    /// Start the high-water mark again from what is admitted now.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn reset_high_water_for_test(&self) {
        self.high_water_for_test
            .store(self.used.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Peak successfully admitted bytes, including allocations already released.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn high_water_for_test(&self) -> usize {
        self.high_water_for_test.load(Ordering::SeqCst)
    }

    /// Successful byte admissions at this accountant, on every calling thread.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn admitted_bytes_for_test(&self) -> usize {
        self.admitted_bytes_for_test.load(Ordering::SeqCst)
    }

    /// Bytes this accountant took through `charge_retained`, the writer's
    /// unconditional retention door. A read session's accountant must report
    /// zero here: a committed image never appends to a change log, and every
    /// byte of the image it does hold was already admitted when it was
    /// decoded.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn retained_charge_bytes_for_test(&self) -> usize {
        self.retained_charge_bytes_for_test.load(Ordering::SeqCst)
    }

    /// Snapshot of current memory state.
    pub fn usage(&self) -> MemoryUsage {
        let limit = match self.limit.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        };
        let used = self.used.load(Ordering::SeqCst);
        let startup_ceiling = self
            .has_ceiling
            .load(Ordering::SeqCst)
            .then(|| self.startup_ceiling.load(Ordering::SeqCst));
        MemoryUsage {
            limit,
            used,
            available: limit.map(|limit| limit.saturating_sub(used)),
            startup_ceiling,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn underflow_count_for_test(&self) -> usize {
        self.underflow_count.load(Ordering::SeqCst)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_tail_admission_failure_for_test(&self) {
        self.tail_admission_failure.store(1, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn tail_admission_failure_consumed_for_test(&self) -> bool {
        self.tail_admission_failure.load(Ordering::SeqCst) == 2
    }

    /// Evict idle graphs of partitions the statement on this thread is not
    /// searching until `short` more bytes are free, for a graph it is about
    /// to load. Returns how many graphs were evicted.
    pub(crate) fn make_room_for_graph_load(&self, short: usize) -> usize {
        self.idle_graph_reclaimer
            .lock()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map_or(0, |vector| vector.make_room_for_graph_load(short))
    }

    pub fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if operation == "prepare_hnsw_tail"
            && self
                .tail_admission_failure
                .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_owned(),
                operation: operation.to_owned(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: "Injected fresh-tail admission exhaustion.".to_owned(),
            });
        }
        let mut allocation = self.try_allocate(bytes);
        #[cfg(feature = "test-seams")]
        if allocation.is_ok() {
            note_budget_ledger(
                &format!("{subsystem}/{operation}"),
                bytes,
                self.used.load(Ordering::SeqCst),
            );
        }
        // A saved graph whose topology does not fit is admitted once the
        // serving pages already resident give way to it: they are re-readable,
        // the topology is what makes its partition searchable at all.
        // Idle graphs of partitions the statement is not searching go first;
        // serving pages give way only after them.
        if allocation.is_err() && operation == "load_retained_vector_generation" {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            if let Some(vector) = vector {
                let short = || bytes.saturating_sub(self.usage().available.unwrap_or(usize::MAX));
                if vector.make_room_for_graph_load(short()) != 0 {
                    allocation = self.try_allocate(bytes);
                }
                if allocation.is_err() && vector.give_back_serving_pages(short()) != 0 {
                    allocation = self.try_allocate(bytes);
                }
            }
        }
        // Serving pages are a cache a read can always refill, so nothing is
        // refused while they hold the memory it needs: they give way to any
        // allocation exactly as they give way to a graph load. The give-back
        // only tries its locks, so it is safe from inside any caller.
        if allocation.is_err() && operation != "load_retained_vector_generation" {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let short = bytes.saturating_sub(self.usage().available.unwrap_or(usize::MAX));
            if vector.is_some_and(|vector| vector.give_back_serving_pages(short) != 0) {
                allocation = self.try_allocate(bytes);
            }
        }
        if allocation.is_err() && matches!(operation, "read/table_scan" | "read/index_range") {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let graphs = vector
                .as_ref()
                .map_or(0, |vector| vector.evict_idle_graphs());
            if graphs != 0 {
                allocation = self.try_allocate(bytes);
            }
        }
        // A received schema unit that was already ADMITTED AS A WHOLE is
        // applying against THIS accountant, and an allocation it did not model
        // does not fit. It draws on the reservation the preflight is holding
        // for it, and past that on a BOUNDED excess.
        //
        // The unit was priced and admitted before its first item mutated
        // anything, so a limit genuinely too small for it was already refused
        // there, leaving the receiver's old complete state. Refusing at the
        // first byte the model got slightly wrong would instead leave a proper
        // subset of an arriving schema installed and durable -- the one outcome
        // the design's "Atomic schema installation" row forbids outright -- and
        // there is no way back: a received DROP TABLE cannot be taken back at
        // all, and compensating DDL written to undo it lands in the REPLICATED
        // log, so one node's memory shortfall would propagate a schema revert
        // to every peer.
        //
        // But the model's error is driven by what a PEER sent, so absorbing it
        // without a ceiling would hand the peer the operator's declared limit.
        // Past `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES` the ordinary typed refusal
        // fires: the limit the operator declared bounds the process, and the
        // most a unit can take past what it was priced at is that constant.
        if allocation.is_err() && self.charge_admitted_unit_excess(bytes) {
            return Ok(());
        }
        allocation.map_err(|err| match err {
            contextdb_core::Error::MemoryBudgetExceeded {
                requested_bytes,
                budget_limit_bytes,
                available_bytes,
                ..
            } => contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_string(),
                operation: operation.to_string(),
                requested_bytes,
                budget_limit_bytes,
                available_bytes,
                hint: hint.to_string(),
            },
            other => other,
        })
    }
}

impl contextdb_vector::MemoryBudget for MemoryAccountant {
    fn transition_reservation_for(
        &self,
        old_bytes: usize,
        new_bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        let mut transition =
            self.transition_reservation(old_bytes, new_bytes, subsystem, operation, hint);
        // Workspace growth preserves ordinary admission's cache give-back:
        // these pages can be filled again and their release is independent
        // of the caller's still-held old reservation.
        if transition.is_err() {
            let vector = self
                .idle_graph_reclaimer
                .lock()
                .as_ref()
                .and_then(std::sync::Weak::upgrade);
            let short = new_bytes
                .saturating_sub(old_bytes)
                .saturating_sub(self.usage().available.unwrap_or(usize::MAX));
            if vector.is_some_and(|vector| vector.give_back_serving_pages(short) != 0) {
                transition =
                    self.transition_reservation(old_bytes, new_bytes, subsystem, operation, hint);
            }
        }
        transition
    }

    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        MemoryAccountant::try_allocate_for(self, bytes, subsystem, operation, hint)
    }

    fn release(&self, bytes: usize) {
        MemoryAccountant::release(self, bytes);
    }

    fn available_bytes(&self) -> Option<usize> {
        self.usage().available
    }

    fn adopt_caller_reservation(&self, bytes: usize) -> contextdb_core::Result<bool> {
        adopt_vector_load_reservation(self, bytes)
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for MemoryAccountant {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        MemoryAccountant::release(self, bytes);
    }
}

#[cfg(test)]
impl MemoryAccountant {
    /// Test-build only: install the one-shot interleaving hook.
    fn set_alloc_race_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.alloc_race_hook.0.lock().unwrap() = Some(hook);
    }

    /// Test-build only: fire (and consume) the interleaving hook, if installed.
    fn fire_alloc_race_hook(&self) {
        let hook = self.alloc_race_hook.0.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Test-build only: install the one-shot hook that runs between the limit
    /// read and the `used` CAS of a request that fits the limit it read.
    pub(crate) fn set_admission_race_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        *self.admission_race_hook.0.lock().unwrap() = Some(hook);
    }

    /// Test-build only: fire (and consume) the admission interleaving hook.
    fn fire_admission_race_hook(&self) {
        let hook = self.admission_race_hook.0.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reservation_transition_at_zero_headroom_keeps_capacity_from_a_concurrent_writer() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(1_000).unwrap();
        let writer_accountant = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer_accountant.clone();
            std::thread::spawn(move || {
                assert!(
                    writer
                        .try_allocate_for(1, "writer", "insert", "test")
                        .is_err()
                );
                assert_eq!(writer.usage().used, 1_000);
            })
            .join()
            .unwrap();
        }));
        accountant
            .transition_reservation(900, 900, "vector_index", "encode", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 1_000);
        let writer_accountant = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer_accountant.clone();
            std::thread::spawn(move || {
                assert!(writer.try_allocate(400).is_err());
                assert_eq!(writer.usage().used, 1_000);
            })
            .join()
            .unwrap();
        }));
        accountant
            .transition_reservation(900, 600, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 700);
        accountant.try_allocate(300).unwrap();
        #[cfg(feature = "test-seams")]
        assert_eq!(accountant.high_water_for_test(), 1_000);
        accountant.release(1_000);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_retries_cas_after_a_concurrent_independent_charge() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(900).unwrap();
        let writer = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let writer = writer.clone();
            std::thread::spawn(move || writer.try_allocate(50).unwrap())
                .join()
                .unwrap();
        }));
        accountant
            .transition_reservation(600, 600, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 950);
        #[cfg(feature = "test-seams")]
        assert_eq!(accountant.high_water_for_test(), 950);
        accountant.release(600);
        assert_eq!(
            accountant.usage().used,
            350,
            "the concurrent writer's charge remains intact"
        );
        accountant.release(350);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_refusal_preserves_old_owner_and_typed_phase_fields() {
        let accountant = MemoryAccountant::with_budget(1_000);
        accountant.try_allocate(900).unwrap();
        let error = accountant
            .transition_reservation(600, 701, "vector_index", "candidate", "phase hint")
            .unwrap_err();
        match error {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => {
                assert_eq!(subsystem, "vector_index");
                assert_eq!(operation, "candidate");
                assert_eq!(requested_bytes, 101);
                assert_eq!(available_bytes, 100);
                assert_eq!(budget_limit_bytes, 1_000);
                assert_eq!(hint, "phase hint");
            }
            other => panic!("expected typed transition refusal, got {other:?}"),
        }
        assert_eq!(accountant.usage().used, 900);
        accountant
            .transition_reservation(600, 700, "vector_index", "candidate", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 1_000);
        #[cfg(feature = "test-seams")]
        assert_eq!(accountant.high_water_for_test(), 1_000);
        accountant.release(1_000);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_rechecks_narrowed_limit_and_retains_old_charge() {
        let accountant = Arc::new(MemoryAccountant::with_budget(1_000));
        accountant.try_allocate(500).unwrap();
        let narrower = accountant.clone();
        accountant.set_admission_race_hook(Box::new(move || {
            let narrower = narrower.clone();
            std::thread::spawn(move || narrower.set_budget(Some(600)).unwrap())
                .join()
                .unwrap();
        }));
        assert!(matches!(
            accountant.transition_reservation(400, 700, "vector_index", "encode", "test"),
            Err(contextdb_core::Error::MemoryBudgetExceeded {
                requested_bytes: 300,
                available_bytes: 100,
                budget_limit_bytes: 600,
                ..
            })
        ));
        assert_eq!(accountant.usage().used, 500);
        // A further narrowing cannot prevent giving dead workspace back.
        accountant.set_budget(Some(200)).unwrap();
        accountant
            .transition_reservation(400, 100, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 200);
        accountant.release(200);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    #[test]
    fn reservation_transition_retries_a_limit_removal_before_refusal() {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant.set_budget(Some(500)).unwrap();
        accountant.try_allocate(500).unwrap();
        let remover = accountant.clone();
        accountant.set_alloc_race_hook(Box::new(move || remover.set_budget(None).unwrap()));
        accountant
            .transition_reservation(400, 800, "vector_index", "persist", "test")
            .unwrap();
        assert_eq!(accountant.usage().used, 900);
        accountant.release(900);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }

    /// The TOCTOU this guards: `try_allocate` loads `limit`, finds the request
    /// over budget, and a concurrent `set_budget(None)` clears the limit before
    /// the failure returns. The re-check must observe the change and retry so
    /// the allocation succeeds under the now-unlimited budget — never a
    /// spurious `MemoryBudgetExceeded`. The hook forces exactly that
    /// interleaving; deleting the re-check makes this fail deterministically.
    #[test]
    fn set_budget_none_racing_allocate_never_spuriously_fails() {
        // A REMOVABLE runtime budget: `no_limit()` + `set_budget(Some(..))`.
        // (A `with_budget` construction sets a startup ceiling, which forbids
        // removing the limit — the race under test needs a removable one.)
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant
            .set_budget(Some(1024))
            .expect("adding a removable runtime budget must succeed");
        accountant
            .try_allocate(1024)
            .expect("filling the budget to the brim must succeed");
        assert_eq!(accountant.usage().available, Some(0));

        let racer = Arc::clone(&accountant);
        accountant.set_alloc_race_hook(Box::new(move || {
            racer
                .set_budget(None)
                .expect("removing the budget must succeed");
        }));

        accountant.try_allocate(1).expect(
            "allocate racing a concurrent set_budget(None) must observe the \
             cleared limit via the re-check and succeed, never spuriously fail",
        );
        assert!(accountant.usage().limit.is_none());
    }

    /// A reservation means the room the preflight found is still there when
    /// THAT unit's items take it, and `used` means what the store is holding.
    ///
    /// Both stop being true the moment two units share one accountant -- two
    /// stores opened on one accountant, which `open_with_config` exists to
    /// allow, or two units that change no table shape and so never meet the
    /// schema publication gate. A reservation that reads a POOL rather than its
    /// own bytes hands the first unit to enter its scope every live
    /// reservation's room, so the second unit does not have the guarantee it
    /// was given; and it then releases room it had already handed away, which
    /// leaves `used` permanently below what the store is holding and the
    /// declared limit that much less effective.
    #[test]
    fn one_schema_unit_never_spends_or_releases_another_unit_s_reserved_room() {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        accountant
            .set_budget(Some(1_000_000))
            .expect("declare a limit both units are priced under");

        let first = accountant
            .reserve_schema_unit(100_000, "sync", "received_schema_unit", "")
            .expect("the first unit's price fits");
        let second = accountant
            .reserve_schema_unit(200_000, "sync", "received_schema_unit", "")
            .expect("the second unit's price fits too");
        assert_eq!(
            accountant.usage().used,
            300_000,
            "both reservations are held"
        );

        {
            // The first unit's items start installing. Entering hands that unit
            // the room ITS OWN reservation is holding -- never the second
            // unit's, which no item of this unit is entitled to spend.
            let _scope = AdmittedSchemaUnit::enter(&first);
            assert_eq!(
                accountant.usage().used,
                200_000,
                "entering a scope hands back the entering unit's 100,000 B and leaves the other \
                 unit's 200,000 B reserved"
            );
        }

        // The first unit is done and its reservation has nothing left to hand
        // back: its room went to its own items. Releasing anything here would
        // be releasing bytes those items are still holding.
        drop(first);
        assert_eq!(
            accountant.usage().used,
            200_000,
            "a spent reservation releases nothing, so `used` still accounts for the second unit"
        );

        drop(second);
        assert_eq!(
            accountant.usage().used,
            0,
            "and the unspent reservation releases exactly its own bytes"
        );
    }
}

/// Fork allocator ownership is independent of the redb page cache. The fork
/// must obtain this admission before opening/growing allocator metadata and
/// report actual capacities, including overlapping resize copies. Naming a
/// charge does not itself supply a structural allocator residency bound.
#[derive(Debug)]
#[allow(dead_code)] // Admission seam awaiting the fork allocator capacity oracle.
pub(crate) struct RedbAllocatorStateCharge {
    reservation: OwnedMemoryReservation,
    pub(crate) resident_bytes: usize,
    pub(crate) resize_copy_bytes: usize,
    pub(crate) region_bookkeeping_bytes: usize,
}

#[allow(dead_code)] // Consumed by the separately owned fork integration.
impl RedbAllocatorStateCharge {
    pub(crate) fn new(accountant: Arc<MemoryAccountant>) -> Self {
        Self {
            reservation: OwnedMemoryReservation::new(accountant),
            resident_bytes: 0,
            resize_copy_bytes: 0,
            region_bookkeeping_bytes: 0,
        }
    }

    /// The caller supplies capacities from the allocator, never file size or
    /// logical page counts. Growth refuses before the corresponding allocation;
    /// shrinking follows release of the old allocation.
    pub(crate) fn resize(
        &mut self,
        resident_bytes: usize,
        resize_copy_bytes: usize,
        region_bookkeeping_bytes: usize,
    ) -> contextdb_core::Result<()> {
        let next = resident_bytes
            .checked_add(resize_copy_bytes)
            .and_then(|bytes| bytes.checked_add(region_bookkeeping_bytes))
            .ok_or_else(|| contextdb_core::Error::MemoryBudgetExceeded {
                subsystem: "redb".into(),
                operation: "redb_allocator_state".into(),
                requested_bytes: usize::MAX,
                available_bytes: 0,
                budget_limit_bytes: usize::MAX,
                hint: "Allocator capacity accounting overflowed.".into(),
            })?;
        let held = self.reservation.bytes();
        if next > held {
            self.reservation.try_grow_for(
                next - held,
                "redb",
                "redb_allocator_state",
                "Raise MEMORY_LIMIT to admit redb allocator metadata.",
            )?;
        } else {
            self.reservation.try_shrink(held - next)?;
        }
        self.resident_bytes = resident_bytes;
        self.resize_copy_bytes = resize_copy_bytes;
        self.region_bookkeeping_bytes = region_bookkeeping_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod redb_allocator_state_tests {
    use super::*;

    #[test]
    fn allocator_capacity_owner_keeps_prior_charge_when_growth_refuses() {
        let accountant = Arc::new(MemoryAccountant::with_budget(128));
        let mut owner = RedbAllocatorStateCharge::new(Arc::clone(&accountant));
        owner.resize(32, 16, 8).unwrap();
        assert_eq!(accountant.usage().used, 56);
        assert!(owner.resize(64, 128, 8).is_err());
        assert_eq!(accountant.usage().used, 56);
        assert_eq!(
            (
                owner.resident_bytes,
                owner.resize_copy_bytes,
                owner.region_bookkeeping_bytes
            ),
            (32, 16, 8)
        );
        owner.resize(32, 0, 8).unwrap();
        assert_eq!(accountant.usage().used, 40);
        drop(owner);
        assert_eq!(accountant.usage().used, 0);
    }
}
