use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

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

/// Budget enforcer for memory-constrained edge devices.
/// All methods are &self — interior mutability via atomics.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemoryAccountant {
    limit: AtomicUsize,
    used: AtomicUsize,
    startup_ceiling: AtomicUsize,
    has_ceiling: AtomicBool,
    #[cfg(test)]
    alloc_race_hook: AllocRaceHook,
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
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
        }
    }

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(bytes),
            has_ceiling: AtomicBool::new(true),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
        }
    }

    /// A persisted live setting, not a process-start ceiling. Reopening a
    /// database must restore enforcement without turning later durable SETs
    /// into forbidden attempts to cross a ceiling the operator never passed.
    pub(crate) fn with_runtime_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
            #[cfg(test)]
            alloc_race_hook: AllocRaceHook::default(),
        }
    }

    /// Attempt to allocate bytes. CAS-based, no TOCTOU.
    pub fn try_allocate(&self, bytes: usize) -> contextdb_core::Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        loop {
            let used = self.used.load(Ordering::SeqCst);
            let limit = self.limit.load(Ordering::SeqCst);
            if limit != 0 {
                let available = limit.saturating_sub(used);
                if bytes > available {
                    // TOCTOU window: `limit` was loaded above; a concurrent
                    // `set_budget(None)` may have cleared it since. The re-check
                    // below closes that window. In test builds a hook lets a
                    // unit test land exactly that interleaving deterministically.
                    #[cfg(test)]
                    self.fire_alloc_race_hook();
                    if self.limit.load(Ordering::SeqCst) != limit {
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
            if self
                .used
                .compare_exchange(used, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Return freed bytes to the budget.
    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let _ = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                Some(used.saturating_sub(bytes))
            });
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

    pub fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        self.try_allocate(bytes).map_err(|err| match err {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
}
