use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

/// Budget enforcer for memory-constrained edge devices.
/// All methods are &self — interior mutability via atomics.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemoryAccountant {
    limit: AtomicUsize,
    used: AtomicUsize,
    startup_ceiling: AtomicUsize,
    has_ceiling: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub limit: Option<usize>,
    pub used: usize,
    pub available: Option<usize>,
    pub startup_ceiling: Option<usize>,
}

impl MemoryAccountant {
    /// No budget enforcement. All allocations succeed. Default behavior.
    pub fn no_limit() -> Self {
        Self {
            limit: AtomicUsize::new(0),
            used: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
        }
    }

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            startup_ceiling: AtomicUsize::new(bytes),
            has_ceiling: AtomicBool::new(true),
        }
    }

    /// Attempt to allocate bytes. CAS-based, no TOCTOU.
    pub fn try_allocate(&self, bytes: usize) -> crate::Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        loop {
            let used = self.used.load(Ordering::SeqCst);
            let limit = self.limit.load(Ordering::SeqCst);
            if limit != 0 {
                let available = limit.saturating_sub(used);
                if bytes > available {
                    return Err(crate::Error::MemoryBudgetExceeded {
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

            let next = used.saturating_add(bytes);
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
    pub fn set_budget(&self, limit: Option<usize>) -> crate::Result<()> {
        if self.has_ceiling.load(Ordering::SeqCst) {
            let ceiling = self.startup_ceiling.load(Ordering::SeqCst);
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(crate::Error::Other(format!(
                        "memory limit {bytes} exceeds startup ceiling {ceiling}"
                    )));
                }
                None => {
                    return Err(crate::Error::Other(
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
    ) -> crate::Result<()> {
        self.try_allocate(bytes).map_err(|err| match err {
            crate::Error::MemoryBudgetExceeded {
                requested_bytes,
                budget_limit_bytes,
                available_bytes,
                ..
            } => crate::Error::MemoryBudgetExceeded {
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
