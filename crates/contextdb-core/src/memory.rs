use std::sync::atomic::{AtomicBool, AtomicUsize};

/// Budget enforcer for memory-constrained edge devices.
/// All methods are &self — interior mutability via atomics.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemoryAccountant {
    limit: AtomicUsize,
    used: AtomicUsize,
    has_limit: AtomicBool,
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
            has_limit: AtomicBool::new(false),
            startup_ceiling: AtomicUsize::new(0),
            has_ceiling: AtomicBool::new(false),
        }
    }

    /// Set a byte ceiling. Allocations exceeding this fail.
    pub fn with_budget(bytes: usize) -> Self {
        Self {
            limit: AtomicUsize::new(bytes),
            used: AtomicUsize::new(0),
            has_limit: AtomicBool::new(true),
            startup_ceiling: AtomicUsize::new(bytes),
            has_ceiling: AtomicBool::new(true),
        }
    }

    /// Attempt to allocate bytes. CAS-based, no TOCTOU.
    pub fn try_allocate(&self, _bytes: usize) -> crate::Result<()> {
        // Stub: always returns Err. Makes M01 fail (INSERT under budget still errors).
        // Makes M04 fail (vector search under budget still errors).
        // Makes M07 fail (BFS under budget still errors).
        // A no-op returning Ok(()) would make M02, M05, M08 pass incorrectly.
        Err(crate::Error::Other("memory accounting stub".into()))
    }

    /// Return freed bytes to the budget.
    pub fn release(&self, _bytes: usize) {
        // Stub: no-op. Makes M03 fail (reclaimed memory not tracked).
    }

    /// Runtime budget adjustment. None removes limit.
    /// Returns Err if new limit exceeds startup ceiling.
    pub fn set_budget(&self, _limit: Option<usize>) -> crate::Result<()> {
        // Stub: always returns Err.
        // Makes M09 fail (SET MEMORY_LIMIT errors instead of succeeding).
        // Makes M10 fail (SET 'none' errors instead of succeeding).
        Err(crate::Error::Other("set_budget stub".into()))
    }

    /// Snapshot of current memory state.
    pub fn usage(&self) -> MemoryUsage {
        // Stub: returns zeros. Makes M09 fail (SHOW returns wrong limit).
        MemoryUsage {
            limit: None,
            used: 0,
            available: None,
            startup_ceiling: None,
        }
    }
}
