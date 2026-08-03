/// Read-only memory-budget snapshot exposed to embedding applications.
#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub limit: Option<usize>,
    pub used: usize,
    pub available: Option<usize>,
    pub startup_ceiling: Option<usize>,
}

/// A read-only memory-budget value for callers that need to describe or
/// inspect a budget without receiving the engine's mutable accounting
/// capability. Database limits are changed through
/// `contextdb_engine::Database::set_memory_limit`, which persists the choice
/// before publishing it to the running engine.
#[derive(Debug)]
pub struct MemoryAccountant {
    usage: MemoryUsage,
}

impl MemoryAccountant {
    pub fn no_limit() -> Self {
        Self {
            usage: MemoryUsage {
                limit: None,
                used: 0,
                available: None,
                startup_ceiling: None,
            },
        }
    }

    pub fn with_budget(bytes: usize) -> Self {
        Self {
            usage: MemoryUsage {
                limit: Some(bytes),
                used: 0,
                available: Some(bytes),
                startup_ceiling: Some(bytes),
            },
        }
    }

    pub fn usage(&self) -> MemoryUsage {
        self.usage.clone()
    }
}
