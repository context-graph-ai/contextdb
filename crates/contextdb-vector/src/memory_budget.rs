use contextdb_core::Result;
use std::fmt::Debug;
use std::sync::Arc;

/// Opaque accounting capability consumed by vector storage. The database
/// engine owns the implementation and never returns that implementation to
/// callers; this crate can reserve and release bytes without exposing a
/// durable-limit mutation handle.
#[doc(hidden)]
pub trait MemoryBudget: Debug + Send + Sync {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()>;

    fn release(&self, bytes: usize);
}

#[derive(Debug)]
struct UnlimitedMemoryBudget;

impl MemoryBudget for UnlimitedMemoryBudget {
    fn try_allocate_for(
        &self,
        _bytes: usize,
        _subsystem: &str,
        _operation: &str,
        _hint: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn release(&self, _bytes: usize) {}
}

pub(crate) fn unlimited_memory_budget() -> Arc<dyn MemoryBudget> {
    Arc::new(UnlimitedMemoryBudget)
}
