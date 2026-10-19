//! Vector fixture copies retain the executing request's real reader capability.
use contextdb_core::{Result, read_memory::ReadMemoryBudget};
use contextdb_vector::MemoryBudget;
use std::sync::Arc;

#[derive(Debug)]
struct ReadVectorBudget(Arc<dyn ReadMemoryBudget>);
impl MemoryBudget for ReadVectorBudget {
    fn try_allocate_for(&self, bytes: usize, _: &str, _: &str, _: &str) -> Result<()> {
        self.0.try_reserve(
            bytes,
            "vector_copy",
            "materialize",
            "Reduce the fixture request's selected copy.",
        )
    }
    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}

pub fn budget(reader: Arc<dyn ReadMemoryBudget>) -> Arc<dyn MemoryBudget> {
    Arc::new(ReadVectorBudget(reader))
}
