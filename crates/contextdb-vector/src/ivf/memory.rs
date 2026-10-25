use crate::MemoryBudgetOwner;
use contextdb_core::{Error, Result};

/// One invoice for an actual allocation or checked disjoint scratch group. Put
/// this field AFTER its covered buffers so physical free precedes settlement.
pub(crate) struct Invoice {
    owner: MemoryBudgetOwner,
    bytes: usize,
}
impl Invoice {
    pub(crate) fn reserve(
        owner: &MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        owner.try_allocate_for_static(
            bytes,
            "vector_index",
            operation,
            "Release retained vector objects or raise the declared memory limit.",
        )?;
        Ok(Self {
            owner: owner.clone(),
            bytes,
        })
    }
}
impl Drop for Invoice {
    fn drop(&mut self) {
        self.owner.release(self.bytes);
    }
}

pub(crate) fn overflow() -> Error {
    Error::MemoryBudgetExceeded {
        subsystem: "vector_index".into(),
        operation: "clustered_layout".into(),
        requested_bytes: usize::MAX,
        available_bytes: 0,
        budget_limit_bytes: 0,
        hint: "Use a representable clustered policy.".into(),
    }
}
pub(crate) fn corrupt() -> Error {
    Error::StoreCorrupted {
        path: "vector index".into(),
        reason: "clustered page or manifest does not match its declared layout".into(),
    }
}
pub(crate) fn bytes<T>(count: usize) -> Result<usize> {
    count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(overflow)
}
