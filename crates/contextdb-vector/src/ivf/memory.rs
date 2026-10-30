use crate::MemoryBudgetOwner;
use contextdb_core::{Error, Result};

/// One invoice for an actual allocation or checked disjoint scratch group. Put
/// this field AFTER its covered buffers so physical free precedes settlement.
pub(crate) struct Invoice {
    owner: MemoryBudgetOwner,
    bytes: usize,
    #[cfg(feature = "test-seams")]
    observation: super::layout_observation::ReservationObservation,
}
impl Invoice {
    pub(crate) fn charged_bytes(&self) -> usize {
        self.bytes
    }
    #[cfg(feature = "test-seams")]
    pub(crate) fn original_owner_for_test(&self) -> usize {
        let (original, _, _) = self.owner.as_ref().original_read_payer_for_test();
        if original == 0 {
            self.owner.as_ref().stable_funder_identity()
        } else {
            original
        }
    }
    pub(crate) fn reserve(
        owner: &MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        #[cfg(feature = "test-seams")]
        let observation =
            super::layout_observation::ReservationObservation::capture(owner, bytes, operation)?;
        owner.try_allocate_for_static(
            bytes,
            "vector_index",
            operation,
            "Release retained vector objects or raise the declared memory limit.",
        )?;
        #[cfg(feature = "test-seams")]
        observation.debited();
        Ok(Self {
            owner: owner.clone(),
            bytes,
            #[cfg(feature = "test-seams")]
            observation,
        })
    }
}
impl Drop for Invoice {
    fn drop(&mut self) {
        #[cfg(feature = "test-seams")]
        self.observation.buffers_freed();
        self.owner.release(self.bytes);
        #[cfg(feature = "test-seams")]
        self.observation.released();
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

pub(crate) fn dimension(
    source: &impl super::storage::ReadPages,
    expected: usize,
    actual: usize,
) -> Error {
    Error::VectorIndexDimensionMismatch {
        index: source.index_identity().clone(),
        expected,
        actual,
    }
}
pub(crate) fn phase(name: impl Into<String>) -> Error {
    Error::InvalidStateTransition(name.into())
}

/// Arithmetic on authenticated stored counters reports corruption, never caller budget refusal.
pub(crate) fn stored_end(cursor: u64, total: u64, batch: u64) -> Result<u64> {
    let remaining = total.checked_sub(cursor).ok_or_else(corrupt)?;
    cursor.checked_add(remaining.min(batch)).ok_or_else(corrupt)
}
pub(crate) fn corrupt_page(key: super::storage::PageKey) -> Error {
    Error::StoreCorrupted {
        path: "vector index".into(),
        reason: format!("clustered page {key:?} is missing or does not match its content identity")
            .into(),
    }
}
