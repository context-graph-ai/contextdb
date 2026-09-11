pub mod cosine;
pub mod hnsw;
pub mod mem;
mod memory_budget;
pub(crate) mod quantized;
pub mod store;
#[cfg(feature = "test-seams")]
pub mod test_seam;

pub use cosine::cosine_similarity;
pub use hnsw::{HnswGraphStats, HnswIndex};
pub use mem::{MemVectorExecutor, VectorSearchDebugTrace};
#[doc(hidden)]
pub use memory_budget::MemoryBudget;
#[doc(hidden)]
pub use memory_budget::VectorWorkspaceReservation;
pub use quantized::{stored_vector_resident_bytes, stored_vector_value};
pub use store::{
    PartitionedVectorDelete, PartitionedVectorEntry, PartitionedVectorMove,
    PreparedPartitionedVectorBatch, PreparedVectorPublication, VectorIndexInfo, VectorIndexLayout,
    VectorIndexLayoutInfo, VectorMaintenanceFailure, VectorMaintenanceFailureDetails,
    VectorMaintenanceNeed, VectorMaintenanceReport, VectorPartitionInfo, VectorPartitionRef,
    VectorRouteQuarantineReason, VectorStore, VectorVersionIdentity,
};
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub use store::{
    VectorGraphCallbackPhaseForTest, VectorJournalTruncationFaultHandle,
    VectorJournalTruncationPhaseForTest, VectorLifecyclePauseHandle,
    VectorMaintenancePreparationPhaseForTest, VectorMemoryOwnershipSnapshot,
    VectorMemoryWorkspacePhaseForTest,
};
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub use store::{VectorMaintenanceProgressPauseHandle, VectorPassiveActivityCounters};

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod observations;
