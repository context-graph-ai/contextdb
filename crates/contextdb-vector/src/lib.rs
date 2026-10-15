#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod auto_planning_test_seams;
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
pub use mem::{
    MemVectorExecutor, OwnedVectorSearchTrace, VectorPartitionSearchRoute,
    VectorPartitionSearchTrace, VectorSearchDebugTrace,
};
#[doc(hidden)]
pub use memory_budget::VectorWorkspaceReservation;
#[doc(hidden)]
pub use memory_budget::{MemoryBudget, PrepaidVectorCopyCharge, VectorCopyAdmission};
#[doc(hidden)]
pub use memory_budget::{ScopedVectorCopy, scoped_index_layout};
pub use quantized::{stored_vector_resident_bytes, stored_vector_value};
#[doc(hidden)]
pub use store::VectorPartitionSourceMaintenanceMetadata;
pub use store::{
    OwnedPartitionedVectorEntries, PartitionedVectorDelete, PartitionedVectorEntry,
    PartitionedVectorMove, PreparedLoadedVectorReplacement, PreparedPartitionedVectorBatch,
    PreparedVectorPolicyLayout, PreparedVectorPublication, PreparedVectorTablePublication,
    PublishedVectorPolicyLayout,
    VectorIndexInfo, VectorIndexLayout, VectorIndexLayoutInfo, VectorMaintenanceFailure,
    VectorMaintenanceFailureDetails, VectorMaintenanceNeed, VectorMaintenanceReport,
    VectorMembershipProvenance, VectorPartitionInfo, VectorPartitionRef,
    VectorRouteQuarantineReason, VectorStore, VectorTeardownReceipt, VectorVersionIdentity,
};
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub use store::{VectorCapWorkCountersForTest, VectorPartitionLimitAuditForTest};
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub use store::{
    VectorGraphCallbackPhaseForTest, VectorJournalTruncationFaultHandle,
    VectorJournalTruncationPhaseForTest, VectorLifecyclePauseHandle,
    VectorMaintenancePreparationPhaseForTest, VectorMemoryWorkspacePhaseForTest,
};
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub use store::{VectorMaintenanceProgressPauseHandle, VectorPassiveActivityCounters};
#[doc(hidden)]
pub use store::{VectorMemoryOwnershipSnapshot, WriterVectorTail};

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod observations;
