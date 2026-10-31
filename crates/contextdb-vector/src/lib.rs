#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod auto_planning_test_seams;
pub mod cosine;
pub mod mem;
mod memory_budget;
pub(crate) mod quantized;
pub mod store;
#[cfg(feature = "test-seams")]
pub mod test_seam;

pub use cosine::cosine_similarity;
pub use mem::{
    MemVectorExecutor, OwnedVectorSearchTrace, VectorPartitionSearchRoute,
    VectorPartitionSearchTrace, VectorSearchDebugTrace,
};
#[doc(hidden)]
pub use memory_budget::VectorWorkspaceReservation;
#[doc(hidden)]
pub use memory_budget::{
    MemoryBudget, MemoryBudgetOwner, PrepaidVectorCopyCharge, VectorCopyAdmission,
};
#[doc(hidden)]
pub use memory_budget::{ScopedVectorCopy, scoped_index_layout};
pub use quantized::{stored_vector_resident_bytes, stored_vector_value};
#[doc(hidden)]
pub use store::VectorPartitionSourceMaintenanceMetadata;
pub use store::{
    OwnedPartitionedVectorEntries, PartitionedVectorDelete, PartitionedVectorEntry,
    PartitionedVectorMove, PreparedLoadedVectorReplacement, PreparedPartitionedVectorBatch,
    PreparedVectorPolicyLayout, PreparedVectorPublication, PreparedVectorTablePublication,
    VectorIndexInfo, VectorIndexLayout, VectorIndexLayoutInfo,
    VectorMaintenanceFailure, VectorMaintenanceFailureDetails, VectorMaintenanceReport, VectorMembershipProvenance, VectorPartitionInfo, VectorPartitionRef,
    VectorStore, VectorTeardownReceipt, VectorVersionIdentity,
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
pub use store::VectorMemoryOwnershipSnapshot;

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod observations;

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use memory_budget::observe_copy_reservations_for_test;

#[doc(hidden)]
pub mod ivf;

#[cfg(feature = "test-seams")]
mod build_observation;
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use build_observation::{
    NativeIndexActivityScopeForTest, VectorBuildObserver, with_vector_build_observer,
};
