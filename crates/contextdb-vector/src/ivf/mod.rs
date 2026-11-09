//! Storage-neutral clustered vector primitives. The native engine supplies authenticated
//! pages, disk admission, snapshots and transactions; this layer owns no file or WAL.
#![expect(
    clippy::result_large_err,
    reason = "The shared engine Error retains inline allocation-free refusal diagnostics."
)]
mod book;
mod catalog;
mod codec;
mod memory;
mod page_work;
mod recovery;
mod search;
pub mod storage;
mod types;
mod versions;
mod writer;
pub use catalog::{
    CatalogState, inspect as inspect_catalog, inspect_and_record as inspect_and_record_catalog,
};
pub use search::{
    query_workspace_allocation_bytes, search, search_with_eligibility, search_with_work,
    visit_visible_canonical,
};
pub use storage::Verdict;
pub use types::{
    BufferState, CandidateIdentity, CandidateSource, CanonicalCandidate, Hit, Mode, Policy,
    RowIdentity, TrainingPolicy, Vector, Work,
};
pub use writer::{ClusteredIndex, RemapProgress};

pub use book::{
    CodebookAllocationLayouts, RetainedCodebook, codebook_allocation_layout, list_block_layout,
    retained_codebook_manifest,
};
pub use writer::{WriterWorkspaceLayout, workspace_allocation_layout};
#[cfg(feature = "test-seams")]
mod layout_observation;
#[cfg(feature = "test-seams")]
pub use layout_observation::{
    IvfConstructorAllocationForTest, IvfConstructorObserverForTest, load_codebook_values_for_test,
    with_ivf_constructor_observer_for_test,
};

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use search::quantized_query_codes_for_test;
