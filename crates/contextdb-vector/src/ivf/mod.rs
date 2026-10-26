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
pub use catalog::{CatalogState, inspect as inspect_catalog};
pub use search::{search, search_with_eligibility, search_with_work, visit_visible_canonical};
pub use storage::Verdict;
pub use types::{
    BufferState, CandidateIdentity, CandidateSource, CanonicalCandidate, Hit, Mode, Policy,
    RowIdentity, TrainingPolicy, Vector, Work,
};
pub use writer::{ClusteredIndex, RemapProgress};
