//! Storage-neutral clustered vector primitives. The native engine supplies authenticated
//! pages, disk admission, snapshots and transactions; this layer owns no file or WAL.
#![expect(
    clippy::result_large_err,
    reason = "The shared engine Error retains inline allocation-free refusal diagnostics."
)]
mod book;
mod codec;
mod memory;
mod search;
pub mod storage;
mod types;
mod writer;
pub use search::search;
pub use types::{BufferState, Hit, Mode, Policy, TrainingPolicy, Vector, Work};
pub use writer::ClusteredIndex;
