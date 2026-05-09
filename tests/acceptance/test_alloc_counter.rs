//! Re-exports the allocator counter installed at `tests/acceptance.rs` crate
//! root. Lives in its own file so test modules can name a stable path.
pub use crate::{ALLOC_COUNT, AllocCounter, COUNT_ENABLED};
