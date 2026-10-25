//! User outcomes of the clustered on-disk vector index: bounded writes, crash
//! atomicity, typed cancellation, the disk ceiling, snapshot isolation,
//! deletes and PURGE, per-Context cost, exact and indexed work, reader memory,
//! and the SQL declaration surface.

#[path = "clustered_vector_index/support.rs"]
mod support;

#[path = "clustered_vector_index/cancellation.rs"]
mod cancellation;
#[path = "clustered_vector_index/crash_cut_points.rs"]
mod crash_cut_points;
#[path = "clustered_vector_index/deletes_and_purge.rs"]
mod deletes_and_purge;
#[path = "clustered_vector_index/disk_ceiling.rs"]
mod disk_ceiling;
#[path = "clustered_vector_index/exact_and_indexed_work.rs"]
mod exact_and_indexed_work;
#[path = "clustered_vector_index/parse_surface.rs"]
mod parse_surface;
#[path = "clustered_vector_index/per_context_cost.rs"]
mod per_context_cost;
#[path = "clustered_vector_index/reader_memory.rs"]
mod reader_memory;
#[path = "clustered_vector_index/snapshot_isolation.rs"]
mod snapshot_isolation;
#[path = "clustered_vector_index/write_buffer.rs"]
mod write_buffer;
