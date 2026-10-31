//! User outcomes of the clustered on-disk vector index: bounded writes, writes
//! across the codebook lifecycle, crash atomicity, typed cancellation, the
//! disk ceiling, rebuild determinism and build work, declared breadth,
//! snapshot isolation, deletes and PURGE, per-Context cost, exact and indexed
//! work, per-depth coverage, the calibration source, the replacement switch,
//! reader memory, small partitions, the removed graph options, the WITH
//! policy carrier, damaged index pages and their read-only inspection,
//! non-finite vectors, objective changes,
//! retired legacy runs, the training seed, and the SQL declaration surface.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]

#[path = "clustered_vector_index/support.rs"]
mod support;

#[path = "clustered_vector_index/build_determinism.rs"]
mod build_determinism;
#[path = "clustered_vector_index/calibration_source.rs"]
mod calibration_source;
#[path = "clustered_vector_index/cancellation.rs"]
mod cancellation;
#[path = "clustered_vector_index/codebook_lifecycle.rs"]
mod codebook_lifecycle;
#[path = "clustered_vector_index/crash_cut_points.rs"]
mod crash_cut_points;
#[path = "clustered_vector_index/declared_breadth.rs"]
mod declared_breadth;
#[path = "clustered_vector_index/deletes_and_purge.rs"]
mod deletes_and_purge;
#[path = "clustered_vector_index/depth_coverage.rs"]
mod depth_coverage;
#[path = "clustered_vector_index/disk_ceiling.rs"]
mod disk_ceiling;
#[path = "clustered_vector_index/exact_and_indexed_work.rs"]
mod exact_and_indexed_work;
#[path = "clustered_vector_index/graph_option_refusal.rs"]
mod graph_option_refusal;
#[path = "clustered_vector_index/import_tail_coverage.rs"]
mod import_tail_coverage;
#[path = "clustered_vector_index/index_corruption.rs"]
mod index_corruption;
#[path = "clustered_vector_index/native_candidate_port.rs"]
mod native_candidate_port;
#[path = "clustered_vector_index/non_finite_vectors.rs"]
mod non_finite_vectors;
#[path = "clustered_vector_index/objective_change.rs"]
mod objective_change;
#[path = "clustered_vector_index/parse_surface.rs"]
mod parse_surface;
#[path = "clustered_vector_index/per_context_cost.rs"]
mod per_context_cost;
#[path = "clustered_vector_index/read_only_damage_inspection.rs"]
mod read_only_damage_inspection;
#[path = "clustered_vector_index/reader_memory.rs"]
mod reader_memory;
#[path = "clustered_vector_index/retired_run_visibility.rs"]
mod retired_run_visibility;
#[path = "clustered_vector_index/small_partitions.rs"]
mod small_partitions;
#[path = "clustered_vector_index/snapshot_isolation.rs"]
mod snapshot_isolation;
#[path = "clustered_vector_index/switch_protocol.rs"]
mod switch_protocol;
#[path = "clustered_vector_index/training_seed.rs"]
mod training_seed;
#[path = "clustered_vector_index/with_carrier.rs"]
mod with_carrier;
#[path = "clustered_vector_index/write_buffer.rs"]
mod write_buffer;

#[path = "clustered_vector_index/engine_stage.rs"]
mod engine_stage;

#[path = "clustered_vector_index/native_seal.rs"]
mod native_seal;

#[path = "clustered_vector_index/public_rust_indexed.rs"]
mod public_rust_indexed;

#[path = "clustered_vector_index/public_rust_auto_and_overlay.rs"]
mod public_rust_auto_and_overlay;

#[path = "clustered_vector_index/reader_turnover_cleanup.rs"]
mod reader_turnover_cleanup;

#[path = "clustered_vector_index/concurrent_publication.rs"]
mod concurrent_publication;

#[path = "clustered_vector_index/live_route.rs"]
mod live_route;

#[path = "clustered_vector_index/native_copy_auth.rs"]
mod native_copy_auth;
