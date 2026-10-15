#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/vector_schema_mixed_version_compatibility_contract.rs"]
mod vector_schema_mixed_version_compatibility_contract;
#[path = "vector/vector_schema_mixed_version_erasure_frontier_contract.rs"]
mod vector_schema_mixed_version_erasure_frontier_contract;
#[path = "vector/vector_schema_mixed_version_push_compatibility_contract.rs"]
mod vector_schema_mixed_version_push_compatibility_contract;
#[path = "vector/vector_schema_mixed_version_recovery_cursor_contract.rs"]
mod vector_schema_mixed_version_recovery_cursor_contract;
#[path = "vector/vector_schema_mixed_version_restart_contract.rs"]
mod vector_schema_mixed_version_restart_contract;
