#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/vector_partition_cleanup_inspection_contract.rs"]
mod vector_partition_cleanup_inspection_contract;
#[path = "vector/vector_partition_declaration_contract.rs"]
mod vector_partition_declaration_contract;
#[path = "vector/vector_partition_query_contract.rs"]
mod vector_partition_query_contract;
#[path = "vector/vector_partition_restart_contract.rs"]
mod vector_partition_restart_contract;
#[path = "vector/vector_partition_scope_resolves_after_bind_contract.rs"]
mod vector_partition_scope_resolves_after_bind_contract;
#[path = "vector/vector_partition_sync_inspection_contract.rs"]
mod vector_partition_sync_inspection_contract;
