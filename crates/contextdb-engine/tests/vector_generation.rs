#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/vector_generation_admission_contract.rs"]
mod vector_generation_admission_contract;
#[path = "vector/vector_generation_authoritative_purge_contract.rs"]
mod vector_generation_authoritative_purge_contract;
#[path = "vector/vector_generation_authoritative_purge_replacement_contract.rs"]
mod vector_generation_authoritative_purge_replacement_contract;
#[path = "vector/vector_generation_ddl_cleanup_contract.rs"]
mod vector_generation_ddl_cleanup_contract;
#[path = "vector/vector_generation_delta_admission_contract.rs"]
mod vector_generation_delta_admission_contract;
#[path = "vector/vector_generation_quarantine_compaction_lock_contract.rs"]
mod vector_generation_quarantine_compaction_lock_contract;
