#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "fixtures/retained_query_rows.rs"]
mod retained_query_rows;
#[path = "vector/vector_generation_authoritative_purge_contract.rs"]
mod vector_generation_authoritative_purge_contract;
#[path = "vector/vector_generation_authoritative_purge_replacement_contract.rs"]
mod vector_generation_authoritative_purge_replacement_contract;
#[path = "vector/vector_generation_ddl_cleanup_contract.rs"]
mod vector_generation_ddl_cleanup_contract;
