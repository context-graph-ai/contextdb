#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/vector_policy_declaration_contract.rs"]
mod vector_policy_declaration_contract;
#[path = "vector/vector_policy_mixed_version_contract.rs"]
mod vector_policy_mixed_version_contract;
#[path = "vector/vector_policy_resolver_contract.rs"]
mod vector_policy_resolver_contract;
#[path = "vector/vector_policy_revision_publication_contract.rs"]
mod vector_policy_revision_publication_contract;
