#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/vector_lazy_consumer_restart_contract.rs"]
mod vector_lazy_consumer_restart_contract;
#[path = "vector/vector_lazy_raw_residency_contract.rs"]
mod vector_lazy_raw_residency_contract;
#[path = "vector/vector_lazy_replay_budget_contract.rs"]
mod vector_lazy_replay_budget_contract;
#[path = "vector/vector_lazy_restart_residency_contract.rs"]
mod vector_lazy_restart_residency_contract;
