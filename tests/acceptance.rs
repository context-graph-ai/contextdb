#![allow(clippy::assertions_on_constants)]

#[path = "acceptance/cli_ux.rs"]
mod cli_ux;
#[path = "acceptance/common.rs"]
mod common;
#[path = "acceptance/contributing.rs"]
mod contributing;
#[path = "acceptance/cross_platform.rs"]
mod cross_platform;
#[path = "acceptance/data_integrity.rs"]
mod data_integrity;
#[path = "acceptance/deployment.rs"]
mod deployment;
#[path = "acceptance/dev_experience.rs"]
mod dev_experience;
#[path = "acceptance/disk_budget.rs"]
mod disk_budget;
#[path = "acceptance/docs_immutable_column.rs"]
mod docs_immutable_column;
#[path = "acceptance/embedding.rs"]
mod embedding;
#[path = "acceptance/infra_failures.rs"]
mod infra_failures;
#[path = "acceptance/long_running.rs"]
mod long_running;
#[path = "acceptance/memory_accounting.rs"]
mod memory_accounting;
#[path = "acceptance/multi_edge.rs"]
mod multi_edge;
#[path = "acceptance/persistence.rs"]
mod persistence;
#[path = "acceptance/query_surface.rs"]
mod query_surface;
#[path = "acceptance/schema_evolution.rs"]
mod schema_evolution;
#[path = "acceptance/schema_lifecycle.rs"]
mod schema_lifecycle;
#[path = "acceptance/sync.rs"]
mod sync;
