#![allow(clippy::assertions_on_constants)]

#[path = "acceptance/auto_stamped_txid.rs"]
mod auto_stamped_txid;
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
#[path = "acceptance/db_lock_visibility.rs"]
mod db_lock_visibility;
#[path = "acceptance/deployment.rs"]
mod deployment;
#[path = "acceptance/dev_experience.rs"]
mod dev_experience;
#[path = "acceptance/disk_budget.rs"]
mod disk_budget;
#[path = "acceptance/docs_immutable_column.rs"]
mod docs_immutable_column;
#[path = "acceptance/docs_query_language_auto_indexes.rs"]
mod docs_query_language_auto_indexes;
#[path = "acceptance/embedding.rs"]
mod embedding;
#[path = "acceptance/engine_cron.rs"]
mod engine_cron;
#[path = "acceptance/event_bus.rs"]
mod event_bus;
#[path = "acceptance/handle_scope_constraint.rs"]
mod handle_scope_constraint;
#[path = "acceptance/infra_failures.rs"]
mod infra_failures;
#[path = "acceptance/long_running.rs"]
mod long_running;
#[path = "acceptance/memory_accounting.rs"]
mod memory_accounting;
#[path = "acceptance/multi_edge.rs"]
mod multi_edge;
#[path = "acceptance/multi_table_atomic_visibility.rs"]
mod multi_table_atomic_visibility;
#[path = "acceptance/persistence.rs"]
mod persistence;
#[path = "acceptance/precommit_context_guard.rs"]
mod precommit_context_guard;
#[path = "acceptance/principal_scoped_handle.rs"]
mod principal_scoped_handle;
#[path = "acceptance/query_surface.rs"]
mod query_surface;
#[path = "acceptance/schema_evolution.rs"]
mod schema_evolution;
#[path = "acceptance/schema_lifecycle.rs"]
mod schema_lifecycle;
#[path = "acceptance/sync.rs"]
mod sync;
#[path = "acceptance/vector_reindex_ordering.rs"]
mod vector_reindex_ordering;
