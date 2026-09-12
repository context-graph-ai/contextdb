#[path = "engine_seams/aggregate_reads_complete_across_bounded_pages.rs"]
mod aggregate_reads_complete_across_bounded_pages;
#[path = "engine_seams/concurrent_statements_keep_their_own_rows_examined.rs"]
mod concurrent_statements_keep_their_own_rows_examined;
#[cfg(unix)]
#[path = "engine_seams/corrupt_store_refused_at_open.rs"]
mod corrupt_store_refused_at_open;
#[path = "engine_seams/database_identity_survives_address_reuse.rs"]
mod database_identity_survives_address_reuse;
#[path = "engine_seams/database_open_options_contract.rs"]
mod database_open_options_contract;
#[path = "engine_seams/edge_property_write_contract.rs"]
mod edge_property_write_contract;
#[path = "engine_seams/graph_cursor_survives_genuine_shrink.rs"]
mod graph_cursor_survives_genuine_shrink;
#[path = "engine_seams/in_list_cursor_pending_runs_are_charged.rs"]
mod in_list_cursor_pending_runs_are_charged;
#[path = "engine_seams/indexed_read_in_a_transaction_traces_its_real_work.rs"]
mod indexed_read_in_a_transaction_traces_its_real_work;
#[path = "engine_seams/memory_accounting_lifecycle_contract.rs"]
mod memory_accounting_lifecycle_contract;
#[path = "engine_seams/migration_source_determines_publication.rs"]
mod migration_source_determines_publication;
#[path = "engine_seams/per_index_independent_progress_tests.rs"]
mod per_index_independent_progress_tests;
#[path = "engine_seams/pruning_report_names_reader_deferred_rows.rs"]
mod pruning_report_names_reader_deferred_rows;
#[path = "engine_seams/rank_policy_joined_row_sees_staged_access_changes.rs"]
mod rank_policy_joined_row_sees_staged_access_changes;
#[path = "engine_seams/retention_respects_edge_visibility.rs"]
mod retention_respects_edge_visibility;
#[path = "engine_seams/snapshot_visible_posting_membership_contract.rs"]
mod snapshot_visible_posting_membership_contract;
#[path = "engine_seams/storage_compaction_online.rs"]
mod storage_compaction_online;
#[path = "engine_seams/uncapped_graph_read_charges_the_frontier_it_resolves.rs"]
mod uncapped_graph_read_charges_the_frontier_it_resolves;
#[cfg(unix)]
#[path = "engine_seams/writable_open_of_a_corrupt_file_names_the_next_step.rs"]
mod writable_open_of_a_corrupt_file_names_the_next_step;
