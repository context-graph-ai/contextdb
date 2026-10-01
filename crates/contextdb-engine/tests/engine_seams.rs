/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
#[allow(dead_code)]
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[path = "engine_seams/a_count_a_constraint_already_bounds_is_not_maintained.rs"]
mod a_count_a_constraint_already_bounds_is_not_maintained;
#[path = "engine_seams/a_declared_count_is_the_only_count_an_append_writes.rs"]
mod a_declared_count_is_the_only_count_an_append_writes;
#[path = "engine_seams/a_held_savepoint_makes_every_commit_list_its_allocated_pages.rs"]
mod a_held_savepoint_makes_every_commit_list_its_allocated_pages;
#[path = "engine_seams/aggregate_reads_complete_across_bounded_pages.rs"]
mod aggregate_reads_complete_across_bounded_pages;
#[path = "engine_seams/an_arriving_count_declaration_meets_the_same_door_a_local_one_does.rs"]
mod an_arriving_count_declaration_meets_the_same_door_a_local_one_does;
#[path = "engine_seams/an_undeclared_prefix_count_still_answers_exactly.rs"]
mod an_undeclared_prefix_count_still_answers_exactly;
#[path = "engine_seams/append_measurement_costs_a_build_that_did_not_ask_for_it_nothing.rs"]
mod append_measurement_costs_a_build_that_did_not_ask_for_it_nothing;
#[path = "engine_seams/concurrent_statements_keep_their_own_rows_examined.rs"]
mod concurrent_statements_keep_their_own_rows_examined;
#[cfg(unix)]
#[path = "engine_seams/corrupt_store_refused_at_open.rs"]
mod corrupt_store_refused_at_open;
#[path = "engine_seams/database_identity_survives_address_reuse.rs"]
mod database_identity_survives_address_reuse;
#[path = "engine_seams/database_open_options_contract.rs"]
mod database_open_options_contract;
#[path = "engine_seams/declaring_a_count_on_a_table_with_rows_is_atomic_and_charged.rs"]
mod declaring_a_count_on_a_table_with_rows_is_atomic_and_charged;
#[path = "engine_seams/each_published_digest_covers_exactly_its_own_content.rs"]
mod each_published_digest_covers_exactly_its_own_content;
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
#[path = "engine_seams/one_event_append_cost_by_stream_depth.rs"]
mod one_event_append_cost_by_stream_depth;
#[path = "engine_seams/one_event_append_work_by_family.rs"]
mod one_event_append_work_by_family;
#[path = "engine_seams/one_vector_append_does_not_rescan_the_directory.rs"]
mod one_vector_append_does_not_rescan_the_directory;
#[path = "engine_seams/per_index_independent_progress_tests.rs"]
mod per_index_independent_progress_tests;
#[path = "engine_seams/pruning_report_names_reader_deferred_rows.rs"]
mod pruning_report_names_reader_deferred_rows;
#[path = "engine_seams/rank_policy_joined_row_sees_staged_access_changes.rs"]
mod rank_policy_joined_row_sees_staged_access_changes;
#[path = "engine_seams/recover_uncommitted_commit_index_tail.rs"]
mod recover_uncommitted_commit_index_tail;
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

#[path = "engine_seams/what_each_declared_index_costs_one_event_append.rs"]
mod what_each_declared_index_costs_one_event_append;
#[path = "engine_seams/what_holds_a_one_kilobyte_memory_limit.rs"]
mod what_holds_a_one_kilobyte_memory_limit;
#[path = "engine_seams/where_an_appends_chunk_rewrites_sit_in_the_proof_tree.rs"]
mod where_an_appends_chunk_rewrites_sit_in_the_proof_tree;
#[path = "engine_seams/writable_row_paging_contract.rs"]
mod writable_row_paging_contract;
#[cfg(target_os = "linux")]
#[path = "engine_seams/writer_memory_per_stored_passage.rs"]
mod writer_memory_per_stored_passage;
#[path = "engine_seams/writer_recycle_baseline_cost.rs"]
mod writer_recycle_baseline_cost;

#[cfg(unix)]
#[path = "engine_seams/writer_file_ownership_survives_recycle.rs"]
mod writer_file_ownership_survives_recycle;

#[path = "engine_seams/buffer_pool_share_evicts_and_recharges_under_the_writer_budget.rs"]
mod buffer_pool_share_evicts_and_recharges_under_the_writer_budget;
#[path = "engine_seams/history_sync_and_commit_cursors_reload_without_full_directories.rs"]
mod history_sync_and_commit_cursors_reload_without_full_directories;
#[cfg(target_os = "linux")]
#[path = "engine_seams/writer_reopen_and_doubling_keep_a_fixed_resident_envelope.rs"]
mod writer_reopen_and_doubling_keep_a_fixed_resident_envelope;

#[path = "engine_seams/aggregate_auto_threshold_precedes_partition_cost_and_keeps_scope.rs"]
mod aggregate_auto_threshold_precedes_partition_cost_and_keeps_scope;
#[path = "engine_seams/crash_cutpoints_recover_one_complete_row_index_vector_log_commit.rs"]
mod crash_cutpoints_recover_one_complete_row_index_vector_log_commit;
#[path = "engine_seams/first_search_after_writer_reopen_uses_saved_pages_without_build.rs"]
mod first_search_after_writer_reopen_uses_saved_pages_without_build;
#[path = "engine_seams/graph_delete_seeks_only_its_edge_identity.rs"]
mod graph_delete_seeks_only_its_edge_identity;

#[path = "engine_seams/resident_history_support.rs"]
mod resident_history_support;
