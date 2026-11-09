#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.
#[path = "vector/a_damaged_index_refuses_only_reads_that_consume_its_postings.rs"]
mod a_damaged_index_refuses_only_reads_that_consume_its_postings;
#[path = "vector/a_damaged_saved_graph_page_refuses_only_its_route.rs"]
mod a_damaged_saved_graph_page_refuses_only_its_route;
#[path = "vector/a_declared_serving_cache_is_honoured.rs"]
mod a_declared_serving_cache_is_honoured;
#[path = "vector/a_filtered_scope_is_charged_for_its_candidates.rs"]
mod a_filtered_scope_is_charged_for_its_candidates;
#[path = "vector/a_find_answers_a_full_page_when_a_candidate_is_withdrawn.rs"]
mod a_find_answers_a_full_page_when_a_candidate_is_withdrawn;
#[path = "vector/a_find_decodes_only_the_rows_it_returns.rs"]
mod a_find_decodes_only_the_rows_it_returns;
#[cfg(unix)]
#[path = "vector/a_find_names_only_the_pairs_it_asks_for.rs"]
mod a_find_names_only_the_pairs_it_asks_for;
#[path = "vector/a_find_over_many_contexts_spends_one_search_budget.rs"]
mod a_find_over_many_contexts_spends_one_search_budget;
#[path = "vector/a_row_checked_find_keeps_recall_and_refusals.rs"]
mod a_row_checked_find_keeps_recall_and_refusals;
#[path = "vector/a_store_reopens_after_every_vector_maintenance_commit.rs"]
mod a_store_reopens_after_every_vector_maintenance_commit;
#[path = "vector/an_exact_search_authenticates_pages_not_every_vector.rs"]
mod an_exact_search_authenticates_pages_not_every_vector;
#[path = "vector/an_exhaustive_find_streams_bodies_it_cannot_hold.rs"]
mod an_exhaustive_find_streams_bodies_it_cannot_hold;
#[path = "vector/equal_distance_rows_keep_canonical_order_across_contexts.rs"]
mod equal_distance_rows_keep_canonical_order_across_contexts;
#[path = "vector/flat_route_below_declared_threshold.rs"]
mod flat_route_below_declared_threshold;
#[path = "vector/indexed_route_does_less_work_than_exact.rs"]
mod indexed_route_does_less_work_than_exact;
#[path = "fixtures/maintained_search/mod.rs"]
mod maintained_search;
#[path = "vector/named_vector_planner_tests.rs"]
mod named_vector_planner_tests;
#[path = "vector/passive_explain_predicts_the_executed_auto_route.rs"]
mod passive_explain_predicts_the_executed_auto_route;
#[path = "vector/public_row_apis_honor_quantized_columns.rs"]
mod public_row_apis_honor_quantized_columns;
#[path = "vector/quantized_vector_read_agreement.rs"]
mod quantized_vector_read_agreement;
#[path = "vector/retained_indexed_rows.rs"]
mod retained_indexed_rows;
#[path = "fixtures/retained_query_rows.rs"]
mod retained_query_rows;
#[path = "vector/row_delete_and_guards_honor_vector_columns.rs"]
mod row_delete_and_guards_honor_vector_columns;
#[path = "vector/the_owner_doors_run_one_search_over_every_layer.rs"]
mod the_owner_doors_run_one_search_over_every_layer;
#[path = "vector/vector_exclusion_persistence_tests.rs"]
mod vector_exclusion_persistence_tests;
#[path = "vector/vector_generation_authoritative_purge_contract.rs"]
mod vector_generation_authoritative_purge_contract;
#[path = "vector/vector_generation_ddl_cleanup_contract.rs"]
mod vector_generation_ddl_cleanup_contract;
#[path = "vector/vector_held_out_native_reference.rs"]
mod vector_held_out_native_reference;
#[path = "vector/vector_inspection_explain_truth_contract.rs"]
mod vector_inspection_explain_truth_contract;
#[path = "vector/vector_lazy_consumer_restart_contract.rs"]
mod vector_lazy_consumer_restart_contract;
#[path = "vector/vector_lazy_raw_residency_contract.rs"]
mod vector_lazy_raw_residency_contract;
#[path = "vector/vector_memory_ownership_reconciliation_contract.rs"]
mod vector_memory_ownership_reconciliation_contract;
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
#[path = "vector/vector_policy_declaration_contract.rs"]
mod vector_policy_declaration_contract;
#[path = "vector/vector_policy_mixed_version_contract.rs"]
mod vector_policy_mixed_version_contract;
#[path = "vector/vector_policy_resolver_contract.rs"]
mod vector_policy_resolver_contract;
#[path = "vector/vector_policy_revision_publication_contract.rs"]
mod vector_policy_revision_publication_contract;
#[path = "vector/vector_rank_policy_exact_audits_the_whole_table.rs"]
mod vector_rank_policy_exact_audits_the_whole_table;
#[path = "vector/vector_received_generation_invalidation_contract.rs"]
mod vector_received_generation_invalidation_contract;
#[path = "vector/vector_resource_maintenance_contract.rs"]
mod vector_resource_maintenance_contract;
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
#[path = "vector/vector_search_answers_quantized_columns_on_every_route.rs"]
mod vector_search_answers_quantized_columns_on_every_route;
#[path = "vector/vector_search_mode_bounded_contract.rs"]
mod vector_search_mode_bounded_contract;
#[path = "vector/vector_serving_merge_contract.rs"]
mod vector_serving_merge_contract;
#[path = "vector/vector_tail_transaction_contract.rs"]
mod vector_tail_transaction_contract;
#[path = "vector/vector_transaction_filter_snapshot_truth_contract.rs"]
mod vector_transaction_filter_snapshot_truth_contract;
#[path = "vector/writer_residency_attribution.rs"]
mod writer_residency_attribution;

#[path = "vector/a_ranked_file_route_read_holds_bounded_memory.rs"]
mod a_ranked_file_route_read_holds_bounded_memory;
#[path = "vector/a_ranked_search_answers_the_same_on_every_read_route.rs"]
mod a_ranked_search_answers_the_same_on_every_read_route;
#[path = "vector/a_search_is_exact_with_no_index_built.rs"]
mod a_search_is_exact_with_no_index_built;
#[path = "vector/a_write_transactions_vector_copies_are_funded_by_the_writer.rs"]
mod a_write_transactions_vector_copies_are_funded_by_the_writer;
