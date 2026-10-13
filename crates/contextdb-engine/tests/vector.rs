#[path = "vector/a_damaged_index_refuses_only_reads_that_consume_its_postings.rs"]
mod a_damaged_index_refuses_only_reads_that_consume_its_postings;
#[path = "vector/a_damaged_saved_graph_page_refuses_only_its_route.rs"]
mod a_damaged_saved_graph_page_refuses_only_its_route;
#[path = "vector/a_declared_breadth_gets_exactly_its_approved_budget.rs"]
mod a_declared_breadth_gets_exactly_its_approved_budget;
#[path = "vector/a_declared_graph_degree_loads_when_its_memory_fits.rs"]
mod a_declared_graph_degree_loads_when_its_memory_fits;
#[path = "vector/a_declared_serving_cache_is_honoured.rs"]
mod a_declared_serving_cache_is_honoured;
#[path = "vector/a_filtered_scope_is_charged_for_its_candidates.rs"]
mod a_filtered_scope_is_charged_for_its_candidates;
#[path = "vector/a_find_answers_a_full_page_when_a_candidate_is_withdrawn.rs"]
mod a_find_answers_a_full_page_when_a_candidate_is_withdrawn;
#[path = "vector/a_find_decodes_only_the_rows_it_returns.rs"]
mod a_find_decodes_only_the_rows_it_returns;
#[path = "vector/a_find_locks_its_serving_cache_once_per_page.rs"]
mod a_find_locks_its_serving_cache_once_per_page;
#[cfg(unix)]
#[path = "vector/a_find_names_only_the_pairs_it_asks_for.rs"]
mod a_find_names_only_the_pairs_it_asks_for;
#[path = "vector/a_find_over_many_contexts_spends_one_search_budget.rs"]
mod a_find_over_many_contexts_spends_one_search_budget;
#[path = "vector/a_graph_search_stays_inside_its_serving_cache.rs"]
mod a_graph_search_stays_inside_its_serving_cache;
#[path = "vector/a_replaced_vector_cannot_end_a_tied_frontier.rs"]
mod a_replaced_vector_cannot_end_a_tied_frontier;
#[cfg(unix)]
#[path = "vector/a_replaced_vector_tied_frontier_walk_diagnostic.rs"]
mod a_replaced_vector_tied_frontier_walk_diagnostic;
#[cfg(unix)]
#[path = "vector/a_saved_graph_load_checks_its_bytes_once.rs"]
mod a_saved_graph_load_checks_its_bytes_once;
#[cfg(unix)]
#[path = "vector/a_saved_graph_search_reports_its_distance_work.rs"]
mod a_saved_graph_search_reports_its_distance_work;
#[path = "vector/a_store_reopens_after_every_vector_maintenance_commit.rs"]
mod a_store_reopens_after_every_vector_maintenance_commit;
#[path = "vector/a_tied_frontier_cannot_hide_a_smaller_row.rs"]
mod a_tied_frontier_cannot_hide_a_smaller_row;
#[cfg(unix)]
#[path = "vector/a_wide_find_admits_every_partition_when_serving_pages_give_way.rs"]
mod a_wide_find_admits_every_partition_when_serving_pages_give_way;
#[cfg(unix)]
#[path = "vector/a_wide_find_is_admitted_at_what_its_loads_hold.rs"]
mod a_wide_find_is_admitted_at_what_its_loads_hold;
#[cfg(unix)]
#[path = "vector/a_wide_find_keeps_its_entry_pages_warm.rs"]
mod a_wide_find_keeps_its_entry_pages_warm;
#[path = "vector/an_exact_search_authenticates_pages_not_every_vector.rs"]
mod an_exact_search_authenticates_pages_not_every_vector;
#[path = "vector/an_exhaustive_find_streams_bodies_it_cannot_hold.rs"]
mod an_exhaustive_find_streams_bodies_it_cannot_hold;
#[path = "vector/auto_chooses_the_cheaper_route_for_each_partition.rs"]
mod auto_chooses_the_cheaper_route_for_each_partition;
#[path = "vector/auto_decides_the_route_once_for_the_whole_scope.rs"]
mod auto_decides_the_route_once_for_the_whole_scope;
#[path = "vector/auto_pays_what_indexed_pays.rs"]
mod auto_pays_what_indexed_pays;
#[cfg(feature = "nightly-tests")]
#[path = "vector/continuous_writers_hold_throughput_and_reader_latency.rs"]
mod continuous_writers_hold_throughput_and_reader_latency;
#[cfg(unix)]
#[path = "vector/each_layer_a_wide_find_loads_keeps_its_share.rs"]
mod each_layer_a_wide_find_loads_keeps_its_share;
#[path = "vector/equal_distance_rows_keep_canonical_order_across_contexts.rs"]
mod equal_distance_rows_keep_canonical_order_across_contexts;
#[path = "vector/first_filtered_search_in_a_fresh_reader_uses_the_saved_graph.rs"]
mod first_filtered_search_in_a_fresh_reader_uses_the_saved_graph;
#[path = "vector/graph_build_distance_evaluations_stay_under_the_ratchet.rs"]
mod graph_build_distance_evaluations_stay_under_the_ratchet;
#[path = "vector/graph_route_profile_quality.rs"]
mod graph_route_profile_quality;
#[path = "vector/indexed_route_does_less_work_than_exact.rs"]
mod indexed_route_does_less_work_than_exact;
#[path = "vector/single_context_search_radius_work_diagnostic.rs"]
mod single_context_search_radius_work_diagnostic;
#[path = "fixtures/maintained_search/mod.rs"]
mod maintained_search;
#[path = "vector/multi_partition_union_recall_beam_diagnostic.rs"]
mod multi_partition_union_recall_beam_diagnostic;
#[path = "vector/multi_partition_union_recovers_nearest_rows.rs"]
mod multi_partition_union_recovers_nearest_rows;
#[path = "vector/named_vector_planner_tests.rs"]
mod named_vector_planner_tests;
#[path = "vector/one_search_covers_every_layer_on_both_routes.rs"]
mod one_search_covers_every_layer_on_both_routes;
#[path = "vector/public_row_apis_honor_quantized_columns.rs"]
mod public_row_apis_honor_quantized_columns;
#[path = "vector/quantized_vector_read_agreement.rs"]
mod quantized_vector_read_agreement;
#[path = "vector/retained_indexed_rows.rs"]
mod retained_indexed_rows;
#[path = "vector/row_delete_and_guards_honor_vector_columns.rs"]
mod row_delete_and_guards_honor_vector_columns;
#[path = "vector/the_owner_doors_run_one_search_over_every_layer.rs"]
mod the_owner_doors_run_one_search_over_every_layer;
#[path = "vector/vector_delete_only_journal_progress_contract.rs"]
mod vector_delete_only_journal_progress_contract;
#[path = "vector/vector_exclusion_persistence_tests.rs"]
mod vector_exclusion_persistence_tests;
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
#[path = "vector/vector_held_out_native_reference.rs"]
mod vector_held_out_native_reference;
#[path = "vector/vector_inspection_explain_truth_contract.rs"]
mod vector_inspection_explain_truth_contract;
#[path = "vector/vector_lazy_consumer_restart_contract.rs"]
mod vector_lazy_consumer_restart_contract;
#[path = "vector/vector_lazy_raw_residency_contract.rs"]
mod vector_lazy_raw_residency_contract;
#[path = "vector/vector_lazy_replay_budget_contract.rs"]
mod vector_lazy_replay_budget_contract;
#[path = "vector/vector_lazy_restart_residency_contract.rs"]
mod vector_lazy_restart_residency_contract;
#[path = "vector/vector_maintained_lifecycle_contract.rs"]
mod vector_maintained_lifecycle_contract;
#[path = "vector/vector_memory_ownership_reconciliation_contract.rs"]
mod vector_memory_ownership_reconciliation_contract;
#[path = "vector/vector_operation_observations.rs"]
mod vector_operation_observations;
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
#[path = "vector/vector_snapshot_frontier_completeness_contract.rs"]
mod vector_snapshot_frontier_completeness_contract;
#[path = "vector/vector_tail_transaction_contract.rs"]
mod vector_tail_transaction_contract;
#[path = "vector/vector_transaction_filter_snapshot_truth_contract.rs"]
mod vector_transaction_filter_snapshot_truth_contract;
#[path = "vector/what_a_declared_search_breadth_buys.rs"]
mod what_a_declared_search_breadth_buys;
#[path = "vector/what_the_approved_budget_does_to_one_large_context.rs"]
mod what_the_approved_budget_does_to_one_large_context;
#[path = "vector/where_one_partition_spends_its_find.rs"]
mod where_one_partition_spends_its_find;
#[path = "vector/writer_residency_attribution.rs"]
mod writer_residency_attribution;

#[cfg(unix)]
#[path = "vector/paged_generation_load_pricing_contract.rs"]
mod paged_generation_load_pricing_contract;
