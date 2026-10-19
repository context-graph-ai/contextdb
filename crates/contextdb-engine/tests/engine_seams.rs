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
#[path = "engine_seams/a_historical_index_read_costs_its_rows_and_dead_groups.rs"]
mod a_historical_index_read_costs_its_rows_and_dead_groups;
#[path = "engine_seams/a_historical_read_costs_the_same_per_returned_row_as_rows_and_history_grow.rs"]
mod a_historical_read_costs_the_same_per_returned_row_as_rows_and_history_grow;
#[path = "engine_seams/a_large_journal_cleanup_is_charged_to_the_writer.rs"]
mod a_large_journal_cleanup_is_charged_to_the_writer;
#[path = "engine_seams/a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics.rs"]
mod a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics;
#[path = "engine_seams/aggregate_reads_complete_across_bounded_pages.rs"]
mod aggregate_reads_complete_across_bounded_pages;
#[path = "engine_seams/an_arriving_count_declaration_meets_the_same_door_a_local_one_does.rs"]
mod an_arriving_count_declaration_meets_the_same_door_a_local_one_does;
#[path = "engine_seams/an_import_tail_charges_each_decoded_vector_once.rs"]
mod an_import_tail_charges_each_decoded_vector_once;
#[path = "engine_seams/an_import_tail_larger_than_the_reader_default_completes.rs"]
mod an_import_tail_larger_than_the_reader_default_completes;
#[path = "engine_seams/an_ownerless_writer_decode_is_charged_to_the_declared_writer_limit.rs"]
mod an_ownerless_writer_decode_is_charged_to_the_declared_writer_limit;
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
#[path = "engine_seams/historical_index_postings_must_cover_their_row_versions.rs"]
mod historical_index_postings_must_cover_their_row_versions;
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
#[path = "engine_seams/storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown.rs"]
mod storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown;
#[path = "engine_seams/storage_compaction_online.rs"]
mod storage_compaction_online;
#[cfg(target_os = "linux")]
#[path = "engine_seams/the_custody_tenant_mirror_is_charged_to_its_writer.rs"]
mod the_custody_tenant_mirror_is_charged_to_its_writer;
#[path = "engine_seams/uncapped_graph_read_charges_the_frontier_it_resolves.rs"]
mod uncapped_graph_read_charges_the_frontier_it_resolves;
#[cfg(unix)]
#[path = "engine_seams/writable_open_of_a_corrupt_file_names_the_next_step.rs"]
mod writable_open_of_a_corrupt_file_names_the_next_step;
#[path = "engine_seams/writer_checkpoint_receipts_follow_commits_and_close.rs"]
mod writer_checkpoint_receipts_follow_commits_and_close;

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

#[path = "engine_seams/a_custody_commit_validates_the_last_write_of_each_key.rs"]
mod a_custody_commit_validates_the_last_write_of_each_key;
#[path = "engine_seams/a_damaged_delivered_terminal_is_refused_never_excluded.rs"]
mod a_damaged_delivered_terminal_is_refused_never_excluded;
#[cfg(target_os = "linux")]
#[path = "engine_seams/a_long_decoder_error_is_bounded_before_it_is_built.rs"]
mod a_long_decoder_error_is_bounded_before_it_is_built;
#[path = "engine_seams/a_malformed_custody_record_refuses_with_the_custody_codec_diagnostic.rs"]
mod a_malformed_custody_record_refuses_with_the_custody_codec_diagnostic;
#[path = "engine_seams/a_partial_closure_never_certifies_custody_evidence.rs"]
mod a_partial_closure_never_certifies_custody_evidence;
#[path = "engine_seams/an_incomplete_custody_index_refuses_without_loading_the_journal.rs"]
mod an_incomplete_custody_index_refuses_without_loading_the_journal;
#[path = "engine_seams/buffer_pool_share_evicts_and_recharges_under_the_writer_budget.rs"]
mod buffer_pool_share_evicts_and_recharges_under_the_writer_budget;
#[cfg(target_os = "linux")]
#[path = "engine_seams/custody_admissions_are_bracketed_and_refusable.rs"]
mod custody_admissions_are_bracketed_and_refusable;
#[path = "engine_seams/custody_commit_validation_admits_before_it_allocates.rs"]
mod custody_commit_validation_admits_before_it_allocates;
#[cfg(target_os = "linux")]
#[path = "engine_seams/custody_decoding_charges_what_it_allocates.rs"]
mod custody_decoding_charges_what_it_allocates;
#[path = "engine_seams/custody_durable_selection_matches_the_resident_journal.rs"]
mod custody_durable_selection_matches_the_resident_journal;
#[path = "engine_seams/custody_fixture.rs"]
mod custody_fixture;
#[cfg(target_os = "linux")]
#[path = "engine_seams/custody_history_commits_do_not_hydrate_the_delivery_journal.rs"]
mod custody_history_commits_do_not_hydrate_the_delivery_journal;
#[path = "engine_seams/custody_long_key_fixture.rs"]
mod custody_long_key_fixture;
#[cfg(target_os = "linux")]
#[path = "engine_seams/custody_reads_admit_every_buffer_before_allocating.rs"]
mod custody_reads_admit_every_buffer_before_allocating;
#[path = "engine_seams/custody_reads_stay_on_the_operations_captured_root.rs"]
mod custody_reads_stay_on_the_operations_captured_root;
#[path = "engine_seams/delivery_status_answers_exactly_on_an_admitted_custody_fixture.rs"]
mod delivery_status_answers_exactly_on_an_admitted_custody_fixture;
#[path = "engine_seams/durable_custody_reads_refuse_missing_or_tampered_companions.rs"]
mod durable_custody_reads_refuse_missing_or_tampered_companions;
#[path = "engine_seams/durable_reads_validate_the_whole_evidence_closure.rs"]
mod durable_reads_validate_the_whole_evidence_closure;
#[path = "engine_seams/history_sync_and_commit_cursors_reload_without_full_directories.rs"]
mod history_sync_and_commit_cursors_reload_without_full_directories;
#[path = "engine_seams/identity_preparation_refusal_leaves_the_store_unchanged.rs"]
mod identity_preparation_refusal_leaves_the_store_unchanged;
#[path = "engine_seams/native_delivery_is_served_after_transaction_repair_and_reopen.rs"]
mod native_delivery_is_served_after_transaction_repair_and_reopen;
#[path = "engine_seams/paged_initial_replacement_and_reclamation_progress_under_writes.rs"]
mod paged_initial_replacement_and_reclamation_progress_under_writes;
#[path = "engine_seams/pending_delivery_pages_resume_without_gaps.rs"]
mod pending_delivery_pages_resume_without_gaps;
#[path = "engine_seams/pending_delivery_reads_only_the_pending_tail.rs"]
mod pending_delivery_reads_only_the_pending_tail;
#[path = "engine_seams/pending_maintenance_stays_flat_as_delivered_history_grows.rs"]
mod pending_maintenance_stays_flat_as_delivered_history_grows;
#[path = "engine_seams/retired_custody_keys_stay_funded_until_the_write_set_drops.rs"]
mod retired_custody_keys_stay_funded_until_the_write_set_drops;
#[cfg(target_os = "linux")]
#[path = "engine_seams/the_closure_memo_frees_its_entries_before_refunding_them.rs"]
mod the_closure_memo_frees_its_entries_before_refunding_them;
#[path = "engine_seams/unpartitioned_vector_membership_pages_reload_at_constant_residency.rs"]
mod unpartitioned_vector_membership_pages_reload_at_constant_residency;
#[path = "engine_seams/version_pruning_and_compaction_resume_finite_safe_batches.rs"]
mod version_pruning_and_compaction_resume_finite_safe_batches;
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
#[path = "engine_seams/paged_constraints_preserve_staging_and_competing_commit_conflicts.rs"]
mod paged_constraints_preserve_staging_and_competing_commit_conflicts;
#[path = "engine_seams/writable_open_never_touches_history_families.rs"]
mod writable_open_never_touches_history_families;
#[path = "engine_seams/writer_reader_root_survives_commit_eviction_and_cleanup.rs"]
mod writer_reader_root_survives_commit_eviction_and_cleanup;

#[path = "engine_seams/resident_history_support.rs"]
mod resident_history_support;

#[path = "engine_seams/retention_expiry_and_prune_examine_fixed_work_as_unrelated_edges_and_rows_grow.rs"]
mod retention_expiry_and_prune_examine_fixed_work_as_unrelated_edges_and_rows_grow;

#[path = "engine_seams/bounded_graph_where_charge_is_independent_of_rejected_candidates.rs"]
mod bounded_graph_where_charge_is_independent_of_rejected_candidates;

#[path = "engine_seams/outbound_sync_refuses_typed_when_provenance_sidecar_read_fails.rs"]
mod outbound_sync_refuses_typed_when_provenance_sidecar_read_fails;

#[path = "engine_seams/incremental_sync_charges_fixed_memory_as_unrelated_ddl_history_grows.rs"]
mod incremental_sync_charges_fixed_memory_as_unrelated_ddl_history_grows;

#[path = "engine_seams/bounded_graph_where_metadata_authorization_charges_request_limits.rs"]
mod bounded_graph_where_metadata_authorization_charges_request_limits;

#[path = "consumer_declared_count_and_pending_seek_contract.rs"]
mod consumer_declared_count_and_pending_seek_contract;

#[path = "grouped_session_top_n_contract.rs"]
mod grouped_session_top_n_contract;

#[path = "engine_seams/writer_tail_reuse_is_bounded_across_physical_opens.rs"]
mod writer_tail_reuse_is_bounded_across_physical_opens;

#[path = "engine_seams/a_replacement_candidate_counts_exactly_what_it_absorbed.rs"]
mod a_replacement_candidate_counts_exactly_what_it_absorbed;
#[path = "engine_seams/a_vector_search_decodes_under_its_request_owner.rs"]
mod a_vector_search_decodes_under_its_request_owner;
#[path = "engine_seams/import_tail_survives_restart_without_history_reconstruction.rs"]
mod import_tail_survives_restart_without_history_reconstruction;
#[path = "engine_seams/one_import_commit_larger_than_the_cohort_share_is_absorbed.rs"]
mod one_import_commit_larger_than_the_cohort_share_is_absorbed;
#[path = "engine_seams/opening_a_store_charges_the_reader_the_same_however_long_its_history.rs"]
mod opening_a_store_charges_the_reader_the_same_however_long_its_history;
#[path = "engine_seams/opening_a_store_does_a_bounded_vector_journal_cleanup.rs"]
mod opening_a_store_does_a_bounded_vector_journal_cleanup;
#[path = "engine_seams/opening_a_store_holds_a_bounded_window_of_queued_events.rs"]
mod opening_a_store_holds_a_bounded_window_of_queued_events;
#[path = "engine_seams/opening_a_store_read_only_does_not_grow_with_history.rs"]
mod opening_a_store_read_only_does_not_grow_with_history;
#[path = "engine_seams/reads_and_writes_are_charged_to_their_own_declared_limits.rs"]
mod reads_and_writes_are_charged_to_their_own_declared_limits;
#[path = "engine_seams/trigger_audit_history_is_charged_once_to_the_declared_reader.rs"]
mod trigger_audit_history_is_charged_once_to_the_declared_reader;
#[path = "engine_seams/vector_journal_cleanup_lets_writers_commit_between_steps.rs"]
mod vector_journal_cleanup_lets_writers_commit_between_steps;

#[path = "engine_seams/reads_are_answered_while_a_maintenance_commit_holds_the_commit_gate.rs"]
mod reads_are_answered_while_a_maintenance_commit_holds_the_commit_gate;

#[path = "engine_seams/a_reads_erasure_check_does_fixed_work_however_much_history_there_is.rs"]
mod a_reads_erasure_check_does_fixed_work_however_much_history_there_is;
