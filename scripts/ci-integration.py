#!/usr/bin/env python3
"""One validated integration inventory and command selection, locally and on Actions."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
FEATURES = 'nightly-tests,test-seams,iroh'
REDB_FEATURED_TARGETS = [
    ('named_root_capture_contract', 'contextdb-internal,test-seams'),
    ('reclaim_step_contract', 'contextdb-internal,test-seams'),
    ('open_table_registry_allocations', 'contextdb-internal,test-seams'),
    ('admission_reads_are_bounded_by_the_format', 'contextdb-internal,test-seams'),
    ('growth_after_compaction_contract', 'contextdb-internal'),
    ('cache_residency', 'contextdb-internal'),
    ('page_window_contract', 'contextdb-internal,test-seams'),
    ('startup_settle_releases_the_replaced_cache_index', 'contextdb-internal,test-seams'),
    ('allocator_residency_contract', 'contextdb-internal,test-seams'),
]
OPTIONAL = {'workspace', 'redb', 'production', 'production-release'}
KINDS = {'engine-tests', 'release-acceptance', 'examples', 'doctests'} | OPTIONAL
ACCEPTANCE_TARGETS = ('acceptance', 'acceptance_long_running', 'acceptance_sync')
# These complete expensive cases must remain independently scheduled.
SCHEMA_REPLAY_SLICES = {
    'engine-family-9': ('schema replay contracts', 'custody_schema_replay_is_proportional_to_affected_tables'),
    'schema-replay-work-limited-to-affected-tables': ('schema replay / work limited to affected tables', 'custody_schema_replay_work_limited_to_affected_tables'),
    'schema-replay-identical-schema-does-no-artifact-work': ('schema replay / identical schema avoids rebuilding data', 'custody_schema_replay_identical_schema_does_no_artifact_work'),
}

# Keep replication and schema transactions in separate required jobs.
SYNC_SLICES = {
    'engine-family-5': ('replication, receipts and policy', 'sync'),
    'sync-schema-transactions': ('schema replication, atomic publish and oversized transport', 'sync_schema_transactions'),
}

# Local proposal: normal correctness and original full qualification are distinct.
# The canonical manifest always describes the full inventory; routing is a view.
FULL_SCHEMA = {value[1] for value in SCHEMA_REPLAY_SLICES.values()}
FULL_ONLY_TARGETS = {'seed_commit_memory'}
OVERSIZED = ('a_received_schema_transaction_never_publishes_a_prefix::'
             'schema_migration_chunked_transport::'
             'oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry')


SYNC_ROUTING_FINGERPRINTS = {'crates/contextdb-engine/tests/sync_schema_transactions.rs': '2de1becdc822e29dc11ef5a28f15420c1a0ae8eab7984fbd2729553666d88052',
 'crates/contextdb-engine/tests/sync/a_received_schema_transaction_never_publishes_a_prefix.rs': '1967918031653bffe25c419a480d97375c36fb0c1a1efe9a6b39e8ae242fd208',
 'crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs': '7b0bf77da9c28e464134bff453f0168b9a09bca5ad520a49a967424cbbcba891'}


# Reviewed normal-only partition of the unchanged three-case native inventory.
LONG_RUNNING_CASES = ('long_running::f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes', 'long_running::f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge', 'long_running::f27_incremental_pull_after_initial_sync')
LONG_RUNNING_PARTITIONS = {'acceptance-long-running-transfer-test': ('long_running::f25_edge_offline_accumulates_ten_thousand_rows_reconnects_and_pushes', 'long_running::f26_large_pull_ten_thousand_rows_from_server_to_fresh_edge'), 'acceptance-long-running-test': ('long_running::f27_incremental_pull_after_initial_sync',)}
LONG_RUNNING_FINGERPRINTS = {'tests/acceptance_long_running.rs': 'c070eb6fe18b5871b42419f33ed2c78c8e938c421384e7f1bcc9f35641b6e04d', 'tests/acceptance/long_running.rs': '619337ba5703466555d071982ddd2bd081eabcb3c27b4fc74e63739a0b6ed438', 'tests/acceptance/common.rs': 'a69470614d22a2823f3a5317f54f084db8ff8366ce0041431d6a0448fe6ee72e', 'tests/support/binary_path_helpers.rs': '05bd95280e47f5141b2a04bec3bcf5ca934c4cc642597cc46e16496d9461cb0c'}

def long_running_contract(root=ROOT):
    for path, expected in LONG_RUNNING_FINGERPRINTS.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
            raise ValueError('long-running case inventory changed; review exact partition: ' + path)


def long_running_counts(entries, log, row_id):
    expected = LONG_RUNNING_PARTITIONS[row_id]
    matches = [e for e in entries if e.get('target') == '../../tests/acceptance_long_running.rs']
    text = re.sub(r'\x1b\[[0-9;]*m', '', log.read_text())
    names = re.findall(r'^test (\S+) \.\.\. ok$', text, re.M)
    if (len(entries) != 1 or len(matches) != 1 or sorted(names) != sorted(expected)
            or tuple(matches[0].get(k) for k in ('passed', 'failed', 'ignored', 'measured', 'filtered_out'))
            != (len(expected), 0, 0, 0, 3 - len(expected))):
        raise ValueError('long-running receipt must prove exact selected cases and all three counts')
    matches[0]['passed_cases'] = names


# Exact ignored-test declarations; none is a passing test result.
QUARANTINE_EXCEPTIONS = {
    ('crates/contextdb-engine/src/database/consolidation_metadata_contract_tests.rs', 'consolidation_publication_with_saved_indexes_preserves_work_and_rechecks_obsolete_failure_marks'): 'requires maintained vector indexes for replacement and obsolete failure marks',
    ('crates/contextdb-engine/src/database/consolidation_metadata_contract_tests.rs', 'consolidation_crash_cuts_with_saved_indexes_recover_unchanged_data'): 'requires saved maintained vector indexes for graph preservation',
    ('crates/contextdb-engine/src/database/consolidation_metadata_contract_tests.rs', 'consolidation_failure_with_saved_indexes_keeps_one_authoritative_declaration'): 'requires saved maintained vector indexes for graph preservation',
    ('crates/contextdb-engine/src/database/consolidation_metadata_contract_tests.rs', 'refused_consolidation_with_saved_indexes_keeps_old_declaration_history_and_graph_authoritative'): 'requires saved maintained vector indexes for graph preservation',
    ('crates/contextdb-engine/src/database/consolidation_metadata_contract_tests.rs', 'consolidation_transaction_rollback_with_saved_indexes_does_not_publish_local_autocommit_metadata'): 'requires saved maintained vector indexes for the indexed rollback cases',
    ('crates/contextdb-engine/src/database/currency_cleanup_atomicity_tests.rs', 'currency_cleanup_reclaims_eligible_generations_at_its_durable_boundary'): 'requires maintained vector generations before testing their reclamation',
    ('crates/contextdb-engine/src/executor.rs', 'eager_and_pull_indexed_vector_routes_share_restricted_refusal_and_recovery'): 'requires maintained vector indexes for indexed refusal and recovery',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'drop_table_waits_for_inflight_vector_builds_then_removes_all_indexes'): 'the vector index route is switched off in this release',

    ('crates/contextdb-engine/tests/engine_seams/outbound_selection_regressions.rs', 'an_excluded_body_larger_than_the_push_budget_does_not_charge_the_push'): 'post-extraction push consumers reread excluded bodies',
    ('crates/contextdb-cli/src/json_output.rs', 'maintenance_json_retains_the_failed_budget_and_real_recovery_command'): 'the vector index route is switched off in this release',
    ('crates/contextdb-cli/tests/read_cli_journeys_ordinary_results.rs', 'maintenance_publishes_one_indexed_route_to_the_writer_and_its_live_reader'): 'the vector index route is switched off in this release',
    ('crates/contextdb-cli/tests/read_cli_journeys_ordinary_results.rs', 'a_closed_writer_leaves_the_plain_read_only_door_the_same_indexed_and_auto_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-cli/tests/read_cli_journeys_ordinary_results.rs', 'a_context_grouped_nearest_neighbour_answers_its_own_tuple_not_a_closer_foreign_one'): 'the vector index route is switched off in this release',  # CDB-284
    ('crates/contextdb-cli/tests/read_cli_journeys_ordinary_results.rs', 'maintenance_run_and_status_publish_their_text_receipts_through_the_real_cli'): 'the vector index route is switched off in this release',  # CDB-284
    ('crates/contextdb-engine/src/database/event_bus/received_purge_discard_tests.rs', 'received_purge_and_public_discard_finish_owned'): 'owned host cannot apply pulled rows',
    ('crates/contextdb-engine/src/database/event_bus/received_purge_discard_tests.rs', 'received_purge_progresses_under_healthy_checkout_renewal_owned'): 'owned host cannot apply pulled rows',
    ('crates/contextdb-engine/src/database/maintenance_resource_tests.rs', 'native_durable_generation_build_and_seal_workspaces_keep_separate_original_invoices'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/src/database/maintenance_resource_tests.rs', 'switching_to_caller_driven_during_a_native_build_reports_pending_before_retry'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/src/database/received_legacy_growth_guards.rs', 'production_legacy_selected_population_2n_succeeds_with_fixed_memory'): 'reader budget grows with rows',
    ('crates/contextdb-engine/src/database/received_legacy_growth_guards.rs', 'production_legacy_selected_population_4n_succeeds_with_fixed_memory'): 'reader budget grows with rows',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_durable_failure_returns_all_writer_custody'): 'deep refusal retains writer custody',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_deep_budget_refusal_returns_all_writer_custody_and_retries_live'): 'the full lifecycle requires extended execution',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'native_route_output_refusal_releases_partial_output_preserves_serving_and_closes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'public_cycle_small_unknown_invalid_catalog_does_not_quarantine'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'native_work_observer_is_inside_the_real_indexed_search'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'a_page_read_does_not_stop_another_search_over_the_same_generation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'a_parked_walk_and_the_insertions_beside_it_all_finish'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'one_buffer_generation_preserves_canonical_bytes_for_every_encoding'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'durable_generation_refuses_corruption_and_incompatible_column_definition'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'mutable_quantized_canonical_insertion_without_rebuilding'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'indexed_predicate_search_gives_absent_candidates_no_distance'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_public_engine_ports.rs', 'indexed_sql_and_rust_scores_publish_the_canonical_body_rerank'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/build_determinism.rs', 'rebuilding_from_the_same_rows_under_one_training_seed_gives_the_same_index_and_answers'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/build_determinism.rs', 'a_build_does_deterministic_work_bounded_by_its_declared_numbers'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/calibration_source.rs', 'a_calibration_source_change_marks_requalification_pending_without_flipping_the_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/calibration_source.rs', 'fifty_source_questions_use_stored_vectors_under_the_default_two_hundred_sample_threshold'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/cancellation.rs', 'a_search_cancelled_while_paging_lists_returns_the_typed_cancellation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/cancellation.rs', 'a_search_cancelled_while_scoring_candidates_returns_the_typed_cancellation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/cancellation.rs', 'a_maintenance_cycle_cancelled_inside_a_build_batch_returns_the_typed_cancellation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/cancellation.rs', 'a_maintenance_cycle_cancelled_inside_a_reclaim_batch_returns_the_typed_cancellation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/codebook_lifecycle.rs', 'writes_before_the_first_codebook_stay_exactly_searchable_and_the_first_publication_assigns_them'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/codebook_lifecycle.rs', 'runs_sealed_while_a_replacement_trains_stay_served_and_are_remapped_in_bounded_resumable_batches'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/codebook_lifecycle.rs', 'a_crash_immediately_before_or_after_the_codebook_switch_leaves_every_row_served'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'a_proof_already_stale_stays_stale_through_compaction'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'a_snapshot_held_across_compaction_keeps_its_answer_and_its_proof'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'a_step_failing_after_it_carried_a_proof_rolls_table_and_proof_back_together'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'automatic_compaction_keeps_a_per_file_import_running'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'compact_now_keeps_every_route_and_moves_native_roots'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'compaction_carries_the_qualification_proof_and_its_receipt_stays'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'damaged_authority_tables_are_not_treated_as_rebuildable_index_data'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'damaged_bytes_in_an_index_table_get_no_fresh_proof_and_the_sweep_goes_on'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'damaged_scope_directory_pages_refuse_before_a_proof_is_carried'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'purging_a_row_erases_families_with_their_owner_entries'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/compaction_keeps_native_roots.rs', 'retired_families_carry_their_proofs_and_their_owners_go_with_them'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/concurrent_publication.rs', 'concurrent_maintenance_publishes_one_complete_generation_for_pinned_readers'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/concurrent_publication.rs', 'a_paused_native_reader_keeps_its_answer_while_vector_writes_commit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/crash_cut_points.rs', 'a_crash_at_either_publication_boundary_leaves_one_complete_generation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/declared_breadth.rs', 'explicit_probes_and_refine_factor_bound_list_pages_and_rescores_exactly'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/deletes_and_purge.rs', 'purge_erases_vector_bytes_from_every_index_page_buffer_generation_and_export'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/deletes_and_purge.rs', 'purge_during_a_multi_cycle_build_abandons_it_and_leaves_no_sampled_vector'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/deletes_and_purge.rs', 'a_purge_refused_on_the_declared_memory_limit_changes_no_index_page'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/depth_coverage.rs', 'only_calibrated_depths_are_covered_each_by_its_own_run'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/depth_coverage.rs', 'auto_takes_the_index_on_a_qualified_partition'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/depth_coverage.rs', 'a_depth_one_run_misses_what_a_depth_one_hundred_prefix_would_find'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'a_training_reservoir_write_past_the_ceiling_is_refused_before_it_executes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'a_build_accumulator_write_past_the_ceiling_is_refused_before_it_executes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'an_unpublished_run_write_past_the_ceiling_is_refused_before_it_executes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'a_prepared_replacement_write_past_the_ceiling_is_refused_before_it_executes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'a_disk_refused_replacement_reports_disk_limit_and_names_the_operator_action'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/engine_stage.rs', 'mutations_before_initial_maintenance_do_not_resurrect_rows_when_native_build_finishes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/engine_stage.rs', 'initialized_live_and_preingest_tombstone_journals_refuse_stale_row_identity'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/engine_stage.rs', 'failure_after_source_and_preingest_journal_staging_rolls_back_both'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/engine_stage.rs', 'native_indexed_file_search_survives_restart_replacement_and_erasure'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/engine_stage.rs', 'first_saved_search_loads_compact_serving_descriptor_as_real_manifest_source'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/exact_and_indexed_work.rs', 'exact_counts_every_vector_and_indexed_reranks_the_tie_trap'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/exact_and_indexed_work.rs', 'exact_rescores_every_allowed_vector_and_indexed_does_strictly_less_work'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'maintenance_to_completion_covers_every_committed_vector'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'new_committed_work_is_covered_again_after_one_more_maintenance_run_and_the_call_builds_nothing'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'the_consumer_loop_finishes_in_finitely_many_cycles_and_is_idle_once_covered'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'the_consumer_loop_covers_a_replaced_passage_after_retention'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'retention_during_an_unfinished_build_keeps_its_change_count'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/import_tail_coverage.rs', 'rows_arriving_during_an_empty_first_build_reach_a_published_index_in_finite_cycles'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/index_corruption.rs', 'damaged_index_pages_refuse_indexed_and_never_stop_writes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/index_corruption.rs', 'an_engine_owned_worker_repairs_a_damaged_index_without_a_caller_cycle'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_candidate_port.rs', 'native_indexed_residual_selects_eligible_nearest_rows_before_limit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_candidate_port.rs', 'native_pinned_residual_uses_captured_rows_after_a_predicate_change'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_copy_auth.rs', 'obsolete_copy_cleanup_refuses_tampered_live_journal'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_copy_auth.rs', 'obsolete_copy_cleanup_refuses_swapped_captured_kind_root'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'two_native_kernel_commits_seal_in_one_atomic_authoritative_sql_commit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'every_fallible_native_seal_boundary_aborts_rows_roots_and_original_owner_scratch'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'captured_native_roots_keep_frozen_basis_separate_from_actual_delta_coverage_and_read_snapshot'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'real_one_list_page_489_search_loads_the_link_used_and_list_boundaries'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'actual_page_identity_damage_refuses_indexed_before_scoring_and_auto_uses_canonical_source'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_seal.rs', 'equal_schema_counters_on_different_index_names_never_share_native_authority'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/objective_change.rs', 'a_changed_expected_population_keeps_the_covered_settings_with_requalification_pending'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/parse_surface.rs', 'a_query_raises_its_own_target_accuracy_on_the_index'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/per_context_cost.rs', 'a_one_context_search_reads_only_its_own_segments_and_fewer_pages_than_all'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/per_context_cost.rs', 'an_unrelated_context_growing_and_republishing_leaves_a_fixed_context_s_work_unchanged'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/public_rust_auto_and_overlay.rs', 'ready_file_auto_semantic_search_executes_the_native_route_reported_by_explain'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/public_rust_auto_and_overlay.rs', 'ready_file_indexed_transaction_overlay_preserves_staged_rows_and_native_committed_search'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/public_rust_indexed.rs', 'ready_file_index_serves_fresh_public_rust_indexed_search_without_building'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/reader_memory.rs', 'index_metadata_is_charged_whole_and_released_while_the_answer_keeps_its_own_funding'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/reader_memory.rs', 'a_reader_smaller_than_one_list_page_is_refused_with_the_typed_reader_refusal'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/reader_turnover_cleanup.rs', 'obsolete_current_tree_entries_are_removed_while_finite_readers_overlap'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/retired_run_visibility.rs', 'a_change_to_a_row_in_a_retired_legacy_run_keeps_old_pages_until_state_c'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/small_partitions.rs', 'default_probes_keep_probing_until_k_eligible_candidates_are_found'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/small_partitions.rs', 'explicit_indexed_on_a_150_row_partition_is_served_complete_from_its_generation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/snapshot_isolation.rs', 'a_pinned_reader_keeps_its_view_through_commit_replacement_and_reclamation'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/switch_protocol.rs', 'every_committed_vector_is_answered_exactly_once_while_legacy_entries_are_remapped'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/switch_protocol.rs', 'a_crash_inside_a_remap_batch_recovers_and_finishes_with_each_row_exactly_once'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/switch_protocol.rs', 'a_replacement_due_while_legacy_entries_are_remapped_waits_for_the_remap_to_finish'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/training_seed.rs', 'a_training_seed_is_declared_persisted_and_inspected'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/clustered_vector_index/write_buffer.rs', 'twice_as_many_separate_commits_never_raise_the_write_buffer_high_water'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses.rs', 'a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses'): 'an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal',
    ('crates/contextdb-engine/tests/engine_seams/a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses.rs', 'a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses_as_received_history_grows'): 'an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal',
    ('crates/contextdb-engine/tests/engine_seams/an_import_tail_charges_each_decoded_vector_once.rs', 'every_commit_the_writer_accepted_is_absorbed_under_the_same_limit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/first_search_after_writer_reopen_uses_saved_pages_without_build.rs', 'first_search_after_writer_reopen_uses_saved_pages_without_build'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/import_tail_survives_restart_without_history_reconstruction.rs', 'cold_import_tail_reconstruction_does_not_examine_n_or_four_n_history'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/import_tail_survives_restart_without_history_reconstruction.rs', 'native_tail_purge_erases_selected_points_and_preserves_survivor_search_after_restart'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/import_tail_survives_restart_without_history_reconstruction.rs', 'native_tail_reopens_and_appends_inside_one_fixed_writer_budget'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/import_tail_survives_restart_without_history_reconstruction.rs', 'native_tail_publications_preserve_an_existing_transaction_snapshot'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/one_import_commit_larger_than_the_cohort_share_is_absorbed.rs', 'one_committed_lsn_larger_than_the_cohort_share_is_absorbed_without_raising_the_limit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/one_import_commit_larger_than_the_cohort_share_is_absorbed.rs', 'a_pinned_reader_keeps_its_answer_while_the_large_commit_is_rebuilt_and_published'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/reader_peak_covers_the_complete_query.rs', 'first_saved_search_reader_bytes_fit_the_fixed_32_record_window'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/engine_seams/reads_and_writes_are_charged_to_their_own_declared_limits.rs', 'import_writes_decode_only_under_the_declared_writer_never_the_reader'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/first_saved_search_owned_worker.rs', 'first_saved_search_scores_on_the_owned_worker_in_one_identity_namespace'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/import_persistence_without_observer.rs', 'ordinary_import_persistence_advances_durable_coverage_without_an_observer'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_calibration_window.rs', 'a_private_candidate_retains_its_real_verdict_across_calibration_queries'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_calibration_window.rs', 'a_rejected_point_reuses_its_actual_failing_sample_before_repeating_successes'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_calibration_window.rs', 'an_immutable_private_query_root_loads_its_scope_control_once'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_initial_reclaim.rs', 'initial_unclustered_payloads_retire_after_remap_without_recreating_old_pages'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_damage.rs', 'actual_page_header_and_payload_damage_refuse_indexed_and_leave_canonical_exact_usable'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_damage.rs', 'a_frozen_source_range_excludes_later_appends_and_preparation_is_counted_once_durably'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_damage.rs', 'a_retained_codebook_is_lazy_after_reopen_and_released_by_the_real_retirement_commit'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_damage.rs', 'dropping_a_state_b_table_releases_its_actual_retained_codebook_before_close'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_public_ports.rs', 'owned_page_rejection_and_shared_page_retirement_preserve_physical_custody'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_public_ports.rs', 'a_failed_page_read_leaves_nothing_resident'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_public_ports.rs', 'persistent_native_cold_damage_is_typed_on_every_public_query_without_rebuild'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_page_public_ports.rs', 'persistent_native_unexpected_eof_is_typed_on_every_public_query_without_rebuild'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/native_query_overrides.rs', 'a_query_breadth_override_changes_actual_probes_and_rescores_without_changing_the_column'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'prv_10_row_vector_with_use_rank_emits_the_indexed_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs', 'oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry'): 'oversized schema pull funding remains incomplete',
    ('crates/contextdb-engine/tests/vector/a_damaged_saved_graph_page_refuses_only_its_route.rs', 'every_kind_of_page_damage_refuses_only_the_damaged_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_declared_breadth_gets_exactly_its_approved_budget.rs', 'opening_and_searching_are_charged_against_the_declared_work'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_disconnected_graph_partition_is_compared_exactly.rs', 'deleted_history_does_not_move_a_connected_partition_off_its_graph'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_find_over_many_contexts_spends_one_search_budget.rs', 'a_find_over_many_contexts_spends_one_search_budget'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_find_over_many_contexts_spends_one_search_budget.rs', 'a_short_page_exhausts_the_eligible_frontier_on_both_routes'): 'the vector index route is switched off in this release',  # CDB-284
    ('crates/contextdb-engine/tests/vector/a_synced_vector_policy_change_keeps_the_receivers_graphs.rs', 'a_synced_search_mode_change_keeps_indexed_answering_without_maintenance'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_synced_vector_policy_change_keeps_the_receivers_graphs.rs', 'a_synced_auto_index_at_change_keeps_indexed_answering_without_maintenance'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/a_synced_vector_policy_change_keeps_the_receivers_graphs.rs', 'a_vector_update_after_a_synced_policy_change_keeps_the_other_partitions_graph'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/read/reader_continuity_contract.rs', 'schema_replay_does_not_interrupt_active_readers'): 'requires a maintained vector index, which this release does not ship',  # CDB-284
    ('crates/contextdb-engine/tests/vector/auto_chooses_the_cheaper_route_for_each_partition.rs', 'auto_never_costs_more_than_the_route_a_caller_could_have_asked_for'): 'the vector index route is switched off in this release',  # CDB-284
    ('crates/contextdb-engine/tests/vector/auto_chooses_the_cheaper_route_for_each_partition.rs', 'in_memory_auto_accounts_for_comparison_and_admission_costs'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/auto_chooses_the_cheaper_route_for_each_partition.rs', 'the_automatic_route_costs_the_same_however_the_scope_is_split'): 'the vector index route is switched off in this release',  # CDB-284
    ('crates/contextdb-engine/tests/vector/continuous_writers_hold_throughput_and_reader_latency.rs', 'continuous_writers_hold_throughput_and_reader_latency'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/flat_route_below_declared_threshold.rs', 'indexed_refuses_a_whole_scope_whose_index_partition_is_unavailable_beside_a_built_one'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/indexed_route_does_less_work_than_exact.rs', 'indexed_route_does_less_work_than_exact'): 'indexed search calibration remains incomplete',
    ('crates/contextdb-engine/tests/vector/passive_explain_predicts_the_executed_auto_route.rs', 'passive_explain_predicts_an_unfiltered_resident_auto_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/passive_explain_predicts_the_executed_auto_route.rs', 'passive_explain_predicts_a_mixed_stored_and_resident_auto_route'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_inspection_explain_truth_contract.rs', 'vector_inspection_is_passive_and_reports_real_pre_and_post_maintenance_facts'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_inspection_explain_truth_contract.rs', 'file_backed_inspection_separates_positive_durable_bytes_from_charged_residency_once'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_inspection_explain_truth_contract.rs', 'engine_owned_ready_inspection_reports_no_recovery_without_inspecting_into_a_build'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_inspection_explain_truth_contract.rs', 'the_same_pending_partition_reports_the_owner_of_maintenance_on_both_show_surfaces'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_lazy_raw_residency_contract.rs', 'id_only_candidate_route_discloses_the_maintained_index'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_maintained_lifecycle_contract.rs', 'engine_owned_file_maintenance_publishes_a_durable_indexed_route_for_a_reopened_reader'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_partition_query_contract.rs', 'restricted_rust_indexed_without_predicate_refuses_before_sources_and_recovers'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_partition_query_contract.rs', 'indexed_refuses_naming_the_restriction_column_then_recovers_via_index'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_policy_resolver_contract.rs', 'a_declared_small_breadth_still_preserves_useful_top_ten_quality'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'in_memory_construction_leaves_same_partition_vector_writes_usable'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'in_memory_construction_leaves_new_partition_vector_writes_usable'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'changing_the_poll_interval_returns_while_engine_maintenance_is_active'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/vector_search_mode_bounded_contract.rs', 'indexed_small_nonempty_scope_refuses_until_maintenance_then_keeps_a_staged_delta_visible'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/vector_serving_merge_contract.rs', 'embedded_graph_scratch_obeys_memory_admission_and_returns_its_charge'): 'the vector index route is switched off in this release',
    ('crates/contextdb-engine/tests/vector/writer_residency_attribution.rs', 'concurrent_deletes_retire_one_staged_admission'): 'concurrent deletes hang retiring one staged admission',
    ('crates/contextdb-redb/src/db.rs', 'measure_the_checked_allocator_save_at_the_minimum_window_budget'): 'allocator paging measurement runs only when explicitly requested',
    ('crates/contextdb-redb/src/tree_store/bounded_bytes.rs', 'insert_after_separator_window_refuses_before_read_and_releases_its_owner'): 'bounded-read refusal and owner-release assertion fails',
    ('crates/contextdb-redb/src/tree_store/page_store/cached_file.rs', 'settle_rebuilds_exact_read_index_and_preserves_above_cap_held_pages'): 'read-index settlement assertion fails',
    ('crates/contextdb-redb/src/tree_store/page_store/page_manager.rs', 'measure_trim_and_install_paging'): 'allocator paging measurement runs only when explicitly requested',
}
OTHER_IGNORED_TESTS = [('crates/contextdb-engine/src/database/coexistence_guards.rs',
  'coexistence_actor',
  'subprocess entry invoked by the fixture launcher'),
 ('crates/contextdb-engine/src/database/currency_version_compaction_tests.rs',
  'debris_corpus_acceptance_shrinks_debris_ledger',
  'requires the DEBRIS_LEDGER_CORPUS debris file'),
 ('crates/contextdb-engine/tests/clustered_vector_index/native_candidate_port.rs',
  'native_exact_formula_scores_every_allowed_vector_once',
  'exact work counters for the int8 scan are unavailable'),
 ('crates/contextdb-engine/tests/engine_seams/a_declared_count_is_the_only_count_an_append_writes.rs',
  'what_the_cg_event_append_costs_before_and_after_the_declaration',
  'measurement: builds two 20,010-event streams'),
 ('crates/contextdb-engine/tests/engine_seams/declaring_a_count_on_a_table_with_rows_is_atomic_and_charged.rs',
  'the_crashing_child',
  'child process of the declaration-crash tests'),
 ('crates/contextdb-engine/tests/engine_seams/one_event_append_cost_by_stream_depth.rs',
  'one_event_append_cost_by_stream_depth',
  'builds stores of up to 20,010 events; run with --ignored'),
 ('crates/contextdb-engine/tests/engine_seams/one_event_append_cost_by_stream_depth.rs',
  'one_event_appended_to_a_long_stream_costs_what_one_appended_to_a_short_stream_costs',
  'the fix this test pins is not implemented yet, and it builds stores of up to 20,010 events; '
  'run with --ignored'),
 ('crates/contextdb-engine/tests/engine_seams/where_an_appends_chunk_rewrites_sit_in_the_proof_tree.rs',
  'where_an_appends_chunk_rewrites_sit_in_the_proof_tree',
  'builds a 5,010-event store; run with --ignored'),
 ('crates/contextdb-engine/tests/vector/an_exact_search_authenticates_pages_not_every_vector.rs',
  'an_exact_search_authenticates_pages_not_every_vector',
  'large exact-search diagnostic; run explicitly when measuring body and proof work'),
 ('crates/contextdb-engine/tests/vector/vector_search_mode_bounded_contract.rs',
  'filtered_many_partition_search_reuses_partition_local_candidates',
  'exact work counters for the int8 scan are unavailable'),
 ('crates/contextdb-engine/tests/vector/vector_serving_merge_contract.rs',
  'closer_noncurrent_rows_do_not_change_current_results_or_add_complete_entry_passes',
  'exact work counters for the int8 scan are unavailable'),
 ('crates/contextdb-server/tests/durable_deletion_and_purge_tests.rs',
  'authoritative_purge_removes_every_engine_held_copy',
  'vector synchronization during purge can time out, report false frontier corruption, or return '
  '1000 nodes instead of 1001'),
 ('tests/acceptance/deployment.rs',
  'f48_monitor_server_health_in_production',
  'server has no health endpoint yet'),
 ('tests/integration/cross_cutting_acceptance_tests.rs',
  'ignored_arm64',
  'requires ARM64 cross compile'),
 ('tests/integration/gate_a_primitives.rs',
  'a11_01_cross_compile_check',
  'requires ARM64 cross-compile CI gate'),
 ('tests/integration/rank_policy_tests.rs',
  'indexed_threshold_1001_indexed_boundary',
  'maintained vector indexing is not available for this ranking test'),
 ('crates/contextdb-engine/tests/read/content_identity_independent_contract.rs',
  'generation_publication_keeps_the_root_equal_to_the_independent_committed_content',
  'requires a maintained vector index, which this release does not ship'),
 ('crates/contextdb-engine/tests/read/reader_continuity_contract.rs',
  'lazy_generation_load_beside_a_writer_keeps_serving',
  'requires a maintained vector index, which this release does not ship'),
 ('crates/contextdb-engine/tests/read/reader_snapshot_registration_lifecycle.rs',
  'lowered_disk_limit_bounds_its_own_metadata_and_preserves_failed_setting',
  "after a refused write the store's handle stays unusable until reopen; failure recovery without reopen is not delivered yet"),
 ('crates/contextdb-engine/tests/read/working_set_bounded_contract.rs',
  'read_only_open_loads_only_the_working_set',
  'requires a maintained vector index, which this release does not ship'),
 ('crates/contextdb-engine/tests/read/working_set_bounded_contract.rs',
  'retained_graph_residency_uses_the_session_ceiling',
  'requires a maintained vector index, which this release does not ship')]


def gate_mode(value=None):
    value = os.environ.get('CI_GATE_MODE', 'full') if value is None else value
    if value not in {'normal', 'full'}:
        raise ValueError('gate mode must be normal or full')
    return value


def routed_targets(targets, mode):
    gate_mode(mode)
    return [t for t in targets if mode == 'full' or t not in FULL_SCHEMA | FULL_ONLY_TARGETS]


def aggregate_module(root=ROOT):
    spec = importlib.util.spec_from_file_location('aggregate_routing', root / 'scripts/ci-aggregate-routing.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def routing_contract(root=ROOT):
    # Exact-source census fence: a renamed/added/moved case cannot silently make
    # the one-case exclusion miss, or expand what it excludes. Refresh by review.
    fingerprints = SYNC_ROUTING_FINGERPRINTS
    for path, expected in fingerprints.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
            raise ValueError('sync routing census changed; review exact case inventory: ' + path)
    quarantine_contract(root)
    long_running_contract(root)


def quarantine_contract(root=ROOT):
    """Require every ignored test to match its registered path, name and reason."""
    for (path, case), reason in QUARANTINE_EXCEPTIONS.items():
        text = (root / path).read_text()
        attribute = f'#[ignore = "{reason}"]'
        declaration = re.compile(re.escape(attribute) + r'\s*(?:#\[[^\]]+\]\s*)*(?:async\s+)?fn\s+' + re.escape(case) + r'\s*\(')
        if len(declaration.findall(text)) != 1:
            raise ValueError('missing, duplicate, or misbound approved quarantine: ' + path + '::' + case)
    # This existing acceleration assertion stays enabled in its nightly target.
    path = 'crates/contextdb-engine/tests/vector/indexed_route_does_less_work_than_exact.rs'
    case = 'single_context_saved_graph_auto_and_indexed_are_faster_than_exact'
    text = (root / path).read_text()
    attributes = re.findall(r'(?m)^((?:#\[[^\n]*\]\s*)+)fn\s+' + re.escape(case) + r'\s*\(', text)
    if attributes != ['#[cfg(feature = "nightly-tests")]\n#[test]\n']:
        raise ValueError('saved-graph acceleration assertion must remain enabled and nightly: ' + case)
    actual = ignored_test_census(root)
    expected = [(path, case, reason) for (path, case), reason in QUARANTINE_EXCEPTIONS.items()]
    expected += OTHER_IGNORED_TESTS
    if len(expected) != len(set((path, case) for path, case, _ in expected)):
        raise ValueError('duplicate ignored-test registration')
    if sorted(actual) != sorted(expected):
        raise ValueError('ignore inventory changed; every declaration must match its registered path, test name and reason')


def ignored_test_census(root=ROOT):
    """Read actual Rust attributes, including inline and conditional ignores."""
    literal = re.compile(r'//[^\n]*|/\*|(?:br|r)(?P<hashes>\#*)".*?"(?P=hashes)|'
                         r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])\'', re.S)
    result = []
    for path in sorted(root.rglob('*.rs')):
        if '.git' in path.parts:
            continue
        text = path.read_text()
        # Mask comments and literals so quoted attribute examples are not tests.
        chunks, cursor = [], 0
        while match := literal.search(text, cursor):
            start, end = match.span()
            if match[0] == '/*':
                depth = 1
                while depth:
                    mark = re.search(r'/\*|\*/', text[end:])
                    if mark is None:
                        raise ValueError('unterminated Rust comment: ' + str(path))
                    depth += 1 if mark[0] == '/*' else -1
                    end += mark.end()
            chunks.extend((text[cursor:start], ' ' * (end - start)))
            cursor = end
        chunks.append(text[cursor:])
        code = ''.join(chunks)
        for attribute in re.finditer(r'#\[\s*(?:ignore|cfg_attr)\b', code):
            start, end, depth = attribute.start(), attribute.end(), 1
            while depth and end < len(code):
                depth += (code[end] == '[') - (code[end] == ']')
                end += 1
            if depth:
                raise ValueError('unterminated Rust attribute: ' + str(path))
            declaration = text[start:end]
            if not re.search(r'\bignore\b', code[start:end]):
                continue
            function = re.search(r'\bfn\s+([A-Za-z0-9_]+)\s*\(', code[end:])
            if function is None:
                raise ValueError('ignore attribute has no test function: ' + str(path))
            reason = re.search(r'\bignore\s*=\s*("(?:[^"\\]|\\.)*")', declaration, re.S)
            reason = re.sub(r'\\\s+', '', reason[1][1:-1]) if reason else ''
            result.append((str(path.relative_to(root)), function[1], reason))
    return result


def sync_routing_counts(entries, mode, companion=False):
    gate_mode(mode)
    matches = [e for e in entries if e.get('target') == 'tests/sync_schema_transactions.rs']
    expected = ((0, 1, 12) if companion else (12, 0, 1) if mode == 'normal'
                else (12, 1, 0))
    if (len(matches) != 1 or tuple(matches[0].get(k) for k in ['passed', 'ignored', 'filtered_out']) != expected
            or any(matches[0].get(k) != 0 for k in ['failed', 'measured'])):
        raise ValueError('sync routing receipt must account for all 13 cases and exact relocation')


# Optional optimization: any inapplicable certificate restores all normal tests.
NORMAL_DEDUP_TARGETS = ('custody_custody_protocol', 'custody_custody_purge_and_discard', 'custody_server_resource_policy_contract_tests', 'custody_tenant_table_policy', 'vector_a_find_answers_a_full_page_when_a_candidate_is_withdrawn', 'vector_a_find_names_only_the_pairs_it_asks_for', 'vector_auto_decides_the_route_once_for_the_whole_scope', 'vector_auto_pays_what_indexed_pays', 'vector_equal_distance_rows_keep_canonical_order_across_contexts', 'vector_exclusion_persistence_tests', 'vector_inspection_explain_truth_contract', 'vector_maintained_lifecycle_contract', 'vector_memory_ownership_reconciliation_contract', 'vector_named_vector_planner_tests', 'vector_operation_observations', 'vector_policy', 'vector_quantized', 'vector_rank_policy_exact_audits_the_whole_table', 'vector_received_generation_invalidation_contract', 'vector_resource_maintenance_contract', 'vector_retained_indexed_rows', 'vector_row_delete_and_guards_honor_vector_columns', 'vector_serving_merge_contract', 'vector_tail_transaction_contract', 'vector_the_owner_doors_run_one_search_over_every_layer', 'vector_transaction_filter_snapshot_truth_contract', 'vector_writer_residency_attribution', 'vector_auto_chooses_the_cheaper_route_for_each_partition', 'bounded_metadata_start_lookup_stays_inside_the_read_limits', 'custody_delivery_outcome', 'indexed_row_memory_charge_measurement', 'memory_limit_admission_holds_for_vectors', 'vector_an_exhaustive_find_streams_bodies_it_cannot_hold', 'vector_multi_partition_union_recovers_nearest_rows', 'vector_partition', 'engine', 'sql', 'vector_generation', 'history_request_memory', 'custody_delivery_manifest', 'vector_a_find_decodes_only_the_rows_it_returns', 'vector_a_find_over_many_contexts_spends_one_search_budget', 'direct_file_image_memory_lifetime', 'graph_property_memory_charge_measurement', 'received_schema_metadata_peak', 'bounded_in_subquery_stays_inside_the_read_limits', 'bounded_read_of_staged_graph_work_stays_within_its_limits', 'bounded', 'vector_lazy', 'vector_schema', 'vector_search_mode_bounded_contract')


def feature_consumers(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (root / 'crates').glob('*/src/**/*.rs')
            if re.search(r'nightly.tests|CARGO_FEATURE|RUSTFLAGS', p.read_text())}


def literal_closure(root, target):
    manifest = tomllib.loads((root / 'crates/contextdb-engine/Cargo.toml').read_text())
    records = [t for t in manifest['test'] if t['name'] == target]
    if len(records) != 1:
        raise ValueError('unknown target registration')
    pending = [root / 'crates/contextdb-engine' / records[0]['path']]
    found = {}
    while pending:
        path = pending.pop().resolve()
        name = str(path.relative_to(root.resolve()))
        if name in found:
            continue
        text = path.read_text()
        if re.search(r'nightly.tests|CARGO_FEATURE|RUSTFLAGS|include!\s*\(\s*[^"\s]', text):
            raise ValueError('feature-bearing or dynamic helper: ' + name)
        found[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        # Literal includes/path modules are closed by their containing file hash.
        explicit = list(re.finditer(r'#\[path\s*=\s*"([^"]+)"\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*;', text))
        for match in explicit:
            pending.append(path.parent / match[1])
        stripped = text
        for match in reversed(explicit):
            stripped = stripped[:match.start()] + stripped[match.end():]
        for child in re.findall(r'include!\s*\(\s*"([^"]+)"\s*\)', stripped):
            pending.append(path.parent / child)
        for child in re.findall(r'(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;', stripped):
            base = path.parent if path.name in {'mod.rs', target + '.rs'} else path.parent / path.stem
            choices = [base / (child + '.rs'), base / child / 'mod.rs']
            matches = [c for c in choices if c.is_file()]
            if len(matches) != 1:
                raise ValueError('unresolved helper ' + child)
            pending.extend(matches)
    return found


def dedup_snapshot(root):
    # Dependencies/features/config can invalidate equality; unrelated Rust edits do not.
    dependencies = list((root / 'crates').glob('*/Cargo.toml'))
    dependencies += [root / x for x in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']]
    dependencies += [p for p in (root / '.cargo').rglob('*') if p.is_file()]
    return dict(dependencies={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in dependencies if p.is_file()},
                feature_consumers=feature_consumers(root))


def normal_dedup_certificate(root=ROOT, check_source=True):
    result = dict(targets={}, reason='dedup disabled: missing or invalid certificate', fallback_targets=list(NORMAL_DEDUP_TARGETS))
    try:
        c = json.loads((root / 'scripts/ci-integration-targets.json').read_text()).get('normal_dedup')
        if not isinstance(c, dict) or c.get('schema_version') != 2 or set(c.get('targets', {})) != set(NORMAL_DEDUP_TARGETS):
            return result
        digest = c.get('sha256')
        payload = {k: v for k, v in c.items() if k != 'sha256'}
        if digest != hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest():
            return result
        if c.get('features') != ['test-seams,iroh', FEATURES] or c.get('profile') != 'test-profile' or c.get('threads') != '1':
            return result
        for target, proof in c['targets'].items():
            if not proof.get('sources') or len(proof['cases']) != proof['case_count'] or any(status not in {'ok', 'ignored'} for _, status in proof['cases']):
                return result
        if check_source:
            if os.environ.get('RUSTFLAGS', '') not in {'', '-D warnings'}:
                raise ValueError('unqualified RUSTFLAGS')
            allowed_profiles = {
                'CARGO_PROFILE_DEV_DEBUG': '0',
                'CARGO_PROFILE_TEST_DEBUG': '0',
            }
            for k, v in os.environ.items():
                if (k.startswith('CARGO_PROFILE_') and allowed_profiles.get(k) != v
                        or k.startswith('CARGO_FEATURE_') or k in {'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_RUSTFLAGS', 'CARGO_BUILD_TARGET', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER'} and v):
                    raise ValueError('unqualified flags: ' + k)
            if c['applicability'].get('feature_consumers') or dedup_snapshot(root) != c['applicability']:
                raise ValueError('dependency or actual feature-consumer change')
            for target, proof in c['targets'].items():
                if literal_closure(root, target) != proof['sources']:
                    raise ValueError('stale or incomplete literal helper closure: ' + target)
                # Changed source can introduce new helpers/cases: never skip it.
                for name, expected in proof['sources'].items():
                    path = root / name
                    if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                        raise ValueError('stale source/helper: ' + name)
        return dict(c, reason='dedup enabled: local case/ignored/source certificates apply', fallback_targets=[])
    except (ValueError, KeyError, TypeError, OSError) as error:
        result['reason'] = 'dedup disabled: ' + str(error)
        return result


def parse_local_list(path):
    targets = {}
    current = None
    finished = set()
    for line in path.read_text().splitlines():
        line = re.sub(r'\x1b\[[0-9;]*m', '', line)
        match = re.search(r'Running tests/(\w+)\.rs \(', line)
        if match:
            current = match[1]
            if current in targets:
                raise ValueError('duplicate target list: ' + current)
            targets[current] = []
        match = re.fullmatch(r'(.+): test', line)
        if match and current:
            targets[current].append(match[1])
        if re.fullmatch(r'\d+ tests?, \d+ benchmarks?', line) and current:
            finished.add(current)
    if set(targets) != set(NORMAL_DEDUP_TARGETS) or finished != set(targets):
        raise ValueError('missing/unknown/incomplete local lists; list exactly the dedup candidate targets')
    if any(len(v) != len(set(v)) for v in targets.values()):
        raise ValueError('duplicate case list')
    return targets


def regenerate_dedup(directory, root=ROOT):
    # Source identity is captured before producing local list files and checked again.
    start = json.loads((directory / 'identity.json').read_text())
    sources = {t: literal_closure(root, t) for t in NORMAL_DEDUP_TARGETS}
    identity = dict(applicability=dedup_snapshot(root), sources=sources)
    if identity['applicability']['feature_consumers']:
        raise ValueError('production feature consumers need independent equality proof; retain normal execution')
    if identity != start:
        raise ValueError('source changed since local list snapshot; capture fresh lists')
    lists = {n: parse_local_list(directory / (n + '.txt')) for n in ['ordinary.list', 'ordinary.ignored', 'nightly.list', 'nightly.ignored']}
    targets = {}
    for t in NORMAL_DEDUP_TARGETS:
        ordinary = sorted(lists['ordinary.list'][t]); nightly = sorted(lists['nightly.list'][t])
        ignored = sorted(lists['ordinary.ignored'][t])
        if ordinary != nightly or ignored != sorted(lists['nightly.ignored'][t]) or not set(ignored) <= set(ordinary):
            raise ValueError('case/ignored inventories differ: ' + t)
        targets[t] = dict(cases=[[case, 'ignored' if case in ignored else 'ok'] for case in ordinary],
                          case_count=len(ordinary), sources=sources[t])
    c = dict(schema_version=2, features=['test-seams,iroh', FEATURES], profile='test-profile', threads='1',
             warning_compile_owner='engine-family-1', targets=targets, applicability=identity['applicability'],
             local_list_sha256={n: hashlib.sha256((directory / (n + '.txt')).read_bytes()).hexdigest() for n in lists})
    c['sha256'] = hashlib.sha256(json.dumps(c, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    manifest = root / 'scripts/ci-integration-targets.json'
    data = json.loads(manifest.read_text()); data['normal_dedup'] = c
    manifest.write_text(json.dumps(data, indent=2) + '\n')
    return c


PLAN_FIELDS = {'dedup_sha', 'dedup_reason', 'source_sha', 'source_fingerprint', 'gate_mode', 'run_default'}


def source_identity(root=ROOT):
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--binary', '--no-ext-diff'], cwd=root)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=root)
    files = {os.fsdecode(n): hashlib.sha256((root / os.fsdecode(n)).read_bytes()).hexdigest()
             for n in untracked.split(b'\0') if n and (root / os.fsdecode(n)).is_file()}
    digest = hashlib.sha256(sha.encode() + diff + json.dumps(files, sort_keys=True).encode()).hexdigest()
    return dict(source_sha=sha, source_fingerprint=digest)


def plan_decision(mode, run_default, root=ROOT):
    certificate = normal_dedup_certificate(root) if mode == 'normal' else dict(targets={}, reason='full qualification')
    return dict(dedup_sha=certificate.get('sha256', '') if certificate['targets'] else '',
                dedup_reason=certificate['reason'], gate_mode=gate_mode(mode), run_default=str(run_default).lower(),
                **source_identity(root))


def validate_decision(decision, mode, run_default, root=ROOT):
    if (not isinstance(decision, dict) or set(decision) != PLAN_FIELDS
            or decision['gate_mode'] != gate_mode(mode) or decision['run_default'] != str(run_default).lower()
            or not isinstance(decision['dedup_sha'], str)
            or not re.fullmatch(r'(?:[0-9a-f]{64})?', decision['dedup_sha'])
            or mode == 'full' and decision['dedup_sha']
            or not isinstance(decision['dedup_reason'], str) or not decision['dedup_reason']):
        raise ValueError('missing/tampered published plan decision or mode')
    identity = source_identity(root)
    if any(decision.get(k) != v for k, v in identity.items()):
        raise ValueError('published plan source identity mismatch')
    if decision['dedup_sha']:
        c = normal_dedup_certificate(root, check_source=False)
        if not c['targets'] or c.get('sha256') != decision['dedup_sha']:
            raise ValueError('published plan certificate SHA mismatch')
    return decision


def runtime_decision(row, mode, root=ROOT):
    decision = row.get('plan_decision')
    if decision is None:
        raise ValueError('slice missing published plan decision')
    validate_decision(decision, mode, boolean(decision.get('run_default', 'invalid')), root)
    if decision['dedup_sha']:
        current = normal_dedup_certificate(root)
        if not current['targets'] or current.get('sha256') != decision['dedup_sha']:
            raise ValueError('planned dedup certificate no longer applicable: ' + current['reason'])
    # Explicit empty decision remains empty even if runtime proof now applies.
    return decision


def active_rows(rows, run_default, mode, decision=None):
    gate_mode(mode)
    if mode == 'normal' and run_default:
        raise ValueError('normal mode requires run_default=false; default suites include full qualification')
    decision = plan_decision(mode, run_default) if decision is None else validate_decision(decision, mode, run_default)
    safe = set(NORMAL_DEDUP_TARGETS) if decision['dedup_sha'] else set()
    selected = []
    for original in rows:
        if not run_default and original['kind'] in OPTIONAL:
            continue
        row = dict(original, targets=list(original['targets']))
        if mode == 'normal':
            row['dedup_planned'] = bool(safe)
        row['plan_decision'] = decision
        if row['kind'] == 'engine-tests':
            row['targets'] = [t for t in routed_targets(row['targets'], mode) if t not in safe]
            if not row['targets']:
                continue
            if row['id'] == 'engine-family-9' and mode == 'normal':
                row['family'] = 'bounded schema replay / all 14 functional oracles'
            if row['id'] == 'sync-schema-transactions' and mode == 'normal':
                row['family'] = 'schema replication and atomic publish / 12 bounded cases'
        if mode == 'normal' and row['id'] == 'acceptance-long-running-test':
            for row_id in LONG_RUNNING_PARTITIONS:
                selected.append(dict(row, id=row_id, cache_parent='acceptance-long-running-test',
                                     family=row['family'] + (' / transfer' if 'transfer' in row_id else ' / incremental')))
            continue
        selected.append(row)
    return selected


def inventory_module(root):
    spec = importlib.util.spec_from_file_location('integration_inventory', root / 'scripts/ci-inventory.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_manifest(root=ROOT):
    routing_contract(root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    if set(manifest) - {'normal_dedup'} != {'schema_version', 'slices', 'pr_jobs', 'normal_bundles'} or manifest['schema_version'] != 2:
        raise ValueError('unsupported integration manifest')
    rows = manifest['slices']
    if not isinstance(rows, list) or not rows:
        raise ValueError('empty integration manifest')
    ids = set()
    for row in rows:
        if (set(row) not in ({'id', 'family', 'kind', 'targets'},
                             {'id', 'family', 'kind', 'targets', 'logical_id', 'cache_parent'})
                or not re.fullmatch(r'[a-z][a-z0-9-]*', row['id']) or row['id'] in ids
                or row['kind'] not in KINDS or not isinstance(row['targets'], list)
                or not row['family'] or any(c in row['family'] for c in '\n\r|')
                or 'test-seams' in row['family']):
            raise ValueError('invalid or duplicate integration slice')
        ids.add(row['id'])
        if ('logical_id' in row) != ('cache_parent' in row):
            raise ValueError('partition metadata must bind logical identity and cache parent together')
        if any(not re.fullmatch(r'[a-zA-Z0-9_-]+', t) for t in row['targets']):
            raise ValueError('invalid target')
        if row['kind'] == 'engine-tests':
            if (not row['targets'] or any(t in ACCEPTANCE_TARGETS for t in row['targets'])
                    and row['targets'] not in ([target] for target in ACCEPTANCE_TARGETS)):
                raise ValueError('test-profile acceptance must be a separate slice')
        elif row['kind'] == 'release-acceptance':
            if row['targets'] not in ([target] for target in ACCEPTANCE_TARGETS):
                raise ValueError('release-profile must run one acceptance partition exactly')
        elif row['targets']:
            raise ValueError('Cargo default/extra classes cannot narrow their targets')
    by_id = {row['id']: row for row in rows}
    for slice_id, (family, target) in SCHEMA_REPLAY_SLICES.items():
        row = by_id.get(slice_id)
        targets = [target]
        if slice_id == 'engine-family-9':
            targets.append('custody_schema_replay_bounded_contract')
        if row != dict(id=slice_id, family=family, kind='engine-tests', targets=targets):
            raise ValueError('schema replay must retain three separate named slices: ' + slice_id)
    for slice_id, (family, target) in SYNC_SLICES.items():
        if by_id.get(slice_id) != dict(id=slice_id, family=family, kind='engine-tests', targets=[target]):
            raise ValueError('sync requires two separate named slices: ' + slice_id)
    if by_id.get('indexed-search-work-latency') != dict(
            id='indexed-search-work-latency', family='indexed-search routing and distance work',
            kind='engine-tests', targets=['vector_indexed_route_does_less_work_than_exact']):
        raise ValueError('saved-graph acceleration assertion must retain its required nightly slice')
    acceptance_parts = {
        'acceptance-test': {
            ('acceptance-test', 'acceptance'),
            ('acceptance-long-running-test', 'acceptance_long_running'),
            ('acceptance-sync-test', 'acceptance_sync')},
        'acceptance-release': {
            ('acceptance-release', 'acceptance'),
            ('acceptance-long-running-release', 'acceptance_long_running'),
            ('acceptance-sync-release', 'acceptance_sync')},
    }
    for logical_id, expected_parts in acceptance_parts.items():
        actual = {(row['id'], row['targets'][0]) for row in rows
                  if row.get('logical_id') == logical_id}
        if actual != expected_parts or any(row.get('cache_parent') != logical_id for row in rows
                                           if row.get('logical_id') == logical_id):
            raise ValueError('acceptance partition or cache-parent binding changed: ' + logical_id)
    vector_parts = {
        ('engine-family-11', ('vector_a_find_decodes_only_the_rows_it_returns',
                              'custody_delivery_manifest')),
        ('engine-family-13', ('vector_a_find_over_many_contexts_spends_one_search_budget',)),
    }
    actual_vector_parts = {(row['id'], tuple(row['targets'])) for row in rows
                           if row.get('logical_id') == 'engine-family-11'}
    if actual_vector_parts != vector_parts or any(
            row.get('cache_parent') != 'engine-family-11' for row in rows
            if row.get('logical_id') == 'engine-family-11'):
        raise ValueError('vector physical partitions or cache-parent binding changed')
    logical_ids = [row.get('logical_id', row['id']) for row in rows]
    if len(rows) != 30 or len(set(logical_ids)) != 25:
        raise ValueError('physical/logical qualification inventory changed')
    for kind in KINDS - {'engine-tests', 'release-acceptance'}:
        if sum(row['kind'] == kind for row in rows) != 1:
            raise ValueError('missing or duplicate integration command class: ' + kind)
    module = inventory_module(root)
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']
    paths = {}
    for member in workspace['members']:
        for directory in root.glob(member):
            path = directory / 'Cargo.toml'
            paths[tomllib.loads(path.read_text())['package']['name']] = path
    redb = root / 'crates/contextdb-redb/Cargo.toml'
    paths[tomllib.loads(redb.read_text())['package']['name']] = redb
    documents = {name: tomllib.loads(path.read_text()) for name, path in paths.items()}
    workspace = dict(workspace, **{'manifest-root': root})
    closure = module.feature_closure('contextdb-engine', FEATURES.split(','), documents, paths, workspace)
    targets = module.target_inventory(documents['contextdb-engine'], paths['contextdb-engine'], 'test')
    expected = {'lib'} | {name for name, required in targets.items() if required <= closure}
    listed = [target for row in rows if row['kind'] == 'engine-tests' for target in row['targets']]
    if len(listed) != len(set(listed)) or set(listed) != expected:
        raise ValueError('integration test-profile target union mismatch (duplicate, missing or extra): '
                         f'missing={sorted(expected-set(listed))}, extra={sorted(set(listed)-expected)}')
    aggregate_module(root).contract(root)
    return rows


def metadata(row):
    kind = row['kind']
    features = '' if kind in {'workspace', 'redb'} else 'nightly-tests' if kind in OPTIONAL else FEATURES
    profile = 'release-profile' if kind in {'release-acceptance', 'production-release'} else 'test-profile'
    scope = 'engine' if kind not in {'workspace', 'redb'} else kind
    name = f'{scope} / {profile} / {row["family"]}'
    return dict(features=features, profile=profile, name=name,
                test_threads='1' if kind not in OPTIONAL and kind != 'examples'
                else 'not-run' if kind == 'examples' else os.environ.get('RUST_TEST_THREADS', 'default-parallel'))


NO_FAIL_FAST = '--no-fail-fast'


def no_fail_fast_contract(command):
    """A test command over several targets must keep running past a failing one."""
    if command[:2] != ['cargo', 'test'] or '--no-run' in command:
        return
    options = command[:command.index('--')] if '--' in command else command
    if options.count('--test') + options.count('--lib') > 1 and NO_FAIL_FAST not in options:
        raise ValueError('integration command stops at the first failing target (--no-fail-fast missing): '
                         + ' '.join(options))


def commands(row, root=ROOT, mode=None):
    """Keep default-target commands intact, including custom harnesses and doctests."""
    mode = gate_mode(mode)
    if mode == 'normal' and any(t in FULL_SCHEMA | FULL_ONLY_TARGETS for t in row['targets']):
        raise ValueError('unrouted full-qualification target in normal mode')
    kind = row['kind']
    base = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', metadata(row)['features']]
    if kind == 'engine-tests':
        aggregates = [t for t in row['targets'] if t in {'custody', 'vector'}
                      or mode == 'full' and t == 'custody_schema_replay_bounded_contract']
        ordinary = [t for t in row['targets'] if t not in aggregates]
        for target in ordinary:
            base += ['--lib', '--bins'] if target == 'lib' else ['--test', target]
        flags = ['--test-threads=1']
        if mode == 'normal' and 'sync_schema_transactions' in row['targets']:
            flags += ['--skip', OVERSIZED]
        if mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
            long_running_contract(root)
            if row['targets'] != ['acceptance_long_running'] or row['cache_parent'] != 'acceptance-long-running-test':
                raise ValueError('long-running partition target/cache envelope changed')
            flags += (['--skip', LONG_RUNNING_CASES[2]] if 'transfer' in row['id']
                      else [LONG_RUNNING_CASES[2], '--exact'])
        result = [base[:3] + [NO_FAIL_FAST] + base[3:] + ['--'] + flags] if ordinary else []
        if mode == 'normal':
            decision = runtime_decision(row, mode, root)
            if decision['dedup_sha'] and set(row['targets']) & set(NORMAL_DEDUP_TARGETS):
                raise ValueError('published dedup selection contains duplicate execution targets')
            if decision['dedup_sha'] and row['id'] == 'engine-family-1':
                compile_only = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', FEATURES]
                for target in NORMAL_DEDUP_TARGETS:
                    compile_only += ['--test', target]
                result.insert(0, compile_only + ['--no-run'])
        # Preserve the original PR feature tuples for relocated full fixtures,
        # as well as the complete original nightly-feature commands above.
        if mode == 'full':
            qualification = [t for t in row['targets'] if t in FULL_SCHEMA]
            if qualification or 'sync_schema_transactions' in row['targets']:
                companion = ['cargo', 'test', '--locked', NO_FAIL_FAST, '-p', 'contextdb-engine',
                             '--features', 'test-seams,iroh']
                for target in qualification or ['sync_schema_transactions']:
                    companion += ['--test', target]
                companion += ['--', '--test-threads=1']
                if not qualification:
                    companion += [OVERSIZED, '--exact']
                result.append(companion)
        for target in aggregates:
            aggregate = aggregate_module(root)
            command = ['cargo', 'test', '--locked', '-p', 'contextdb-engine',
                       '--features', metadata(row)['features'], '--test', target, '--', '--test-threads=1']
            for prefix in aggregate.skip_prefixes(root, target):
                command += ['--skip', prefix]
            aggregate.validate_command(root, target, command)
            result.append(command)
        for command in result:
            no_fail_fast_contract(command)
        return result
    if kind == 'release-acceptance':
        return [base + ['--release', '--test', row['targets'][0], '--', '--test-threads=1']]
    if kind == 'examples':
        return [base + ['--examples', '--no-run']]
    if kind == 'doctests':
        return [base + ['--doc', '--', '--test-threads=1']]
    if kind == 'workspace':
        return [['cargo', 'test', '--locked', '--workspace']]
    if kind == 'redb':
        target = Path(os.environ.get('CARGO_TARGET_DIR', str(root / 'target')))
        default = ['cargo', 'test', '--manifest-path', 'crates/contextdb-redb/Cargo.toml',
                   '--target-dir', str(target / 'redb'), '--locked']
        return [default] + [default + ['--features', features, '--test', name,
                                       '--', '--test-threads=1']
                            for name, features in REDB_FEATURED_TARGETS]
    if kind == 'production':
        return [base]
    if kind == 'production-release':
        # The mixed acceptance and sync targets require private observation
        # fixtures; the long-running target still proves the production layout.
        seams = base[:-1] + ['nightly-tests,test-seams']
        return [(seams if target in ('acceptance', 'acceptance_sync') else base)
                + ['--release', '--test', target] for target in ACCEPTANCE_TARGETS]
    raise ValueError('unknown command class')


def boolean(value):
    if value not in {'true', 'false'}:
        raise ValueError('integration mode must be true or false')
    return value == 'true'


def matrix(rows, run_default, mode=None, decision=None):
    mode = gate_mode(mode)
    decision = plan_decision(mode, run_default) if decision is None else validate_decision(decision, mode, run_default)
    return {'include': [dict(id=row['id'], name=('normal correctness' if mode == 'normal' else 'full qualification') + ' / ' + metadata(row)['name'], **decision)
                        for row in active_rows(rows, run_default, mode, decision)]}


ENVELOPE_FIELDS = {'kind', 'id', 'name', 'pr', 'integration'}
def pr_inventory(root=ROOT):
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    rows = manifest.get('pr_jobs')
    if manifest.get('schema_version') != 2 or not isinstance(rows, list) or len(rows) != 26:
        raise ValueError('invalid canonical PR inventory')
    ids = [row.get('shard') for row in rows if isinstance(row, dict)]
    if len(ids) != 26 or len(set(ids)) != 26:
        raise ValueError('missing or duplicate canonical PR job')
    parents = {row['shard']: row.get('cache_parent', row['shard']) for row in rows}
    expected = {
        'contextdb-engine-test-seams-4': {
            'contextdb-engine-test-seams-4', 'contextdb-engine-test-seams-13',
            'contextdb-engine-test-seams-14'},
        'contextdb-engine-test-seams-11': {
            'contextdb-engine-test-seams-11', 'contextdb-engine-test-seams-16'},
    }
    for parent, parts in expected.items():
        if {shard for shard, bound in parents.items() if bound == parent} != parts:
            raise ValueError('reviewed PR partition changed: ' + parent)
    if any(parent not in expected and parent != shard for shard, parent in parents.items()):
        raise ValueError('unreviewed PR cache parent binding')
    if len(set(parents.values())) != 23:
        raise ValueError('logical PR inventory changed')
    return rows


def combined_matrix(rows, enabled, decision, root=ROOT):
    validate_decision(decision, 'normal', False, root)
    envelopes = [dict(kind='pr', id='pr-' + row['shard'],
                      name='normal correctness / ' + row['name'], pr=row, integration=None)
                 for row in pr_inventory(root)]
    if enabled:
        envelopes += [dict(kind='integration', id='integration-' + row['id'],
                           name=row['name'], pr=None, integration=row)
                      for row in matrix(rows, False, 'normal', decision)['include']]
    validate_combined({'include': envelopes}, rows, enabled, decision, root)
    return {'include': envelopes}


NORMAL_BUNDLES_SHA256 = 'd03c10cdf5287b6da4072ae7a83a7024e8840e98e3dfdd8961b66e97da0e434c'
SLOT_FIELDS = ENVELOPE_FIELDS | {'cache_key'}


def _bundle_slots(value, enabled, decision, root=ROOT):
    """Expand the reviewed bundle layout without changing a canonical row payload."""
    validate_combined(value, rows=load_manifest(root), enabled=enabled, decision=decision, root=root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    definition = manifest.get('normal_bundles')
    if (not isinstance(definition, dict)
            or set(definition) != {'schema_version', 'bundles', 'fallback_additions'}
            or definition['schema_version'] != 1):
        raise ValueError('invalid normal bundle manifest')
    encoded = json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(encoded).hexdigest() != NORMAL_BUNDLES_SHA256:
        raise ValueError('normal bundle manifest differs from reviewed design')
    bundles = definition['bundles']
    additions = definition['fallback_additions']
    if (not isinstance(bundles, list) or len(bundles) != 16
            or any(not isinstance(bundle, list) or not 1 <= len(bundle) <= 4 for bundle in bundles)
            or any(not isinstance(row_id, str) for bundle in bundles for row_id in bundle)
            or not isinstance(additions, dict)
            or additions != {'integration-engine-family-10': 12,
                              'integration-engine-family-11': 12,
                              'integration-engine-family-13': 4}):
        raise ValueError('invalid reviewed normal bundle layout')
    planned = [list(bundle) for bundle in bundles]
    if enabled and not decision['dedup_sha']:
        for row_id, one_based_bundle in additions.items():
            planned[one_based_bundle - 1].append(row_id)
    if any(len(bundle) > 4 for bundle in planned):
        raise ValueError('normal bundle exceeds the four reviewed constituent slots')
    flattened = [row_id for bundle in planned for row_id in bundle]
    if len(flattened) != len(set(flattened)):
        raise ValueError('normal bundles contain duplicate canonical rows')
    by_id = {row['id']: row for row in value['include']}
    selected = [[by_id[row_id] for row_id in bundle if row_id in by_id] for bundle in planned]
    selected = [bundle for bundle in selected if bundle]
    expanded = [row for bundle in selected for row in bundle]
    if len(expanded) != len(by_id) or {row['id'] for row in expanded} != set(by_id):
        raise ValueError('normal bundles are not the exact canonical row union')
    return selected


def bundle_matrix(value, rows, enabled, decision, root=ROOT):
    validate_combined(value, rows, enabled, decision, root)
    result = []
    for index, bundle in enumerate(_bundle_slots(value, enabled, decision, root), 1):
        slots = []
        for envelope in bundle:
            key = ('test-' + envelope['pr'].get('cache_parent', envelope['pr']['shard'])
                   if envelope['kind'] == 'pr'
                   else 'integration-' + envelope['integration'].get(
                       'cache_parent', envelope['integration']['id'])) + '-debug0-jobs2-test'
            if envelope['kind'] == 'integration' and envelope['integration']['id'] in LONG_RUNNING_PARTITIONS:
                key = 'integration-acceptance-long-running-test-debug0-jobs2-test'
            slots.append(dict(envelope, cache_key=key))
        name = f'bundle {index:02d} / ' + ' + '.join(
            slot['name'].removeprefix('normal correctness / ') for slot in slots)
        result.append(dict(id=f'normal-bundle-{index:02d}', name=name,
                           slot1=slots[0], slot2=slots[1] if len(slots) > 1 else None,
                           slot3=slots[2] if len(slots) > 2 else None,
                           slot4=slots[3] if len(slots) > 3 else None))
    return {'include': result}


def validate_bundle_matrix(value, combined, rows, enabled, decision, root=ROOT):
    expected = bundle_matrix(combined, rows, enabled, decision, root)
    if value != expected:
        raise ValueError('published normal bundle matrix differs from canonical expansion')


def validate_combined(value, rows, enabled, decision, root=ROOT):
    # The outer normal envelope is intentionally empty for integration rows when
    # impact classifies them off, but its published decision is never optional.
    # Validate before deriving that empty partition so a stale/tampered plan
    # cannot be accepted merely because no slice will execute.
    validate_decision(decision, 'normal', False, root)
    if not isinstance(value, dict) or set(value) != {'include'} or not isinstance(value['include'], list):
        raise ValueError('invalid combined matrix envelope')
    expected_pr = {'pr-' + row['shard']: row for row in pr_inventory(root)}
    expected_integration = {row['id']: row for row in matrix(rows, False, 'normal', decision)['include']} if enabled else {}
    expected_ids = set(expected_pr) | {'integration-' + key for key in expected_integration}
    found = set()
    for envelope in value['include']:
        if not isinstance(envelope, dict) or set(envelope) != ENVELOPE_FIELDS or envelope['id'] in found:
            raise ValueError('invalid or duplicate combined matrix envelope')
        found.add(envelope['id'])
        if envelope['kind'] == 'pr' and envelope['id'] in expected_pr:
            row = expected_pr[envelope['id']]
            if envelope != dict(kind='pr', id=envelope['id'], name='normal correctness / ' + row['name'], pr=row, integration=None):
                raise ValueError('PR matrix payload drift')
        elif envelope['kind'] == 'integration' and envelope['id'].removeprefix('integration-') in expected_integration:
            row = expected_integration[envelope['id'].removeprefix('integration-')]
            if envelope != dict(kind='integration', id=envelope['id'], name=row['name'], pr=None, integration=row):
                raise ValueError('integration matrix payload drift')
        else:
            raise ValueError('unknown combined matrix envelope')
    if found != expected_ids:
        raise ValueError('combined matrix missing or extra jobs')


def normal_required(needs, rows, enabled, published, published_bundles, decision, root=ROOT):
    expected = {'impact', 'test-bundles'}
    if not isinstance(needs, dict) or set(needs) != expected:
        raise ValueError('normal validator needs exact impact and three dispatch jobs')
    if any(needs[name].get('result') != 'success' for name in expected):
        raise ValueError('normal planning or bundle execution did not succeed')
    validate_combined(published, rows, enabled, decision, root)
    validate_bundle_matrix(published_bundles, published, rows, enabled, decision, root)


def required(needs, rows, enabled, run_default, mode=None):
    mode = gate_mode(mode)
    if not isinstance(needs, dict) or set(needs) != {'plan', 'slice'} or needs['plan'].get('result') != 'success':
        raise ValueError('missing/failed/unexpected integration jobs')
    outputs = needs['plan'].get('outputs', {})
    decision = {key: outputs[key] for key in PLAN_FIELDS if key in outputs}
    validate_decision(decision, mode, run_default)
    if (outputs.get('enabled') != str(enabled).lower()
            or json.loads(outputs.get('matrix', 'null')) != matrix(rows, run_default, mode, decision)):
        raise ValueError('integration published plan does not match source/mode/decision inventory')
    if needs['slice'].get('result') != ('success' if enabled else 'skipped'):
        raise ValueError('required integration slices did not succeed (or unexpected skip)')


def workflow_contract(root=ROOT):
    """Narrow reviewed YAML shape; offline controls independently parse and mutate it."""
    text = (root / '.github/workflows/integration.yml').read_text()
    for workflow in ['ci.yml', 'integration.yml', 'rc2-tests.yml']:
        workflow_text = (root / '.github/workflows' / workflow).read_text()
        if re.search(r'^\s+CARGO_PROFILE_TEST_(OPT_LEVEL|DEBUG_ASSERTIONS|OVERFLOW_CHECKS):', workflow_text, re.M):
            raise ValueError('test profile must come from Cargo.toml: ' + workflow)
    expected_jobs = ['plan', 'slice', 'result']
    if re.findall(r'^  ([\w-]+):$', text.split('\njobs:\n', 1)[1], re.M) != expected_jobs:
        raise ValueError('integration workflow job inventory changed')
    required_snippets = [
        '    needs: plan\n    if: needs.plan.outputs.enabled == \'true\'\n',
        '      fail-fast: false\n      matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}',
        '    name: ${{ matrix.name }}', "    timeout-minutes: ${{ needs.plan.outputs.gate_mode == 'normal' && 30 || 330 }}",
        '          CI_INTEGRATION_SLICE: ${{ matrix.id }}',
        "  CI_INTEGRATION_ENABLED: ${{ github.event_name == 'workflow_dispatch' || inputs.enabled }}",
        '  CI_INTEGRATION_DEFAULT: ${{ inputs.run_default }}',
        '  CI_GATE_MODE: ${{ inputs.gate_mode }}',
        '      gate_mode: ${{ steps.plan.outputs.gate_mode }}',
        '      dedup_sha: ${{ steps.plan.outputs.dedup_sha }}',
        '      dedup_reason: ${{ steps.plan.outputs.dedup_reason }}',
        '      source_sha: ${{ steps.plan.outputs.source_sha }}',
        '      source_fingerprint: ${{ steps.plan.outputs.source_fingerprint }}',
        '          CI_INTEGRATION_PLAN: ${{ toJSON(matrix) }}',
        '      matrix: ${{ steps.plan.outputs.matrix }}',
        '      enabled: ${{ steps.plan.outputs.enabled }}',
        '      run_default: ${{ steps.plan.outputs.run_default }}',
        '        run: bash scripts/verify integration "${CI_INTEGRATION_SLICE}"',
        '    needs: [plan, slice]\n    if: always()\n',
        '          CI_INTEGRATION_NEEDS: ${{ toJSON(needs) }}',
        '        run: python3 -B scripts/ci-integration.py required',
        '        run: python3 -B scripts/ci-integration.py plan',
        "        if: inputs.path_classes_artifact != ''",
        "          PATH_CLASSES_FILE: ${{ inputs.path_classes_artifact != '' && format('{0}/ci-impact/path-classes.json', runner.temp) || '' }}",
        "    if: always()\n    runs-on: ubuntu-latest",
    ]
    for snippet in required_snippets:
        if text.count(snippet) != 1:
            raise ValueError('integration workflow lost guarded routing: ' + snippet)
    if ('continue-on-error:' in text or 'max-parallel:' in text
            or len(re.findall(r'^    if:', text, re.M)) != 2
            or len(re.findall(r'^        if:', text, re.M)) != 2):
        raise ValueError('unsupported integration failure/concurrency override')
    spec = importlib.util.spec_from_file_location('integration_required', root / 'scripts/ci-required.py')
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    top_permissions = re.search(r'^permissions:\n(.*?)(?=^\S)', text, re.M | re.S)
    if top_permissions is None or guard.block_fields(top_permissions[1], 2) != {'contents': 'read\n'}:
        raise ValueError('integration must remain read-only')
    fields = {'plan': {'name', 'runs-on', 'timeout-minutes', 'outputs', 'steps'},
              'slice': {'needs', 'if', 'name', 'runs-on', 'timeout-minutes', 'strategy', 'steps'},
              'result': {'name', 'needs', 'if', 'runs-on', 'timeout-minutes', 'steps'}}
    for name in expected_jobs:
        block = re.search(r'^  ' + name + r':\n(.*?)(?=^  [\w-]+:|\Z)', text, re.M | re.S)[1]
        if set(guard.block_fields(block, 4)) != fields[name]:
            raise ValueError('unsupported integration job execution metadata: ' + name)
    runs = re.findall(r'^        run: (.*)$', text, re.M)
    if runs != ['python3 -B scripts/ci-integration.py plan', 'bash scripts/ci-free-disk.sh',
                'sudo apt-get update && sudo apt-get install -y ripgrep',
                'bash scripts/verify integration "${CI_INTEGRATION_SLICE}"',
                'python3 -B scripts/ci-integration.py required']:
        raise ValueError('integration command execution changed')
    if (re.findall(r'^\s+shell: (.*)$', text, re.M) != ['bash']
            or text.count('  run:\n    shell: bash') != 1):
        raise ValueError('integration shell override')
    verify = (root / 'scripts/verify').read_text()
    if verify.count('exec python3 -B scripts/ci-integration.py run "$@"') != 1:
        raise ValueError('integration entrypoint delegation changed')


def capture(command, env, log):
    start = time.monotonic()
    with log.open('wb') as output:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for block in iter(lambda: process.stdout.read1(65536), b''):
            output.write(block)
            sys.stdout.buffer.write(block)
            sys.stdout.buffer.flush()
        status = process.wait()
    return status, time.monotonic() - start


def slice_status_contract(ran, planned, status):
    """Every planned command of a slice runs, and the slice fails if any one of them failed."""
    if len(ran) != planned:
        raise ValueError(f'integration slice stopped after a failing command: ran {len(ran)} of {planned}')
    failed = [command['exit_status'] for command in ran if command['exit_status']]
    if (status == 0) != (not failed) or (failed and status != failed[0]):
        raise ValueError('integration slice status does not report its failed command')


def run_slice(row, out, mode=None):
    mode = gate_mode(mode)
    out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    try:
        runtime_decision(row, mode)
    except (ValueError, KeyError, TypeError) as error:
        (out / 'receipt.json').write_text(json.dumps(dict(exit_status=1, gate_mode=mode, slice=row,
            plan_decision=row.get('plan_decision'), refusal_reason=str(error), commands=[], targets=[]), indent=2) + '\n')
        return 1
    env = dict(os.environ, RUSTFLAGS='-D warnings')
    target = Path(env.get('CARGO_TARGET_DIR', str(ROOT / 'target')))
    if not target.is_absolute():
        target = ROOT / target
    env['CARGO_BIN_EXE_contextdb'] = str(target / 'debug/contextdb')
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain=v1', '-z'], cwd=ROOT)
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--binary', '--no-ext-diff'], cwd=ROOT)
    toolchain = subprocess.check_output(['rustc', '-Vv'], cwd=ROOT, text=True)
    receipt = dict(schema_version=1, gate_mode=mode,
                   plan_decision=row['plan_decision'],
                   normal_dedup=dict(sha256=row['plan_decision']['dedup_sha'], reason=row['plan_decision']['dedup_reason'],
                       fallback_targets=[] if row['plan_decision']['dedup_sha'] else list(NORMAL_DEDUP_TARGETS)) if mode == 'normal' else None,
                   dedup_execution_owner='PR test-seams,iroh' if mode == 'normal' else None, source_sha=sha, source_dirty=bool(dirty),
                   relocated_cases=([dict(case=OVERSIZED, destination="full qualification; both feature tuples")]
                                    if mode == "normal" and "sync_schema_transactions" in row["targets"] else []),
                   source_status_sha256=hashlib.sha256(dirty).hexdigest(),
                   source_diff_sha256=hashlib.sha256(diff).hexdigest(), toolchain=toolchain,
                   slice=row, companion_features=('test-seams,iroh' if mode == 'full' and
                       (set(row['targets']) & FULL_SCHEMA or 'sync_schema_transactions' in row['targets']) else None),
                   **metadata(row), rustflags=env['RUSTFLAGS'],
                   hosted_job_elapsed_seconds=None, hosted_queue_seconds=None,
                   hosted_timing_status='pending-terminal-Actions-job-metadata',
                   timing_boundary='local runner entry through receipt generation; excludes hosted checkout/toolchain/cache/artifacts',
                   commands=[], targets=[], counts_status='pending', exit_status=1)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT)
    receipt['untracked_source_sha256'] = {
        os.fsdecode(path): hashlib.sha256((ROOT / os.fsdecode(path)).read_bytes()).hexdigest()
        for path in untracked.split(b'\0') if path and (ROOT / os.fsdecode(path)).is_file()}
    receipt['fixture_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                 for path in sorted((ROOT / 'crates').glob('*/tests/**/*')) if path.is_file()}
    receipt['integration_manifest_sha256'] = hashlib.sha256((ROOT / 'scripts/ci-integration-targets.json').read_bytes()).hexdigest()
    receipt['hosted_identity'] = {key: os.environ.get(key) for key in [
        'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_REPOSITORY']}
    module = inventory_module(ROOT)
    status = 1
    try:
        build = ['cargo', 'build', '--locked', '-p', 'contextdb-cli', '-p', 'contextdb-server']
        status, seconds = capture(build, env, out / 'build.log')
        receipt['binary_build_seconds'] = seconds
        if status == 0:
            # Run the whole slice: a failing command must not hide the reds of the
            # commands after it (its aggregate and companion commands included).
            planned = commands(row, mode=mode)
            failures = []
            for index, command in enumerate(planned):
                log = out / (f'{index}.doc.log' if row['kind'] == 'doctests' else f'{index}.log')
                command_status, seconds = capture(command, env, log)
                receipt['commands'].append(dict(argv=command, exit_status=command_status, cargo_command_seconds=seconds))
                entries = module.timing_receipt([log], metadata(row)['test_threads'])['targets']
                actual_features = command[command.index('--features') + 1] if '--features' in command else ''
                for entry in entries:
                    entry.update(features=actual_features, command_index=index)
                receipt['targets'].extend(entries)
                if command_status == 0 and mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
                    long_running_counts(entries, log, row['id'])
                if command_status == 0 and '--no-run' not in command and 'sync_schema_transactions' in row['targets']:
                    sync_routing_counts(entries, mode, companion=index > 0)
                if command_status:
                    failures.append(command_status)
            status = failures[0] if failures else 0
            slice_status_contract(receipt['commands'], len(planned), status)
    except BaseException:
        status = status or 1
        raise
    finally:
        receipt['exit_status'] = status
        receipt['runner_elapsed_seconds'] = time.monotonic() - start
        receipt['counts_status'] = 'completed-target-summaries-only; incomplete/custom-harness counts unknown'
        for entry in receipt['targets']:
            entry['declared_count'] = sum(entry[k] for k in ['passed', 'failed', 'ignored', 'measured', 'filtered_out'])
            entry['runnable_count'] = entry['passed'] + entry['failed']
        (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        lines = [f'### {metadata(row)["name"]}', '',
                 '| Target | Profile | Features | Declared | Runnable | Cargo test seconds | Hosted job / queue seconds |',
                 '|---|---|---|---:|---:|---:|---|']
        for entry in receipt['targets']:
            lines.append(f'| {entry["target"]} | {receipt["profile"]} | {entry.get("features", receipt["features"]) or "Cargo defaults"} | '
                         f'{entry["declared_count"]} | {entry["runnable_count"]} | {entry["duration_seconds"]} | pending / pending |')
        if not receipt['targets']:
            lines.append(f'| {row["targets"] or row["kind"]} | {receipt["profile"]} | {receipt["features"] or "Cargo defaults"} | unknown | unknown | not available | pending / pending |')
        lines += ['', f'Gate mode: {mode}. Relocated cases: {receipt["relocated_cases"]}.', f'Exit: {status}. Runner seconds: {receipt["runner_elapsed_seconds"]:.3f}.',
                  receipt['timing_boundary'], receipt['counts_status'], '']
        (out / 'summary.md').write_text('\n'.join(lines))
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                summary.write('\n'.join(lines))
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'required', 'normal-required', 'check', 'run', 'shard-plan', 'shard-receipt', 'dedup-targets', 'dedup-snapshot', 'regenerate-dedup', 'aggregate-skips'])
    parser.add_argument('--aggregate-features')
    parser.add_argument('slice', nargs='?')
    args = parser.parse_args()
    if args.action not in {'run', 'shard-plan', 'shard-receipt', 'dedup-snapshot', 'regenerate-dedup', 'aggregate-skips'} and args.slice:
        raise ValueError('slice selection is only supported by run')
    if args.action == 'dedup-targets':
        print(' '.join('--test ' + t for t in NORMAL_DEDUP_TARGETS))
        return 0
    if args.action in {'dedup-snapshot', 'regenerate-dedup'}:
        directory = Path(args.slice)
        if args.action == 'dedup-snapshot':
            directory.mkdir(parents=True, exist_ok=True)
            identity = dict(applicability=dedup_snapshot(ROOT), sources={t: literal_closure(ROOT, t) for t in NORMAL_DEDUP_TARGETS})
            (directory / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')
        else:
            regenerate_dedup(directory)
        return 0
    mode = gate_mode()
    rows = load_manifest()
    if args.action == 'aggregate-skips':
        aggregate = aggregate_module()
        if frozenset((args.aggregate_features or '').split(',')) not in aggregate.PROFILES:
            raise ValueError('aggregate shard feature envelope changed')
        if args.slice == aggregate.FULL_ONLY_RULE['full_skip_owner'] and mode != 'full':
            raise ValueError('normal schema replay must retain the main namespace')
        for prefix in aggregate.skip_prefixes(ROOT, args.slice):
            print(prefix)
        return 0
    if args.action == 'shard-receipt':
        sync_routing_counts(json.loads(Path(args.slice).read_text())['targets'], mode)
        return 0
    if args.action == 'shard-plan':
        lines = (ROOT / 'scripts/ci-shard-targets.txt').read_text().splitlines()
        matches = [line.split(': ', 1)[1].split() for line in lines
                   if line.startswith(str(args.slice) + ': ')]
        if len(matches) != 1:
            raise ValueError('unknown/duplicate split shard')
        targets = [t for t in matches[0] if mode == 'full' or t not in FULL_SCHEMA | FULL_ONLY_TARGETS]
        if not targets:
            raise ValueError('routing must not produce an empty shard')
        skip = OVERSIZED if mode == 'normal' and 'sync_schema_transactions' in targets else ''
        print(' '.join(targets) + '|' + skip)
        return 0
    workflow_contract()
    enabled = boolean(os.environ.get('CI_INTEGRATION_ENABLED', 'true'))
    run_default = boolean(os.environ.get('CI_INTEGRATION_DEFAULT', 'true'))
    if args.action == 'check':
        print(json.dumps(matrix(rows, run_default, mode), indent=2))
    elif args.action == 'plan':
        decision = plan_decision(mode, run_default)
        integration_matrix = matrix(rows, run_default, mode, decision)
        outputs = dict(matrix=json.dumps(integration_matrix, separators=(',', ':')),
                       enabled=str(enabled).lower(), **decision)
        if mode == 'normal':
            combined = combined_matrix(rows, enabled, decision)
            bundles = bundle_matrix(combined, rows, enabled, decision)
            outputs['combined_matrix'] = json.dumps(combined, separators=(',', ':'))
            outputs['bundle_matrix'] = json.dumps(bundles, separators=(',', ':'))
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            for key, value in outputs.items():
                output.write(f'{key}={value}\n')
        if not enabled:
            reason = os.environ.get('IMPACT_REASON', '')
            if not reason:
                raise ValueError('inactive integration requires a classified skip reason')
            print('Extended integration deliberately not required: ' + reason)
            classes_file = os.environ.get('PATH_CLASSES_FILE')
            classes = json.loads(Path(classes_file).read_text()) if classes_file else {}
            print('Path classes: ' + json.dumps(classes, ensure_ascii=True))
            qualification = os.environ.get('PERFORMANCE_QUALIFICATION', '')
            label = 'not affected' if qualification == 'unaffected' else 'measurement pending'
            print('Performance measurement: ' + label + ' (not a qualification and not a required gate)')
            print('All original PR checks remain required.')
            if os.environ.get('GITHUB_STEP_SUMMARY'):
                with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
                    summary.write('Extended integration deliberately not required: ' + reason + '\n')
    elif args.action == 'required':
        required(json.loads(os.environ['CI_INTEGRATION_NEEDS']), rows, enabled, run_default, mode)
        print('All required integration slices succeeded' if enabled else 'Classified integration skip; original PR checks remain required')
    elif args.action == 'normal-required':
        if mode != 'normal' or run_default:
            raise ValueError('normal validator requires normal/false')
        decision = {key: os.environ[key.upper()] for key in PLAN_FIELDS}
        normal_required(json.loads(os.environ['CI_INTEGRATION_NEEDS']), rows, enabled,
                        json.loads(os.environ['CI_COMBINED_MATRIX']),
                        json.loads(os.environ['CI_BUNDLE_MATRIX']), decision)
        print('Normal mixed matrix and published routing succeeded')
    else:
        import tempfile
        base = Path(os.environ.get('CI_INTEGRATION_OUT', str(ROOT / 'target/ci-integration-out')))
        base.mkdir(parents=True, exist_ok=True)
        invocation = Path(tempfile.mkdtemp(prefix='run-', dir=base))
        try:
            raw = os.environ.get('CI_INTEGRATION_PLAN')
            if not raw and os.environ.get('GITHUB_ACTIONS') == 'true':
                raise ValueError('hosted slice missing published matrix decision')
            published = json.loads(raw) if raw is not None else None
            decision = {key: published[key] for key in PLAN_FIELDS if key in published} if published is not None else plan_decision(mode, run_default)
            validate_decision(decision, mode, run_default)
            if published is not None:
                expected = [x for x in matrix(rows, run_default, mode, decision)['include'] if x['id'] == args.slice]
                if expected != [published]:
                    raise ValueError('slice matrix source/mode/SHA/inventory does not match published decision')
            selected = active_rows(rows, run_default, mode, decision)
        except (ValueError, KeyError, TypeError) as error:
            (invocation / 'receipt.json').write_text(json.dumps(dict(exit_status=1, refusal_reason=str(error), commands=[], targets=[]), indent=2) + '\n')
            return 1
        if args.slice:
            selected = [row for row in selected if row['id'] == args.slice]
            if not selected:
                raise ValueError('unknown or inactive integration slice')
        if not enabled:
            raise ValueError('run cannot silently skip disabled integration')
        statuses = [run_slice(row, invocation / row['id'], mode) for row in selected]
        return next((status for status in statuses if status), 0)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f'::error::integration: {error}', file=sys.stderr)
        sys.exit(1)
