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


# Normal mode relocates these slow cases to full qualification and nightly, which run every case;
# the faster variants of the same promises stay in every gate.
NORMAL_RELOCATED = {
    'sync_schema_transactions': OVERSIZED,
    'bounded': ('bounded_pull_execution_contract::'
                'unrestricted_graph_traversal_obeys_request_budget_beyond_legacy_visited_ceiling'),
    'integration': 'gate_a_primitives::a3_11_bfs_max_visited_limit',
}


def relocated_cases(targets, mode):
    return [NORMAL_RELOCATED[t] for t in targets if mode == 'normal' and t in NORMAL_RELOCATED]


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

    ('crates/contextdb-engine/tests/engine_seams/outbound_selection_regressions.rs', 'an_excluded_body_larger_than_the_push_budget_does_not_charge_the_push'): 'post-extraction push consumers reread excluded bodies',
    ('crates/contextdb-engine/src/database/event_bus/received_purge_discard_tests.rs', 'received_purge_and_public_discard_finish_owned'): 'owned host cannot apply pulled rows',
    ('crates/contextdb-engine/src/database/event_bus/received_purge_discard_tests.rs', 'received_purge_progresses_under_healthy_checkout_renewal_owned'): 'owned host cannot apply pulled rows',
    ('crates/contextdb-engine/src/database/received_legacy_growth_guards.rs', 'production_legacy_selected_population_2n_succeeds_with_fixed_memory'): 'reader budget grows with rows',
    ('crates/contextdb-engine/src/database/received_legacy_growth_guards.rs', 'production_legacy_selected_population_4n_succeeds_with_fixed_memory'): 'reader budget grows with rows',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_durable_failure_returns_all_writer_custody'): 'deep refusal retains writer custody',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_deep_budget_refusal_returns_all_writer_custody_and_retries_live'): 'the full lifecycle requires extended execution',
    ('crates/contextdb-engine/tests/engine_seams/a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses.rs', 'a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses'): 'an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal',
    ('crates/contextdb-engine/tests/engine_seams/a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses.rs', 'a_destination_reupload_resends_what_the_edge_held_and_holds_none_of_what_it_suppresses_as_received_history_grows'): 'an edge that pulled schema cannot re-upload after a move; the companion row pins the refusal',
    ('crates/contextdb-engine/tests/sync/schema_migration_chunked_transport.rs', 'oversized_authenticated_schema_stays_invisible_until_one_commit_and_retry'): 'oversized schema pull funding remains incomplete',
    ('crates/contextdb-engine/tests/read/reader_continuity_contract.rs', 'schema_replay_does_not_interrupt_active_readers'): 'requires a maintained vector index, which this release does not ship',
    ('crates/contextdb-engine/tests/vector/indexed_route_does_less_work_than_exact.rs', 'indexed_route_does_less_work_than_exact'): 'indexed search calibration remains incomplete',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'in_memory_construction_leaves_same_partition_vector_writes_usable'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'in_memory_construction_leaves_new_partition_vector_writes_usable'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/vector_resource_maintenance_contract.rs', 'changing_the_poll_interval_returns_while_engine_maintenance_is_active'): 'vector index construction remains disabled',
    ('crates/contextdb-engine/tests/vector/a_filtered_scope_is_charged_for_its_candidates.rs', 'historical_vector_placement_survives_pruning_and_reconstruction_until_versions_reclaim'): 'Reclaiming obsolete versions does not give back the memory their durable partition registry owners held.',
    ('crates/contextdb-engine/tests/vector/flat_route_below_declared_threshold.rs', 'a_refused_exact_route_admission_leaves_no_charge_and_no_partial_answer'): 'A refused exact search does not name the route-slot admission it was refused at.',
    ('crates/contextdb-engine/tests/vector/row_delete_and_guards_honor_vector_columns.rs', 'a_non_vector_guard_commit_loads_no_unrelated_vector_body'): 'A guard commit on a non-vector column still decodes an unrelated stored vector body.',
    ('crates/contextdb-engine/tests/vector/vector_inspection_explain_truth_contract.rs', 'restricted_explain_admits_only_visible_identities_at_equal_headroom'): "A restricted explain still reveals the real memory limit instead of the caller's headroom.",
    ('crates/contextdb-engine/tests/vector/passive_explain_predicts_the_executed_auto_route.rs', 'passive_explain_predicts_a_filtered_resident_auto_route'): 'A passive explain names a different exact-route reason than the executed query reports.',
    ('crates/contextdb-engine/tests/vector/vector_memory_ownership_reconciliation_contract.rs', 'long_typed_key_move_and_snapshot_retirement_admit_before_mutation_and_release_once'): 'A refused one-byte-short key update leaves its reservation charged instead of releasing every owner.',
    ('crates/contextdb-engine/tests/vector/vector_memory_ownership_reconciliation_contract.rs', 'received_image_replacement_is_idempotent_and_its_prepublication_failure_preserves_receipt'): 'Dropping the last received-image handle leaves the process memory aggregate far above its starting value.',
    ('crates/contextdb-engine/tests/vector/vector_memory_ownership_reconciliation_contract.rs', 'retained_journey_scalar_publications_bound_unique_payloads_and_admit_a_small_write'): 'Reading stored index entries fails because the engine decode requires an operation memory owner.',
    ('crates/contextdb-engine/tests/vector/vector_partition_query_contract.rs', 'negative_zero_does_not_over_match_and_two_pow_53_does_not_under_match'): 'Plain equality on a numeric partition key treats a negative-zero literal as bucket zero.',
    ('crates/contextdb-engine/tests/vector/vector_partition_query_contract.rs', 'key_aligned_restriction_narrows_prefixes_with_no_relational_scan'): 'A key-aligned partition restriction does more access checks than the selected row needs.',
    ('crates/contextdb-engine/tests/vector/vector_partition_sync_inspection_contract.rs', 'a_refused_lowered_partition_limit_holds_the_tenants_pull_until_its_use_falls'): "A synced lowered partition limit below the edge's use is applied instead of being refused.",
    ('crates/contextdb-engine/tests/vector/vector_partition_sync_inspection_contract.rs', 'show_vector_partitions_supports_all_sql_forms'): 'SHOW VECTOR_PARTITIONS answers differently across the SQL forms it supports.',
    ('crates/contextdb-engine/tests/vector/vector_policy_mixed_version_contract.rs', 'current_transport_holds_missing_capabilities_until_the_receiver_gains_them'): 'A current-version transport applies a vector policy table to a receiver that lacks the capability instead of holding it back.',
    ('crates/contextdb-engine/tests/vector/vector_schema_mixed_version_recovery_cursor_contract.rs', 'cut_last_held_back_page_then_upgrade_recovers_without_skipping_ordinary_or_graph_work'): 'After a cut and an upgrade the next exchange does not replay the graph edge exactly once.',
    ('crates/contextdb-engine/tests/vector/vector_schema_mixed_version_recovery_cursor_contract.rs', 'held_table_recovery_keeps_ordinary_rows_and_graph_edges_exactly_once_without_cursor_skip'): 'Recovery of a held table delivers its graph edge a different number of times than exactly once.',
    ('crates/contextdb-engine/tests/vector/vector_search_mode_bounded_contract.rs', 'a_filtered_search_resolves_one_mode_on_every_door'): 'An exact-route vector query publishes no route trace.',
    ('crates/contextdb-engine/tests/vector/vector_search_mode_bounded_contract.rs', 'concurrent_direct_exact_scores_hold_their_charge_through_result_consumption'): 'Concurrent exact scores do not stay charged to their owner through result consumption.',
    ('crates/contextdb-engine/tests/vector/vector_search_mode_bounded_contract.rs', 'explain_reports_the_route_forced_exact_mode_the_query_actually_took'): 'An executed exact-mode query publishes no vector disclosure.',
    ('crates/contextdb-engine/tests/vector/vector_transaction_filter_snapshot_truth_contract.rs', 'database_late_lower_commit_keeps_unique_vectors_across_snapshots_restart_and_sync'): 'Reading stored index entries fails because the engine decode requires an operation memory owner.',
    ('crates/contextdb-engine/tests/vector/vector_transaction_filter_snapshot_truth_contract.rs', 'rust_semantic_filter_merges_staged_matches_and_removals_before_vector_scoring'): 'A committed filter source reports no relational index entries touched.',
    ('crates/contextdb-engine/tests/vector/vector_transaction_filter_snapshot_truth_contract.rs', 'filters_choose_only_allowed_work_and_name_the_same_budget_refusal_on_every_door'): 'An ordinary SQL read does not report the one equality posting it reads.',
    ('crates/contextdb-cli/tests/read_cli_journeys_ordinary_results.rs', 'a_context_grouped_nearest_neighbour_answers_its_own_tuple_not_a_closer_foreign_one'): 'The CLI reports a refused USE VECTOR INDEXED query without its typed vector_indexed_route_unavailable error kind.',
    ('tests/integration/indexed_scan_filter_tests.rs', 'drop_table_unregisters_all_indexes_and_releases_storage'): 'Index entry introspection reports zero entries for a table with two populated indexes.',
    ('tests/integration/version_cleanup_scoping_tests.rs', 'version_cleanup_releases_a_pruned_rows_vector_copy'): 'Physical vector entry introspection reports zero entries for a store whose vectors are file-resident.',
    ('crates/contextdb-engine/src/database/currency_version_compaction_tests.rs', 'compaction_bounds_versions_and_change_log'): 'Physical version and change-log introspection report zero for a pinned table after updates.',
    ('crates/contextdb-engine/src/database/coexistence_guards.rs', 'turnover_deadline'): 'A store turnover with a deadline does not return while another holder keeps the store held.',
    ('crates/contextdb-engine/tests/read/raw_vector_on_demand_source_contract.rs', 'mixed_file_reads_point_load_exact_vectors_and_keep_their_snapshot_after_recycle'): 'Storage reclamation after a reader handle recycle intermittently fails with a reader-coordination contention refusal.',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'native_non_route_values_are_not_read_while_healthy_routes_keep_serving'): 'These tests need the clustered vector index, which this release does not ship, so their native fixture never reports ready.',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'public_cycle_cold_catalog_refusal_keeps_serving_and_closes'): 'These tests need the clustered vector index, which this release does not ship, so their native fixture never reports ready.',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'public_cycle_cold_covered_cleanup_refusal_keeps_serving_and_closes'): 'These tests need the clustered vector index, which this release does not ship, so their native fixture never reports ready.',
    ('crates/contextdb-engine/src/database/vector_public_cycle_admission_tests.rs', 'public_cycle_cold_journal_refusal_keeps_serving_and_closes'): 'These tests need the clustered vector index, which this release does not ship, so their native fixture never reports ready.',
    ('tests/integration/transactional_callback_delivery.rs', 'crash_during_external_delivery_recovers_uncertainty_before_retry'): 'A read-only open of a store left unclean by a crash during an external delivery succeeds instead of requiring writable recovery first.',
    ('crates/contextdb-engine/tests/engine_seams/custody_reads_admit_every_buffer_before_allocating.rs', 'custody_reads_admit_every_buffer_before_allocating'): 'An intermittent custody read refusal leaves its key list holding bytes that were never admitted against the reader limit.',
    ('crates/contextdb-engine/tests/read/authoritative_purge_reader_publication.rs', 'stopped_direct_publication_releases_on_process_death_without_holding_commit_lock'): 'An authoritative purge is refused as contended instead of waiting when an ordinary commit runs at the same moment.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'many_pending_deletions_under_a_tight_limit_are_refused_before_they_are_built'): 'A pending-deletions history request under a tight limit is refused as a corrupt store with a malformed lineage record instead of a typed memory refusal.',
    ('crates/contextdb-engine/tests/vector/writer_residency_attribution.rs', 'concurrent_deletes_retire_one_staged_admission'): 'concurrent deletes hang retiring one staged admission',
    ('crates/contextdb-redb/src/db.rs', 'measure_the_checked_allocator_save_at_the_minimum_window_budget'): 'allocator paging measurement runs only when explicitly requested',
    ('crates/contextdb-redb/src/tree_store/bounded_bytes.rs', 'insert_after_separator_window_refuses_before_read_and_releases_its_owner'): 'bounded-read refusal and owner-release assertion fails',
    ('crates/contextdb-redb/src/tree_store/page_store/cached_file.rs', 'settle_rebuilds_exact_read_index_and_preserves_above_cap_held_pages'): 'read-index settlement assertion fails',
    ('crates/contextdb-redb/src/tree_store/page_store/page_manager.rs', 'measure_trim_and_install_paging'): 'allocator paging measurement runs only when explicitly requested',
    ('crates/contextdb-engine/tests/bounded/bounded_access_gate_accounting.rs', 'a_gated_relational_read_charges_the_grant_table_once_not_once_per_row'): 'a gated read charges the grant table once for every candidate row instead of once for the whole read',
    ('crates/contextdb-engine/tests/bounded/bounded_growth_reservation_contract.rs', 'a_traversal_that_accumulates_edges_grows_its_state_in_amortized_steps'): 'a traversal that accumulates edges asks for its memory growth one slot at a time instead of in amortized steps',
    ('crates/contextdb-engine/tests/bounded/bounded_memory_refusal_typing.rs', 'a_read_that_exhausts_the_database_memory_budget_preserves_the_store_refusal'): 'a traversal read that cannot fit in the database memory budget completes instead of being refused',
    ('crates/contextdb-engine/tests/bounded/bounded_read_sees_the_same_transaction_graph_overlay.rs', 'a_bounded_probe_inside_an_open_transaction_hides_the_edge_that_transaction_removed'): 'deleting an edge by its endpoints removes nothing when the edge was created by an SQL insert',
    ('crates/contextdb-engine/src/persistence/proof_allocation_oracle_tests.rs', 'a_cross_thread_free_is_attributed_only_to_its_allocation_window'): 'the allocation provenance check accepts a changed payer and an address reused without evidence of its free',
    ('crates/contextdb-engine/tests/owner_read/owner_read_service_contract.rs', 'six_typed_request_limits_and_active_timeout_use_their_frozen_boundaries'): 'a read whose work finishes exactly at the active time limit is refused instead of succeeding',
    ('crates/contextdb-engine/tests/reader_release/a_purge_during_hydration_is_never_served.rs', 'a_purge_after_an_owner_drain_is_never_served_to_the_owner_route_reader'): 'a reader that survived an owner drain still serves a table after a committed purge erased it',
    ('crates/contextdb-engine/tests/reader_release/a_writable_open_proceeds_while_a_reader_holds_its_snapshot.rs', 'a_writable_open_proceeds_while_a_healthy_reader_holds_its_snapshot'): 'a reader holding a snapshot sees rows that a writer commits after opening the store',
    ('crates/contextdb-engine/tests/reader_release/a_writer_open_waits_once_for_a_starting_reader.rs', 'a_default_open_waits_then_opens'): 'a reader that was acquiring when a writer arrived sees the rows the writer commits afterwards',
    ('crates/contextdb-engine/tests/reader_release/a_writer_open_waits_once_for_a_starting_reader.rs', 'a_reader_paused_before_its_decode_does_not_hold_the_writer'): 'a reader that was acquiring when a writer arrived sees the rows the writer commits afterwards',
    ('crates/contextdb-engine/tests/reader_release/a_writer_open_waits_once_for_a_starting_reader.rs', 'a_writer_arriving_during_a_reader_s_acquisition_waits_once_and_a_late_reader_routes_behind_it'): 'a reader that was acquiring when a writer arrived sees the rows the writer commits afterwards',
    ('crates/contextdb-engine/tests/reader_release/a_writer_open_waits_once_for_a_starting_reader.rs', 'a_writer_completes_only_after_the_last_of_two_readers_leaves'): 'a reader is refused as not serving while a writer holds the store and has not yet published a serving decision',
    ('crates/contextdb-engine/tests/writecost_cycle_bound.rs', 'without_a_cap_one_cycle_finishes_a_backlog_that_the_cap_split'): 'one uncapped maintenance cycle leaves more than 128 entries of a parked reclaim backlog unreclaimed',
    ('crates/contextdb-engine/tests/writecost_reclaim.rs', 'an_idle_tail_drains_through_maintenance_cycles_alone'): 'maintenance cycles alone do not drain an idle reclaim tail down to the cycle own bookkeeping',
    ('tests/acceptance/multi_table_atomic_visibility.rs', 't21_01_concurrent_reader_never_sees_torn_three_paradigm_commit'): 'the visibility proof reads a snapshot beyond the captured frontier and the engine now refuses that read',
    ('tests/acceptance/schema_lifecycle.rs', 'f23_schema_mismatch_between_edge_and_server_is_detected'): 'pushing a mismatched schema from an edge to a server prints neither a schema mismatch nor a migration notice',
    ('tests/integration/bounded_tables_tests.rs', 'c2_one_maintenance_cycle_prunes_retained_rows_and_compacts_currency_versions'): 'a maintenance cycle that collapses superseded row versions does not lower the memory charge',
    ('tests/integration/bounded_tables_tests.rs', 'c7_compaction_rides_the_same_fragmentation_threshold_as_the_currency_path'): 'pruning every large row does not leave the file dead space dominated enough to trigger compaction',
    ('tests/integration/change_log_table_index_consistency_tests.rs', 'the_index_matches_the_change_log_after_ordinary_appends'): 'the change log table index drifts from the entries of the real change log',
    ('tests/integration/compaction_separation_tests.rs', 'the_automatic_compaction_path_is_interval_gated_not_per_cycle'): 'a churned file does not become dead space dominated enough for the first maintenance cycle to compact it',
    ('tests/integration/composite_foreign_key_tests.rs', 'ddl_sync_descriptor_comparator_ignores_irrelevant_whitespace'): 'a structured foreign key mismatch received over sync is reported as a conflict instead of failing the apply',
    ('tests/integration/ddl_sync_tests.rs', 'ds10d_sync_alter_unique_name_collision_rebuilds_auto_index_storage'): 'a synced unique index rename leaves a durable index posting with two key parts where its declaration has one',
    ('tests/integration/indexed_scan_filter_tests.rs', 'ord08_tie_break_row_id_ascending_cross_peer_byte_identical'): 'two edges that applied the same changes answer an indexed equality read with different row orders',
    ('tests/integration/retention_tests.rs', 'mr4_pruning_under_concurrent_read'): 'a retention cycle counts 32 rows as deferred to a live reader instead of all 100 expired rows',
    ('tests/integration/retention_tests.rs', 'retention_defers_physical_reclaim_to_a_two_pass_statement'): 'a retention cycle counts 32 rows as deferred to a live statement instead of all 100 expired rows',
    ('tests/integration/snapshot_pin_removal_pass_race_tests.rs', 'pin_snapshot_blocks_for_a_racing_removal_pass_and_then_protects_normally'): 'pinning a snapshot returns immediately while a removal pass with no established floor is in flight',
    ('tests/integration/state_propagation.rs', 't03_fk_propagation_depth_limit'): 'foreign key state propagation continues past the declared maximum depth of three',
    ('tests/integration/storage_scale_hardening_tests.rs', 'ssh_c3_maint_edge_indegree'): 'the edge in-degree maintenance count does not match one shared exact storage for its unique and graph edge indexes',
    ('crates/contextdb-server/tests/hub_ordered_sync_mutation_arbitration_tests.rs', 'stale_pulled_delete_is_not_reoffered_to_erase_a_later_hub_edit'): 'an edge applying settled hub state is refused when a row history ends in an accepted delete but a fresh row with the same key is visible',
    ('crates/contextdb-engine/src/persistence/historical_index_commitment.rs', 'a_wide_key_is_refused_by_its_reader_without_drawing_on_the_writer'): 'a one mebibyte historical index key is refused by the storage read instead of by the reader own key admission',
    ('crates/contextdb-engine/src/persistence/read_error_classification_tests.rs', 'one_read_error_in_a_proof_page_is_io'): 'a single read error inside a proof page is reported as store corruption instead of an input output error',
    ('crates/contextdb-engine/tests/reader_release/a_writer_open_waits_once_for_a_starting_reader.rs', 'a_companionless_reader_in_its_section_makes_the_writer_wait_then_open'): 'a reader that was acquiring when a writer arrived sees the rows the writer commits afterwards',
    ('crates/contextdb-engine/src/database/bounded_retention_preparation_tests.rs', 'reclamation_admits_vector_and_edge_identities_without_payload_copies'): 'Reclaiming an expired row with wide edge properties asks the accountant for more than the same reclamation with a short property plus one page, so the pruning pass still holds more than its pages and identities need.',
    ('crates/contextdb-engine/src/database/checkpointed_commit_floor_tests.rs', 'a_checkpointed_commit_does_the_same_fixed_work_at_10_and_20000_stored_events'): 'a checkpointed commit does not finish within five minutes at ten and at twenty thousand stored events',
    ('crates/contextdb-engine/src/database/checkpointed_commit_floor_tests.rs', 'a_checkpointed_commit_finalizes_each_pending_root_once'): 'a checkpointed commit does not finish within five minutes, so finalizing each pending root once cannot be checked',
    ('crates/contextdb-engine/src/database/checkpointed_commit_floor_tests.rs', 'a_checkpointed_commit_publishes_each_changed_table_to_the_directory_once'): 'a checkpointed commit does not finish within five minutes, so publishing each changed table once cannot be checked',
    ('crates/contextdb-engine/src/database/checkpointed_commit_floor_tests.rs', 'a_checkpointed_commit_writes_each_changed_tables_witness_once'): 'a checkpointed commit does not finish within five minutes, so writing each changed table witness once cannot be checked',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_budget_refusal_releases_population_and_retries_at_fixed_budget'): 'a legacy received-schema stage refused for memory does not finish within five minutes of releasing its population and retrying',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_plugin_cancellation_releases_prepared_population_and_retries'): 'a legacy received-schema stage cancelled by a plugin does not finish within five minutes of releasing its population and retrying',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_refused_stage_retries_live_at_the_same_budget'): 'a refused legacy received-schema stage does not finish within five minutes of retrying live at the same budget',
    ('crates/contextdb-engine/src/database/received_legacy_growth_guards.rs', 'production_legacy_selected_population_n_succeeds_with_fixed_memory'): 'a legacy received-schema stage over the base population does not finish within five minutes at a fixed memory budget',
    ('crates/contextdb-engine/tests/bounded/composite_prefix_newest_row_examines_constant_entries.rs', 'composite_prefix_newest_row_examines_constant_entries'): 'a newest-row read over a composite prefix does not finish within five minutes',
    ('crates/contextdb-engine/tests/bounded/newest_event_merges_staged_order_without_sort.rs', 'newest_event_merges_staged_order_without_sort'): 'merging staged order for the newest event without a sort does not finish within five minutes',
    ('crates/contextdb-engine/src/persistence/relational_posting_prune.rs', 'short_index_declarations_are_funded_before_native_allocation'): 'funding a short index declaration before native allocation does not finish within five minutes',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_schema_installed_in_one_transaction_reserves_what_it_keeps'): 'Committing a six-table schema in one transaction reserves 440032 bytes on top of the 408 KB its staged tables already hold, twice what the schema keeps.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_transaction_open_across_an_index_set_change_settles_the_rows_it_staged'): 'A schema change received over sync while a local transaction is open fails with a metadata checkpoint identity error instead of applying and settling the transaction.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_received_schema_group_refused_for_memory_installs_none_of_itself'): 'A received group of twelve indexes is priced at 10528 bytes but costs 12864 bytes once installed, so the price does not cover the real cost.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_tiny_declared_limit_still_opens_an_empty_store_that_answers_and_can_raise_itself'): 'A store opened under a tiny memory limit cannot take a write after the operator raises the limit, because its page cache stays at the tiny share.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_memory_hold_on_a_table_survives_the_receiver_closing_and_reopening'): 'After a restart under its own limit a later unit for a held table is refused with an untyped storage-pages refusal instead of the typed memory hold refusal.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'a_small_sync_answer_over_a_large_history_is_limited_by_its_own_size'): 'A refused small sync answer over a large history peaks at 1194224 bytes, above the 860245 bytes of history charge, so the refused request copies the history.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'a_history_answer_built_from_persisted_state_is_admitted_as_it_is_built'): 'A fallback history answer holds 2728684 bytes while the accountant is told 1975116 bytes, so the answer is under-charged.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'lineage_authentication_scratch_is_admitted_before_copy'): 'A reader holding competing admitted work cannot be set up at the declared reader limit, so admission of the lineage authentication copy before it is made is not proven.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'a_refused_lineage_scratch_reservation_copies_nothing'): 'The refused authentication copy of a lineage record asks for 2097590 bytes where the test measures a record of 1310915 bytes, so the copy size is not the one the record needs.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'a_history_walk_that_outgrows_the_reader_is_refused_by_the_reader_and_gives_everything_back'): 'A refused history walk leaves the reader holding 1238507 bytes of loaded history, more than an eighth of its room.',
    ('crates/contextdb-engine/tests/memory_limit_admission_holds_for_vectors.rs', 'admission_stops_before_the_process_is_over_the_limit'): 'Storing twice the memory limit in vector and document edges, one committed batch at a time, takes about twenty minutes on a hosted runner, so the held-bytes bound is not checked there.',
    ('crates/contextdb-engine/tests/sql/in_list_scope_uses_the_declared_composite_index.rs', 'oversized_two_list_product_is_refused_by_the_declared_memory_budget'): 'A product of two long value lists is no longer refused by the declared memory budget because the open cursor now counts zero bytes for its pending runs.',
    ('crates/contextdb-engine/tests/sql/ordered_and_bounded_finite_index_runs.rs', 'ten_to_forty_thousand_value_lists_have_subquadratic_shared_planning_work'): 'A bounded read of a large value list plans a full table scan of 8960064 bytes instead of bounded index runs and is refused at sixteen mebibytes.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'commit_rejects_staged_vector_insert_after_same_ref_sync_shape_change'): 'A schema change received over sync while a staged vector insert is open fails with a metadata checkpoint identity error instead of refusing the insert at commit.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'commit_rejects_staged_vector_update_after_same_ref_sync_shape_change'): 'A schema change received over sync while a staged vector update is open fails with a metadata checkpoint identity error instead of refusing the update at commit.',
    ('crates/contextdb-engine/tests/staged_predecode_row.rs', 'a_file_backed_received_migration_refuses_an_oversized_row_before_decoding_it'): 'A received migration of an oversized row is refused at the pending table root admission instead of at the affected row before it is decoded.',
    ('tests/integration/memory_accounting_tests.rs', 'm12_open_with_config_wires_accountant'): 'A memory refusal at the last admission of an insert leaves 127368 bytes charged to the store instead of returning the bill to where it was.',
    ('tests/integration/memory_accounting_tests.rs', 'mt2_update_rejection_leaves_accounting_unchanged'): 'A refused update leaves 113896 bytes charged to the store instead of leaving the bill unchanged.',
    ('tests/integration/persistence_tests.rs', 'p19_data_integrity_at_scale'): 'A thousand-hop graph traversal on a reopened ten-thousand-row store is refused at the default limit because the traversal keeps about sixteen kilobytes per hop.',
    ('crates/contextdb-engine/tests/engine_seams/aggregate_auto_threshold_decides_the_route_and_keeps_scope.rs', 'aggregate_auto_threshold_decides_the_route_and_keeps_scope'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'bounded_limits_are_data_derived_exact_and_return_typed_refusals'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'cursor_owns_database_and_execution_resources_after_the_caller_drops_its_handle'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'cursor_pages_are_complete_byte_bounded_and_per_fetch_budgeted'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'cursor_terminal_paths_release_accounted_memory_and_snapshot_pin'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'every_bounded_source_has_exact_work_memory_and_cancellation_matrix'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/bounded/bounded_pull_execution_contract.rs', 'vector_rank_and_access_candidate_work_is_bounded_before_materialization'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/disk_ceiling.rs', 'a_replacement_build_under_an_exhausted_ceiling_is_refused_and_writes_nothing'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/src/executor/bounded/filter_metadata_independent_tests.rs', 'authorized_vector_modes_keep_their_captured_rows_and_grants_after_later_writes'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/live_route.rs', 'live_database_search_during_six_staged_writers'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/live_route.rs', 'owner_channel_search_during_six_staged_writers'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/native_copy_auth.rs', 'obsolete_copy_cleanup_refuses_foreign_live_journal_table'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/objective_change.rs', 'a_raised_target_keeps_only_the_settings_that_meet_it_until_requalification'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/engine_seams/paged_initial_replacement_and_reclamation_progress_under_writes.rs', 'paged_initial_replacement_and_reclamation_progress_under_writes'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/clustered_vector_index/read_only_damage_inspection.rs', 'a_read_only_session_reports_live_damage_in_plain_words_and_the_next_writer_persists_and_rebuilds'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/engine_seams/saved_search_and_append_work_ceiling.rs', 'an_admitted_batch_stays_inside_its_primitive_ceiling_on_doubled_history'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/engine_seams/saved_search_and_append_work_ceiling.rs', 'first_saved_search_stays_inside_its_primitive_ceiling'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'alter_table_drop_vector_column_waits_for_inflight_build_then_removes_index'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'alter_table_rename_vector_column_waits_for_inflight_build_then_moves_index'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'same_table_vision_sql_update_and_search_completes_while_text_build_paused'): 'The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'prv_22_row_vector_query_removes_null_target_vectors_and_accounts_overlay'): 'A statement overlay serves a row and vector query from slack instead of refusing before it clones its state.',
    ('tests/acceptance/trigger.rs', 'including_sync_forbidden_effect_families_are_typed_atomic_and_audited'): 'A received trigger vector delete returns the generic effect error instead of the dedicated received trigger error.',
    ('crates/contextdb-engine/tests/clustered_vector_index/training_seed.rs', 'a_training_seed_travels_with_synced_ddl'): 'A training seed does not travel with synced table DDL.',
    ('crates/contextdb-engine/tests/sync/unattested_schema_forwarding.rs', 'authenticated_receiver_refuses_forwarded_unattested_schema_with_its_table'): 'An authenticated receiver does not refuse forwarded unattested schema whose provenance does not bind its table.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_panic_between_publication_components_is_recovered_by_restart'): 'A panic between the components of a received schema publication is not recovered by a restart.',
    ('crates/contextdb-engine/tests/engine_seams/a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses.rs', 'a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses'): 'A pending read after a pull still walks the received declarations it suppresses.',
    ('crates/contextdb-engine/tests/engine_seams/a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses.rs', 'a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses'): 'A push after a pull still holds the received declarations it suppresses.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_received_table_alter_does_not_clone_unrelated_event_metadata_before_admission'): 'A one-table alter received over sync still copies unrelated event metadata before it is admitted.',
    ('crates/contextdb-engine/tests/declared_memory_limit_bounds_a_writer.rs', 'a_refused_index_rebuild_returns_the_rows_charge_on_disk'): 'A refused index rebuild does not return the charge of its row on a file-backed receiver.',
    ('crates/contextdb-engine/tests/graph_property_memory_charge_measurement.rs', 'a_stored_property_value_is_charged_what_holding_it_costs'): 'The charge for a stored graph property value does not match the memory the process holds for it.',
    ('crates/contextdb-engine/tests/engine_seams/an_exact_identity_search_stays_inside_its_work_ceiling.rs', 'an_exact_identity_search_over_a_maintained_partition_stays_inside_its_work_ceiling'): 'An exact identity search makes more billing calls than its work ceiling allows.',
    ('crates/contextdb-engine/tests/engine_seams/bounded_graph_where_metadata_authorization_charges_request_limits.rs', 'bounded_graph_where_metadata_authorization_charges_request_limits'): 'A bounded graph query with metadata authorization does not charge its request limits.',
    ('crates/contextdb-engine/tests/bounded/composite_prefix_newest_row_examines_constant_entries.rs', 'historical_in_list_rebuild_seeks_each_declared_run_from_its_own_edge'): 'A historical rebuild of a value list touches the gap and the newer root instead of only the two listed postings.',
    ('crates/contextdb-engine/src/database/ordinary_native_owner_tests.rs', 'retained_trace_and_live_answer_have_independent_actual_invoice_cuts'): 'A retained trace and a live answer do not take independent actual invoice cuts because the retained image is allocated before execution.',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'a_cancelled_page_in_flight_releases_its_protection'): 'A cancelled page in flight does not release the protection it held.',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'a_page_in_flight_defers_the_removal_of_the_group_it_is_reading'): 'A page in flight does not defer the removal of the group it is reading.',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'a_refused_resident_page_releases_everything_it_admitted'): 'A refused resident page does not release everything it admitted.',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'an_oversized_group_after_a_log_gap_is_refused_before_any_copy'): 'An oversized group after a log gap is not refused before it is copied.',
    ('crates/contextdb-engine/src/database/schema_stage_registry.rs', 'fixed_writer_open_controls_use_the_process_ceiling_and_refund_once'): 'The fixed controls of a writer open do not use the process ceiling and refund once.',
    ('crates/contextdb-engine/src/database/writer_read.rs', 'staged_insert_copy_writer_refusal_preserves_the_committed_frontier'): 'A writer refusal while a staged insert is copied does not preserve the committed frontier.',
    ('crates/contextdb-engine/tests/engine_seams/every_writer_checkpoint_names_every_charged_byte.rs', 'writer_checkpoints_name_an_open_schema_stage_and_a_calibration_capture'): 'A writer checkpoint is taken while a calibration capture is held and does not name every charged byte.',
    ('crates/contextdb-engine/src/executor/bounded.rs', 'a_drained_paged_vector_cursor_returns_every_byte_it_took'): 'A drained paged vector cursor does not return every byte it took on close.',
    ('crates/contextdb-engine/src/executor/bounded.rs', 'closing_a_part_drained_paged_vector_cursor_returns_every_byte'): 'Closing a partly drained paged vector cursor does not return every byte it took.',
    ('crates/contextdb-engine/tests/schema/find_show_status_stay_fast_while_schema_replays.rs', 'find_show_status_stay_fast_while_schema_replays'): 'A resident read is refused for memory while schema replays instead of staying readable.',
    ('crates/contextdb-engine/tests/engine_seams/graph_cursor_survives_genuine_shrink.rs', 'a_graph_edge_cursor_emits_every_visible_edge_when_retention_compacts_entries_it_cannot_see'): 'A graph edge cursor does not emit every visible edge when retention compacts entries below its position.',
    ('crates/contextdb-engine/tests/engine_seams/in_list_cursor_pending_runs_are_charged.rs', 'an_open_cursors_settlement_includes_the_runs_it_still_owes'): 'An open cursor over a long value list reports no bytes held for the runs it has not reached.',
    ('crates/contextdb-engine/tests/history_request_memory.rs', 'many_row_changes_admit_their_aux_indexes_before_building_them_and_clean_up_on_refusal'): 'Many row changes do not admit their auxiliary indexes before building them or clean up on refusal.',
    ('crates/contextdb-engine/src/memory_accounting/undeclared_writer_ceiling_tests.rs', 'an_undeclared_writer_page_cache_fits_the_process_ceiling'): 'The page cache of an undeclared writer does not fit the process ceiling.',
    ('crates/contextdb-engine/tests/engine_seams/memory_accounting_lifecycle_contract.rs', 'reclaiming_superseded_versions_after_reopen_settles_where_a_reclaimed_store_reopens'): 'Reclaiming superseded versions after a reopen does not settle where a store reclaimed before the reopen settles.',
    ('crates/contextdb-engine/tests/native_backing_allocator.rs', 'non_test_engine_native_controls_match_actual_allocator_requests_and_final_frees'): 'The native controls of the engine do not match the actual allocator requests and final frees.',
    ('crates/contextdb-engine/tests/received_schema_metadata_peak.rs', 'one_table_migration_peak_ignores_unrelated_catalog_and_provenance'): 'A one-table received migration copies unrelated catalog and provenance metadata into its peak.',
    ('crates/contextdb-engine/src/persistence/empty_declared_index_readiness_tests.rs', 'empty_declared_index_publishes_marker_and_postings_for_a_lazy_ordered_read'): 'A lazy ordered read of an empty declared index is refused for memory instead of publishing its marker and postings.',
    ('crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs', 'a_read_is_answered_although_a_full_resident_set_is_holding_the_last_declared_bytes'): 'A read is refused for memory when a full resident set holds the last declared bytes instead of being answered.',
    ('crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs', 'a_tight_session_completes_a_long_walk_instead_of_refusing_it_for_cumulative_reuse'): 'A tight session is refused when it opens its retained committed image instead of completing a long walk.',
    ('crates/contextdb-engine/tests/engine_seams/pruning_report_names_reader_deferred_rows.rs', 'a_pass_that_defers_rows_for_a_registered_reader_reports_the_deferred_count'): 'A retention pass does not report how many expired rows it left in place for a registered reader.',
    ('crates/contextdb-engine/tests/partition_insert_cost.rs', 'r4_the_same_insert_fits_the_same_headroom_as_unrelated_partitions_grow_16x'): 'The same insert does not fit the same headroom when unrelated partitions grow sixteen times.',
    ('crates/contextdb-engine/tests/engine_seams/snapshot_visible_posting_membership_contract.rs', 'registered_snapshot_survives_commit_before_capture_and_index_replacement_before_first_pull'): 'A registered snapshot does not survive a commit before capture and an index replacement before its first pull.',
    ('crates/contextdb-engine/tests/engine_seams/storage_compaction_online.rs', 'automatic_storage_memory_refusal_keeps_the_store_usable'): 'An automatic storage compaction that is refused for memory does not leave the store usable.',
    ('crates/contextdb-engine/src/sync_server/native_delivery_tests.rs', 'a_result_refusal_leaves_its_page_without_durable_delivery_status'): 'A refused result does not leave its page without a durable delivery status.',
    ('crates/contextdb-engine/src/sync_server/native_delivery_tests.rs', 'the_iroh_native_path_answers_refusal_and_cancellation_with_typed_errors'): 'The native iroh path does not answer a refusal and a cancellation with typed errors.',
    ('crates/contextdb-engine/tests/engine_seams/trigger_audit_history_is_charged_once_to_the_declared_reader.rs', 'trigger_audit_history_is_charged_once_to_the_declared_reader'): 'Trigger audit history is not charged once to the declared reader.',
    ('crates/contextdb-engine/tests/engine_seams/uncapped_graph_read_charges_the_frontier_it_resolves.rs', 'a_traversal_that_resolves_many_starts_is_refused_when_only_one_frontier_fits'): 'A traversal that resolves many starts is not refused when only one frontier fits, because a frontier costs the store nothing.',
    ('crates/contextdb-engine/tests/engine_seams/uncapped_graph_read_charges_the_frontier_it_resolves.rs', 'a_traversal_that_resolves_no_start_is_not_refused_for_a_frontier_it_never_builds'): 'A traversal that resolves no start is checked against a frontier cost the store does not charge.',
    ('crates/contextdb-engine/src/vector_read_memory.rs', 'installed_provenance_rejects_reuse_and_payer_changes'): 'An installed read memory provenance does not reject reuse and payer changes.',
    ('crates/contextdb-engine/tests/engine_seams/what_holds_a_one_kilobyte_memory_limit.rs', 'a_one_kilobyte_store_says_what_is_holding_its_budget'): 'A one kilobyte store does not name the schema admission that holds its budget.',
    ('crates/contextdb-engine/tests/engine_seams/writer_reopen_and_doubling_keep_a_fixed_resident_envelope.rs', 'writer_copy_operations_keep_fixed_work_with_wide_unrelated_schemas'): 'Writer copy operations do not keep fixed work when unrelated schemas are wide.',
    ('crates/contextdb-engine/tests/engine_seams/database_open_options_contract.rs', 'open_options_preserve_owner_limits_plugin_identity_and_access_getters'): 'Opening a database with options still registers a read session open at startup, which the open options contract says must not happen.',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'a_refused_second_writer_construction_leaves_the_real_holder_and_its_lock_untouched'): 'A live writer does not refuse a raw open of its store file by a separate process, so a refused second construction does not leave the holder in sole ownership.',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'authenticated_external_page_holds_survive_an_actual_storage_handle_recycle'): 'An authenticated external page hold does not survive a storage handle recycle, so a row written after the recycle is not read back as expected.',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'explicit_close_releases_the_descriptor_while_the_database_value_is_still_alive'): 'A live writer does not refuse a raw open of its store file by a separate process.',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'writer_descriptor_survives_all_three_recycle_boundaries_and_refuses_every_alias'): 'A writer descriptor does not refuse a raw writer open by a separate process across its recycle boundaries.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'ga08_delete_visibility_respects_pre_and_post_delete_snapshots'): 'A deleted graph edge is still visible to a snapshot taken after the delete.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'ga09_same_transaction_staged_edge_is_visible_only_inside_transaction'): 'A graph edge deleted inside a transaction is still visible to that transaction.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'ga18_undirected_pinned_single_hop_probes_forward_and_reverse_maps'): 'An undirected pinned single hop does not return the edges found through the forward and reverse maps.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'ga27_unpinned_edge_scan_respects_same_tx_graph_overlay'): 'An unpinned edge scan does not honour the graph overlay of its own transaction.',
    ('crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs', 'ga28_multihop_bfs_respects_same_tx_graph_overlay'): 'A multihop traversal does not honour the graph overlay of its own transaction.',
    ('crates/contextdb-engine/tests/schema/schema_replay_is_proportional_to_affected_tables.rs', 'largest_affected_replay_preserves_existing_rows_vectors_and_trigger'): 'replaying the largest affected schema writes a durable artifact that the affected set proof does not anticipate',
    ('crates/contextdb-engine/tests/schema/schema_replay_work_limited_to_affected_tables.rs', 'schema_replay_is_proportional_to_affected_tables'): 'replaying an affected schema writes a historical root witness for an index that no affected table owns',
    ('crates/contextdb-engine/tests/engine_seams/writer_reopen_and_doubling_keep_a_fixed_resident_envelope.rs', 'writer_reopen_and_doubling_keep_a_fixed_resident_envelope'): 'reopening and doubling a writer saturates its fixed cache share instead of keeping a fixed resident envelope',
    ('crates/contextdb-engine/src/database/received_legacy_overlay_work_guards.rs', 'legacy_stage_overlay_steps_are_linear_in_rows_replaced_at_2043'): 'a legacy received schema stage replacing 2043 rows does not finish within five minutes',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_durable_failure_releases_prepared_population_and_retries'): 'a legacy received schema stage that fails durably does not finish within five minutes of releasing its population and retrying',
    ('crates/contextdb-engine/src/database/received_legacy_growth_lifecycle_guards.rs', 'production_legacy_grown_stage_remains_invisible_before_final_publication'): 'a grown legacy received schema stage does not finish within five minutes before its final publication',
    ('crates/contextdb-engine/src/persistence/high_fanout_tests.rs', 'canonical_incremental_insert_delete_cascades_match_sorted_rebuild_and_compaction'): 'incremental cascade inserts and deletes do not finish within five minutes of matching a sorted rebuild and compaction',
    ('crates/contextdb-engine/src/persistence/read_error_classification_tests.rs', 'one_read_error_while_opening_and_committing_is_an_io_error_not_corruption'): 'opening and committing a store under an injected read error does not finish within five minutes',
    ('crates/contextdb-engine/tests/schema/schema_replay_is_proportional_to_affected_tables.rs', 'affected_schema_closure_keeps_foreign_keys_propagation_and_triggers'): 'replaying an affected schema closure with foreign keys, propagation and triggers does not finish within five minutes',
    ('crates/contextdb-engine/tests/schema/schema_replay_is_proportional_to_affected_tables.rs', 'identical_schema_replay_does_zero_actual_artifact_work'): 'replaying an identical schema does not finish within five minutes',
    ('crates/contextdb-engine/tests/schema/schema_replay_is_proportional_to_affected_tables.rs', 'schema_replay_is_proportional_to_affected_tables'): 'replaying a schema in proportion to its affected tables does not finish within five minutes',
    ('crates/contextdb-engine/tests/engine_seams/a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics.rs', 'a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics_as_the_store_grows'): 'a maintenance cycle that prunes nothing does not finish within five minutes as the store grows',
    ('crates/contextdb-engine/tests/engine_seams/incremental_sync_charges_fixed_memory_as_unrelated_ddl_history_grows.rs', 'incremental_sync_charges_sublinear_memory_as_unrelated_ddl_history_grows'): 'incremental sync over a growing unrelated schema history does not finish within five minutes',
    ('crates/contextdb-engine/tests/engine_seams/incremental_sync_charges_fixed_memory_as_unrelated_ddl_history_grows.rs', 'incremental_sync_fixed_delta_charges_sublinear_memory_as_unrelated_ddl_history_grows'): 'incremental sync of a fixed change over a growing unrelated schema history does not finish within five minutes',
    ('crates/contextdb-engine/tests/engine_seams/writer_memory_per_stored_passage.rs', 'writer_memory_per_stored_passage'): 'measuring writer memory per stored passage does not finish within five minutes',
    ('crates/contextdb-engine/src/database/authoritative_purge_public_contract_tests.rs', 'public_purge_batch_is_atomic_across_injected_persistence_failure_and_reopen'): 'a purge workspace grows with the width of an unrelated nested schema instead of staying fixed',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'a_resident_page_admits_the_same_bytes_as_the_tail_doubles'): 'a resident pull page admits more bytes when the tail behind it doubles',
    ('crates/contextdb-engine/src/database/pull_page_contract_tests.rs', 'pending_deletes_are_selected_a_page_at_a_time'): 'pending deletes are not selected one page at a time',
    ('crates/contextdb-engine/src/persistence/search_page_reuse_tests.rs', 'retained_proof_pages_are_accounted_to_the_declared_session'): 'proof pages held by an open cursor are not charged to the declared session',
    ('crates/contextdb-engine/tests/engine_seams/a_count_a_constraint_already_bounds_is_not_maintained.rs', 'the_cg_event_append_stages_nine_fewer_status_members'): 'an append stages index prefix counts that a declared constraint already bounds',
    ('crates/contextdb-engine/tests/engine_seams/a_declared_count_is_the_only_count_an_append_writes.rs', 'a_declared_count_is_the_only_count_an_append_writes'): 'an append writes counts beyond the declared count of each index',
    ('crates/contextdb-engine/tests/engine_seams/a_large_journal_cleanup_is_charged_to_the_writer.rs', 'a_journal_larger_than_the_shipped_reader_memory_is_cleaned_under_the_writer'): 'a journal larger than the shipped reader memory is reported unavailable instead of being cleaned under the writer budget',
    ('crates/contextdb-engine/tests/engine_seams/a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses.rs', 'a_pending_read_after_a_pull_holds_none_of_the_received_declarations_it_suppresses_as_received_history_grows'): 'a pending read after a pull holds received declarations that it should suppress as received history grows',
    ('crates/contextdb-engine/tests/engine_seams/a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses.rs', 'a_push_after_a_pull_holds_none_of_the_received_declarations_it_suppresses_as_received_history_grows'): 'a push after a pull receives a different number of declarations above the frontier than the hub sent',
    ('crates/contextdb-engine/tests/engine_seams/aggregate_reads_complete_across_bounded_pages.rs', 'sum_cursor_completes_over_a_filtered_store_larger_than_the_work_ceiling'): 'a byte total over a filtered store larger than the work ceiling is not the total of every row the filter keeps',
    ('crates/contextdb-engine/tests/engine_seams/buffer_pool_share_evicts_and_recharges_under_the_writer_budget.rs', 'buffer_pool_share_evicts_and_recharges_under_the_writer_budget'): 'the buffer pool share does not stay within the declared bounded writer share',
    ('crates/contextdb-engine/tests/engine_seams/census_samples_around_maintenance_name_every_writer_byte.rs', 'every_census_sample_between_caller_driven_maintenance_steps_names_every_writer_byte'): 'caller driven maintenance steps do not move the writer accountant between census samples as the census expects',
    ('crates/contextdb-engine/tests/engine_seams/one_event_append_work_by_family.rs', 'one_event_append_work_by_family'): 'the per family mutation counts of a one event append differ from the declared counts',
    ('crates/contextdb-engine/tests/engine_seams/one_vector_append_does_not_rescan_the_directory.rs', 'deleting_one_vector_hashes_only_what_that_commit_changed'): 'deleting one vector hashes no membership record where exactly the changed one should be hashed',
    ('crates/contextdb-engine/tests/engine_seams/opening_a_store_holds_a_bounded_window_of_queued_events.rs', 'a_purge_beside_a_large_unrelated_backlog_holds_no_more_than_a_small_one'): 'a purge beside a larger unrelated event backlog holds more memory at its peak than beside a small one',
    ('crates/contextdb-engine/tests/engine_seams/opening_a_store_holds_a_bounded_window_of_queued_events.rs', 'opening_a_store_holds_the_same_window_however_many_events_are_queued'): 'opening a store holds a different amount of memory when four times as many events are queued',
    ('crates/contextdb-engine/tests/engine_seams/reads_and_writes_are_charged_to_their_own_declared_limits.rs', 'a_reader_too_small_for_a_read_refuses_with_its_own_label_and_returns_its_charge'): 'a read that is refused for a too small reader fails at a page read instead of with the reader own label',
    ('crates/contextdb-engine/tests/engine_seams/storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown.rs', 'the_first_write_after_compaction_grows_the_file_by_the_stop_ratio_not_double'): 'the first write after compaction grows the file by more than the declared stop ratio',
    ('crates/contextdb-engine/tests/engine_seams/unpartitioned_vector_membership_pages_reload_at_constant_residency.rs', 'unpartitioned_vector_membership_pages_reload_at_constant_residency'): 'unpartitioned vector membership pages do not reload at constant residency',
    ('crates/contextdb-engine/tests/engine_seams/version_pruning_and_compaction_resume_finite_safe_batches.rs', 'version_pruning_and_compaction_resume_finite_safe_batches'): 'version pruning and compaction do not resume in finite safe batches',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'reopen_fault_holds_real_ownership_while_the_failed_persistence_is_alive_and_releases_after_close'): 'a failed reopen releases the outer file descriptor while its persistence is still alive',
    ('crates/contextdb-engine/tests/engine_seams/writer_file_ownership_survives_recycle.rs', 'repeated_close_recycle_and_reopen_fault_cycles_return_descriptor_count_to_baseline'): 'a failed writer stops refusing a separate process exclusive open after a failed reopen cycle',
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


def registered_ignored_functions():
    return {case for _, case in QUARANTINE_EXCEPTIONS} | {case for _, case, _ in OTHER_IGNORED_TESTS}


def run_verdict(text):
    """(ran, why): libtest passed or failed a test, or selected only tests that are registered ignores."""
    clean = re.sub(r'\x1b\[[0-9;]*m', '', text)
    summaries = re.findall(r'test result: \w+\. (\d+) passed; (\d+) failed;', clean)
    if sum(int(passed) + int(failed) for passed, failed in summaries) > 0:
        return True, ''
    ignored = re.findall(r'^test (\S+) \.\.\. ignored', clean, re.M)
    if not ignored:
        return False, 'libtest selected no test (0 passed, 0 failed, none ignored)'
    unregistered = sorted({name for name in ignored if name.split('::')[-1] not in registered_ignored_functions()})
    if unregistered:
        return False, 'libtest selected only ignored tests and these are not registered ignores: ' + ', '.join(unregistered[:5])
    return True, 'every selected test is a registered ignore'


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
        declaration = re.compile(re.escape(attribute) + r'\s*(?:#\[[^\]]+\]\s*)*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+' + re.escape(case) + r'\s*\(')
        if len(declaration.findall(text)) != 1:
            raise ValueError('missing, duplicate, or misbound approved quarantine: ' + path + '::' + case)
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
NORMAL_DEDUP_TARGETS = ('custody_custody_protocol', 'custody_custody_purge_and_discard', 'custody_server_resource_policy_contract_tests', 'custody_tenant_table_policy', 'vector_a_find_answers_a_full_page_when_a_candidate_is_withdrawn', 'vector_a_find_names_only_the_pairs_it_asks_for', 'vector_equal_distance_rows_keep_canonical_order_across_contexts', 'vector_exclusion_persistence_tests', 'vector_inspection_explain_truth_contract', 'vector_memory_ownership_reconciliation_contract', 'vector_named_vector_planner_tests', 'vector_policy', 'vector_quantized', 'vector_rank_policy_exact_audits_the_whole_table', 'vector_received_generation_invalidation_contract', 'vector_resource_maintenance_contract', 'vector_retained_indexed_rows', 'vector_row_delete_and_guards_honor_vector_columns', 'vector_serving_merge_contract', 'vector_tail_transaction_contract', 'vector_the_owner_doors_run_one_search_over_every_layer', 'vector_transaction_filter_snapshot_truth_contract', 'vector_writer_residency_attribution', 'bounded_metadata_start_lookup_stays_inside_the_read_limits', 'custody_delivery_outcome', 'indexed_row_memory_charge_measurement', 'memory_limit_admission_holds_for_vectors', 'vector_an_exhaustive_find_streams_bodies_it_cannot_hold', 'vector_partition', 'engine', 'sql', 'vector_generation', 'history_request_memory', 'custody_delivery_manifest', 'vector_a_find_decodes_only_the_rows_it_returns', 'vector_a_find_over_many_contexts_spends_one_search_budget', 'direct_file_image_memory_lifetime', 'graph_property_memory_charge_measurement', 'received_schema_metadata_peak', 'bounded_in_subquery_stays_inside_the_read_limits', 'bounded_read_of_staged_graph_work_stays_within_its_limits', 'bounded', 'vector_lazy', 'vector_schema', 'vector_search_mode_bounded_contract')


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
    if len(rows) != 29 or len(set(logical_ids)) != 24:
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


def commands(row, root=ROOT, mode=None, fragment=None):
    """Keep default-target commands intact, including custom harnesses and doctests."""
    mode = gate_mode(mode)
    sliced = {}
    if fragment is not None:
        if row['kind'] not in {'engine-tests', 'release-acceptance'} or not set(fragment['targets']) <= set(row['targets']):
            raise ValueError('fragment does not belong to this row')
        sliced = dict(fragment.get('tests', {}))
        row = dict(row, targets=list(fragment['targets']))
    if mode == 'normal' and any(t in FULL_SCHEMA | FULL_ONLY_TARGETS for t in row['targets']):
        raise ValueError('unrouted full-qualification target in normal mode')
    kind = row['kind']
    base = ['cargo', 'test', '--locked', '-p', 'contextdb-engine', '--features', metadata(row)['features']]
    if kind == 'engine-tests':
        aggregates = [t for t in row['targets'] if t in {'custody', 'vector'}
                      or mode == 'full' and t == 'custody_schema_replay_bounded_contract']
        ordinary = [t for t in row['targets'] if t not in aggregates and t not in sliced]
        for target in ordinary:
            base += ['--lib', '--bins'] if target == 'lib' else ['--test', target]
        flags = ['--test-threads=1']
        for case in relocated_cases(row['targets'], mode):
            flags += ['--skip', case]
        if mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
            long_running_contract(root)
            if row['targets'] != ['acceptance_long_running'] or row['cache_parent'] != 'acceptance-long-running-test':
                raise ValueError('long-running partition target/cache envelope changed')
            flags += (['--skip', LONG_RUNNING_CASES[2]] if 'transfer' in row['id']
                      else [LONG_RUNNING_CASES[2], '--exact'])
        result = [base[:3] + [NO_FAIL_FAST] + base[3:] + ['--'] + flags] if ordinary else []
        for target, spec in sliced.items():
            result.append(['cargo', 'test', '--locked', NO_FAIL_FAST, '-p', 'contextdb-engine', '--features',
                           metadata(row)['features']] + (['--lib'] if target == 'lib' else ['--test', target])
                          + ['--'] + flags + slice_flags(spec))
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
            if not aggregate.emits(root, target, metadata(row)['features']):
                continue                    # every module is owned by its own slice here: nothing to select
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
        extra = slice_flags(sliced[row['targets'][0]]) if sliced else []
        return [base + ['--release', '--test', row['targets'][0], '--', '--test-threads=1'] + extra]
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
    if manifest.get('schema_version') != 2 or not isinstance(rows, list) or len(rows) != 25:
        raise ValueError('invalid canonical PR inventory')
    ids = [row.get('shard') for row in rows if isinstance(row, dict)]
    if len(ids) != 25 or len(set(ids)) != 25:
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
    if len(set(parents.values())) != 22:
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


NORMAL_BUNDLES_SHA256 = '45792a9dfc271f569e8bee914affa9adac2ae463e8b2cf9c1cf78257e31adb83'
SLOT_FIELDS = ENVELOPE_FIELDS | {'cache_key', 'fragment', 'build'}
# A fragment may only name a target that runs as an ordinary libtest binary.
UNSLICEABLE_TARGETS = {'custody', 'vector', 'custody_schema_replay_bounded_contract'}


def split_shard_items(shard, mode, root=ROOT):
    """The routed target items of a split PR shard, or None for an unsplit shard."""
    lines = (root / 'scripts/ci-shard-targets.txt').read_text().splitlines()
    matches = [line.split(': ', 1)[1].split() for line in lines if line.startswith(str(shard) + ': ')]
    if len(matches) > 1:
        raise ValueError('unknown/duplicate split shard')
    if not matches:
        return None
    targets = [t for t in matches[0] if mode == 'full' or t not in FULL_SCHEMA | FULL_ONLY_TARGETS]
    if not targets:
        raise ValueError('routing must not produce an empty shard')
    return targets


def crate_targets(root, crate, features):
    """Target tokens of an unsplit crate shard: lib (with its bins) and every built test binary."""
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
    closure = module.feature_closure(crate, features.split(',') if features else [], documents, paths, workspace)
    if crate == 'contextdb-engine' and not features:
        return ['lib']      # the default engine shard runs only its library and binaries
    tests = module.target_inventory(documents[crate], paths[crate], 'test')
    return ['lib'] + sorted(name for name, required in tests.items() if required <= closure)


def row_universe(envelope, active, root=ROOT):
    """Every target token a split row must place exactly once; unsplittable rows refuse."""
    if envelope['kind'] == 'pr':
        row = envelope['pr']
        if row.get('constituents'):
            raise ValueError('a bundle row cannot be split: ' + envelope['id'])
        items = split_shard_items(row['shard'], 'normal', root)
        items = items if items is not None else crate_targets(root, row['crate'], row['features'])
        aggregate = aggregate_module(root)
        return [t for t in items if aggregate.emits(root, t, row['features'])]
    row = active.get(envelope['integration']['id'])
    if (row is None or row['kind'] not in {'engine-tests', 'release-acceptance'}
            or row['id'] in LONG_RUNNING_PARTITIONS):
        raise ValueError('this integration row cannot be split: ' + envelope['id'])
    return list(row['targets'])


def slice_flags(spec):
    """libtest arguments selecting exactly the tests one slice owns."""
    if set(spec) == {'only'}:
        return list(spec['only']) + ['--exact']
    if set(spec) == {'except'}:
        flags = ['--exact']
        for name in spec['except']:
            flags += ['--skip', name]
        return flags
    raise ValueError('a test slice is only or except')


def slice_selects(spec, test):
    return test in spec['only'] if 'only' in spec else test not in spec['except']


def validate_fragments(row_id, universe, fragments):
    """The fragments of one row place every target once; sliced targets split their tests exactly."""
    parts = [fragment['part'] for fragment in fragments]
    if len(parts) != len(set(parts)) or any(not isinstance(p, int) or p < 1 for p in parts):
        raise ValueError('fragment parts must be distinct positive integers: ' + row_id)
    whole, sliced = {}, {}
    for fragment in fragments:
        tests = fragment.get('tests', {})
        if (not fragment['targets'] or len(fragment['targets']) != len(set(fragment['targets']))
                or not set(tests) <= set(fragment['targets'])):
            raise ValueError('invalid fragment targets: ' + row_id)
        for target in fragment['targets']:
            if target in tests:
                sliced.setdefault(target, []).append(tests[target])
            else:
                whole.setdefault(target, []).append(fragment['part'])
    if any(len(v) > 1 for v in whole.values()) or set(whole) & set(sliced):
        raise ValueError('a target is placed twice: ' + row_id)
    if set(whole) | set(sliced) != set(universe):
        raise ValueError('fragments do not place every target exactly once: ' + row_id + ' missing='
                         + str(sorted(set(universe) - set(whole) - set(sliced))))
    for target, specs in sliced.items():
        only = [s['only'] for s in specs if set(s) == {'only'}]
        rest = [s['except'] for s in specs if set(s) == {'except'}]
        names = [n for s in only for n in s]
        if (target in UNSLICEABLE_TARGETS or len(specs) < 2 or len(rest) != 1 or len(only) + 1 != len(specs)
                or not names or len(names) != len(set(names)) or sorted(rest[0]) != sorted(names)
                or any(not isinstance(n, str) or not n for n in names)):
            raise ValueError('test slices of ' + row_id + '/' + target
                             + ' must be disjoint only-lists plus one except-rest naming all of them')


def _bundle_slots(value, enabled, decision, root=ROOT):
    """Expand the reviewed job layout; a row is placed whole or as a proven partition of fragments."""
    rows = load_manifest(root)
    validate_combined(value, rows=rows, enabled=enabled, decision=decision, root=root)
    manifest = json.loads((root / 'scripts/ci-integration-targets.json').read_text())
    definition = manifest.get('normal_bundles')
    if (not isinstance(definition, dict) or set(definition) != {'schema_version', 'job_count', 'jobs'}
            or definition['schema_version'] != 3):
        raise ValueError('invalid normal bundle manifest')
    encoded = json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(encoded).hexdigest() != NORMAL_BUNDLES_SHA256:
        raise ValueError('normal bundle manifest differs from reviewed design')
    jobs = definition['jobs']
    if (not isinstance(jobs, list) or definition['job_count'] != len(jobs)
            or any(not isinstance(job, list) or not 1 <= len(job) <= 4 for job in jobs)):
        raise ValueError('invalid reviewed normal bundle layout')
    planned, whole, fragmented = [], [], {}
    for job in jobs:
        slots = []
        for slot in job:
            if isinstance(slot, str):
                whole.append(slot)
                slots.append((slot, None))
            elif (isinstance(slot, dict) and set(slot) in ({'row', 'part', 'targets'}, {'row', 'part', 'targets', 'tests'})
                    and isinstance(slot['row'], str) and isinstance(slot['targets'], list)):
                fragmented.setdefault(slot['row'], []).append(slot)
                slots.append((slot['row'], slot))
            else:
                raise ValueError('invalid reviewed normal bundle slot')
        planned.append(slots)
    if len(whole) != len(set(whole)) or set(whole) & set(fragmented):
        raise ValueError('normal bundles contain duplicate canonical rows')
    by_id = {row['id']: row for row in value['include']}
    active = {row['id']: row for row in active_rows(rows, False, 'normal', decision)}
    resolved = {}
    for row_id, fragments in fragmented.items():
        if row_id not in by_id:
            continue
        universe = row_universe(by_id[row_id], active, root)
        kept = {}
        for fragment in fragments:
            targets = [t for t in fragment['targets'] if t in universe]
            if targets:
                kept[fragment['part']] = dict(
                    row=row_id, part=fragment['part'], targets=targets,
                    **({'tests': {t: s for t, s in fragment['tests'].items() if t in universe}}
                       if fragment.get('tests') else {}))
        validate_fragments(row_id, universe, list(kept.values()))
        resolved[row_id] = kept
    selected = []
    for slots in planned:
        chosen = []
        for row_id, fragment in slots:
            envelope = by_id.get(row_id)
            if envelope is None:
                continue
            if fragment is None:
                chosen.append((envelope, None))
            elif fragment['part'] in resolved.get(row_id, {}):
                chosen.append((envelope, resolved[row_id][fragment['part']]))
        if chosen:
            selected.append(chosen)
    expanded = [envelope['id'] for slots in selected for envelope, fragment in slots if fragment is None]
    expanded += sorted({envelope['id'] for slots in selected for envelope, fragment in slots if fragment is not None})
    if len(expanded) != len(by_id) or set(expanded) != set(by_id):
        raise ValueError('normal bundles are not the exact canonical row union')
    return selected


def bundle_matrix(value, rows, enabled, decision, root=ROOT):
    validate_combined(value, rows, enabled, decision, root)
    active = {row['id']: row for row in active_rows(rows, False, 'normal', decision)}
    result = []
    for index, bundle in enumerate(_bundle_slots(value, enabled, decision, root), 1):
        slots = []
        for envelope, fragment in bundle:
            key = ('test-' + envelope['pr'].get('cache_parent', envelope['pr']['shard'])
                   if envelope['kind'] == 'pr'
                   else 'integration-' + envelope['integration'].get(
                       'cache_parent', envelope['integration']['id'])) + '-debug0-jobs2-test'
            if envelope['kind'] == 'integration' and envelope['integration']['id'] in LONG_RUNNING_PARTITIONS:
                key = 'integration-acceptance-long-running-test-debug0-jobs2-test'
            slot = dict(envelope, cache_key=key, build=build_key(envelope, active),
                        fragment=None if fragment is None else dict(row=fragment['row'], part=fragment['part']))
            if fragment is not None:
                slot['name'] = envelope['name'] + ' / part ' + str(fragment['part'])
            slots.append(slot)
        name = f'bundle {index:02d} / ' + ' + '.join(
            slot['name'].removeprefix('normal correctness / ') for slot in slots)
        result.append(dict(id=f'normal-bundle-{index:02d}', name=name,
                           slot1=slots[0], slot2=slots[1] if len(slots) > 1 else None,
                           slot3=slots[2] if len(slots) > 2 else None,
                           slot4=slots[3] if len(slots) > 3 else None))
    return {'include': result}


# Slots of these crate/feature/profile shapes share one compiled target built once per run.
# The integration runner compiles with warnings denied; the PR shard runner uses the workflow's empty RUSTFLAGS.
# A shared build must use the consumer's RUSTFLAGS exactly, or Cargo sees different units and rebuilds them.
INTEGRATION_RUSTFLAGS = '-D warnings'
SHARED_BUILDS = {
    'contextdb-engine~test-seams+iroh~test': dict(crate='contextdb-engine', features='test-seams,iroh', release=False, probe='setting_refusals', rustflags=''),
    'contextdb-engine~nightly-tests+test-seams+iroh~test': dict(crate='contextdb-engine', features=FEATURES, release=False, probe='history_request_memory', rustflags=INTEGRATION_RUSTFLAGS),
    'contextdb-engine~nightly-tests+test-seams+iroh~release': dict(crate='contextdb-engine', features=FEATURES, release=True, probe='acceptance_sync', rustflags=INTEGRATION_RUSTFLAGS),
    'contextdb-server~default~test': dict(crate='contextdb-server', features='', release=False, probe='iroh_transport_tests', rustflags=''),
    'contextdb-server~test-seams~test': dict(crate='contextdb-server', features='test-seams', release=False, probe='iroh_transport_tests', rustflags=''),
}


def shared_build_name(crate, features, release):
    return '~'.join([crate, features.replace(',', '+') or 'default', 'release' if release else 'test'])


def build_key(envelope, active):
    """The shared build a slot restores, or None when the slot compiles for itself."""
    if envelope['kind'] == 'pr':
        row = envelope['pr']
        if row.get('constituents'):
            return None
        name = shared_build_name(row['crate'], row['features'], False)
    else:
        row = active.get(envelope['integration']['id'])
        if row is None or row['kind'] not in {'engine-tests', 'release-acceptance', 'examples', 'doctests'}:
            return None
        name = shared_build_name('contextdb-engine', metadata(row)['features'], row['kind'] == 'release-acceptance')
    return name if name in SHARED_BUILDS else None


def build_commands(name):
    """The one compile a shared build performs; test jobs then find these units fresh."""
    spec = SHARED_BUILDS[name]
    base = ['cargo', 'test', '--locked', '--no-run'] + (['--release'] if spec['release'] else [])
    base += ['-p', spec['crate']] + (['--features', spec['features']] if spec['features'] else [])
    commands = [['cargo', 'build', '--locked', '-p', 'contextdb-cli', '-p', 'contextdb-server'],
                base + (['--bins'] if spec['release'] else ['--lib', '--bins']) + ['--test', spec['probe']]]
    return commands


def build_matrix_of(bundles):
    names = sorted({slot['build'] for bundle in bundles['include']
                    for slot in (bundle['slot1'], bundle['slot2'], bundle['slot3'], bundle['slot4'])
                    if slot is not None and slot['build'] is not None})
    return {'include': [dict(id='build-' + name, name='build / ' + name, build=name) for name in names]}


def fragment_from_env():
    raw = os.environ.get('CI_FRAGMENT', '')
    if raw in {'', 'null'}:
        return None
    fragment = json.loads(raw)
    if not isinstance(fragment, dict):
        raise ValueError('CI_FRAGMENT must be a fragment object')
    return fragment


def resolve_fragment(row_id, reference, rows, decision):
    """The published matrix names a fragment by row and part; the runner reads its targets and tests from the reviewed layout."""
    if reference != dict(row=row_id, part=reference.get('part')) or not isinstance(reference['part'], int):
        raise ValueError('a fragment reference is its row and part: ' + row_id)
    combined = combined_matrix(rows, True, decision)
    for bundle in _bundle_slots(combined, True, decision):
        for envelope, fragment in bundle:
            if envelope['id'] == row_id and fragment is not None and fragment['part'] == reference['part']:
                return fragment
    raise ValueError('fragment is not published for this row: ' + row_id)


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


def validate_build_matrix(value, bundles):
    if value != build_matrix_of(bundles):
        raise ValueError('published shared build matrix differs from the bundle matrix')


def normal_required(needs, rows, enabled, published, published_bundles, decision, root=ROOT, published_builds=None):
    expected = {'impact', 'test-bundles'}
    if not isinstance(needs, dict) or set(needs) != expected:
        raise ValueError('normal validator needs exact impact and three dispatch jobs')
    if any(needs[name].get('result') != 'success' for name in expected):
        raise ValueError('normal planning or bundle execution did not succeed')
    validate_combined(published, rows, enabled, decision, root)
    validate_bundle_matrix(published_bundles, published, rows, enabled, decision, root)
    if published_builds is not None:
        validate_build_matrix(published_builds, published_bundles)


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


def run_slice(row, out, mode=None, fragment=None):
    mode = gate_mode(mode)
    full_row = row
    if fragment is not None:
        row = dict(row, targets=list(fragment['targets']))
    out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    try:
        runtime_decision(row, mode)
    except (ValueError, KeyError, TypeError) as error:
        (out / 'receipt.json').write_text(json.dumps(dict(exit_status=1, gate_mode=mode, slice=row,
            plan_decision=row.get('plan_decision'), refusal_reason=str(error), commands=[], targets=[]), indent=2) + '\n')
        return 1
    env = dict(os.environ, RUSTFLAGS=INTEGRATION_RUSTFLAGS)
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
                   relocated_cases=[dict(case=case, destination="full qualification; both feature tuples" if case == OVERSIZED
                                         else "full qualification and nightly")
                                    for case in relocated_cases(row["targets"], mode)],
                   source_status_sha256=hashlib.sha256(dirty).hexdigest(),
                   source_diff_sha256=hashlib.sha256(diff).hexdigest(), toolchain=toolchain,
                   slice=row, fragment=fragment, companion_features=('test-seams,iroh' if mode == 'full' and
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
            planned = commands(full_row, mode=mode, fragment=fragment)
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
                filtered = '--exact' in command or '--skip' in command
                ran_a_test, why = run_verdict(log.read_text(errors='replace'))
                if (command_status == 0 and '--no-run' not in command and filtered and row['kind'] in {'engine-tests', 'release-acceptance'}
                        and not ran_a_test):
                    print('::error::a filtered command ran no test: ' + why + ', so it cannot pass: ' + ' '.join(command), file=sys.stderr)
                    command_status = 1
                if command_status == 0 and mode == 'normal' and row['id'] in LONG_RUNNING_PARTITIONS:
                    long_running_counts(entries, log, row['id'])
                if command_status == 0 and '--no-run' not in command and 'sync_schema_transactions' in row['targets']:
                    sync_routing_counts(entries, mode, companion=index > 0)
                if command_status:
                    failures.append(command_status)
            status = failures[0] if failures else 0
            if (status == 0 and row['kind'] in {'engine-tests', 'release-acceptance'}
                    and not run_verdict(''.join(p.read_text(errors='replace') for p in sorted(out.glob('*.log')) if p.name != 'build.log'))[0]
                    and not any('--no-run' in c['argv'] for c in receipt['commands'])):
                print('::error::slice ' + row['id'] + ' ran no test at all, so it cannot pass', file=sys.stderr)
                status = 1
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
    parser.add_argument('action', choices=['plan', 'required', 'normal-required', 'check', 'run', 'shard-plan', 'fragment-plan', 'build-run', 'ran-tests', 'aggregate-emits', 'shard-receipt', 'dedup-targets', 'dedup-snapshot', 'regenerate-dedup', 'aggregate-skips'])
    parser.add_argument('--aggregate-features')
    parser.add_argument('slice', nargs='?')
    args = parser.parse_args()
    if args.action not in {'run', 'shard-plan', 'fragment-plan', 'build-run', 'ran-tests', 'aggregate-emits', 'shard-receipt', 'dedup-snapshot', 'regenerate-dedup', 'aggregate-skips'} and args.slice:
        raise ValueError('slice selection is only supported by run')
    if args.action == 'ran-tests':
        # stdin: the libtest output of one filtered run or of a whole shard; the label names it in the message.
        ran, why = run_verdict(sys.stdin.read())
        if not ran:
            print('::error::' + str(args.slice) + ' ran no test: ' + why + ', so this step cannot pass', file=sys.stderr)
            return 1
        return 0
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
    # These read only a certificate or a target list; the manifest scan costs a second and a half per process,
    # which the shard runner would pay several times per job.
    if args.action == 'aggregate-emits':
        if not aggregate_module().emits(ROOT, args.slice, args.aggregate_features or ''):
            print('every module of ' + str(args.slice) + ' is owned by its own slice in this feature set: nothing to run', file=sys.stderr)
            return 1
        return 0
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
        targets = split_shard_items(args.slice, mode)
        if targets is None:
            raise ValueError('unknown/duplicate split shard')
        skip = ' '.join(relocated_cases(targets, mode))
        print(' '.join(targets) + '|' + skip)
        return 0
    rows = load_manifest()
    if args.action == 'build-run':
        if args.slice not in SHARED_BUILDS:
            raise ValueError('unknown shared build: ' + str(args.slice))
        for command in build_commands(args.slice):
            print('+ ' + ' '.join(command), flush=True)
            status = subprocess.call(command, cwd=ROOT, env=dict(os.environ, RUSTFLAGS=SHARED_BUILDS[args.slice]['rustflags']))
            if status:
                return status
        return 0
    if args.action == 'fragment-plan':
        # A fragment of a PR shard: the items it runs whole, then one tab-separated line per sliced target.
        fragment = fragment_from_env()
        if fragment is None or mode != 'normal' or fragment.get('row') != 'pr-' + str(args.slice):
            raise ValueError('fragment-plan needs a normal-mode fragment of this shard')
        fragment = resolve_fragment(fragment['row'], fragment, rows, plan_decision('normal', False))
        tests = fragment.get('tests', {})
        print(' '.join(t for t in fragment['targets'] if t not in tests))
        print('extras' if split_shard_items(args.slice, mode) is not None else 'none')
        for target, spec in tests.items():
            print('\t'.join([target] + slice_flags(spec)))
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
            outputs['build_matrix'] = json.dumps(build_matrix_of(bundles), separators=(',', ':'))
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
        normal_required(json.loads(Path(os.environ['CI_INTEGRATION_NEEDS_FILE']).read_text()), rows, enabled,
                        json.loads(Path(os.environ['CI_COMBINED_MATRIX_FILE']).read_text()),
                        json.loads(Path(os.environ['CI_BUNDLE_MATRIX_FILE']).read_text()), decision,
                        published_builds=json.loads(Path(os.environ['CI_BUILD_MATRIX_FILE']).read_text()))
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
        fragment = fragment_from_env()
        if fragment is not None:
            if mode != 'normal' or len(selected) != 1:
                raise ValueError('a fragment runs one normal-mode slice')
            fragment = resolve_fragment('integration-' + selected[0]['id'], fragment, rows, decision)
        statuses = [run_slice(row, invocation / row['id'], mode, **({'fragment': fragment} if fragment else {})) for row in selected]
        return next((status for status in statuses if status), 0)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f'::error::integration: {error}', file=sys.stderr)
        sys.exit(1)
