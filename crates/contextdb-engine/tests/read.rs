/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[cfg(unix)]
#[path = "read/a_budget_refusal_is_never_reported_as_a_corrupt_store.rs"]
mod a_budget_refusal_is_never_reported_as_a_corrupt_store;
// A crashed store opens for a reader only where the boot it crashed on can be
// identified (Linux); elsewhere the reader is told the store needs its writer.
#[cfg(target_os = "linux")]
#[path = "read/a_crashed_writers_store_opens_for_a_reader.rs"]
mod a_crashed_writers_store_opens_for_a_reader;
#[path = "read/a_damaged_edge_tombstone_keeps_its_typed_diagnosis.rs"]
mod a_damaged_edge_tombstone_keeps_its_typed_diagnosis;
#[cfg(unix)]
#[path = "read/a_finished_read_leaves_no_runtime_files.rs"]
mod a_finished_read_leaves_no_runtime_files;
#[cfg(unix)]
#[path = "read/a_finished_reader_removes_its_own_registration_record.rs"]
mod a_finished_reader_removes_its_own_registration_record;
#[path = "read/a_live_read_charge_is_the_reads_own.rs"]
mod a_live_read_charge_is_the_reads_own;
#[path = "read/a_local_drop_leaves_the_remaining_history_readable.rs"]
mod a_local_drop_leaves_the_remaining_history_readable;
#[cfg(unix)]
#[path = "read/a_read_only_consumer_opens_a_read_only_store.rs"]
mod a_read_only_consumer_opens_a_read_only_store;
#[cfg(unix)]
#[path = "read/a_received_table_replacement_retires_every_content_identity.rs"]
mod a_received_table_replacement_retires_every_content_identity;
#[path = "read/a_reopened_store_never_reuses_a_committed_transaction.rs"]
mod a_reopened_store_never_reuses_a_committed_transaction;
#[cfg(unix)]
#[path = "read/a_statement_on_the_file_route_costs_what_the_statement_costs.rs"]
mod a_statement_on_the_file_route_costs_what_the_statement_costs;
#[path = "read/an_erasure_invalidates_exactly_the_snapshots_older_than_it.rs"]
mod an_erasure_invalidates_exactly_the_snapshots_older_than_it;
#[cfg(unix)]
#[cfg(unix)]
#[path = "read/an_undeclared_count_answers_exactly_on_every_route.rs"]
mod an_undeclared_count_answers_exactly_on_every_route;
#[path = "read/authoritative_purge_reader_publication.rs"]
mod authoritative_purge_reader_publication;
#[path = "read/completed_read_totals_reach_an_out_of_crate_observer.rs"]
mod completed_read_totals_reach_an_out_of_crate_observer;
#[cfg(unix)]
#[path = "read/content_identity_independent_contract.rs"]
mod content_identity_independent_contract;
#[path = "read/cursor_admission_defers_to_waiting_schema_change.rs"]
mod cursor_admission_defers_to_waiting_schema_change;
#[path = "read/cursor_idle_registration_expiry.rs"]
mod cursor_idle_registration_expiry;
#[path = "read/cursor_retained_row_charge.rs"]
mod cursor_retained_row_charge;
#[path = "read/cursor_schema_change_same_session.rs"]
mod cursor_schema_change_same_session;
#[cfg(unix)]
#[path = "read/cursor_survives_writer_handover.rs"]
mod cursor_survives_writer_handover;
#[path = "read/direct_file_reader_contract_tests.rs"]
mod direct_file_reader_contract_tests;
#[cfg(unix)]
#[path = "read/disk_usage_reads_the_same_on_every_route.rs"]
mod disk_usage_reads_the_same_on_every_route;
#[cfg(unix)]
#[path = "read/every_read_classified_statement_runs_in_a_reading_session.rs"]
mod every_read_classified_statement_runs_in_a_reading_session;
#[cfg(unix)]
#[path = "read/existing_only_open_contract_tests.rs"]
mod existing_only_open_contract_tests;
#[cfg(unix)]
#[path = "read/graceful_owner_drop_completes_accepted_read.rs"]
mod graceful_owner_drop_completes_accepted_read;
#[path = "read/history_answer_is_complete_and_in_author_order.rs"]
mod history_answer_is_complete_and_in_author_order;
#[cfg(unix)]
#[path = "read/json_decode_admission_contract.rs"]
mod json_decode_admission_contract;
#[cfg(unix)]
#[path = "read/ledger_job_state_through_a_read_session_matches_the_writable_answer.rs"]
mod ledger_job_state_through_a_read_session_matches_the_writable_answer;
#[cfg(unix)]
#[path = "read/maintained_count_advice_is_executable.rs"]
mod maintained_count_advice_is_executable;
#[cfg(unix)]
#[path = "read/metadata_body_round_trip.rs"]
mod metadata_body_round_trip;
#[path = "read/metadata_byte_ceiling_holds_on_the_idle_file.rs"]
mod metadata_byte_ceiling_holds_on_the_idle_file;
#[cfg(unix)]
#[path = "read/metadata_door_route_parity.rs"]
mod metadata_door_route_parity;
#[cfg(unix)]
#[path = "read/open_reads_compact_vector_and_config_state.rs"]
mod open_reads_compact_vector_and_config_state;
#[cfg(target_os = "linux")]
#[path = "read/owner_cursor_decode_memory_release.rs"]
mod owner_cursor_decode_memory_release;
#[cfg(unix)]
#[path = "read/raw_vector_on_demand_source_contract.rs"]
mod raw_vector_on_demand_source_contract;
#[path = "read/read_contract_encoding.rs"]
mod read_contract_encoding;
#[path = "read/read_only_open_loads_only_the_working_set.rs"]
mod read_only_open_loads_only_the_working_set;
#[path = "read/read_persistence_coordination_contract_tests.rs"]
mod read_persistence_coordination_contract_tests;
#[path = "read/read_progress_reporting.rs"]
mod read_progress_reporting;
#[path = "read/read_registration_stays_answerable_under_a_removal_pass.rs"]
mod read_registration_stays_answerable_under_a_removal_pass;
#[cfg(unix)]
#[path = "read/read_schema_carries_the_scope_label_form.rs"]
mod read_schema_carries_the_scope_label_form;
#[path = "read/read_session_application_contract.rs"]
mod read_session_application_contract;
#[path = "read/read_session_declared_principal_authority.rs"]
mod read_session_declared_principal_authority;
#[path = "read/read_session_declared_visibility.rs"]
mod read_session_declared_visibility;
#[path = "read/read_session_route_contract.rs"]
mod read_session_route_contract;
#[cfg(unix)]
#[path = "read/read_snapshot_instant.rs"]
mod read_snapshot_instant;
#[path = "read/read_visibility_route_parity.rs"]
mod read_visibility_route_parity;
#[cfg(unix)]
#[path = "read/reader_breadcrumbs_live_in_the_default_runtime_location.rs"]
mod reader_breadcrumbs_live_in_the_default_runtime_location;
#[cfg(unix)]
#[path = "read/reader_snapshot_registration_lifecycle.rs"]
mod reader_snapshot_registration_lifecycle;
#[path = "read/reads_do_not_pay_for_the_writers_retention.rs"]
mod reads_do_not_pay_for_the_writers_retention;
#[cfg(unix)]
#[path = "read/recorded_owner_state_answers_the_reader.rs"]
mod recorded_owner_state_answers_the_reader;
#[cfg(unix)]
#[path = "read/relational_on_demand_source_contract.rs"]
mod relational_on_demand_source_contract;
#[cfg(unix)]
#[path = "read/saved_row_key_integrity.rs"]
mod saved_row_key_integrity;
#[cfg(unix)]
#[path = "read/session_charged_bytes.rs"]
mod session_charged_bytes;
#[cfg(unix)]
#[path = "read/vector_compaction_publication_contract.rs"]
mod vector_compaction_publication_contract;
#[path = "read/verified_ordered_run_work_resume_contract.rs"]
mod verified_ordered_run_work_resume_contract;
#[path = "read/writer_held_store_names_its_holder.rs"]
mod writer_held_store_names_its_holder;
#[path = "read/writer_ordered_rows_use_exact_versions.rs"]
mod writer_ordered_rows_use_exact_versions;
#[path = "read/writing_handle_bounded_read_transaction_boundaries.rs"]
mod writing_handle_bounded_read_transaction_boundaries;

#[cfg(unix)]
#[path = "read/reader_continuity_contract.rs"]
mod reader_continuity_contract;
#[cfg(unix)]
#[path = "read/reader_operation_expiry_contract.rs"]
mod reader_operation_expiry_contract;
#[cfg(unix)]
#[path = "read/reader_proof_support.rs"]
mod reader_proof_support;
#[cfg(unix)]
#[path = "read/working_set_bounded_contract.rs"]
mod working_set_bounded_contract;

#[path = "read/content_size_independent_contract.rs"]
mod content_size_independent_contract;
