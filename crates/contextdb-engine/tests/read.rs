/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[path = "read/completed_read_totals_reach_an_out_of_crate_observer.rs"]
mod completed_read_totals_reach_an_out_of_crate_observer;
#[path = "read/cursor_admission_defers_to_waiting_schema_change.rs"]
mod cursor_admission_defers_to_waiting_schema_change;
#[path = "read/cursor_idle_registration_expiry.rs"]
mod cursor_idle_registration_expiry;
#[path = "read/cursor_retained_row_charge.rs"]
mod cursor_retained_row_charge;
#[path = "read/cursor_schema_change_same_session.rs"]
mod cursor_schema_change_same_session;
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
#[path = "read/metadata_body_round_trip.rs"]
mod metadata_body_round_trip;
#[path = "read/metadata_byte_ceiling_holds_on_the_idle_file.rs"]
mod metadata_byte_ceiling_holds_on_the_idle_file;
#[cfg(unix)]
#[path = "read/metadata_door_route_parity.rs"]
mod metadata_door_route_parity;
#[path = "read/read_contract_encoding.rs"]
mod read_contract_encoding;
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
#[path = "read/recorded_owner_state_answers_the_reader.rs"]
mod recorded_owner_state_answers_the_reader;
#[path = "read/writer_held_store_names_its_holder.rs"]
mod writer_held_store_names_its_holder;
#[path = "read/writing_handle_bounded_read_transaction_boundaries.rs"]
mod writing_handle_bounded_read_transaction_boundaries;
