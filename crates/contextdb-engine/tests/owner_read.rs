/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[cfg(unix)]
#[path = "owner_read/a_bad_owner_channel_root_never_vetoes_the_reader_wait.rs"]
mod a_bad_owner_channel_root_never_vetoes_the_reader_wait;
#[cfg(unix)]
#[path = "owner_read/a_claim_window_resolves_to_the_writer_s_own_answer.rs"]
mod a_claim_window_resolves_to_the_writer_s_own_answer;
#[cfg(unix)]
#[path = "owner_read/a_claimed_store_is_never_reported_absent.rs"]
mod a_claimed_store_is_never_reported_absent;
#[cfg(unix)]
#[path = "owner_read/a_companionless_store_is_owned_from_its_first_claim.rs"]
mod a_companionless_store_is_owned_from_its_first_claim;
#[cfg(unix)]
#[path = "owner_read/a_coordination_wait_ends_when_its_caller_says_so.rs"]
mod a_coordination_wait_ends_when_its_caller_says_so;
#[cfg(unix)]
#[path = "owner_read/a_store_is_owned_from_the_moment_it_is_claimed.rs"]
mod a_store_is_owned_from_the_moment_it_is_claimed;
#[path = "owner_read/owner_channel_interrupts_without_a_report.rs"]
mod owner_channel_interrupts_without_a_report;
#[cfg(unix)]
#[path = "owner_read/owner_channel_lifetime.rs"]
mod owner_channel_lifetime;
#[cfg(unix)]
#[path = "owner_read/owner_mismatch_is_answered_before_the_owner_closes.rs"]
mod owner_mismatch_is_answered_before_the_owner_closes;
#[path = "owner_read/owner_only_session_never_touches_the_file.rs"]
mod owner_only_session_never_touches_the_file;
#[path = "owner_read/owner_read_admission_contract.rs"]
mod owner_read_admission_contract;
#[path = "owner_read/owner_read_client_contract.rs"]
mod owner_read_client_contract;
#[cfg(unix)]
#[path = "owner_read/owner_read_cross_process_cancellation.rs"]
mod owner_read_cross_process_cancellation;
#[path = "owner_read/owner_read_lifecycle_contract.rs"]
mod owner_read_lifecycle_contract;
#[path = "owner_read/owner_read_memory_ceiling_surfacing.rs"]
mod owner_read_memory_ceiling_surfacing;
#[cfg(unix)]
#[path = "owner_read/owner_read_panic_fails_the_request_not_the_connection.rs"]
mod owner_read_panic_fails_the_request_not_the_connection;
#[cfg(unix)]
#[path = "owner_read/owner_read_service_contract.rs"]
mod owner_read_service_contract;
#[cfg(unix)]
#[path = "owner_read/owner_report_surface.rs"]
mod owner_report_surface;
#[cfg(unix)]
#[path = "owner_read/owner_route_honors_declared_byte_budgets.rs"]
mod owner_route_honors_declared_byte_budgets;
#[path = "owner_read/owner_route_progress_and_cancellation.rs"]
mod owner_route_progress_and_cancellation;
