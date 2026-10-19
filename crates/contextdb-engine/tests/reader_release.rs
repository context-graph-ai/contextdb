#[cfg(unix)]
#[path = "reader_release/a_caught_signal_does_not_interrupt_a_default_writer_wait.rs"]
mod a_caught_signal_does_not_interrupt_a_default_writer_wait;
// A crashed store opens for a reader only where the boot it crashed on can be
// identified (Linux); elsewhere the reader is told the store needs its writer.
#[cfg(target_os = "linux")]
#[path = "reader_release/a_crashed_stores_reader_walk_runs_outside_admission.rs"]
mod a_crashed_stores_reader_walk_runs_outside_admission;
#[cfg(unix)]
#[path = "reader_release/a_dead_writer_process_releases_the_whole_admission.rs"]
mod a_dead_writer_process_releases_the_whole_admission;
#[cfg(unix)]
#[path = "reader_release/a_paused_reader_belongs_to_the_journey_that_stopped_it.rs"]
mod a_paused_reader_belongs_to_the_journey_that_stopped_it;
#[cfg(unix)]
#[path = "reader_release/a_purge_during_hydration_is_never_served.rs"]
mod a_purge_during_hydration_is_never_served;
#[cfg(unix)]
#[path = "reader_release/a_reader_probe_never_refuses_a_writer.rs"]
mod a_reader_probe_never_refuses_a_writer;
#[cfg(unix)]
#[path = "reader_release/a_registration_without_a_fence_refuses_purge.rs"]
mod a_registration_without_a_fence_refuses_purge;
// The cancellable wait's helper exists on Linux x86_64 and aarch64 only.
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[path = "reader_release/a_resolved_quarantine_releases_the_helper_stack.rs"]
mod a_resolved_quarantine_releases_the_helper_stack;
#[cfg(unix)]
#[path = "reader_release/a_writable_open_proceeds_while_a_reader_holds_its_snapshot.rs"]
mod a_writable_open_proceeds_while_a_reader_holds_its_snapshot;
#[cfg(unix)]
#[path = "reader_release/a_writer_open_waits_once_for_a_starting_reader.rs"]
mod a_writer_open_waits_once_for_a_starting_reader;
#[cfg(unix)]
#[path = "reader_release/admission_is_closed_by_the_writer_claim.rs"]
mod admission_is_closed_by_the_writer_claim;
#[cfg(unix)]
#[path = "reader_release/admission_is_keyed_on_the_physical_store.rs"]
mod admission_is_keyed_on_the_physical_store;
#[cfg(unix)]
#[path = "reader_release/classification_is_a_registered_reader.rs"]
mod classification_is_a_registered_reader;
#[cfg(unix)]
#[path = "reader_release/the_reader_release_wait_follows_the_real_hold.rs"]
mod the_reader_release_wait_follows_the_real_hold;
#[cfg(unix)]
#[path = "reader_release/the_reader_release_wait_never_arms_a_timer.rs"]
mod the_reader_release_wait_never_arms_a_timer;
// The cancellable wait's helper exists on Linux x86_64 and aarch64 only.
#[cfg(unix)]
#[path = "reader_release/a_paused_classification_does_not_hold_up_readers_or_writers.rs"]
mod a_paused_classification_does_not_hold_up_readers_or_writers;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[path = "reader_release/the_writer_wait_helper_is_contained.rs"]
mod the_writer_wait_helper_is_contained;
#[cfg(unix)]
#[path = "reader_release/waiting_for_a_store_s_readers_to_release_it.rs"]
mod waiting_for_a_store_s_readers_to_release_it;
#[cfg(unix)]
#[path = "reader_release/writer_acquisition_support.rs"]
mod writer_acquisition_support;
