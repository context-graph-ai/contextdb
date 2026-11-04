//! Passive observations of the declarations one outbound sync extraction reads,
//! keeps, suppresses and validates, for integration tests.
//!
//! An outbound push or pending read never re-sends a declaration it received
//! from a peer, except that a destination re-upload resends what the edge held
//! at the move. These counters let a test prove that suppressing received
//! declarations retains none of them. Every place that reads, keeps,
//! suppresses or validates an outbound declaration reports here, whatever
//! shape the read takes; a change to the outbound path keeps reporting the same
//! facts.
//!
//! - `walked`: one declaration occurrence read from history for outbound work.
//! - `collected`: one occurrence placed into outbound output, the change set
//!   the final filter receives.
//! - `suppressed`: one received occurrence the extraction decided not to send.
//! - `suppressed_held_peak`: the most suppressed occurrences alive at the same
//!   moment, counted from decode until freed, anywhere in the extraction. A
//!   path that classifies one decoded window at a time reports 1.
//! - `suppressed_output_peak_bytes`: the most decoded bytes of suppressed
//!   occurrences sitting in outbound output (or any collection kept past their
//!   own window) at the same moment. A path that drops a received occurrence
//!   in its own window reports 0.
//! - `validation_records_peak`: the most per-group or per-occurrence records
//!   (group states, mark sets) the arrival and superseded checks held at the
//!   same moment.
//! - `resident_ddl_priced_bytes`: bytes the resident declaration log priced for
//!   this extraction before reading it.
//! - `reupload_epoch_reads`: reads of the destination re-upload epoch.
//! - `fallback_collector_hits`: entries into the four whole-history collectors
//!   named by [`FallbackCollector`].

use std::cell::Cell;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OutboundDdlWork {
    pub walked: u64,
    pub collected: u64,
    pub suppressed: u64,
    pub suppressed_held_peak: u64,
    pub suppressed_output_peak_bytes: u64,
    pub validation_records_peak: u64,
    pub resident_ddl_priced_bytes: u64,
    pub reupload_epoch_reads: u64,
    pub fallback_collector_hits: [u64; 4],
}

/// The whole-history collectors counted by `fallback_collector_hits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackCollector {
    /// `raw_ddl_log_occurrence` answering from the whole durable history.
    RawOccurrenceDurableHistory = 0,
    /// `persisted_state_from_snapshot_projected` reading history from LSN 0.
    PersistedStateFromSnapshot = 1,
    /// `persisted_state_since_through` reading history from LSN 0.
    PersistedStateSinceThrough = 2,
    /// `received_schema_as_authored` reading the whole provenance history.
    ReceivedSchemaAsAuthored = 3,
}

thread_local! {
    static WORK: Cell<Option<OutboundDdlWork>> = const { Cell::new(None) };
}

/// Run `operation` and return what the outbound declaration path did on this
/// thread while it ran.
pub fn observe<T>(operation: impl FnOnce() -> T) -> (T, OutboundDdlWork) {
    struct Restore(Option<OutboundDdlWork>);
    impl Drop for Restore {
        fn drop(&mut self) {
            WORK.with(|work| work.set(self.0));
        }
    }
    let restore = Restore(WORK.with(|work| work.replace(Some(OutboundDdlWork::default()))));
    let value = operation();
    let counters = WORK.with(|work| work.get().expect("observation remains installed"));
    drop(restore);
    (value, counters)
}

fn update(change: impl FnOnce(&mut OutboundDdlWork)) {
    WORK.with(|slot| {
        if let Some(mut work) = slot.get() {
            change(&mut work);
            slot.set(Some(work));
        }
    });
}

pub(crate) fn note_walked() {
    update(|work| work.walked += 1);
}

pub(crate) fn note_collected() {
    update(|work| work.collected += 1);
}

/// `count` received occurrences were suppressed; `held` of them were alive at
/// one moment, and `output_bytes` of their decoded bytes sat in outbound output
/// at that moment.
pub(crate) fn note_suppressed(count: u64, held: u64, output_bytes: u64) {
    update(|work| {
        work.suppressed += count;
        work.suppressed_held_peak = work.suppressed_held_peak.max(held);
        work.suppressed_output_peak_bytes = work.suppressed_output_peak_bytes.max(output_bytes);
    });
}

pub(crate) fn note_validation_records(held: u64) {
    update(|work| work.validation_records_peak = work.validation_records_peak.max(held));
}

pub(crate) fn note_resident_ddl_priced(bytes: usize) {
    update(|work| work.resident_ddl_priced_bytes += bytes as u64);
}

pub(crate) fn note_reupload_epoch_read() {
    update(|work| work.reupload_epoch_reads += 1);
}

pub(crate) fn note_fallback(collector: FallbackCollector) {
    update(|work| work.fallback_collector_hits[collector as usize] += 1);
}
