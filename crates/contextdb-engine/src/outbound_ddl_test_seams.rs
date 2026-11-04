//! Passive observations of what one outbound sync operation does with
//! declaration history, for integration tests.
//!
//! Every observation sits in an engine access function that outbound work
//! cannot route around, never in the outbound walk itself, so a walk is judged
//! by what it reads and what reaches the code after it, not by what it says
//! about itself:
//!
//! - `walked`: every `(source LSN, ordinal)` the declaration history cursor
//!   decodes, in order (`next_ddl_history`).
//! - `walked_held`: for each entry of `walked`, the bytes the database's
//!   memory accountant had charged when the cursor began decoding it. It is
//!   the accountant's own number, read in the access function, so whatever a
//!   walk keeps charged from one occurrence to the next (a buffered group, an
//!   admitted output vector, a retained window) shows up between consecutive
//!   entries without the walk reporting it.
//! - `collected`: every `(source LSN, ordinal)` in the change set the final
//!   received-declaration filter receives (`filter_outbound_received_ddl`).
//!   What was walked and is not here was dropped before the filter.
//!   Decodes made by the point read of one stored occurrence
//!   (`raw_ddl_log_occurrence`, which the evidence checks use) are not walked
//!   history; that occurrence is recorded in `raw_reads` instead.
//! - `arrival_lookups` / `superseded_lookups`: every per-occurrence read of the
//!   received-arrival and superseded evidence stores.
//! - `digests_checked`: every occurrence whose evidence digest was recomputed
//!   to check a mark against the stored declaration.
//! - `validation_records_peak`: the most per-group and per-occurrence records
//!   the existing arrival and superseded validators held at one moment,
//!   counted at each insertion (so state retained before an early refusal is
//!   included) and including the arrival marks the filter still holds while
//!   superseded validation runs. Any new set or map an outbound walk builds to
//!   validate or classify received or superseded groups is counted through
//!   these same counters ([`note_validation_records`] for each insertion, or
//!   a [`hold_validation_records`] guard while it stays alive); the scalar
//!   state of the one group in progress is not a record. A record kept for
//!   dropped history therefore breaks the exact expectation the tests assert.
//! - `events`: the destination re-upload epoch read and every frontier handed
//!   to the final filter, the re-upload delete synthesis and the arrival
//!   filter, in order with the walked occurrences.
//! - `sent`: the provenance of every declaration placed into a push request
//!   (`wire_changeset_with_arrivals_lineages_and_ddl_provenance`), and
//!   `sent_ddl` the declarations in those requests.
//! - `fallback_collector_hits`: entries into the four whole-history collectors
//!   named by [`FallbackCollector`].
//!
//! Observation is per thread: the operation must run on the observing
//! thread, and a test asserts nonzero walked work so an operation moved off
//! that thread fails loudly instead of passing on empty observations.

use contextdb_core::Lsn;
use std::cell::RefCell;

/// One declaration occurrence: its local source LSN and ordinal.
pub type Occurrence = (Lsn, u32);

/// One step of outbound work, in the order it happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundDdlEvent {
    /// The destination re-upload epoch was read; its held-through frontier.
    EpochRead(Option<Lsn>),
    /// The history cursor decoded this occurrence.
    Walked(Occurrence),
    /// The final received-declaration filter ran with this frontier.
    FilterFrontier(Option<Lsn>),
    /// Re-upload delete synthesis ran through this frontier.
    DeleteFrontier(Lsn),
    /// The arrived-by-sync row filter ran with this frontier.
    ArrivalFrontier(Option<Lsn>),
}

/// The provenance of one declaration placed into a push request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentDdl {
    /// The sender's source LSN and ordinal.
    pub source: Occurrence,
    /// The author block: node, the author's source LSN and ordinal.
    pub author: Option<(String, Lsn, u32)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OutboundDdlWork {
    /// Source accountant high-water immediately before the first durable reply commit.
    pub pre_reply_high_water: Option<usize>,
    pub walked: Vec<Occurrence>,
    pub walked_held: Vec<usize>,
    pub collected: Vec<Occurrence>,
    pub raw_reads: Vec<Occurrence>,
    pub arrival_lookups: Vec<Occurrence>,
    pub superseded_lookups: Vec<Occurrence>,
    pub digests_checked: Vec<Occurrence>,
    pub validation_records_peak: u64,
    pub events: Vec<OutboundDdlEvent>,
    pub sent: Vec<SentDdl>,
    pub sent_ddl: u64,
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

struct Observation {
    work: OutboundDdlWork,
    /// Validation records held by a caller while a validator runs.
    records_held: u64,
    /// A point read of one stored occurrence is in progress.
    in_raw_read: bool,
}

thread_local! {
    static WORK: RefCell<Option<Observation>> = const { RefCell::new(None) };
}

/// Run `operation` and return what outbound declaration work did on this
/// thread while it ran.
pub fn observe<T>(operation: impl FnOnce() -> T) -> (T, OutboundDdlWork) {
    struct Restore(Option<Observation>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            WORK.with(|work| *work.borrow_mut() = previous);
        }
    }
    let restore = Restore(WORK.with(|work| {
        work.borrow_mut().replace(Observation {
            work: OutboundDdlWork::default(),
            records_held: 0,
            in_raw_read: false,
        })
    }));
    let value = operation();
    let observed = WORK.with(|work| {
        work.borrow_mut()
            .take()
            .expect("observation remains installed")
            .work
    });
    drop(restore);
    (value, observed)
}

/// Capture before the real durable reply commit, without resetting the accountant.
/// Later reply commits cannot overwrite the extraction/build phase's boundary.
pub(crate) fn note_pre_reply_high_water(peak: usize) {
    update(|observation| {
        observation.work.pre_reply_high_water.get_or_insert(peak);
    });
}

fn update(change: impl FnOnce(&mut Observation)) {
    WORK.with(|slot| {
        if let Some(observation) = slot.borrow_mut().as_mut() {
            change(observation);
        }
    });
}

/// The ordinal a declaration log key names: `"<lsn>"` holds the only
/// occurrence of its LSN, `"<lsn>:<index>"` one of several.
pub(crate) fn note_walked_key(lsn: Lsn, key: &str) {
    let ordinal = key
        .split_once(':')
        .and_then(|(_, index)| index.parse::<u32>().ok())
        .unwrap_or(0);
    update(|observation| {
        if observation.in_raw_read {
            return;
        }
        observation.work.walked.push((lsn, ordinal));
        observation
            .work
            .events
            .push(OutboundDdlEvent::Walked((lsn, ordinal)));
    });
}

/// The accountant's charged bytes as the cursor begins decoding the occurrence
/// [`note_walked_key`] just recorded; ignored inside a point read, as that is.
pub(crate) fn note_walked_held(used: usize) {
    update(|observation| {
        if observation.in_raw_read {
            return;
        }
        observation.work.walked_held.push(used);
    });
}

/// A point read of one stored occurrence, open until dropped.
pub(crate) struct RawRead(bool);

pub(crate) fn raw_read(occurrence: Occurrence) -> RawRead {
    let mut outer = false;
    update(|observation| {
        observation.work.raw_reads.push(occurrence);
        outer = !observation.in_raw_read;
        observation.in_raw_read = true;
    });
    RawRead(outer)
}

impl Drop for RawRead {
    fn drop(&mut self) {
        if self.0 {
            update(|observation| observation.in_raw_read = false);
        }
    }
}

pub(crate) fn note_collected(occurrences: impl Iterator<Item = Occurrence>) {
    update(|observation| observation.work.collected.extend(occurrences));
}

pub(crate) fn note_arrival_lookup(occurrence: Occurrence) {
    update(|observation| observation.work.arrival_lookups.push(occurrence));
}

pub(crate) fn note_superseded_lookup(occurrence: Occurrence) {
    update(|observation| observation.work.superseded_lookups.push(occurrence));
}

pub(crate) fn note_digest_checked(occurrence: Occurrence) {
    update(|observation| observation.work.digests_checked.push(occurrence));
}

/// A validator now holds `records` records of its own.
pub(crate) fn note_validation_records(records: usize) {
    update(|observation| {
        let live = observation.records_held + records as u64;
        observation.work.validation_records_peak =
            observation.work.validation_records_peak.max(live);
    });
}

/// Validation records a caller keeps alive while it runs further validation.
pub(crate) struct HeldValidationRecords(u64);

pub(crate) fn hold_validation_records(records: usize) -> HeldValidationRecords {
    let records = records as u64;
    update(|observation| observation.records_held += records);
    note_validation_records(0);
    HeldValidationRecords(records)
}

impl Drop for HeldValidationRecords {
    fn drop(&mut self) {
        let records = self.0;
        update(|observation| observation.records_held -= records);
    }
}

pub(crate) fn note_event(event: OutboundDdlEvent) {
    update(|observation| observation.work.events.push(event));
}

pub(crate) fn note_sent(ddl: usize, provenance: impl Iterator<Item = SentDdl>) {
    update(|observation| {
        observation.work.sent_ddl += ddl as u64;
        observation.work.sent.extend(provenance);
    });
}

pub(crate) fn note_fallback(collector: FallbackCollector) {
    update(|observation| observation.work.fallback_collector_hits[collector as usize] += 1);
}
