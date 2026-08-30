//! What a read has done so far, told to the caller while it is still running.
//!
//! A read that takes a while is otherwise indistinguishable from a read that
//! is stuck: the caller sits in one blocking call with nothing to show and no
//! way to tell "still working" from "wedged". So a caller may hand a session
//! an observer, and the read tells it what it has done so far -- rows and
//! bytes assembled, items examined, milliseconds spent -- while the answer is
//! still being built, and store bytes loaded while the store is still being
//! opened.
//!
//! The report is made on the thread running the read, inline, with no
//! buffering and no channel: what the observer is told is what had happened at
//! the moment it was told, and nothing arrives after the operation has
//! returned. Because the report is made from inside the read, the observer's
//! own work is part of that read's cost; an observer that blocks blocks the
//! read.

use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::sync::Arc;

/// Which part of a read a report describes.
///
/// The phase travels the read channel, so its variants are positional wire:
/// a new phase appends after the last one and no existing phase moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadPhase {
    /// The store is being opened and its committed image loaded.
    Hydrating,
    /// A statement is being executed and its answer assembled.
    Executing,
}

/// What one read has done so far.
///
/// Every counter is the read's own progress against the ceiling of the same
/// name, so a caller can render "so far" against the limit that will stop it.
/// A counter that does not belong to the reported phase is zero rather than
/// stale: a hydrating read has assembled no rows, and an executing read is
/// loading no store bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadProgress {
    pub phase: ReadPhase,
    /// Complete rows assembled so far; zero while hydrating.
    pub rows: u64,
    /// Canonical bytes assembled so far, the counter `result_bytes` and
    /// `cursor_page_bytes` bound; zero while hydrating.
    pub bytes: u64,
    /// Store bytes loaded so far; zero outside hydration.
    pub loaded_bytes: u64,
    /// Bytes this hydration expects to load, when that total is known. A
    /// number that would have to be estimated is reported as absent rather
    /// than guessed.
    pub total_bytes: Option<u64>,
    /// Items examined so far, the counter `work` bounds.
    pub work: u64,
    /// Active execution milliseconds so far, on the same monotonic clock
    /// `active_ms` bounds.
    pub active_ms: u64,
}

/// A caller that wants to be told what a read has done while it runs.
pub trait ReadProgressObserver: Send + Sync {
    fn progress(&self, progress: ReadProgress);

    /// The totals a read finished on, told once, after it has stopped doing
    /// work and before it returns.
    ///
    /// This is ACCOUNTING, not liveness, and the two are deliberately
    /// separate. A caller watching a read run is told about it only when the
    /// intervals below say there is something new to say -- that is the whole
    /// promise of `progress`, and a read that finishes inside one interval
    /// still says nothing there. A caller ADDING UP what reads did needs the
    /// number regardless, or a completed scan of 250 rows counts as zero work
    /// it never measured. So the totals come through here instead, exactly
    /// once per completed read.
    ///
    /// It is in-process only: nothing about it is on the local channel, and
    /// an owner serving a reader over that channel publishes nothing extra.
    /// The default does nothing, so an observer that only wants liveness is
    /// unaffected and no caller has to know this exists.
    #[doc(hidden)]
    fn completed(&self, totals: ReadProgress) {
        let _ = totals;
    }
}

/// How much a read must do between two reports.
///
/// A report is worth making when there is something new to say, and worth
/// withholding when there is not: a read that examines a handful of items
/// finishes before any progress display would be useful, and a read that
/// reported every item would spend more time reporting than reading. So the
/// first report is made once a read has passed one interval's worth of work,
/// and one further report is made per interval after that.
pub(crate) const WORK_REPORTING_INTERVAL: u64 = 1_000;
/// How many rows of the answer must be assembled between two reports.
///
/// Examined items and assembled rows are not the same news and do not arrive
/// together: a read that must sort before it can answer examines the whole
/// table with no row of the answer in hand, and then assembles the answer with
/// nothing left to examine. A caller told only about examined items would
/// watch that second half in silence, with the row count stuck at zero right
/// up to the moment the answer arrived. So assembling rows earns its own
/// reports, one per page's worth -- the shipped row ceiling, which is what a
/// caller's own sense of "a lot of rows" is scaled to.
pub(crate) const ROW_REPORTING_INTERVAL: u64 = 500;
pub(crate) const HYDRATION_REPORTING_INTERVAL: u64 = 1024 * 1024;

/// The observer of the read this thread is running, and what hydration has
/// loaded for it so far.
///
/// A read runs to completion on the thread that asked for it, so the thread is
/// where the answer to "who wants to hear about this read" lives. A session
/// puts its observer in force for exactly the span of one operation and takes
/// it back afterwards, so an observer is never told about a read its caller
/// did not ask for, and no report can arrive once the operation has returned.
struct ActiveRead {
    observer: Arc<dyn ReadProgressObserver>,
    loaded_bytes: Cell<u64>,
    reported_loaded_bytes: Cell<u64>,
}

thread_local! {
    static ACTIVE_READ: RefCell<Option<Arc<ActiveRead>>> = const { RefCell::new(None) };
}

/// Put an observer in force for the span of one operation, answering with
/// whatever was there before so the caller can put it back.
fn install(observer: Option<Arc<ActiveRead>>) -> Option<Arc<ActiveRead>> {
    ACTIVE_READ.with(|slot| slot.replace(observer))
}

/// Run one operation with this caller's progress observer in force.
///
/// The observer is taken back even if the operation panics, so a read that
/// came apart cannot leave a later, unrelated read reporting to it.
pub(crate) fn with_progress_observer<T>(
    observer: Option<&Arc<dyn ReadProgressObserver>>,
    operation: impl FnOnce() -> T,
) -> T {
    let Some(observer) = observer else {
        return operation();
    };
    struct Restore {
        previous: Option<Arc<ActiveRead>>,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            install(self.previous.take());
        }
    }
    #[allow(clippy::arc_with_non_send_sync)]
    let previous = install(Some(Arc::new(ActiveRead {
        observer: Arc::clone(observer),
        loaded_bytes: Cell::new(0),
        reported_loaded_bytes: Cell::new(0),
    })));
    let _restore = Restore { previous };
    operation()
}

/// Whoever asked to hear about the read this thread is running.
pub(crate) fn observer_for_this_read() -> Option<Arc<dyn ReadProgressObserver>> {
    ACTIVE_READ.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|active| Arc::clone(&active.observer))
    })
}

/// Count store bytes this hydration has loaded, and report once each further
/// interval's worth has arrived.
///
/// Hydration has no rows, no bytes of answer, and no examined items to report,
/// and the bytes it expects to load are not known before it has loaded them --
/// a store file's length counts space the image does not occupy -- so those
/// counters are reported as zero and the expected total as absent.
pub(crate) fn note_hydrated_bytes(bytes: u64) {
    let report = ACTIVE_READ.with(|slot| {
        let borrowed = slot.borrow();
        let active = borrowed.as_ref()?;
        let loaded = active.loaded_bytes.get().saturating_add(bytes);
        active.loaded_bytes.set(loaded);
        if loaded
            < active
                .reported_loaded_bytes
                .get()
                .saturating_add(HYDRATION_REPORTING_INTERVAL)
        {
            return None;
        }
        active.reported_loaded_bytes.set(loaded);
        Some((Arc::clone(&active.observer), loaded))
    });
    let Some((observer, loaded_bytes)) = report else {
        return;
    };
    observer.progress(ReadProgress {
        phase: ReadPhase::Hydrating,
        rows: 0,
        bytes: 0,
        loaded_bytes,
        total_bytes: None,
        work: 0,
        active_ms: 0,
    });
}

#[cfg(test)]
mod final_accounting_proofs {
    use super::*;
    use crate::{Database, ReadSession, ReadSessionOptions};
    use contextdb_core::Value;
    use contextdb_core::read_contract::ReadLimits;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Keeps the two surfaces apart, so a proof can say which one spoke.
    #[derive(Default)]
    struct Recorder {
        liveness: Mutex<Vec<ReadProgress>>,
        totals: Mutex<Vec<ReadProgress>>,
    }

    impl Recorder {
        fn liveness(&self) -> Vec<ReadProgress> {
            self.liveness.lock().expect("liveness records").clone()
        }

        fn totals(&self) -> Vec<ReadProgress> {
            self.totals.lock().expect("total records").clone()
        }
    }

    impl ReadProgressObserver for Recorder {
        fn progress(&self, progress: ReadProgress) {
            self.liveness
                .lock()
                .expect("liveness records")
                .push(progress);
        }

        fn completed(&self, totals: ReadProgress) {
            self.totals.lock().expect("total records").push(totals);
        }
    }

    /// An observer that wants only liveness: the accounting seam's default
    /// does nothing, so nothing about it reaches a caller who never asked.
    struct LivenessOnly;

    impl ReadProgressObserver for LivenessOnly {
        fn progress(&self, _progress: ReadProgress) {}
    }

    fn seeded_store(directory: &std::path::Path, rows: usize) -> std::path::PathBuf {
        let path = directory.join("accounted.db");
        let database = Database::open(&path).expect("claim a store to seed");
        database
            .execute(
                "CREATE TABLE counted (id INTEGER PRIMARY KEY, label TEXT)",
                &HashMap::new(),
            )
            .expect("create the counted table");
        for row in 0..rows {
            database
                .execute(
                    "INSERT INTO counted (id, label) VALUES ($id, $label)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(row as i64)),
                        ("label".to_owned(), Value::Text(format!("row-{row}"))),
                    ]),
                )
                .expect("seed one row");
        }
        database.close().expect("release the seeded store");
        path
    }

    /// A read short enough to finish inside one reporting interval says
    /// NOTHING to a caller watching it run -- that withholding is the whole
    /// promise of the liveness surface -- and still accounts for exactly what
    /// it examined and assembled.
    #[test]
    fn a_short_read_stays_silent_on_liveness_and_still_accounts_for_itself() {
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), 3);
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let answer = session
            .execute("SELECT label FROM counted ORDER BY id", &HashMap::new())
            .expect("the committed file answers");
        assert_eq!(answer.rows.len(), 3);

        assert!(
            recorder
                .liveness()
                .iter()
                .all(|report| report.phase != ReadPhase::Executing),
            "a read that finished inside one interval reports no executing liveness: {:?}",
            recorder.liveness()
        );
        let totals = recorder.totals();
        assert_eq!(
            totals.len(),
            1,
            "a completed read accounts for itself exactly once: {totals:?}"
        );
        assert_eq!(totals[0].phase, ReadPhase::Executing);
        assert_eq!(totals[0].rows, 3, "the totals are the read's real answer");
        assert!(
            totals[0].work > 0,
            "the totals carry the work nobody would otherwise have measured: {:?}",
            totals[0]
        );
    }

    /// An aggregate walks every row it folds, and the seam must say so. A
    /// caller adding up what reads cost cannot be handed one row and told
    /// nothing was examined to produce it.
    #[test]
    fn an_aggregate_accounts_for_every_row_it_walked() {
        const WALKED: usize = 250;
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), WALKED);
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let answer = session
            .execute("SELECT COUNT(*) FROM counted", &HashMap::new())
            .expect("the committed file folds the count");
        assert_eq!(answer.rows.len(), 1, "an aggregate answers one row");

        let totals = recorder.totals();
        assert_eq!(
            totals.len(),
            1,
            "one completed read accounts once: {totals:?}"
        );
        assert!(
            totals[0].work >= WALKED as u64,
            "an aggregate that folded {WALKED} rows examined at least that many: {:?}",
            totals[0]
        );
    }

    /// A cursor hands its pages back one at a time, and each page accounts for
    /// the work that produced it.
    #[test]
    fn a_cursor_page_accounts_for_the_rows_it_walked() {
        const WALKED: usize = 100;
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), WALKED);
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let cursor = session
            .open_cursor("SELECT label FROM counted ORDER BY id", &HashMap::new())
            .expect("the committed file opens a cursor");
        let published = cursor.first_page().rows.len();
        assert!(published > 0, "the first page hands back rows");

        let totals = recorder.totals();
        assert!(
            !totals.is_empty(),
            "a published page accounts for itself: {totals:?}"
        );
        let examined = totals.iter().map(|total| total.work).max().unwrap_or(0);
        assert!(
            examined >= published as u64,
            "a page of {published} rows examined at least that many: {totals:?}"
        );
    }

    /// The shape an in-process library consumer reads through: a session on
    /// the live database it already holds. It must account exactly as the
    /// committed-file route does.
    #[test]
    fn a_live_database_session_accounts_for_every_row_it_walked() {
        const WALKED: usize = 250;
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = directory.path().join("in-process.db");
        let database = Database::open(&path).expect("claim a store");
        database
            .execute(
                "CREATE TABLE counted (id INTEGER PRIMARY KEY, label TEXT)",
                &HashMap::new(),
            )
            .expect("create the counted table");
        for row in 0..WALKED {
            database
                .execute(
                    "INSERT INTO counted (id, label) VALUES ($id, $label)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(row as i64)),
                        ("label".to_owned(), Value::Text(format!("row-{row}"))),
                    ]),
                )
                .expect("seed one row");
        }
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();
        let session = database
            .read_session_with_progress(ReadLimits::default(), observer)
            .expect("the holder reads its own live store");

        let answer = session
            .execute("SELECT COUNT(*) FROM counted", &HashMap::new())
            .expect("the live store folds the count");
        assert_eq!(answer.rows.len(), 1, "an aggregate answers one row");

        let totals = recorder.totals();
        assert_eq!(
            totals.len(),
            1,
            "one completed read accounts once: {totals:?}"
        );
        assert!(
            totals[0].work >= WALKED as u64,
            "an in-process aggregate that folded {WALKED} rows examined at least that many: {:?}",
            totals[0]
        );

        // The same holder, paging instead of folding: a published page
        // accounts for the rows it walked to produce.
        let cursor = session
            .open_cursor("SELECT label FROM counted ORDER BY id", &HashMap::new())
            .expect("the live store opens a cursor");
        let published = cursor.first_page().rows.len();
        assert!(published > 0, "the first page hands back rows");
        let paged = recorder.totals();
        let examined = paged.iter().map(|total| total.work).max().unwrap_or(0);
        assert!(
            examined >= published as u64,
            "an in-process page of {published} rows examined at least that many: {paged:?}"
        );
        drop(cursor);
        drop(session);
        database.close().expect("release the store");
    }

    /// A read that examined nothing still accounts for itself. A caller
    /// counting completions cannot have some reads silently missing from the
    /// tally just because they were cheap.
    #[test]
    fn a_read_that_finds_nothing_still_accounts_exactly_once() {
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), 0);
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let answer = session
            .execute("SELECT label FROM counted ORDER BY id", &HashMap::new())
            .expect("an empty table answers");
        assert!(answer.rows.is_empty(), "there is nothing to find");

        assert_eq!(
            recorder.totals().len(),
            1,
            "one completed read is one total, whatever it cost: {:?}",
            recorder.totals()
        );
    }

    /// A read long enough to have ALREADY reported liveness still accounts
    /// exactly once at the end. The completion total does not consult what
    /// liveness happened to have said, so counts that match the last report
    /// cannot silence it.
    #[test]
    fn a_read_that_already_reported_liveness_still_accounts_exactly_once() {
        // Comfortably past the work interval, so liveness has certainly
        // spoken before this read ends.
        const WALKED: usize = 2_000;
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), WALKED);
        let recorder = Arc::new(Recorder::default());
        let observer: Arc<dyn ReadProgressObserver> = recorder.clone();

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let answer = session
            .execute("SELECT COUNT(*) FROM counted", &HashMap::new())
            .expect("the committed file folds the count");
        assert_eq!(answer.rows.len(), 1);

        let liveness = recorder.liveness();
        assert!(
            liveness
                .iter()
                .any(|report| report.phase == ReadPhase::Executing),
            "a read this long reports liveness while it runs: {liveness:?}"
        );
        let totals = recorder.totals();
        assert_eq!(
            totals.len(),
            1,
            "and it still accounts exactly once at the end: {totals:?}"
        );
        assert!(
            totals[0].work >= WALKED as u64,
            "the total is the read's real cost: {:?}",
            totals[0]
        );
    }

    /// An observer that implements only the liveness surface still works, and
    /// hears nothing it did not ask for.
    #[test]
    fn an_observer_that_only_wants_liveness_is_untouched() {
        let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
        let path = seeded_store(directory.path(), 2);
        let observer: Arc<dyn ReadProgressObserver> = Arc::new(LivenessOnly);

        let session =
            ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
                .expect("an idle store reads from its committed file");
        let answer = session
            .execute("SELECT label FROM counted ORDER BY id", &HashMap::new())
            .expect("the committed file answers");
        assert_eq!(answer.rows.len(), 2, "the read is unaffected by the seam");
    }
}
