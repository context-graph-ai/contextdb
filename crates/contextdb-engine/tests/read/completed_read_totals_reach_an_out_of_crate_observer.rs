//! What a finished read cost, told to the caller that is adding it up.
//!
//! Liveness and accounting are two different promises and this suite holds
//! both from OUTSIDE the crate, where the consumer that needs them actually
//! lives. Liveness is withheld on purpose: a read that finishes inside one
//! reporting interval says nothing while it runs, because there was never
//! anything worth interrupting the caller with. Accounting is not optional: a
//! caller totalling what its reads examined cannot have a completed scan
//! counted as zero work just because it was quick.
//!
//! So the totals arrive through their own door, exactly once per completed
//! read, and an observer that only wanted liveness is untouched by it -- it
//! implements one method, hears the same nothing it heard before, and does not
//! have to know the other door exists.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::{
    ReadPhase, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Rows few enough that the read finishes inside one reporting interval, and
/// a store small enough that hydration does not cross one either.
const ROWS: usize = 3;

/// A consumer that wants both surfaces: what happened while the read ran, and
/// what it finished on.
#[derive(Default)]
struct AccountingObserver {
    liveness: Mutex<Vec<ReadProgress>>,
    totals: Mutex<Vec<ReadProgress>>,
}

impl AccountingObserver {
    fn liveness(&self) -> Vec<ReadProgress> {
        self.liveness.lock().expect("liveness reports").clone()
    }

    fn totals(&self) -> Vec<ReadProgress> {
        self.totals.lock().expect("completed totals").clone()
    }
}

impl ReadProgressObserver for AccountingObserver {
    fn progress(&self, progress: ReadProgress) {
        self.liveness
            .lock()
            .expect("liveness reports")
            .push(progress);
    }

    fn completed(&self, totals: ReadProgress) {
        self.totals.lock().expect("completed totals").push(totals);
    }
}

/// A consumer that wants liveness and nothing else. It implements one method,
/// exactly as every consumer written before the accounting door existed did.
#[derive(Default)]
struct LivenessOnlyObserver {
    reports: AtomicUsize,
}

impl ReadProgressObserver for LivenessOnlyObserver {
    fn progress(&self, _progress: ReadProgress) {
        self.reports.fetch_add(1, Ordering::SeqCst);
    }
}

fn seeded_store(directory: &Path) -> PathBuf {
    let path = directory.join("accounted.db");
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE counted (id INTEGER PRIMARY KEY, label TEXT)",
            &HashMap::new(),
        )
        .expect("create the counted table");
    for row in 0..ROWS {
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

/// The read is short, so liveness stays silent; the totals arrive anyway,
/// once, carrying the answer's real size and the work nobody else measured.
#[test]
fn a_short_read_says_nothing_about_liveness_and_still_accounts_for_itself() {
    let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
    let path = seeded_store(directory.path());
    let watching = Arc::new(AccountingObserver::default());
    let observer: Arc<dyn ReadProgressObserver> = watching.clone();

    let session = ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
        .expect("an idle store reads from its committed file");
    let answer = session
        .execute("SELECT label FROM counted ORDER BY id", &HashMap::new())
        .expect("the committed file answers");
    assert_eq!(answer.rows.len(), ROWS);

    assert!(
        watching.liveness().is_empty(),
        "a read that finished inside one interval had nothing worth reporting: {:?}",
        watching.liveness()
    );
    let totals = watching.totals();
    assert_eq!(
        totals.len(),
        1,
        "a completed read accounts for itself exactly once: {totals:?}"
    );
    assert_eq!(
        totals[0].rows as usize, ROWS,
        "the totals are the read's real answer, not a placeholder: {:?}",
        totals[0]
    );
    assert!(
        totals[0].work > 0,
        "the totals carry the work a caller adding up its reads would otherwise lose: {:?}",
        totals[0]
    );
    assert_eq!(
        totals[0].phase,
        ReadPhase::Executing,
        "the totals describe the execution that produced the answer: {:?}",
        totals[0]
    );
}

/// A consumer that implements only the liveness surface is untouched: it hears
/// the same nothing, and the accounting door costs it no code at all.
#[test]
fn an_observer_that_only_wanted_liveness_hears_nothing_new() {
    let directory = tempfile::TempDir::new().expect("task-scoped accounting directory");
    let path = seeded_store(directory.path());
    let watching = Arc::new(LivenessOnlyObserver::default());
    let observer: Arc<dyn ReadProgressObserver> = watching.clone();

    let session = ReadSession::open_with_progress(&path, ReadSessionOptions::default(), observer)
        .expect("an idle store reads from its committed file");
    let answer = session
        .execute("SELECT label FROM counted ORDER BY id", &HashMap::new())
        .expect("the committed file answers");
    assert_eq!(answer.rows.len(), ROWS);

    assert_eq!(
        watching.reports.load(Ordering::SeqCst),
        0,
        "the read was short, and the accounting door does not leak into liveness"
    );
}

/// The public phase vocabulary is the two phases a read has always had. The
/// completed totals ride the accounting door, so nothing about them adds a
/// phase a channel peer or a consumer's match would have to learn.
///
/// This match is exhaustive on a public enum that is not `non_exhaustive`, so
/// a variant added anywhere fails to compile here rather than reaching a
/// consumer unannounced.
#[test]
fn the_public_phase_vocabulary_is_the_two_phases_a_read_has() {
    for phase in [ReadPhase::Hydrating, ReadPhase::Executing] {
        let word = match phase {
            ReadPhase::Hydrating => "Hydrating",
            ReadPhase::Executing => "Executing",
        };
        assert_eq!(
            serde_json::to_string(&phase).expect("a phase travels the read channel"),
            format!("\"{word}\""),
            "the phase keeps the wire word its position already promised"
        );
    }
}
