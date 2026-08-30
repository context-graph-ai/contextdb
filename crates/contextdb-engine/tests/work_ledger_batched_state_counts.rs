//! Counting a batch of jobs, and watching an embedded read work.
//!
//! A process that holds an open database is the same consumer as a process
//! that opens a store by path: it is entitled to the same answers through the
//! same doors. Two of those entitlements are pinned here.
//!
//! The first is the batch count. A caller that tracks a set of submitted jobs
//! wants to know how many of them are still waiting, running, finished,
//! failed or withdrawn. Asking that question one job at a time is a loop the
//! caller writes; re-deriving what "finished" means from the ledger's tables
//! is a second copy of a rule the ledger already owns, and a copy is where the
//! two answers start to disagree. So the ledger answers for a whole batch, in
//! one call, through the very derivation a single job's state comes from --
//! and the answer covers exactly the ids the caller handed over, including the
//! ones the ledger has never heard of, so the buckets always add up to the
//! batch the caller asked about and never to the size of the ledger.
//!
//! That call carries a cost promise as well as a value promise. A caller
//! that asks about a fixed set of jobs on every pass of a loop is paying for
//! that set: the rows the ledger reads to answer are the rows keyed by the
//! ids handed over, so the price of the question is decided by the batch and
//! not by how much other work the ledger happens to be holding. A ledger that
//! has grown by an order of magnitude around an unchanged batch answers that
//! batch for what it cost before.
//!
//! The second is progress. A read that takes a while tells its caller what it
//! has done so far, and that is worth exactly as much to a consumer holding an
//! open database as it is to one reading a closed file. Both doors run the
//! same statement over the same rows, so both must report the same work: a
//! paged walk and a total assembled across cursor pages move the observer the
//! same way through the embedded door as through the path-opened one, and
//! every fetch of a multi-page read says something rather than going quiet
//! between the first page and the last.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Error, Value};
use contextdb_engine::work_ledger::{
    ClaimInsert, InputRef, JobSpec, JobState, JobStateCounts, cancel_job, insert_claim,
    install_work_ledger_schema, job_state, job_state_counts, job_state_counts_with_work,
    record_failure, record_result, submit_job, submit_job_in_tx,
};
use contextdb_engine::{
    Database, ReadPhase, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// The wall-clock instant every fixture job is submitted at. Lease expiry is
/// the caller's clock, never the engine's, so every state below is decided by
/// the instant handed to the derivation rather than by real time passing.
const T0: i64 = 1_700_000_000_000;
/// How long a live lease has left at `T0`.
const LEASE_MS: i64 = 5 * 60_000;
/// Jobs per reachable state, and ids per never-submitted group.
const PER_STATE: usize = 6;
/// The unrelated family whose rows must not reach another batch's answer.
const OTHER_FAMILY_JOBS: usize = 2_000;
/// Jobs written per transaction while building the unrelated family.
const SUBMIT_BATCH: usize = 500;
/// The batch a caller tracks across a loop, and holds fixed while the ledger
/// around it grows.
const TRACKED_JOBS: usize = 100;
/// The jobs that grow up around that batch. Each job of either family carries
/// one claim row and one failure row, so this many unrelated jobs put ten
/// times the tracked batch's own state rows into the two tables the
/// derivation reads by `job_id`.
const UNRELATED_JOBS: usize = TRACKED_JOBS * 10;
/// How much more the same batch may cost once the ledger around it has grown
/// by an order of magnitude. Finding a job's rows in a larger table is a
/// slightly deeper walk, so the figure is allowed to drift upward by a
/// constant. Anything past that is the size of the ledger, rather than the
/// size of the batch, deciding what the caller pays -- which is the promise
/// this arm exists to hold.
const GROWTH_ALLOWANCE: u64 = 2;

/// Rows enough that a walk of them passes the reporting interval several
/// times over, and payload enough that each row is real bytes.
const FIXTURE_ROWS: i64 = 6_000;
const PAYLOAD_BYTES: usize = 512;
const INSERT_BATCH: i64 = 500;
/// The rows a filtered walk is looking for. They are far apart, and the run of
/// rows after the last one is as long as the runs between them, so every fetch
/// of a one-row page -- including the one that discovers the walk is over --
/// examines more than one reporting interval's worth of rows.
const NEEDLES: [i64; 4] = [1_200, 2_400, 3_600, 4_800];
/// What one embedded read or one cursor fetch may examine here: small enough
/// that a total over the fixture takes several pages, large enough that a walk
/// between two needles fits in one.
const WORK_CEILING: u64 = 2_500;

// ---------------------------------------------------------------- job states

/// The batch answer, counted the long way: the caller's own loop over the
/// per-job derivation.
#[derive(Debug, Default, PartialEq, Eq)]
struct Buckets {
    pending: usize,
    leased: usize,
    done: usize,
    failed: usize,
    cancelled: usize,
    unknown: usize,
}

impl Buckets {
    fn total(&self) -> usize {
        self.pending + self.leased + self.done + self.failed + self.cancelled + self.unknown
    }
}

/// What the ledger answered, in the shape the caller's own loop produces, so
/// the two can be compared as one value rather than field by field.
fn as_buckets(counts: &JobStateCounts) -> Buckets {
    Buckets {
        pending: counts.pending,
        leased: counts.leased,
        done: counts.done,
        failed: counts.failed,
        cancelled: counts.cancelled,
        unknown: counts.unknown,
    }
}

/// The caller's loop: one canonical state read per id, an id the ledger has
/// never seen counted as such rather than skipped.
fn fold_per_id(database: &Database, job_ids: &[String], now_ms: i64) -> Buckets {
    let mut folded = Buckets::default();
    for job_id in job_ids {
        match job_state(database, job_id, now_ms) {
            Ok(JobState::Pending) => folded.pending += 1,
            Ok(JobState::Leased { .. }) => folded.leased += 1,
            Ok(JobState::Done) => folded.done += 1,
            Ok(JobState::Failed) => folded.failed += 1,
            Ok(JobState::Cancelled) => folded.cancelled += 1,
            Err(Error::NotFound(_)) => folded.unknown += 1,
            Err(error) => panic!("reading the state of {job_id} must not fail: {error:?}"),
        }
    }
    folded
}

fn spec(job_id: &str, max_attempts: i64) -> JobSpec {
    JobSpec::builder(job_id, "media.transcode", "batch", "node-submitter")
        .input_refs(vec![InputRef::ledger_input()])
        .max_attempts(max_attempts)
        .submitted_at_ms(T0)
        .build()
}

fn submit(database: &Database, job_id: &str, max_attempts: i64) {
    submit_job(
        database,
        &spec(job_id, max_attempts),
        &[b"payload" as &[u8]],
    )
    .unwrap_or_else(|error| panic!("submit {job_id}: {error:?}"));
}

fn take_lease(database: &Database, job_id: &str, attempt: i64, deadline_ms: i64) {
    let taken = insert_claim(database, job_id, attempt, "node-worker", deadline_ms, T0)
        .unwrap_or_else(|error| panic!("claim {job_id}: {error:?}"));
    assert_eq!(
        taken,
        ClaimInsert::Inserted,
        "the fixture's claim on {job_id} is the first one"
    );
}

/// A ledger holding several jobs in every state a job can reach, plus ids that
/// were never submitted at all.
fn seed_states(database: &Database) -> Vec<String> {
    install_work_ledger_schema(database).expect("install the work ledger schema");
    let mut batch = Vec::new();

    for index in 0..PER_STATE {
        let job_id = format!("job-pending-{index}");
        submit(database, &job_id, 2);
        batch.push(job_id);
    }
    for index in 0..PER_STATE {
        let job_id = format!("job-leased-{index}");
        submit(database, &job_id, 2);
        take_lease(database, &job_id, 1, T0 + LEASE_MS);
        batch.push(job_id);
    }
    for index in 0..PER_STATE {
        let job_id = format!("job-done-{index}");
        submit(database, &job_id, 2);
        take_lease(database, &job_id, 1, T0 + LEASE_MS);
        record_result(
            database,
            &job_id,
            1,
            "node-worker",
            b"output",
            serde_json::json!({"receipt": index}),
            T0 + 10,
        )
        .unwrap_or_else(|error| panic!("record the result of {job_id}: {error:?}"));
        batch.push(job_id);
    }
    for index in 0..PER_STATE {
        let job_id = format!("job-failed-{index}");
        submit(database, &job_id, 2);
        for attempt in 1..=2 {
            take_lease(database, &job_id, attempt, T0 + LEASE_MS);
            record_failure(
                database,
                &job_id,
                attempt,
                "node-worker",
                "boom",
                T0 + attempt,
            )
            .unwrap_or_else(|error| panic!("record a failure of {job_id}: {error:?}"));
        }
        batch.push(job_id);
    }
    for index in 0..PER_STATE {
        let job_id = format!("job-cancelled-{index}");
        submit(database, &job_id, 2);
        cancel_job(
            database,
            &job_id,
            "node-submitter",
            Some("withdrawn"),
            T0 + 5,
        )
        .unwrap_or_else(|error| panic!("cancel {job_id}: {error:?}"));
        batch.push(job_id);
    }
    for index in 0..PER_STATE {
        batch.push(format!("job-never-submitted-{index}"));
    }

    batch
}

#[test]
fn batched_counts_agree_with_per_id_state() {
    let database = Database::open_memory();
    let batch = seed_states(&database);
    let now_ms = T0 + 60_000;

    let counted = job_state_counts(&database, &batch, now_ms)
        .expect("a holder of the database counts the states of its own batch");
    let folded = fold_per_id(&database, &batch, now_ms);

    assert_eq!(
        as_buckets(&counted),
        folded,
        "the batch answer is the canonical per-job derivation folded, so it must match a caller \
         that walks the same ids one at a time"
    );
    assert_eq!(
        folded.total(),
        batch.len(),
        "every id the caller handed over lands in exactly one bucket, including the ones the \
         ledger has never seen"
    );
    assert_eq!(
        (
            folded.pending,
            folded.leased,
            folded.done,
            folded.failed,
            folded.cancelled,
            folded.unknown
        ),
        (
            PER_STATE, PER_STATE, PER_STATE, PER_STATE, PER_STATE, PER_STATE
        ),
        "the fixture reaches every state, so no bucket is proven by an empty one: {folded:?}"
    );
}

/// A second, unrelated family of jobs: many of them, in a mix of states, none
/// of which belongs in another caller's answer.
fn seed_other_family(database: &Database) {
    let mut written = 0usize;
    while written < OTHER_FAMILY_JOBS {
        let last = (written + SUBMIT_BATCH).min(OTHER_FAMILY_JOBS);
        let tx = database.begin().expect("begin an unrelated-family batch");
        while written < last {
            submit_job_in_tx(database, tx, &spec(&format!("other-{written}"), 2))
                .unwrap_or_else(|error| panic!("submit other-{written}: {error:?}"));
            written += 1;
        }
        database
            .commit(tx)
            .expect("commit an unrelated-family batch");
    }
    for index in (0..OTHER_FAMILY_JOBS).step_by(5) {
        take_lease(database, &format!("other-{index}"), 1, T0 + LEASE_MS);
    }
    for index in (0..OTHER_FAMILY_JOBS).step_by(11) {
        record_result(
            database,
            &format!("other-{index}"),
            1,
            "node-worker",
            b"output",
            serde_json::json!({"receipt": index}),
            T0 + 10,
        )
        .unwrap_or_else(|error| panic!("record the result of other-{index}: {error:?}"));
    }
    for index in (0..OTHER_FAMILY_JOBS).step_by(13) {
        cancel_job(
            database,
            &format!("other-{index}"),
            "node-submitter",
            None,
            T0 + 5,
        )
        .unwrap_or_else(|error| panic!("cancel other-{index}: {error:?}"));
    }
}

#[test]
fn batched_counts_are_scoped_to_the_batch() {
    let database = Database::open_memory();
    let batch = seed_states(&database);
    seed_other_family(&database);
    let now_ms = T0 + 60_000;

    let counted = job_state_counts(&database, &batch, now_ms)
        .expect("counting one batch is not disturbed by the rest of the ledger");
    let folded = fold_per_id(&database, &batch, now_ms);

    assert_eq!(
        as_buckets(&counted),
        folded,
        "a ledger holding {OTHER_FAMILY_JOBS} other jobs answers this batch exactly as a caller \
         walking this batch's ids does"
    );
    assert_eq!(
        folded.total(),
        batch.len(),
        "the answer covers the batch that was asked about, not the ledger it was asked of"
    );

    // The whole ledger really does hold far more than the batch, so the
    // equality above is scoping rather than an accident of an empty store.
    let everything = database
        .execute("SELECT job_id FROM work_jobs", &HashMap::new())
        .expect("read every job the ledger holds");
    assert_eq!(
        everything.rows.len(),
        OTHER_FAMILY_JOBS + PER_STATE * 5,
        "the fixture ledger holds the other family as well as the batch"
    );
}

/// A family of jobs that each carry a claim row and a failure row, so every
/// job in it has state rows in the two tables that hold a job's attempts.
/// One failure short of the attempt limit leaves each job pending, which is
/// beside the point here -- what matters is that the rows exist and are keyed
/// by `job_id`.
fn seed_family(database: &Database, prefix: &str, jobs: usize) -> Vec<String> {
    let mut ids = Vec::with_capacity(jobs);
    let mut written = 0usize;
    while written < jobs {
        let last = (written + SUBMIT_BATCH).min(jobs);
        let tx = database.begin().expect("begin a family batch");
        while written < last {
            let job_id = format!("{prefix}-{written}");
            submit_job_in_tx(database, tx, &spec(&job_id, 3))
                .unwrap_or_else(|error| panic!("submit {job_id}: {error:?}"));
            ids.push(job_id);
            written += 1;
        }
        database.commit(tx).expect("commit a family batch");
    }
    for job_id in &ids {
        take_lease(database, job_id, 1, T0 + LEASE_MS);
        record_failure(database, job_id, 1, "node-worker", "boom", T0 + 1)
            .unwrap_or_else(|error| panic!("record a failure of {job_id}: {error:?}"));
    }
    ids
}

#[test]
fn batched_counts_work_does_not_grow_with_unrelated_ledger_rows() {
    let database = Database::open_memory();
    install_work_ledger_schema(&database).expect("install the work ledger schema");
    let tracked = seed_family(&database, "tracked", TRACKED_JOBS);
    let now_ms = T0 + 60_000;

    let (first_counts, first_work) = job_state_counts_with_work(&database, &tracked, now_ms)
        .expect("count a tracked batch and hear what answering it cost");
    assert_eq!(
        as_buckets(&first_counts).total(),
        tracked.len(),
        "the answer covers the batch that was asked about"
    );
    assert!(
        first_work.rows_examined > 0,
        "answering reads rows, so the work reported is a real figure rather than an unfilled one"
    );

    // Ten times the tracked batch's own state rows, all of them belonging to
    // jobs nobody asked about.
    let unrelated = seed_family(&database, "unrelated", UNRELATED_JOBS);
    assert_eq!(
        unrelated.len(),
        tracked.len() * 10,
        "the ledger really did grow by an order of magnitude around the tracked batch"
    );

    let (second_counts, second_work) = job_state_counts_with_work(&database, &tracked, now_ms)
        .expect("count the same batch again, in the grown ledger");

    assert_eq!(
        as_buckets(&second_counts),
        as_buckets(&first_counts),
        "the same batch in the same states answers the same way, so the two costs below are the \
         price of one identical question asked twice"
    );
    assert!(
        second_work.rows_examined <= first_work.rows_examined * GROWTH_ALLOWANCE,
        "a caller tracking {} jobs pays for those jobs: answering cost {} rows in a small ledger \
         and {} rows once {} unrelated jobs had been added, which is the ledger's size and not \
         the batch's deciding the price",
        tracked.len(),
        first_work.rows_examined,
        second_work.rows_examined,
        unrelated.len()
    );
}

// ------------------------------------------------------------- read progress

/// Everything one observer was told.
#[derive(Default)]
struct Recorder {
    reports: Mutex<Vec<ReadProgress>>,
}

impl ReadProgressObserver for Recorder {
    fn progress(&self, progress: ReadProgress) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(progress);
    }
}

impl Recorder {
    fn executing(&self) -> Vec<ReadProgress> {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|progress| progress.phase == ReadPhase::Executing)
            .copied()
            .collect()
    }

    fn clear(&self) {
        self.reports
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// The work this one operation had reached when it last said anything.
    fn work_reached(&self) -> u64 {
        self.executing()
            .iter()
            .map(|progress| progress.work)
            .max()
            .unwrap_or(0)
    }
}

fn observer(recorder: &Arc<Recorder>) -> Arc<dyn ReadProgressObserver> {
    Arc::clone(recorder) as Arc<dyn ReadProgressObserver>
}

fn payload(row: i64) -> String {
    let mut text = format!("row-{row}-");
    while text.len() < PAYLOAD_BYTES {
        text.push('p');
    }
    text
}

/// A store big enough that reading it is not instant, with findable rows far
/// apart.
fn seed_store(path: &Path) {
    let database = Database::open(path).expect("open the fixture writer");
    database
        .execute(
            "CREATE TABLE walk_rows (id INTEGER PRIMARY KEY, marker TEXT, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    let mut next = 0;
    while next < FIXTURE_ROWS {
        let tx = database.begin().expect("begin a fixture batch");
        let last = (next + INSERT_BATCH).min(FIXTURE_ROWS);
        while next < last {
            let marker = if NEEDLES.contains(&next) {
                "needle"
            } else {
                "hay"
            };
            database
                .execute_in_tx(
                    tx,
                    "INSERT INTO walk_rows (id, marker, payload) VALUES ($id, $marker, $payload)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(next)),
                        ("marker".to_owned(), Value::Text(marker.to_owned())),
                        ("payload".to_owned(), Value::Text(payload(next))),
                    ]),
                )
                .unwrap_or_else(|error| panic!("insert fixture row {next}: {error}"));
            next += 1;
        }
        database.commit(tx).expect("commit a fixture batch");
    }
    database.close().expect("the fixture writer closes cleanly");
}

/// One read or one cursor fetch per entry: the answer it produced and the work
/// its observer had been told about by the time it returned.
#[derive(Debug, PartialEq, Eq)]
struct WorkTold {
    rows: usize,
    work: u64,
}

fn needle_params() -> HashMap<String, Value> {
    HashMap::from([("marker".to_owned(), Value::Text("needle".to_owned()))])
}

/// The bounds a consumer declares here: one row per cursor page, so reaching
/// each findable row is its own walk, and a work ceiling several pages below
/// the size of the store.
fn declared_limits() -> ReadLimits {
    ReadLimits {
        work: WORK_CEILING,
        cursor_page_rows: 1,
        ..ReadLimits::default()
    }
}

/// Walk the findable rows one page at a time, recording what each operation
/// told the observer.
fn paged_walk(session: &ReadSession, recorder: &Arc<Recorder>) -> Vec<WorkTold> {
    recorder.clear();
    let mut cursor = session
        .open_cursor(
            "SELECT id FROM walk_rows WHERE marker = $marker",
            &needle_params(),
        )
        .expect("open a cursor over the findable rows");
    let mut told = vec![WorkTold {
        rows: cursor.first_page().rows.len(),
        work: recorder.work_reached(),
    }];
    let mut more = cursor.first_page().has_more;
    while more {
        assert!(
            told.len() < NEEDLES.len() * 4 + 16,
            "the walk produced {} pages without ever ending",
            told.len()
        );
        recorder.clear();
        let page = cursor.fetch(None).expect("fetch the next page of the walk");
        told.push(WorkTold {
            rows: page.rows.len(),
            work: recorder.work_reached(),
        });
        more = page.has_more;
    }
    cursor.close().expect("close the walk");
    told
}

/// Assemble a total across cursor pages, recording what each operation told
/// the observer, and return the total alongside it.
fn paged_total(session: &ReadSession, recorder: &Arc<Recorder>) -> (Value, Vec<WorkTold>) {
    recorder.clear();
    let mut cursor = session
        .open_cursor("SELECT COUNT(*) FROM walk_rows", &HashMap::new())
        .expect("open the total as a cursor");
    let mut told = vec![WorkTold {
        rows: cursor.first_page().rows.len(),
        work: recorder.work_reached(),
    }];
    let mut more = cursor.first_page().has_more;
    let mut answer = cursor
        .first_page()
        .rows
        .first()
        .and_then(|row| row.first())
        .cloned();
    while more {
        assert!(
            told.len() < 64,
            "the total was never completed after {} pages",
            told.len()
        );
        recorder.clear();
        let page = cursor
            .fetch(None)
            .expect("fetch the next page of the total");
        told.push(WorkTold {
            rows: page.rows.len(),
            work: recorder.work_reached(),
        });
        if let Some(row) = page.rows.first() {
            answer = row.first().cloned();
        }
        more = page.has_more;
    }
    cursor.close().expect("close the total");
    (
        answer.expect("the last page of a total carries the answer"),
        told,
    )
}

#[test]
fn embedded_session_reports_progress() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("embedded-progress.db");
    seed_store(&path);

    let database = Database::open(&path).expect("hold the store open for reading");
    let embedded_recorder = Arc::new(Recorder::default());
    let embedded = database
        .read_session_with_progress(declared_limits(), observer(&embedded_recorder))
        .expect("a holder of the database reads it with progress reported");
    let embedded_walk = paged_walk(&embedded, &embedded_recorder);
    let (embedded_answer, embedded_total) = paged_total(&embedded, &embedded_recorder);
    drop(embedded);
    database.close().expect("the store closes cleanly");

    let file_recorder = Arc::new(Recorder::default());
    let opened = ReadSession::open_with_progress(
        &path,
        ReadSessionOptions {
            limits: declared_limits(),
            ..ReadSessionOptions::default()
        },
        observer(&file_recorder),
    )
    .expect("open the same store by path with progress reported");
    let file_walk = paged_walk(&opened, &file_recorder);
    let (file_answer, file_total) = paged_total(&opened, &file_recorder);

    assert_eq!(
        embedded_answer,
        Value::Int64(FIXTURE_ROWS),
        "the embedded door totals every row of the store"
    );
    assert_eq!(
        embedded_answer, file_answer,
        "both doors total the same store"
    );
    assert_eq!(
        embedded_walk, file_walk,
        "a paged walk through the door of an open database examines the same rows, page for \
         page, as the same walk through a store opened by path, and says so"
    );
    assert_eq!(
        embedded_total, file_total,
        "a total assembled across cursor pages reports the same work through both doors"
    );
    assert!(
        embedded_walk.iter().all(|told| told.work > 0),
        "every page of the walk reports the work it did: {embedded_walk:?}"
    );
    assert!(
        embedded_total.iter().all(|told| told.work > 0),
        "every page of the total reports the work it did: {embedded_total:?}"
    );
}

#[test]
fn embedded_session_progress_is_present_on_every_fetch() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("every-fetch.db");
    seed_store(&path);

    let database = Database::open(&path).expect("hold the store open for reading");
    let recorder = Arc::new(Recorder::default());
    let session = database
        .read_session_with_progress(declared_limits(), observer(&recorder))
        .expect("a holder of the database reads it with progress reported");

    recorder.clear();
    let mut cursor = session
        .open_cursor(
            "SELECT id FROM walk_rows WHERE marker = $marker",
            &needle_params(),
        )
        .expect("open a cursor over the findable rows");
    assert!(
        !recorder.executing().is_empty(),
        "the page produced when the cursor opens reports its progress"
    );

    let mut fetches = 0usize;
    let mut more = cursor.first_page().has_more;
    while more {
        recorder.clear();
        let page = cursor.fetch(None).expect("fetch the next page");
        fetches += 1;
        assert!(
            !recorder.executing().is_empty(),
            "fetch {fetches} walked its way to the next page and must say so, not go quiet \
             between the first page and the last"
        );
        assert!(
            recorder
                .executing()
                .iter()
                .all(|progress| progress.work > 0),
            "fetch {fetches} reports the items it examined"
        );
        more = page.has_more;
        assert!(fetches < 64, "the cursor never ended");
    }
    assert!(
        fetches >= NEEDLES.len() - 1,
        "the fixture takes several fetches to walk, and took {fetches}"
    );

    cursor.close().expect("close the walk");
    drop(session);
    database.close().expect("the store closes cleanly");
}
