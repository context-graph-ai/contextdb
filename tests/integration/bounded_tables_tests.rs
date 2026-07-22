//! Bounded tables — engine-side acceptance for the maintenance lifecycle,
//! pruning reports, per-table sizing, durable trigger-audit retention, and the
//! one-way `PUSH ONLY` table policy.
//!
//! Before this change, the maintenance lifecycle surface (`maintenance_status`,
//! `run_maintenance_cycle`), the reporting fields on `PruningReport`, the
//! per-table size answer, the durable trigger-audit retention, and the one-way
//! `PUSH ONLY` table policy did not exist yet.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Time
//! moves through `Wallclock::test_clock_guard` and maintenance is DRIVEN
//! SYNCHRONOUSLY on the test thread (the clock override is thread-local, so an
//! engine background thread would see the real clock). Loop LIFECYCLE is
//! asserted separately from pruning CORRECTNESS.

use contextdb_core::{Value, Wallclock};
use contextdb_engine::{
    Database, REDB_COMPACT_FRAGMENTATION_THRESHOLD, RETENTION_CLOCK_SKEW_TOLERANCE,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("{table} scan must succeed: {err}"))
        .rows
        .len()
}

/// A movable mock clock in milliseconds. Every timestamp the engine records on
/// this thread — row `created_at`, trigger-audit entries, the prune-time
/// judgement — reads through it.
struct MockClock(Arc<AtomicU64>);

impl MockClock {
    fn install(start_millis: u64) -> (Self, contextdb_core::WallclockTestClockGuard) {
        let cell = Arc::new(AtomicU64::new(start_millis));
        let guard = {
            let cell = Arc::clone(&cell);
            Wallclock::test_clock_guard(move || cell.load(Ordering::SeqCst))
        };
        (Self(cell), guard)
    }

    fn set(&self, millis: u64) {
        self.0.store(millis, Ordering::SeqCst);
    }

    fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

const T0: u64 = 1_700_000_000_000;
/// The retention window every fixture below declares, in milliseconds. Kept as
/// one constant so the clock-rule tests can be written as
/// `tolerance + RETENTION_WINDOW_MILLIS` instead of hard-coded hours.
const RETENTION_WINDOW_MILLIS: u64 = 60 * 60 * 1000;

fn create_retained_table(db: &Database, name: &str) {
    db.execute(
        &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT) RETAIN 1 HOURS"),
        &p(),
    )
    .unwrap_or_else(|err| panic!("retained table {name} must create: {err}"));
}

fn skew_tolerance_millis() -> u64 {
    RETENTION_CLOCK_SKEW_TOLERANCE.as_millis() as u64
}

fn insert_rows(db: &Database, table: &str, ids: std::ops::Range<i64>, body: &str) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(body.to_string()));
        db.execute(
            &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
            &row,
        )
        .unwrap_or_else(|err| panic!("insert into {table} must succeed: {err}"));
    }
}

/// The three currency tables the existing engine-owned maintenance thread
/// serves. Declaring one is how a database becomes currency-eligible.
fn create_currency_table(db: &Database) {
    db.execute(
        "CREATE TABLE work_capabilities (id TEXT PRIMARY KEY, generation INTEGER)",
        &p(),
    )
    .expect("currency table must create");
}

fn upsert_currency_row(db: &Database, id: &str, generation: i64) {
    let mut row = p();
    row.insert("id".to_string(), Value::Text(id.to_string()));
    row.insert("generation".to_string(), Value::Int64(generation));
    db.execute(
        "INSERT INTO work_capabilities (id, generation) VALUES ($id, $generation) \
         ON CONFLICT (id) DO UPDATE SET generation = $generation",
        &row,
    )
    .expect("currency upsert must succeed");
}

// ---------------------------------------------------------------------------
// C1 — retention runs by itself
// ---------------------------------------------------------------------------

/// A database that holds a retained table has the engine-owned maintenance loop
/// running the moment `open` returns — no consumer call, no interval setter.
#[test]
fn c1_retained_table_starts_maintenance_at_open() {
    let db = Database::open_memory();
    create_retained_table(&db, "windows");

    let status = db.maintenance_status();
    assert!(
        status.retention_enabled,
        "a database holding a RETAIN table must report retention enabled: {status:?}"
    );
    assert!(
        status.running,
        "the engine-owned maintenance loop must be running with no consumer involvement: {status:?}"
    );
}

/// Restart is the case the seed calls out: the retained table arrives from disk,
/// not from a `CREATE TABLE` this process ran, and the loop must still start.
#[test]
fn c1_maintenance_restarts_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bounded.db");

    {
        let db = Database::open(&path).expect("open");
        create_retained_table(&db, "windows");
    }

    let db = Database::open(&path).expect("reopen");
    let status = db.maintenance_status();
    assert!(
        status.retention_enabled,
        "the retained table must be recognised after restart: {status:?}"
    );
    assert!(
        status.running,
        "maintenance must restart itself after reopen with no consumer call: {status:?}"
    );
}

/// The control that keeps C1 honest: a database with nothing to maintain pays
/// nothing. Without this a "always spawn a thread" implementation would pass
/// the two tests above.
#[test]
fn c1_database_with_nothing_to_maintain_runs_no_maintenance_loop() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE plain (id INTEGER PRIMARY KEY)", &p())
        .expect("plain table must create");

    let status = db.maintenance_status();
    assert!(
        !status.retention_enabled && !status.currency_compaction_enabled,
        "a database with no retained and no currency table has nothing to maintain: {status:?}"
    );
    assert!(
        !status.running,
        "no maintenance thread may run for a database with nothing to maintain: {status:?}"
    );
    assert_eq!(
        status.active_maintenance_loops, 0,
        "and no loop is spawned at all: {status:?}"
    );
}

// ---------------------------------------------------------------------------
// C2 — one maintenance runtime, both jobs
// ---------------------------------------------------------------------------

/// Today the single `pruning_runtime.handle` slot makes the two jobs mutually
/// exclusive: whichever starts last stops the other. Declaring a retained table
/// on a currency-eligible database must leave BOTH enabled and ONE loop running.
#[test]
fn c2_currency_and_retention_share_one_running_maintenance_runtime() {
    let db = Database::open_memory();
    create_currency_table(&db);
    let currency_only = db.maintenance_status();
    assert!(
        currency_only.currency_compaction_enabled && currency_only.running,
        "currency compaction must be running before the retained table arrives: {currency_only:?}"
    );

    create_retained_table(&db, "windows");
    let both = db.maintenance_status();
    assert!(
        both.currency_compaction_enabled,
        "declaring a retained table must not disable currency compaction: {both:?}"
    );
    assert!(
        both.retention_enabled,
        "the retained table must enable retention: {both:?}"
    );
    assert!(
        both.running,
        "one maintenance loop must be running for both jobs: {both:?}"
    );
    // The decisive count: a boolean cannot tell one shared loop from two
    // independently spawned ones, and C2 is precisely about the single slot.
    assert_eq!(
        both.active_maintenance_loops, 1,
        "both jobs must be dispatched by ONE loop, not a loop each: {both:?}"
    );
    assert_eq!(
        both.running,
        both.active_maintenance_loops > 0,
        "`running` is exactly the diagnostic loop count being non-zero: {both:?}"
    );
}

/// The behavioural half of C2, driven synchronously: ONE cycle both prunes
/// expired retained rows and collapses superseded currency versions.
#[test]
fn c2_one_maintenance_cycle_prunes_retained_rows_and_compacts_currency_versions() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    create_currency_table(&db);
    create_retained_table(&db, "windows");

    insert_rows(&db, "windows", 1..6, "expiring");
    // One insert plus seven updates: eight physical versions of one logical
    // row, seven of them superseded. The expected count comes from the fixture,
    // never from the report the test is judging.
    for generation in 0..8 {
        upsert_currency_row(&db, "cap-1", generation);
    }
    assert_eq!(row_count(&db, "windows"), 5);
    assert_eq!(
        row_count(&db, "work_capabilities"),
        1,
        "SQL already reads one logical row BEFORE compaction — which is why row \
         count alone cannot witness the currency half"
    );
    let used_before = db.accountant().usage().used;

    clock.advance(RETENTION_WINDOW_MILLIS * 2);

    let report = db
        .run_maintenance_cycle()
        .expect("one maintenance cycle must run both jobs");
    assert_eq!(
        report.pruning.pruned_rows, 5,
        "the retention half of the cycle must prune the expired rows: {report:?}"
    );
    assert_eq!(
        report.currency.pruned_versions, 7,
        "the currency half of the SAME cycle must collapse exactly the seven \
         superseded versions the fixture created: {report:?}"
    );
    // Independent witness: the memory accountant is charged per physical row
    // version and released when versions are removed, so a fabricated report
    // cannot move it.
    let used_after = db.accountant().usage().used;
    assert!(
        used_after < used_before,
        "physical version collapse must be observable outside the report \
         (accountant used {used_before} -> {used_after})"
    );
    assert_eq!(row_count(&db, "windows"), 0);
    assert_eq!(
        row_count(&db, "work_capabilities"),
        1,
        "currency compaction keeps exactly the current version of the logical row"
    );
}

// ---------------------------------------------------------------------------
// C6 — the clock rule
// ---------------------------------------------------------------------------

/// A row ages by the timestamp it carries, judged against the holder's clock at
/// prune time — not by when the prune happened to run. The young row inserted
/// AFTER the clock moved is the guard that makes the claim non-trivial.
#[test]
fn c6_row_ages_by_its_carried_timestamp_against_the_holder_clock() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    create_retained_table(&db, "windows");

    insert_rows(&db, "windows", 1..4, "old");
    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    insert_rows(&db, "windows", 10..13, "young");

    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 3,
        "exactly the rows whose carried timestamp is older than the window may prune: {report:?}"
    );
    assert_eq!(
        row_count(&db, "windows"),
        3,
        "rows written after the clock moved are inside the window and must survive"
    );
}

/// A worker whose clock runs far ahead writes rows stamped in the future. Such
/// a row would never age out; the engine may not pin it silently — the
/// maintenance report counts it, per table, every cycle it stays pinned.
#[test]
fn c6_future_dated_row_beyond_skew_tolerance_is_counted_in_the_report() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    create_retained_table(&db, "windows");

    // A skewed worker whose stamp lands JUST OUTSIDE the tolerance — derived
    // from the constant itself, so this test never dictates its value.
    let skew_millis = skew_tolerance_millis();
    clock.set(T0 + skew_millis + 1);
    insert_rows(&db, "windows", 1..2, "future-dated");
    clock.set(T0);

    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 0,
        "a future-dated row is not deleted early: {report:?}"
    );
    assert_eq!(
        report.future_dated_rows, 1,
        "a row stamped beyond the skew tolerance ({skew_millis} ms) must be counted, \
         never silently pinned: {report:?}"
    );
    assert!(
        report
            .future_dated_tables
            .iter()
            .any(|table| table == "windows"),
        "the report must name the table holding the pinned row: {report:?}"
    );
}

/// Ordinary worker skew is not an operator problem: a stamp inside the
/// tolerance is treated as the holder's now, so it ages normally and is never
/// reported. Without this the tolerance would be decoration.
#[test]
fn c6_stamp_within_skew_tolerance_ages_normally_and_is_not_reported() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    create_retained_table(&db, "windows");

    // JUST INSIDE the tolerance, again derived from the constant.
    let skew_millis = skew_tolerance_millis();
    clock.set(T0 + skew_millis.saturating_sub(1));
    insert_rows(&db, "windows", 1..2, "slightly-ahead");
    clock.set(T0);

    let first = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        first.future_dated_rows, 0,
        "a stamp inside the skew tolerance is normal clock drift, not an operator event: {first:?}"
    );
    assert_eq!(
        first.pruned_rows, 0,
        "the row is not yet expired: {first:?}"
    );

    // Past the stamp's own window even after clamping: tolerance + the window.
    clock.set(T0 + skew_millis + RETENTION_WINDOW_MILLIS + 1);
    let second = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        second.pruned_rows, 1,
        "a within-tolerance row still ages out on the holder's clock: {second:?}"
    );
    assert_eq!(
        second.future_dated_rows, 0,
        "and it is never reported as an operator event: {second:?}"
    );
}

// ---------------------------------------------------------------------------
// C7 — disk actually comes back
// ---------------------------------------------------------------------------

fn open_fragmentable_db(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open file-backed db");
    create_retained_table(&db, "windows");
    db
}

/// The prune report must answer the operator's three questions: how many rows
/// went, how many bytes came back, and did the file itself shrink.
#[test]
fn c7_prune_report_states_rows_bytes_reclaimed_and_whether_the_file_shrank() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));

    let payload = "x".repeat(8 * 1024);
    insert_rows(&db, "windows", 1..401, &payload);
    clock.advance(RETENTION_WINDOW_MILLIS * 2);

    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 400,
        "every expired row prunes: {report:?}"
    );
    assert!(
        report.reclaimed_bytes > 0,
        "the report must state the bytes reclaimed: {report:?}"
    );
    let before = report
        .file_bytes_before
        .expect("a file-backed database reports its file size before the cycle");
    let after = report
        .file_bytes_after
        .expect("a file-backed database reports its file size after the cycle");
    assert_eq!(
        report.file_shrank,
        after < before,
        "file_shrank must state what actually happened to the file \
         (before={before}, after={after}): {report:?}"
    );
    assert_eq!(
        db.disk_file_size(),
        Some(after),
        "the reported post-cycle file size must be the real file size"
    );
}

/// `reclaimed_bytes` must describe the data that actually went. Two populations
/// with the same row count and materially different payloads must report
/// materially different reclaim — a constant, or a row-count-derived number,
/// fails this.
#[test]
fn c7_reclaimed_bytes_track_the_size_of_what_was_pruned() {
    fn reclaim_for(dir: &tempfile::TempDir, name: &str, payload_bytes: usize) -> u64 {
        let (clock, _guard) = MockClock::install(T0);
        let db = open_fragmentable_db(&dir.path().join(name));
        insert_rows(&db, "windows", 1..201, &"q".repeat(payload_bytes));
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        let report = db.run_pruning_cycle_checked().expect("prune cycle");
        assert_eq!(
            report.pruned_rows, 200,
            "both populations prune the same row count: {report:?}"
        );
        report.reclaimed_bytes
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let small = reclaim_for(&dir, "small.db", 64);
    let large = reclaim_for(&dir, "large.db", 8 * 1024);

    assert!(
        large > small,
        "the same number of much larger rows must reclaim more \
         (small={small}, large={large})"
    );
    assert!(
        large >= small.saturating_mul(4),
        "and the difference must scale with the payload, not be a token increment \
         (small={small}, large={large})"
    );
}

/// The compaction decision must ride the SAME conservative threshold the
/// currency path uses — not a second, looser retention-only number.
#[test]
fn c7_compaction_rides_the_same_fragmentation_threshold_as_the_currency_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));

    let payload = "y".repeat(8 * 1024);
    insert_rows(&db, "windows", 1..401, &payload);
    clock.advance(RETENTION_WINDOW_MILLIS * 2);

    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.compacted,
        report.fragmentation_before >= REDB_COMPACT_FRAGMENTATION_THRESHOLD,
        "compaction must trigger exactly on the shared threshold \
         ({REDB_COMPACT_FRAGMENTATION_THRESHOLD}), fragmentation was {}: {report:?}",
        report.fragmentation_before
    );
    assert!(
        report.compacted,
        "pruning 400 of 400 large rows must leave the file dead-space dominated \
         and trip compaction: {report:?}"
    );
    let before = report.file_bytes_before.expect("file size before");
    let after = report.file_bytes_after.expect("file size after");
    assert!(
        after < before,
        "a compacted file must actually shrink ({before} -> {after}): {report:?}"
    );
}

/// The compaction DECISION side of C7, on a small prune over a live file: the
/// decision must ride the shared threshold in both directions and the report
/// must describe what actually happened to the file.
///
/// The absolute below-threshold assertions this test was written to carry were
/// withdrawn on measurement. `fragmentation_before` is a file-level density
/// ratio, and on redb this workload never lands stably beneath the 0.5 line:
/// per-row-commit inserts leave ~70% allocator slack (0.7491 with 3.2 MB live);
/// ballast that is itself pruned leaves a near-empty file in which anything
/// later reads as ~all-dead (0.7584); and densely batched live ballast measured
/// 0.4953, 0.5263, 0.5712 and 0.6310 across four shapes — the single
/// sub-threshold reading sat 0.005 under the line, which is a coin flip, not a
/// gate. So the below-threshold state is not representable as a deterministic
/// fixture here. What survives is the equivalence (which binds at whatever ratio
/// the engine measures) plus report honesty; the operational claim that steady
/// churn does not compact every cycle is carried by smoke S3's plateau, where it
/// is frozen.
#[test]
fn c7_a_small_prune_on_a_live_file_reports_its_compaction_decision_honestly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));
    db.execute(
        "CREATE TABLE ballast (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("non-retained ballast table");

    // Live ballast: 6,400 x 512 B rows, 400 to a commit, in a table with no
    // retention — so the file is full of data that survives the prune below and
    // the cycle under test really is a SMALL prune over a live file.
    let payload = "b".repeat(512);
    for batch in 0..16i64 {
        let mut params = p();
        let mut tuples = Vec::new();
        for slot in 0..400i64 {
            let id = batch * 400 + slot;
            params.insert(format!("i{slot}"), Value::Int64(id));
            params.insert(format!("b{slot}"), Value::Text(payload.clone()));
            tuples.push(format!("($i{slot}, $b{slot})"));
        }
        db.execute(
            &format!(
                "INSERT INTO ballast (id, body) VALUES {}",
                tuples.join(", ")
            ),
            &params,
        )
        .expect("batched ballast insert");
    }

    insert_rows(&db, "windows", 500..505, "small");
    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    let report = db.run_pruning_cycle_checked().expect("small prune cycle");

    assert_eq!(
        report.pruned_rows, 5,
        "only the five small retained rows expire; the ballast is not retained: {report:?}"
    );
    assert_eq!(
        row_count(&db, "ballast"),
        6_400,
        "and the live ballast is untouched by the cycle"
    );
    assert_eq!(
        report.compacted,
        report.fragmentation_before >= REDB_COMPACT_FRAGMENTATION_THRESHOLD,
        "the compaction decision must ride the shared threshold \
         ({REDB_COMPACT_FRAGMENTATION_THRESHOLD}) in BOTH directions, whatever \
         ratio the file measures: {report:?}"
    );

    let before = report.file_bytes_before.expect("file size before");
    let after = report.file_bytes_after.expect("file size after");
    assert_eq!(
        report.file_shrank,
        after < before,
        "file_shrank must state what actually happened ({before} -> {after}): {report:?}"
    );
    assert_eq!(
        db.disk_file_size(),
        Some(after),
        "and the reported size must be the real one"
    );
    if !report.compacted {
        assert!(
            !report.file_shrank,
            "a cycle that did not compact cannot have shrunk the file: {report:?}"
        );
    }
}

/// Maintenance must not lock a writer out. Proven with channel handshakes, not
/// timing: a separate writer thread commits a batch BEFORE the cycle is entered
/// and another batch AFTER it returns, and both batches are readable. Both
/// batches go through the same live handle the maintenance cycle runs on, so a
/// cycle that took a lock and failed to release it deadlocks this test rather
/// than passing it.
///
/// Deliberately NOT asserted here: a write landing DURING the cycle's critical
/// section. `run_maintenance_cycle` is synchronous with no mid-cycle observation
/// seam, and inventing one would be a `src/` testability touch. "No stall
/// during" is frozen as smoke S3, where the real bound lives.
#[test]
fn c7_writers_commit_before_and_after_a_maintenance_cycle() {
    use std::sync::mpsc;

    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = Arc::new(open_fragmentable_db(&dir.path().join("bounded.db")));

    let payload = "z".repeat(8 * 1024);
    insert_rows(&db, "windows", 1..401, &payload);
    clock.advance(RETENTION_WINDOW_MILLIS * 2);

    db.execute("CREATE TABLE live_writes (id INTEGER PRIMARY KEY)", &p())
        .expect("writer table must create");

    let committed = Arc::new(AtomicU64::new(0));
    let (wrote_first_batch, first_batch_done) = mpsc::channel::<()>();
    let (cycle_finished, resume_writer) = mpsc::channel::<()>();

    let writer = {
        let db = Arc::clone(&db);
        let committed = Arc::clone(&committed);
        std::thread::spawn(move || {
            let write = |id: i64| {
                let mut row = p();
                row.insert("id".to_string(), Value::Int64(id));
                db.execute("INSERT INTO live_writes (id) VALUES ($id)", &row)
                    .expect("a writer is never locked out by maintenance");
                committed.fetch_add(1, Ordering::SeqCst);
            };
            for id in 0..50i64 {
                write(id);
            }
            wrote_first_batch.send(()).expect("signal first batch");
            resume_writer.recv().expect("wait for the cycle to finish");
            for id in 100..150i64 {
                write(id);
            }
        })
    };

    first_batch_done
        .recv()
        .expect("the writer must commit its first batch before the cycle starts");
    assert_eq!(
        committed.load(Ordering::SeqCst),
        50,
        "exactly the pre-cycle batch is committed when the cycle is entered"
    );

    let report = db.run_maintenance_cycle().expect("maintenance cycle");
    cycle_finished.send(()).expect("release the writer");
    writer.join().expect("writer thread must not panic");

    assert_eq!(
        committed.load(Ordering::SeqCst),
        100,
        "the writer commits again after the cycle returns — maintenance left no \
         lock behind"
    );
    assert_eq!(
        row_count(&db, "live_writes"),
        100,
        "and every committed write is readable afterwards"
    );
    assert_eq!(
        report.pruning.pruned_rows, 400,
        "the cycle still did its work: {report:?}"
    );
}

// ---------------------------------------------------------------------------
// C8 — honest per-table size
// ---------------------------------------------------------------------------

/// The size answer is per table, moves with inserts and prunes, and carries the
/// whole-file number ALONGSIDE as itself — never as an attribution.
#[test]
fn c8_table_size_estimate_tracks_inserts_and_prunes_with_whole_file_alongside() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));

    let empty = db.table_size_estimate("windows").expect("size answer");
    assert_eq!(empty.row_count, 0);
    assert_eq!(
        empty.whole_file_bytes,
        db.disk_file_size(),
        "whole-file bytes are reported as the whole file, not as the table's share"
    );

    let payload = "w".repeat(4 * 1024);
    insert_rows(&db, "windows", 1..201, &payload);
    let filled = db.table_size_estimate("windows").expect("size answer");
    assert_eq!(filled.row_count, 200, "row count is exact: {filled:?}");
    assert!(
        filled.estimated_live_bytes > empty.estimated_live_bytes,
        "the estimate must grow with the data it describes: {empty:?} -> {filled:?}"
    );
    assert!(
        filled.estimated_live_bytes >= 200 * 4 * 1024,
        "the estimate must at least account for the payload it stores: {filled:?}"
    );

    clock.advance(2 * 60 * 60 * 1000);
    db.run_pruning_cycle_checked().expect("prune cycle");
    let pruned = db.table_size_estimate("windows").expect("size answer");
    assert_eq!(
        pruned.row_count, 0,
        "pruned rows leave the count: {pruned:?}"
    );
    assert!(
        pruned.estimated_live_bytes < filled.estimated_live_bytes,
        "the estimate must fall after a prune: {filled:?} -> {pruned:?}"
    );
}

/// Two tables in one file: each answer describes its own table, and neither
/// claims the file. This is what "never a claim of exact physical attribution"
/// has to mean operationally.
#[test]
fn c8_estimates_are_per_table_and_never_claim_the_whole_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));
    create_retained_table(&db, "other_windows");

    insert_rows(&db, "windows", 1..101, &"a".repeat(4 * 1024));
    insert_rows(&db, "other_windows", 1..11, "b");

    let big = db.table_size_estimate("windows").expect("size answer");
    let small = db
        .table_size_estimate("other_windows")
        .expect("size answer");
    assert!(
        big.estimated_live_bytes > small.estimated_live_bytes,
        "each answer must describe its own table: {big:?} vs {small:?}"
    );

    // Deliberately NOT asserted: that the per-table estimates sum to at most the
    // whole-file size. The criteria call these estimates and refuse exact
    // physical attribution, so demanding additivity would force the estimator to
    // model shared structure (indexes, overhead) it is not specified to model.
    // Whole-file bytes are checked as themselves, alongside.
    assert_eq!(
        big.whole_file_bytes,
        db.disk_file_size(),
        "the whole-file number is the whole file, reported alongside as itself"
    );
    assert_eq!(
        big.whole_file_bytes, small.whole_file_bytes,
        "both tables live in one file and say so identically — the whole-file \
         figure is never divided up between them"
    );
    assert_eq!(
        db.table_size_estimates()
            .expect("all table size answers")
            .iter()
            .filter(|estimate| estimate.table == "windows")
            .count(),
        1,
        "every table gets exactly one answer"
    );
}

// ---------------------------------------------------------------------------
// C10 — the engine's own unbounded table
// ---------------------------------------------------------------------------

fn open_audited_db(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open file-backed db");
    db.execute("CREATE TABLE fire (id INTEGER PRIMARY KEY)", &p())
        .expect("trigger source table");
    db.execute("CREATE TRIGGER fire_guard ON fire WHEN INSERT", &p())
        .expect("trigger");
    db.register_trigger_callback("fire_guard", |_, _| Ok(()))
        .expect("trigger callback");
    db.complete_initialization().expect("initialization");
    db
}

fn fire_trigger(db: &Database, ids: std::ops::Range<i64>) {
    for id in ids {
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(id));
        db.execute("INSERT INTO fire (id) VALUES ($id)", &row)
            .expect("trigger firing insert");
    }
}

fn audit_history_len(db: &Database) -> usize {
    db.trigger_audit_history(Default::default())
        .expect("durable audit history")
        .len()
}

/// The durable audit is retained by default — a consumer that never configures
/// anything still gets a bounded table. The recent batch surviving is what stops
/// a "wipe it all" implementation from passing.
#[test]
fn c10_durable_trigger_audit_history_is_bounded_by_default_retention() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_audited_db(&dir.path().join("audited.db"));

    let retention = db
        .trigger_audit_retention()
        .expect("the durable audit must carry a shipped default retention");
    let ring = db.trigger_audit_ring_capacity();

    fire_trigger(&db, 0..(ring as i64 + 50));
    assert!(
        audit_history_len(&db) > ring,
        "the existing contract holds: durable history exceeds the in-memory ring"
    );

    clock.advance(retention.as_millis() as u64 * 2);
    fire_trigger(&db, 100_000..100_010);

    let report = db.run_maintenance_cycle().expect("maintenance cycle");
    assert!(
        report.pruned_trigger_audit_rows >= ring as u64,
        "the cycle must report the audit rows it aged out: {report:?}"
    );
    assert_eq!(
        audit_history_len(&db),
        10,
        "exactly the entries inside the retention window survive — bounded, not neutered"
    );
    assert_eq!(
        db.trigger_audit_log().len(),
        ring,
        "the in-memory ring keeps its own bounded semantics untouched"
    );
}

/// A trigger-heavy workload run across retention cycles plateaus instead of
/// growing without bound. State-based: the count after the second churn round
/// does not exceed the count after the first.
#[test]
fn c10_trigger_heavy_workload_leaves_the_durable_audit_bounded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_audited_db(&dir.path().join("audited.db"));
    let retention = db.trigger_audit_retention().expect("default retention");
    let window = retention.as_millis() as u64;

    fire_trigger(&db, 0..500);
    clock.advance(window * 2);
    fire_trigger(&db, 1_000..1_500);
    db.run_maintenance_cycle().expect("first cycle");
    let after_first = audit_history_len(&db);

    clock.advance(window * 2);
    fire_trigger(&db, 2_000..2_500);
    db.run_maintenance_cycle().expect("second cycle");
    let after_second = audit_history_len(&db);

    assert_eq!(
        after_first, 500,
        "only the current window's firings remain after the first cycle"
    );
    assert!(
        after_second <= after_first,
        "steady trigger churn must plateau, not accumulate \
         ({after_first} -> {after_second})"
    );
}

/// A second guaranteed fact: DROP TABLE never deletes the dropped table's
/// audit rows. They must age out by the same retention as everything else.
#[test]
fn c10_dropped_table_orphan_audit_rows_age_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_audited_db(&dir.path().join("audited.db"));
    let retention = db.trigger_audit_retention().expect("default retention");

    fire_trigger(&db, 0..25);
    db.execute("DROP TABLE fire", &p())
        .expect("dropping the trigger's table must succeed");
    assert_eq!(
        audit_history_len(&db),
        25,
        "DROP TABLE leaves the audit rows orphaned — that is the gap being closed"
    );

    clock.advance(retention.as_millis() as u64 * 2);
    let report = db.run_maintenance_cycle().expect("maintenance cycle");
    assert_eq!(
        report.pruned_trigger_audit_rows, 25,
        "orphaned audit rows age out by the same retention: {report:?}"
    );
    assert_eq!(
        audit_history_len(&db),
        0,
        "no audit row outlives its retention because its table was dropped"
    );
}

/// A push-only declaration renders `SYNC PUSH ONLY` and none of the other three
/// directions — a render that prints two of them, or a different one, is wrong
/// in a way a bare `contains` check waves through.
fn assert_renders_exactly_push_only(rendered: &str) {
    for spelling in [
        "SYNC OFF",
        "SYNC PUSH ONLY",
        "SYNC PULL ONLY",
        "SYNC TWO WAY",
    ] {
        let found = rendered.matches(spelling).count();
        let wanted = usize::from(spelling == "SYNC PUSH ONLY");
        assert_eq!(
            found, wanted,
            "the declaration must render SYNC PUSH ONLY and nothing else, but \
             '{spelling}' appears {found} time(s), got:\n{rendered}"
        );
    }
}

// ---------------------------------------------------------------------------
// C4 — one-way policy, declared and durable (engine-side declaration half;
// the transport half lives in contextdb-server's bounded_tables_sync_tests.rs)
// ---------------------------------------------------------------------------

/// The direction is declared in DDL and persisted with the table — an app never
/// re-registers it on start. Retention alone declares no direction, so the bare
/// `SYNC SAFE` table beside it stays two-way (the engine default).
#[test]
fn c4_push_only_direction_persists_in_table_meta_across_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("policy.db");

    {
        let db = Database::open(&path).expect("open");
        db.execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
             RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
            &p(),
        )
        .expect("declared one-way retained table must create");
        db.execute(
            "CREATE TABLE implied (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE",
            &p(),
        )
        .expect("bare SYNC SAFE keeps working");
    }

    let db = Database::open(&path).expect("reopen");
    let meta = db.table_meta("windows").expect("table meta after restart");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    assert_renders_exactly_push_only(&rendered);
    assert!(meta.sync_safe, "SYNC SAFE survives alongside it");
    let implied = db.table_meta("implied").expect("implied table meta");
    let implied_rendered = contextdb_engine::cli_render::render_table_meta("implied", &implied);
    for one_way in ["PUSH ONLY", "PULL ONLY", "SYNC OFF"] {
        assert!(
            !implied_rendered.contains(one_way),
            "a retained SYNC SAFE table that declared no direction stays two-way — \
             retention decides no direction, got:\n{implied_rendered}"
        );
    }
}

/// A retained table that declares two-way is accepted: retention says when rows
/// expire, the direction clause says where they go, and the two are independent.
#[test]
fn c4_two_way_retained_table_is_accepted_at_declaration() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY) \
         RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
        &p(),
    )
    .expect("a retained two-way table must be accepted at declaration");
    let meta = db.table_meta("windows").expect("table meta");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE") && rendered.contains("SYNC TWO WAY"),
        "both declarations must be visible to an operator, got:\n{rendered}"
    );
}

/// `.schema` renders whatever direction the table declares, and the rendered DDL
/// re-parses to the same declaration — it is never invisible to an operator.
#[test]
fn c4_schema_render_round_trips_the_one_way_direction() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
        &p(),
    )
    .expect("create");
    let meta = db.table_meta("windows").expect("table meta");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the rendered schema must show the retention window, got:\n{rendered}"
    );
    assert_renders_exactly_push_only(&rendered);

    let db2 = Database::open_memory();
    db2.execute(&rendered, &p())
        .unwrap_or_else(|err| panic!("rendered DDL must execute: {err}\n{rendered}"));
    let round_tripped = db2.table_meta("windows").expect("round-tripped meta");
    assert_eq!(
        contextdb_engine::cli_render::render_table_meta("windows", &round_tripped),
        rendered,
        "the declaration must survive the .schema round trip unchanged"
    );
}

/// Multi-hub is the other refused combination. A retained table is delivered to
/// exactly one hub — the second distinct peer is refused loudly, and the first
/// peer registering twice is idempotent (a reconnect is not a second hub).
#[test]
fn c4_second_sync_peer_on_a_retained_table_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
        &p(),
    )
    .expect("create");

    db.register_retention_sync_peer("hub-node-a")
        .expect("the first hub must register");
    db.register_retention_sync_peer("hub-node-a")
        .expect("re-registering the same hub is a reconnect, not a second hub");
    assert_eq!(db.retention_sync_peer().as_deref(), Some("hub-node-a"));

    let err = db
        .register_retention_sync_peer("hub-node-b")
        .expect_err("a second distinct hub must be refused while a retained table exists");
    let message = err.to_string();
    assert!(
        message.contains("hub-node-b") && message.contains("hub-node-a"),
        "the refusal must name both hubs so the operator can see the conflict, got: {message}"
    );
    assert_eq!(
        db.retention_sync_peer().as_deref(),
        Some("hub-node-a"),
        "a refused registration must not replace the established hub"
    );
}

/// The refusal is worthless if a reboot forgets which hub was first: the
/// established hub is persisted in database metadata and reloaded on open, so
/// hub B cannot present itself as the first hub after a restart.
#[test]
fn c4_the_established_sync_peer_survives_restart_and_still_refuses_a_second() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer.db");

    {
        let db = Database::open(&path).expect("open");
        db.execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
            &p(),
        )
        .expect("create");
        db.register_retention_sync_peer("hub-node-a")
            .expect("first hub registers");
    }

    let db = Database::open(&path).expect("reopen");
    assert_eq!(
        db.retention_sync_peer().as_deref(),
        Some("hub-node-a"),
        "the established hub must be read back from database metadata after restart"
    );
    db.register_retention_sync_peer("hub-node-a")
        .expect("the same hub reconnecting after a restart is still not a second hub");

    let err = db
        .register_retention_sync_peer("hub-node-b")
        .expect_err("a reboot must not let a different hub claim first place");
    assert!(
        err.to_string().contains("hub-node-a"),
        "the refusal must name the hub it remembered across the restart, got: {err}"
    );
    assert_eq!(
        db.retention_sync_peer().as_deref(),
        Some("hub-node-a"),
        "and the established hub is unchanged"
    );
}

/// The ALTER path must not be a side door into the refused posture: the
/// nested direction form is a parse error there exactly as at CREATE, and the
/// table keeps the declaration it already had.
#[test]
fn c4_alter_set_retain_with_the_nested_direction_form_is_refused_and_changes_nothing() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
        &p(),
    )
    .expect("create");

    let err = db
        .execute(
            "ALTER TABLE windows SET RETAIN 12 HOURS SYNC SAFE TWO WAY",
            &p(),
        )
        .expect_err("ALTER must refuse the nested direction form just as CREATE does");
    let message = err.to_string();
    assert!(
        message.to_uppercase().contains("SYNC TWO WAY"),
        "the refusal must name the separate direction clause to write instead, got: {message}"
    );

    let meta = db.table_meta("windows").expect("table meta");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    assert_renders_exactly_push_only(&rendered);
    assert_eq!(
        meta.default_ttl_seconds,
        Some(48 * 60 * 60),
        "and must not apply the new window either — the whole statement is refused"
    );
    assert!(meta.sync_safe, "SYNC SAFE is likewise untouched");
}

/// The inverse corner. An ordinary table syncs two-way by default; declaring
/// retention on it says when its rows expire and nothing about where they go,
/// so it is still two-way afterwards.
#[test]
fn c4_alter_adding_retain_to_a_two_way_table_leaves_it_two_way() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("create");

    db.execute("ALTER TABLE windows SET RETAIN 48 HOURS SYNC SAFE", &p())
        .expect("adding retention to an existing table must succeed");
    let meta = db.table_meta("windows").expect("table meta");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    for one_way in ["PUSH ONLY", "PULL ONLY", "SYNC OFF"] {
        assert!(
            !rendered.contains(one_way),
            "declaring retention must not convert the table to {one_way}, got:\n{rendered}"
        );
    }
    assert_eq!(
        meta.default_ttl_seconds,
        Some(48 * 60 * 60),
        "while the window it did declare lands: {meta:?}"
    );
}

/// Retention without `SYNC SAFE` is the same story with no delivery promise at
/// all: a window, and nothing said about direction.
#[test]
fn c4_alter_adding_retain_without_sync_safe_changes_no_direction() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE windows (id INTEGER PRIMARY KEY)", &p())
        .expect("create");
    db.execute("ALTER TABLE windows SET RETAIN 6 HOURS", &p())
        .expect("adding a bare retention window must succeed");

    let meta = db.table_meta("windows").expect("table meta");
    let rendered = contextdb_engine::cli_render::render_table_meta("windows", &meta);
    for one_way in ["PUSH ONLY", "PULL ONLY", "SYNC OFF"] {
        assert!(
            !rendered.contains(one_way),
            "no declaration path may attach a direction the writer never wrote \
             ({one_way}), got:\n{rendered}"
        );
    }
    assert!(
        !meta.sync_safe,
        "and SYNC SAFE stays off — the delete-after-delivery gate is a separate promise"
    );
}

// ---------------------------------------------------------------------------
// C3/C4 — `SYNC SAFE` requires a key the hub can identify a row by
//
// A table with no primary key never gets its rows into the pushed changeset,
// yet the push reports success and the watermark advances over those LSNs. The
// SYNC SAFE gate then opens on rows the hub never received and retention
// deletes them. So the delete-only-after-DELIVERY promise may not be declared
// on a table that cannot be delivered.
// ---------------------------------------------------------------------------

/// The key requirement is refused at the door, and the refusal has to teach BOTH
/// halves: that a key is required, and that its values must be unique ACROSS THE
/// NODES that write them — two edges pushing the same surrogate key collide at
/// the hub under InsertIfNotExists, which is the same silent-drop class one
/// layer along.
///
/// The wording stays in the engine's own vocabulary (node, origin). The engine
/// is schema-agnostic, so a message that reached for a consumer's nouns
/// would put that consumer's vocabulary into engine source — which is exactly
/// what the product-noun grep forbids.
fn assert_keyless_sync_safe_refusal(message: &str) {
    let lower = message.to_lowercase();
    assert!(
        lower.contains("sync safe"),
        "the refusal must name the declaration it is refusing, got: {message}"
    );
    assert!(
        lower.contains("primary key"),
        "the refusal must name the requirement, got: {message}"
    );
    assert!(
        lower.contains("unique") && (lower.contains("node") || lower.contains("origin")),
        "the refusal must carry the cross-node caution — key values must be \
         unique across the nodes that write them (an origin/node identifier, or \
         globally unique ids). The engine is schema-agnostic, so the \
         wording must stay generic: no consumer-domain nouns — got: {message}"
    );
}

#[test]
fn c4_sync_safe_on_a_keyless_table_is_refused_at_declaration() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE windows (origin_id TEXT, series TEXT, value REAL) \
             RETAIN 48 HOURS SYNC SAFE",
            &p(),
        )
        .expect_err("SYNC SAFE on a table with no primary key must be refused");
    assert_keyless_sync_safe_refusal(&err.to_string());
    assert!(
        db.table_meta("windows").is_none(),
        "the whole statement is refused — no half-created table"
    );
}

#[test]
fn c4_alter_adding_sync_safe_to_a_keyless_table_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (origin_id TEXT, series TEXT, value REAL)",
        &p(),
    )
    .expect("a keyless table is fine until it promises delivery");

    let err = db
        .execute("ALTER TABLE windows SET RETAIN 48 HOURS SYNC SAFE", &p())
        .expect_err("the ALTER path must refuse the same combination");
    assert_keyless_sync_safe_refusal(&err.to_string());

    let meta = db.table_meta("windows").expect("the table still exists");
    assert!(
        !meta.sync_safe,
        "a refused ALTER leaves no delivery promise behind: {meta:?}"
    );
    assert_eq!(
        meta.default_ttl_seconds, None,
        "and applies no part of the statement, not even the window: {meta:?}"
    );
    assert_eq!(
        meta.sync_direction, None,
        "and establishes no sync policy: {meta:?}"
    );
}

/// The same refusal on the arrival path. A peer that spells the combination in
/// synced DDL is refused before anything is applied — otherwise a foreign edge
/// could install locally the very declaration every local door rejects, and this
/// engine would then delete undelivered rows on its behalf.
#[test]
fn c4_sync_safe_on_a_keyless_table_is_refused_from_synced_ddl() {
    use contextdb_core::Lsn;
    use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};

    let db = Database::open_memory();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::CreateTable {
                    name: "windows".into(),
                    columns: vec![
                        ("origin_id".into(), "TEXT".into()),
                        ("value".into(), "REAL".into()),
                    ],
                    constraints: vec!["RETAIN 48 HOURS SYNC SAFE".into()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(1)],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("arriving DDL must be refused exactly as the local paths are");
    assert_keyless_sync_safe_refusal(&err.to_string());
    assert!(
        db.table_meta("windows").is_none(),
        "a refused arrival applies no schema side effects"
    );
}

/// The negative control that keeps the refusal narrow: plain `RETAIN` on a
/// keyless table makes no delivery promise at all — it prunes locally — so it
/// stays allowed and must keep working.
#[test]
fn c4_plain_retention_on_a_keyless_table_stays_allowed() {
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (origin_id TEXT, series TEXT, value REAL) RETAIN 1 HOURS",
        &p(),
    )
    .expect("retention without a delivery promise needs no key");

    let meta = db.table_meta("windows").expect("table meta");
    assert_eq!(
        meta.default_ttl_seconds,
        Some(60 * 60),
        "the window is declared: {meta:?}"
    );
    assert!(
        !meta.sync_safe,
        "and no delivery promise is attached: {meta:?}"
    );

    let mut row = p();
    row.insert("origin_id".to_string(), Value::Text("n-1".to_string()));
    row.insert("series".to_string(), Value::Text("rate".to_string()));
    row.insert("value".to_string(), Value::Float64(1.5));
    db.execute(
        "INSERT INTO windows (origin_id, series, value) VALUES ($origin_id, $series, $value)",
        &row,
    )
    .expect("insert into the keyless retained table");
    assert_eq!(row_count(&db, "windows"), 1);

    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 1,
        "a keyless retained table still ages out locally: {report:?}"
    );
    assert_eq!(row_count(&db, "windows"), 0);
}

// ---------------------------------------------------------------------------
// C7 — a retained table's pruned rows leave no change-log residue
//
// Every commit appends to the change log, and the generic retention prune never
// touched it — only the currency path did. So a retained table's rows aged out
// while their change-log entries accumulated forever, and the file grew linearly
// under steady churn no matter how little data was live.
//
// SCOPE, deliberately narrow: this is about RESIDUE from pruned rows, NOT about
// the change log being bounded in general. The unconditional per-commit append
// stays exactly as it is for tables with no retention — that is a separately
// ledgered gap, and every count asserted below is filtered to one table so that
// nothing here can be read as a global boundedness claim.
//
// The log is an optimization: `changes_since` falls back to persisted state on
// any gap (the ordinary post-restart path), and a wiped edge restores from a
// snapshot at Lsn(0) rather than a replay. Dropping the entries in the same
// commit as the rows is therefore safe — the tests below pin both the drop AND
// the guarantees that must survive it.
// ---------------------------------------------------------------------------

fn change_log_entries_for(db: &Database, table: &str) -> usize {
    use contextdb_engine::composite_store::ChangeLogEntry;
    db.change_log_since(contextdb_core::Lsn(0))
        .into_iter()
        .filter(|entry| match entry {
            ChangeLogEntry::RowInsert { table: name, .. }
            | ChangeLogEntry::RowDelete { table: name, .. } => name.as_str() == table,
            _ => false,
        })
        .count()
}

fn create_control_table(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("non-retained control table");
}

#[test]
fn c7_retention_prune_drops_the_pruned_rows_change_log_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));
    create_control_table(&db);

    insert_rows(&db, "windows", 1..11, "expiring");
    insert_rows(&db, "notes", 1..4, "kept");
    assert!(
        change_log_entries_for(&db, "windows") >= 10,
        "the fixture's premise: every commit appended an entry"
    );
    let control_entries = change_log_entries_for(&db, "notes");
    assert!(control_entries >= 3);

    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(report.pruned_rows, 10, "the rows go: {report:?}");

    assert_eq!(
        change_log_entries_for(&db, "windows"),
        0,
        "and their change-log entries go WITH them — otherwise the file grows \
         linearly under churn no matter how little data is live"
    );
    assert_eq!(
        change_log_entries_for(&db, "notes"),
        control_entries,
        "while a table that pruned nothing keeps every entry it had"
    );
    assert_eq!(row_count(&db, "notes"), 3, "and its rows are untouched");
}

/// The drop must be part of the prune's own commit, not an in-memory tidy-up: a
/// reopen reloads the PERSISTED log, so a half-applied truncation shows up here.
#[test]
fn c7_the_change_log_truncation_is_committed_with_the_prune() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bounded.db");

    {
        let (clock, _guard) = MockClock::install(T0);
        let db = open_fragmentable_db(&path);
        create_control_table(&db);
        insert_rows(&db, "windows", 1..11, "expiring");
        insert_rows(&db, "notes", 1..4, "kept");
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        assert_eq!(
            db.run_pruning_cycle_checked().expect("prune").pruned_rows,
            10
        );
        assert_eq!(change_log_entries_for(&db, "windows"), 0);
    }

    let db = Database::open(&path).expect("reopen");
    assert_eq!(
        change_log_entries_for(&db, "windows"),
        0,
        "the persisted change log must come back truncated — an in-memory-only \
         drop leaves the file growing exactly as before"
    );
    assert_eq!(
        row_count(&db, "notes"),
        3,
        "and the live control rows survive the round trip"
    );
}

/// The first guarantee that must survive truncation: a restore payload is built
/// from committed state, not from the log, so it still carries every live row
/// after the log was truncated. (The full wiped-edge restore over real sync is
/// `c5_wipe_and_restore_pull_does_not_resurrect_pruned_rows` in the sync suite;
/// this is the engine-level payload check that truncation did not hollow it.)
#[test]
fn c7_a_restore_payload_after_truncation_still_carries_every_live_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));
    create_control_table(&db);

    insert_rows(&db, "windows", 1..11, "expiring");
    insert_rows(&db, "notes", 1..4, "kept");
    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    insert_rows(&db, "windows", 100..103, "still-young");
    db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(change_log_entries_for(&db, "windows"), 3);

    let payload = db.changes_since(contextdb_core::Lsn(0));
    let rows_for = |table: &str| payload.rows.iter().filter(|row| row.table == table).count();
    assert_eq!(
        rows_for("notes"),
        3,
        "every live row of the untouched table is still offered: {:?}",
        payload.rows
    );
    assert_eq!(
        rows_for("windows"),
        3,
        "as is every live row of the pruned table — truncation removes the aged \
         rows' entries, never the survivors': {:?}",
        payload.rows
    );
}

/// The second guarantee, asserted as BEHAVIOUR: with an unconfirmed `SYNC SAFE`
/// backlog present, a prune leaves that backlog's change-log entries alone,
/// because it removes no rows at all. An undelivered backlog still needs its
/// entries for the retry push.
///
/// This deliberately does NOT reach for a watermark parameter on the truncation
/// — there is none, and there should be none. The invariant is enforced once, at
/// the existing prune gate (`row_is_prunable` refuses unconfirmed `SYNC SAFE`
/// rows, so pruned implies confirmed), and truncation simply follows whatever
/// the prune removed. This test is the tripwire that fires if anyone ever
/// loosens that gate.
#[test]
fn c7_unconfirmed_sync_safe_entries_are_never_truncated() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = Database::open(dir.path().join("bounded.db")).expect("open");
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 1 HOURS SYNC SAFE SYNC PUSH ONLY",
        &p(),
    )
    .expect("retained, delivery-promising table");

    insert_rows(&db, "windows", 1..21, "unconfirmed");
    let entries_before = change_log_entries_for(&db, "windows");
    assert!(entries_before >= 20);

    // Nothing has been confirmed: the watermark is still closed.
    assert_eq!(db.sync_watermark(), contextdb_core::Lsn(0));
    clock.advance(48 * 60 * 60 * 1000);
    let report = db.run_pruning_cycle_checked().expect("prune cycle");

    assert_eq!(
        report.pruned_rows, 0,
        "no unconfirmed row may be pruned, however old: {report:?}"
    );
    assert_eq!(
        change_log_entries_for(&db, "windows"),
        entries_before,
        "so not one of their change-log entries may be dropped either — the retry \
         push still needs them"
    );
    assert_eq!(row_count(&db, "windows"), 20);
}

/// The residue witness, at CI scale and fully deterministic: after ten rounds of
/// write-and-expire on ONE RETAINED TABLE, that table's pruned rows have left no
/// change-log residue behind.
///
/// Scoped to the retained table on purpose — `change_log_entries_for` filters by
/// table name, so this asserts nothing about the log as a whole. Tables with no
/// retention still append per commit and are not this test's business.
#[test]
fn c7_repeated_expire_and_prune_leaves_no_residue_for_a_retained_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));

    let mut written = 0i64;
    for round in 0..10i64 {
        let start = round * 100 + 1;
        insert_rows(&db, "windows", start..(start + 20), "churn");
        written += 20;
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        db.run_pruning_cycle_checked().expect("prune cycle");
    }
    assert_eq!(
        written, 200,
        "the fixture really did write 200 rows across the rounds"
    );
    assert_eq!(
        row_count(&db, "windows"),
        0,
        "and expired every one of them"
    );

    let entries = change_log_entries_for(&db, "windows");
    assert!(
        entries <= 20,
        "a retained table's pruned rows must leave no change-log residue: \
         {written} rows written and expired, {entries} entries still naming the \
         table"
    );
}

/// The guard for the commit-index trim, and the only part of that contract
/// expressible on today's public surface: a database that pruned and was then
/// reopened must answer `changes_since` IDENTICALLY.
///
/// Trimming the commit index is safe precisely because no consumer can ask below
/// the surviving change log's floor, and because `open` reconstructs the index
/// from live data anyway. Both halves show up here: if a trim ever removed an
/// entry a reader still needs, the pre-reopen answer (served from the in-memory
/// log) and the post-reopen answer (served from reconstructed state) would
/// diverge — which is exactly the shape of the bug this guard exists to catch.
#[test]
fn c7_changes_since_answers_identically_across_a_prune_and_reopen() {
    use contextdb_engine::sync_types::ChangeSet;

    fn fingerprint(changes: &ChangeSet) -> Vec<(String, bool, String)> {
        let mut rows = changes
            .rows
            .iter()
            .map(|row| {
                (
                    row.table.clone(),
                    row.deleted,
                    format!("{:?}", row.natural_key),
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bounded.db");

    let before = {
        let (clock, _guard) = MockClock::install(T0);
        let db = open_fragmentable_db(&path);
        create_control_table(&db);

        insert_rows(&db, "windows", 1..11, "expiring");
        insert_rows(&db, "notes", 1..4, "kept");
        // A DELETE must be among the surviving entries, and the pull must cross
        // it. The delete arms of changes_since resolve `snapshot_at(lsn - 1)` —
        // they ask about the LSN BELOW their own entry — so a pure-insert
        // fixture asks that question for nobody and would sail past a floor that
        // trimmed the predecessor away. The failure it would hide is silent: an
        // all-invisible snapshot drops the delete from the changeset, no crash.
        db.execute("DELETE FROM notes WHERE id = 2", &p())
            .expect("delete a surviving control row");
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        // Written after the clock moved: still inside the window, so the pruned
        // and surviving rows are interleaved in LSN order rather than cleanly
        // split — a trim that took one entry too many shows up here.
        insert_rows(&db, "windows", 100..104, "still-young");

        let report = db.run_pruning_cycle_checked().expect("prune cycle");
        assert_eq!(
            report.pruned_rows, 10,
            "the fixture really pruned: {report:?}"
        );
        let changes = db.changes_since(contextdb_core::Lsn(0));
        assert!(
            changes.rows.iter().any(|row| row.deleted),
            "the fixture's premise: a delete must be present in the pull, or the \
             snapshot_at(lsn - 1) path is never exercised: {:?}",
            changes.rows
        );
        fingerprint(&changes)
    };

    let db = Database::open(&path).expect("reopen");
    let after = fingerprint(&db.changes_since(contextdb_core::Lsn(0)));

    assert_eq!(
        after, before,
        "a pruned database must answer changes_since identically before and after \
         a reopen — the in-memory-log answer and the reconstructed answer may \
         never diverge"
    );
    assert_eq!(
        before
            .iter()
            .filter(|(table, ..)| table == "windows")
            .count(),
        4,
        "and the answer is the live rows: the four young ones, not the ten pruned"
    );
    assert_eq!(
        before
            .iter()
            .filter(|(table, deleted, _)| table == "notes" && *deleted)
            .count(),
        1,
        "plus the untouched table's rows, INCLUDING the delete the pull must cross"
    );
}

/// The never-trim-to-empty case. When a prune expires everything, the surviving
/// change log is empty — a floor derived from "the lowest surviving entry" has
/// no lowest entry to derive from, and a trim that follows it literally empties
/// the index. The database must keep answering correctly afterwards: the next
/// write and read round-trip, and a pull still reconstructs it.
#[test]
fn c7_a_prune_that_expires_everything_leaves_the_database_answering_correctly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bounded.db");

    {
        let (clock, _guard) = MockClock::install(T0);
        let db = open_fragmentable_db(&path);

        insert_rows(&db, "windows", 1..11, "expiring");
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        let report = db.run_pruning_cycle_checked().expect("prune cycle");
        assert_eq!(
            report.pruned_rows, 10,
            "everything expires — the empty-log case: {report:?}"
        );
        assert_eq!(row_count(&db, "windows"), 0);

        // A write after the log went empty must still commit and read back.
        insert_rows(&db, "windows", 100..103, "after-the-sweep");
        assert_eq!(
            row_count(&db, "windows"),
            3,
            "a write after a total sweep must commit and be readable"
        );
        let changes = db.changes_since(contextdb_core::Lsn(0));
        assert_eq!(
            changes
                .rows
                .iter()
                .filter(|row| row.table == "windows")
                .count(),
            3,
            "and a pull must carry it: {:?}",
            changes.rows
        );
        // The structural half: an emptied log has no minimum to derive a floor
        // from, and a trim that follows it literally would empty the index.
        let (entries, _) = db.commit_index_census_for_test();
        assert!(
            entries >= 1,
            "the index must never be trimmed to empty, even when the prune \
             expired everything"
        );
    }

    let db = Database::open(&path).expect("reopen");
    assert_eq!(
        row_count(&db, "windows"),
        3,
        "and it must survive the reopen — a database trimmed to empty and then \
         written to may not lose the write"
    );
}

/// The commit index holds one entry per commit and nothing ever removed them, so
/// it grew with every write forever. The prune must trim it — but NOT to the
/// surviving change log's minimum LSN, because `changes_since`'s delete arms
/// resolve `snapshot_at(lsn - 1)`: they ask about the LSN BELOW their own entry.
/// Trim to the minimum and the oldest surviving delete resolves against nothing,
/// degrades to an all-invisible snapshot, and is silently dropped from the
/// changeset. So the floor is the ANCHOR: the greatest entry strictly below the
/// surviving minimum is kept, and everything below THAT goes.
fn surviving_change_log_lsns(db: &Database) -> std::collections::BTreeSet<contextdb_core::Lsn> {
    db.change_log_since(contextdb_core::Lsn(0))
        .iter()
        .map(|entry| entry.lsn())
        .collect()
}

#[test]
fn c7_prune_trims_the_commit_index_to_the_surviving_change_log_floor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));
    create_control_table(&db);

    insert_rows(&db, "windows", 1..31, "expiring");
    clock.advance(RETENTION_WINDOW_MILLIS * 2);
    insert_rows(&db, "notes", 1..4, "kept");
    insert_rows(&db, "windows", 100..104, "still-young");

    let (entries_before, _) = db.commit_index_census_for_test();
    let report = db.run_pruning_cycle_checked().expect("prune cycle");
    assert_eq!(
        report.pruned_rows, 30,
        "the fixture really pruned: {report:?}"
    );

    let surviving = surviving_change_log_lsns(&db);
    let surviving_min = *surviving
        .iter()
        .next()
        .expect("survivors remain, so the log is not empty");
    let (entries_after, lowest_retained) = db.commit_index_census_for_test();

    assert!(
        entries_after < entries_before,
        "the prune must trim the index ({entries_before} -> {entries_after})"
    );
    assert_eq!(
        report.pruned_commit_index_entries,
        (entries_before - entries_after) as u64,
        "and must report exactly what it trimmed: {report:?}"
    );
    assert!(
        lowest_retained < surviving_min,
        "the ANCHOR must survive — an entry strictly below the lowest LSN any \
         consumer can name, or the oldest surviving delete resolves \
         snapshot_at(lsn - 1) against nothing and is silently dropped \
         (lowest_retained={lowest_retained:?}, surviving_min={surviving_min:?})"
    );
    // The anchor rule stated exactly, rather than inferred from a count bound:
    // exactly ONE entry may sit below the lowest LSN a consumer can name. Fewer
    // is the silent-drop bug above; more is unreclaimed growth.
    assert_eq!(
        db.commit_index_entries_below_for_test(surviving_min),
        1,
        "exactly one entry — the anchor — may remain below the surviving \
         minimum {surviving_min:?}: fewer drops the oldest delete, more is the \
         growth this trim exists to stop"
    );
    assert!(
        entries_after <= surviving.len() + 1,
        "and nothing else accumulates above it either: at most one entry beneath \
         the surviving log's {} distinct LSNs, found {entries_after}",
        surviving.len()
    );
}

/// The trim must ride the prune's own commit. A reopen reconstructs the index
/// from live data, so a trim that only touched memory comes back from disk with
/// every old entry still in it — which is what this catches.
#[test]
fn c7_the_commit_index_trim_is_committed_with_the_prune() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bounded.db");

    let (entries_after_prune, surviving_lsn_count) = {
        let (clock, _guard) = MockClock::install(T0);
        let db = open_fragmentable_db(&path);
        create_control_table(&db);

        insert_rows(&db, "windows", 1..31, "expiring");
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        insert_rows(&db, "notes", 1..4, "kept");
        insert_rows(&db, "windows", 100..104, "still-young");
        assert_eq!(
            db.run_pruning_cycle_checked().expect("prune").pruned_rows,
            30
        );

        let (entries, _) = db.commit_index_census_for_test();
        (entries, surviving_change_log_lsns(&db).len())
    };

    let db = Database::open(&path).expect("reopen");
    let (entries_after_reopen, _) = db.commit_index_census_for_test();
    assert!(
        entries_after_reopen <= entries_after_prune,
        "the persisted index must come back trimmed — an in-memory-only trim \
         reloads every entry it pretended to remove ({entries_after_prune} before \
         reopen, {entries_after_reopen} after)"
    );
    assert!(
        entries_after_reopen <= surviving_lsn_count + 1,
        "and the reloaded index is still bounded by the surviving log plus its \
         anchor ({surviving_lsn_count} + 1), found {entries_after_reopen}"
    );
}

/// The bounded witness, mirroring the change-log one: rounds of write-and-expire
/// leave the index sized by what is live plus the recent tail, not by the total
/// number of commits ever made.
#[test]
fn c7_repeated_expire_and_prune_leaves_the_commit_index_sized_by_live_data() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (clock, _guard) = MockClock::install(T0);
    let db = open_fragmentable_db(&dir.path().join("bounded.db"));

    let mut commits = 0usize;
    for round in 0..10i64 {
        let start = round * 100 + 1;
        insert_rows(&db, "windows", start..(start + 20), "churn");
        commits += 20;
        clock.advance(RETENTION_WINDOW_MILLIS * 2);
        db.run_pruning_cycle_checked().expect("prune cycle");
    }
    assert_eq!(commits, 200, "the fixture really made 200 write commits");
    assert_eq!(row_count(&db, "windows"), 0, "and expired every row");

    let (entries, _) = db.commit_index_census_for_test();
    assert!(
        entries <= 30,
        "the commit index must be sized by live data plus a recent tail, not by \
         the {commits} commits ever made: {entries} entries left"
    );
}
