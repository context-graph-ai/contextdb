#![cfg(feature = "test-seams")]
//! A total over a table bigger than one read may examine.
//!
//! Every read threshold is declared, and `work` is what ONE ordinary read or
//! ONE cursor fetch may examine. A consumer that wants a COUNT or a byte SUM
//! over a table far larger than that ceiling is therefore stuck between two
//! bad answers: raise the ceiling until the whole table fits -- which is the
//! uncapped scan the ceiling exists to prevent -- or accept no total at all.
//!
//! The way out is the cursor. The consumer opens the aggregate statement as a
//! cursor and fetches until the store says there is no more: each fetch
//! examines at most `work` items, the pages before the last carry no rows
//! because a total is not a total until the input ends, and the final page
//! carries exactly one row holding the whole answer. The declared ceiling is
//! never raised and the store is never scanned in one unbounded pass.
//!
//! The one-shot door is unchanged and deliberately so: an aggregate whose
//! input is larger than `work` is refused with the typed work refusal, which
//! is what tells a consumer to reach for the cursor instead of guessing why an
//! answer was slow.
//!
//! `SUM` is the second aggregate this contract covers, and it must mean the
//! same thing at every door a reader can knock on: the eager executor, an
//! in-process bounded view of a live store, and a direct read of a closed
//! file. Same value, same column name, same count of rows examined.

use contextdb_core::read_contract::{
    CursorPage, DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadClientTimeouts,
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::executor::bounded_read_test_support::{
    self as bounded, TestTelemetry, TestWorkSource,
};
use contextdb_engine::{Database, QueryResult, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// The declared ceiling on what one ordinary read or one cursor fetch may
/// examine, for the fixtures that are deliberately larger than it.
const WORK_CEILING: u64 = 1_000;

/// Comfortably more rows than the ceiling admits, and not a multiple of it, so
/// the last page is a partial one rather than an exactly-filled one.
const BIG_ROWS: i64 = 12_345;

/// The byte-column fixture: large enough that a total is worth asking for,
/// small enough to answer in one bounded read.
const CHUNK_ROWS: i64 = 1_000;

/// Every seventh chunk has no recorded length. `SUM` skips those; `COUNT(*)`
/// still counts the rows.
const NULL_EVERY: i64 = 7;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// A synchronous read has nothing to wait for; the deadline vocabulary is
/// satisfied without ever consulting a real clock.
#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

/// Ceilings far above anything a fixture needs, so a ceiling is never what
/// makes two doors disagree.
fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// The declared limits of a consumer that will not raise its work ceiling:
/// one thousand items per read, and a cursor page ceiling that never exceeds
/// the result-row ceiling.
fn work_capped_limits() -> ReadLimits {
    ReadLimits {
        work: WORK_CEILING,
        result_rows: 1_024,
        cursor_page_rows: 64,
        ..roomy_limits()
    }
}

fn request(
    sql: &str,
    bound: HashMap<String, Value>,
    limits: ReadLimits,
) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, limits, Arc::new(FrozenClock))
}

fn session_options(limits: ReadLimits) -> ReadSessionOptions {
    ReadSessionOptions {
        limits,
        timeouts: ReadClientTimeouts::default(),
        ..ReadSessionOptions::default()
    }
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create a task-scoped runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped runtime root");
    root
}

// --- (a) one byte total, every door ----------------------------------------

const CONTEXT_UNDER_TEST: Uuid = Uuid::from_u128(0x00C0_0000_0000_0000_0000_0000_0000_0001);
const OTHER_CONTEXT: Uuid = Uuid::from_u128(0x00C0_0000_0000_0000_0000_0000_0000_0002);

/// What the seeded chunks add up to, computed from the rows actually written
/// rather than from a constant that would have to be maintained alongside the
/// fixture.
struct SeededChunks {
    expected_sum: i64,
    expected_rows: usize,
}

/// Each chunk carries its own byte length, so no two rows collapse into one
/// value and a total really has to visit all of them. Every seventh length is
/// missing, and half the chunks belong to a different context.
fn seed_chunks(path: &std::path::Path) -> SeededChunks {
    let database = Database::open(path).expect("create the file-backed chunk fixture");
    database
        .execute(
            "CREATE TABLE chunks (id INTEGER PRIMARY KEY, context_id UUID, label TEXT, \
             byte_len INTEGER)",
            &empty(),
        )
        .expect("create the chunk table");
    let mut expected_sum: i64 = 0;
    let mut expected_rows: usize = 0;
    database
        .execute("BEGIN", &empty())
        .expect("open the seeding transaction");
    for id in 0..CHUNK_ROWS {
        let mine = id % 2 == 0;
        let context = if mine {
            CONTEXT_UNDER_TEST
        } else {
            OTHER_CONTEXT
        };
        let byte_len = if id % NULL_EVERY == 0 {
            Value::Null
        } else {
            // Distinct per row, and never a value another row also holds.
            let length = id * 3 + 11;
            if mine {
                expected_sum += length;
            }
            Value::Int64(length)
        };
        if mine {
            expected_rows += 1;
        }
        database
            .execute(
                "INSERT INTO chunks (id, context_id, label, byte_len) \
                 VALUES ($id, $context, $label, $byte_len)",
                &params([
                    ("id", Value::Int64(id)),
                    ("context", Value::Uuid(context)),
                    ("label", Value::Text(format!("chunk-{id}"))),
                    ("byte_len", byte_len),
                ]),
            )
            .expect("store a chunk");
    }
    database
        .execute("COMMIT", &empty())
        .expect("commit the seeded chunks");
    database
        .close()
        .expect("release the idle file for direct reading");
    SeededChunks {
        expected_sum,
        expected_rows,
    }
}

const CHUNK_SUM_SQL: &str = "SELECT SUM(byte_len) FROM chunks WHERE context_id = $c";
const CHUNK_SUM_ALIASED_SQL: &str =
    "SELECT SUM(byte_len) AS total_bytes FROM chunks WHERE context_id = $c";

fn one_aggregate_row(what: &str, answer: &QueryResult) -> Value {
    assert_eq!(
        answer.rows.len(),
        1,
        "{what} answers a total with exactly one row, and returned {:?}",
        answer.rows
    );
    assert_eq!(
        answer.rows[0].len(),
        1,
        "{what} answers a single total, and returned {:?}",
        answer.rows[0]
    );
    answer.rows[0][0].clone()
}

/// A byte total is the same number, under the same column name, having
/// examined the same rows, whichever door a reader knocks on: the eager
/// executor, an in-process bounded view of the live store, or a direct read of
/// the closed file.
#[test]
fn sum_of_a_byte_column_agrees_on_every_route() {
    let directory = tempfile::TempDir::new().expect("task-scoped chunk directory");
    let path = directory.path().join("chunk-totals.db");
    let runtime_root = secure_runtime_root(&directory, "chunk-totals-runtime");
    let seeded = seed_chunks(&path);
    let bound = params([("c", Value::Uuid(CONTEXT_UNDER_TEST))]);

    let live = Database::open(&path).expect("reopen the seeded store");
    let eager = live
        .execute(CHUNK_SUM_SQL, &bound)
        .expect("the executor answers a byte total");
    let embedded = live
        .read_session(roomy_limits())
        .expect("open an in-process bounded view")
        .execute(CHUNK_SUM_SQL, &bound)
        .expect("an in-process bounded read answers a byte total");
    let aliased = live
        .read_session(roomy_limits())
        .expect("open an in-process bounded view for the aliased total")
        .execute(CHUNK_SUM_ALIASED_SQL, &bound)
        .expect("an in-process bounded read answers an aliased byte total");
    live.close().expect("release the file for direct reading");

    let direct_session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, session_options(roomy_limits()))
    })
    .expect("an idle file selects the direct route");
    assert_eq!(direct_session.route(), ReadRoute::File);
    let direct = direct_session
        .execute(CHUNK_SUM_SQL, &bound)
        .expect("a direct read of the closed file answers a byte total");

    let expected = Value::Int64(seeded.expected_sum);
    println!(
        "OBSERVED byte total over {} chunks: executor {:?} | in-process {:?} | direct file {:?}",
        seeded.expected_rows, eager.rows, embedded.rows, direct.rows
    );

    for (what, answer) in [
        ("the executor", &eager),
        ("an in-process bounded read", &embedded),
        ("a direct read of the closed file", &direct),
    ] {
        assert_eq!(
            one_aggregate_row(what, answer),
            expected,
            "{what} must add up the {} recorded lengths this context holds, skipping the \
             rows whose length is missing",
            seeded.expected_rows
        );
        assert_eq!(
            answer.columns.len(),
            1,
            "{what} names exactly the one column the statement projects, and named {:?}",
            answer.columns
        );
        assert!(
            answer.columns[0].eq_ignore_ascii_case("sum"),
            "{what} names an unaliased total after the aggregate that produced it, so a caller \
             can find the column without counting positions; it named {:?}",
            answer.columns
        );
    }

    assert_eq!(
        aliased.columns,
        vec!["total_bytes".to_owned()],
        "an explicit alias is the caller's name for the total and always wins"
    );
    assert_eq!(
        one_aggregate_row("an aliased in-process bounded read", &aliased),
        expected,
        "naming the total does not change it"
    );

    assert_eq!(
        embedded.columns, eager.columns,
        "the doors must not disagree about what the total is called"
    );
    assert_eq!(
        direct.columns, eager.columns,
        "the doors must not disagree about what the total is called"
    );

    assert!(
        eager.trace.rows_examined > 0,
        "a total over {CHUNK_ROWS} stored chunks examines rows; the executor reported none, so \
         the figure an operator reads to size a query is unusable"
    );
    assert_eq!(
        embedded.trace.rows_examined, eager.trace.rows_examined,
        "an in-process bounded read examined {} rows where the executor examined {} for the same \
         statement over the same committed rows",
        embedded.trace.rows_examined, eager.trace.rows_examined
    );
    assert_eq!(
        direct.trace.rows_examined, eager.trace.rows_examined,
        "a direct read of the closed file examined {} rows where the executor examined {} for the \
         same statement over the same committed rows",
        direct.trace.rows_examined, eager.trace.rows_examined
    );
}

// --- (b) and (c) a total across cursor pages -------------------------------

/// A table deliberately larger than the work ceiling, with a distinct length
/// per row and a flag that excludes half of them.
fn seed_big_table() -> Arc<Database> {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE big (id INTEGER PRIMARY KEY, byte_len INTEGER, keep INTEGER)",
            &empty(),
        )
        .expect("create the oversized table");
    database
        .execute("BEGIN", &empty())
        .expect("open the seeding transaction");
    for id in 0..BIG_ROWS {
        database
            .execute(
                "INSERT INTO big (id, byte_len, keep) VALUES ($id, $byte_len, $keep)",
                &params([
                    ("id", Value::Int64(id)),
                    ("byte_len", Value::Int64(id * 3 + 11)),
                    ("keep", Value::Int64(i64::from(id % 2 == 0))),
                ]),
            )
            .expect("store an oversized-table row");
    }
    database
        .execute("COMMIT", &empty())
        .expect("commit the oversized table");
    Arc::new(database)
}

/// What the rows the predicate keeps add up to, computed from the same
/// arithmetic that wrote them rather than from a constant.
fn expected_kept_sum() -> i64 {
    (0..BIG_ROWS)
        .filter(|id| id % 2 == 0)
        .map(|id| id * 3 + 11)
        .sum()
}

/// The pages of one cursor, each carried with the telemetry of the fetch that
/// produced it. An exhausted cursor reports an all-zero breakdown, so a page's
/// own figures are kept as the page arrives rather than read back afterwards.
struct DrainedCursor {
    pages: Vec<(CursorPage, TestTelemetry)>,
}

impl DrainedCursor {
    fn last(&self) -> &(CursorPage, TestTelemetry) {
        self.pages.last().expect("a cursor produces a page at open")
    }

    /// What one page charged against the ceiling the caller declared. This is
    /// the figure the ceiling governs, and it counts the steps a source takes
    /// over an item rather than the items themselves -- which is why the page
    /// count below is derived from an unpaged reading of the same statement
    /// instead of from rows divided by the ceiling.
    fn work_charged(&self, page: usize) -> u64 {
        self.pages[page].1.work_units
    }

    fn table_scan_work(&self, page: usize) -> u64 {
        self.pages[page]
            .1
            .source_work
            .get(&TestWorkSource::TableScan)
            .copied()
            .unwrap_or(0)
    }

    fn total_work_charged(&self) -> u64 {
        (0..self.pages.len())
            .map(|page| self.work_charged(page))
            .sum()
    }
}

/// The same statement read with nothing forcing it to page: the answer, the
/// rows it examined, and the work it charged.
///
/// The cursor is held to these MEASURED figures rather than to a page count
/// guessed from rows divided by the ceiling. What the ceiling governs is work,
/// and a source charges more than one unit per item, so a hand-derived page
/// count would pin today's charge model rather than the promise -- which is
/// that paging an answer costs a reader nothing beyond reading it in one go.
struct OneShotOracle {
    value: Value,
    rows_examined: u64,
    work_units: u64,
}

fn one_shot_oracle(database: &Arc<Database>, sql: &str) -> OneShotOracle {
    let outcome = bounded::execute(database, &request(sql, empty(), roomy_limits()))
        .unwrap_or_else(|error| {
            panic!("a bounded read with room to spare answers {sql}: {error:?}")
        });
    OneShotOracle {
        value: one_aggregate_row("a bounded read with room to spare", &outcome.result),
        rows_examined: outcome.result.trace.rows_examined,
        work_units: outcome.telemetry.work_units,
    }
}

/// Open the statement as a cursor and fetch until the store says there is no
/// more, keeping each page's own breakdown.
fn drain_cursor(
    database: &Arc<Database>,
    sql: &str,
    bound: HashMap<String, Value>,
) -> DrainedCursor {
    // A page that never settles must fail as a bounded assertion rather than
    // hang the suite; the ceiling is far above the pages the contract expects.
    let page_ceiling = usize::try_from(BIG_ROWS)
        .expect("the oversized fixture fits this platform")
        .div_ceil(usize::try_from(WORK_CEILING).expect("the work ceiling fits this platform"))
        * 4
        + 16;
    let opened = bounded::open_cursor(
        Arc::clone(database),
        &request(sql, bound, work_capped_limits()),
    )
    .unwrap_or_else(|error| panic!("a consumer must be able to open {sql} as a cursor: {error:?}"));
    let mut cursor = opened.cursor;
    let mut pages = vec![(opened.first_page, opened.telemetry)];
    while pages
        .last()
        .expect("a cursor produces a page at open")
        .0
        .has_more
    {
        assert!(
            pages.len() < page_ceiling,
            "the cursor over {sql} produced {} pages without ever reporting the answer complete",
            pages.len()
        );
        let fetched = cursor
            .fetch(None, OwnerReadCancellation::new())
            .unwrap_or_else(|error| {
                panic!(
                    "fetch {} of the cursor over {sql} must stay inside the declared ceilings \
                     rather than refuse -- paging is how a reader answers this without raising \
                     them; the pages before it charged {:?} against a ceiling of {WORK_CEILING} \
                     and carried {:?} rows: {error:?}",
                    pages.len() + 1,
                    pages.iter().map(|(_, t)| t.work_units).collect::<Vec<_>>(),
                    pages
                        .iter()
                        .map(|(page, _)| page.rows.len())
                        .collect::<Vec<_>>()
                )
            });
        pages.push((fetched.page, fetched.telemetry));
    }
    cursor.close().expect("close the drained cursor");
    DrainedCursor { pages }
}

/// The shared shape of both cursor totals: no page charges more than the
/// declared ceiling, no page before the last carries a row, the last page
/// carries the whole answer, and paging costs the reader nothing that reading
/// the same statement in one go would not have cost.
fn assert_paged_total(
    what: &str,
    drained: &DrainedCursor,
    expected: Value,
    oracle: &OneShotOracle,
) {
    let expected_pages = usize::try_from(oracle.work_units.div_ceil(WORK_CEILING))
        .expect("the page count fits this platform");
    println!(
        "OBSERVED {what}: {} pages; work per page {:?}; table-scan work per page {:?}; unpaged \
         read examined {} rows for {} units of work",
        drained.pages.len(),
        (0..drained.pages.len())
            .map(|page| drained.work_charged(page))
            .collect::<Vec<_>>(),
        (0..drained.pages.len())
            .map(|page| drained.table_scan_work(page))
            .collect::<Vec<_>>(),
        oracle.rows_examined,
        oracle.work_units
    );

    assert_eq!(
        oracle.value, expected,
        "reading {what} without paging must produce the same total, or the two doors are not \
         answering the same question"
    );

    for (index, (page, telemetry)) in drained.pages.iter().enumerate() {
        assert!(
            telemetry.work_units <= WORK_CEILING,
            "page {} of {what} charged {} units of work against a declared ceiling of \
             {WORK_CEILING}; a consumer that has to raise its ceiling to get a total is back to \
             the uncapped scan the ceiling exists to prevent",
            index + 1,
            telemetry.work_units
        );
        if index + 1 < drained.pages.len() {
            assert!(
                page.rows.is_empty(),
                "page {} of {what} is not the last, and a total is not a total until the input \
                 ends, so it must carry no rows; it carried {:?}",
                index + 1,
                page.rows
            );
            assert!(
                page.has_more,
                "page {} of {what} is not the last, so it must say there is more to fetch",
                index + 1
            );
        }
    }

    let (last_page, _) = drained.last();
    assert!(
        !last_page.has_more,
        "the page carrying the total of {what} is the end of the read"
    );
    assert_eq!(
        last_page.rows.len(),
        1,
        "{what} ends with exactly one row holding the whole answer, and ended with {:?}",
        last_page.rows
    );
    assert_eq!(
        last_page.rows[0],
        vec![expected],
        "{what} must report the total of every row its predicate keeps"
    );
    assert_eq!(
        oracle.rows_examined,
        u64::try_from(BIG_ROWS).expect("the oversized fixture fits the read vocabulary"),
        "an unpaged reading of {what} examines each of the {BIG_ROWS} stored rows once; a \
         different figure means the page comparison below is measured against the wrong baseline"
    );
    assert_eq!(
        drained.total_work_charged(),
        oracle.work_units,
        "{what} charged {} units of work across its pages where the same statement read in one \
         go charged {}; a reader that stays inside its declared ceilings must not be made to pay \
         for the same rows twice",
        drained.total_work_charged(),
        oracle.work_units
    );
    assert_eq!(
        drained.pages.len(),
        expected_pages,
        "{} units of work spent {WORK_CEILING} at a time is {expected_pages} fetches; {what} took \
         {}, so a page stopped short of the work its caller declared it could spend",
        oracle.work_units,
        drained.pages.len()
    );
}

/// A consumer with a thousand-item work ceiling can still count a table of
/// twelve thousand rows, by fetching the count as a cursor.
#[test]
fn count_cursor_completes_over_a_store_larger_than_the_work_ceiling() {
    let database = seed_big_table();
    let oracle = one_shot_oracle(&database, "SELECT COUNT(*) FROM big");
    let drained = drain_cursor(&database, "SELECT COUNT(*) FROM big", empty());
    assert_paged_total(
        "a count over a store larger than the work ceiling",
        &drained,
        Value::Int64(BIG_ROWS),
        &oracle,
    );
    assert!(
        drained.last().0.columns[0].eq_ignore_ascii_case("count"),
        "the paged count names its column after the aggregate that produced it, and named {:?}",
        drained.last().0.columns
    );
}

/// The same for a byte total whose predicate keeps half the rows: the answer
/// is the arithmetic sum of the rows kept, not of the rows examined.
#[test]
fn sum_cursor_completes_over_a_filtered_store_larger_than_the_work_ceiling() {
    let database = seed_big_table();
    let oracle = one_shot_oracle(&database, "SELECT SUM(byte_len) FROM big WHERE keep = 1");
    let drained = drain_cursor(
        &database,
        "SELECT SUM(byte_len) FROM big WHERE keep = 1",
        empty(),
    );
    assert_paged_total(
        "a byte total over the kept half of a store larger than the work ceiling",
        &drained,
        Value::Int64(expected_kept_sum()),
        &oracle,
    );
    assert!(
        drained.last().0.columns[0].eq_ignore_ascii_case("sum"),
        "the paged total names its column after the aggregate that produced it, and named {:?}",
        drained.last().0.columns
    );
}

// --- (d) the one-shot door is unchanged ------------------------------------

/// The one-shot door does not silently do the unbounded scan the cursor exists
/// to avoid. It refuses, in the typed vocabulary, naming the ceiling that
/// stopped it and the statement to change.
#[test]
fn one_shot_aggregate_over_the_work_ceiling_is_refused() {
    let database = seed_big_table();
    let session = database
        .read_session(work_capped_limits())
        .expect("open an in-process bounded view with a declared work ceiling");
    let refusal = session
        .execute("SELECT COUNT(*) FROM big", &empty())
        .expect_err("a one-shot count over a store larger than the work ceiling is refused");

    let Error::ReadFailure(failure) = &refusal else {
        panic!(
            "a read stopped by a declared ceiling must be the typed refusal a caller can branch \
             on, and was {refusal}"
        );
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "a ceiling refusal keeps the kind the read contract already publishes for it"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("a ceiling refusal carries the detail its kind promises: {failure:?}");
    };
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Work,
        "the refusal names the work ceiling, so a caller knows to page the answer rather than \
         widen an unrelated limit"
    );
    assert_eq!(
        detail.value, WORK_CEILING,
        "the refusal reports the ceiling the caller declared"
    );
    let statement = detail.statement.as_ref().unwrap_or_else(|| {
        panic!(
            "a work refusal names the statement part to change; without it an operator is told a \
             number and nothing they can act on: {detail:?}"
        )
    });
    assert!(
        statement.statement.contains("COUNT"),
        "the refusal names the statement it stopped, and named {:?}",
        statement.statement
    );
}

// --- (e) what SUM means ----------------------------------------------------

fn assert_plan_error(what: &str, result: contextdb_core::Result<QueryResult>) {
    match result {
        Ok(answer) => panic!("{what} must be refused, and answered {:?}", answer.rows),
        Err(Error::PlanError(_)) => {}
        Err(other) => panic!(
            "{what} must be refused as a plan error a caller can read, and failed with {other}"
        ),
    }
}

/// Adding up text is not a total; it is a statement the store cannot plan, and
/// saying so is the whole difference between a refused query and a wrong
/// number.
#[test]
fn sum_over_a_text_column_is_a_plan_error() {
    let directory = tempfile::TempDir::new().expect("task-scoped chunk directory");
    let path = directory.path().join("text-total.db");
    seed_chunks(&path);
    let database = Database::open(&path).expect("reopen the seeded store");

    assert_plan_error(
        "the executor asked to total a text column",
        database.execute("SELECT SUM(label) FROM chunks", &empty()),
    );
    assert_plan_error(
        "an in-process bounded read asked to total a text column",
        database
            .read_session(roomy_limits())
            .expect("open an in-process bounded view")
            .execute("SELECT SUM(label) FROM chunks", &empty()),
    );
}

/// A total that will not fit in the integer it is reported as is a failure the
/// caller hears about. Wrapping around would hand back a negative byte count
/// as though it were the answer.
#[test]
fn sum_of_ints_that_overflow_is_a_typed_failure() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE huge (id INTEGER PRIMARY KEY, n INTEGER)",
            &empty(),
        )
        .expect("create the overflow table");
    for id in 0..2 {
        database
            .execute(
                "INSERT INTO huge (id, n) VALUES ($id, $n)",
                &params([("id", Value::Int64(id)), ("n", Value::Int64(i64::MAX))]),
            )
            .expect("store a row at the integer ceiling");
    }

    for (what, result) in [
        (
            "the executor",
            database.execute("SELECT SUM(n) FROM huge", &empty()),
        ),
        (
            "an in-process bounded read",
            database
                .read_session(roomy_limits())
                .expect("open an in-process bounded view")
                .execute("SELECT SUM(n) FROM huge", &empty()),
        ),
    ] {
        match result {
            Ok(answer) => panic!(
                "{what} totalled two rows of the largest integer and answered {:?}; a total that \
                 wraps around reports a number that is not the sum",
                answer.rows
            ),
            Err(Error::PlanError(message)) => assert!(
                message.contains("out of range"),
                "{what} must refuse the overflowing total in the words the store already uses for \
                 an integer that will not fit, and said {message:?}"
            ),
            Err(other) => panic!(
                "{what} must refuse the overflowing total as a plan error a caller can read, and \
                 failed with {other}"
            ),
        }
    }
}

/// Nothing to add up is not zero bytes; it is no answer. Nothing to count is
/// zero rows. The two aggregates part company on an empty input, and a
/// consumer telling "no chunks recorded" from "chunks recorded, all empty"
/// depends on it.
#[test]
fn sum_of_an_empty_input_is_null_and_count_is_zero() {
    let directory = tempfile::TempDir::new().expect("task-scoped chunk directory");
    let path = directory.path().join("empty-total.db");
    seed_chunks(&path);
    let database = Database::open(&path).expect("reopen the seeded store");
    let absent = params([(
        "c",
        Value::Uuid(Uuid::from_u128(0x00C0_0000_0000_0000_0000_0000_0000_00FF)),
    )]);
    const SQL: &str = "SELECT SUM(byte_len), COUNT(*) FROM chunks WHERE context_id = $c";

    for (what, answer) in [
        (
            "the executor",
            database
                .execute(SQL, &absent)
                .expect("the executor answers a total over no rows"),
        ),
        (
            "an in-process bounded read",
            database
                .read_session(roomy_limits())
                .expect("open an in-process bounded view")
                .execute(SQL, &absent)
                .expect("an in-process bounded read answers a total over no rows"),
        ),
    ] {
        assert_eq!(
            answer.rows.len(),
            1,
            "{what} answers an aggregate over no rows with one row, and answered {:?}",
            answer.rows
        );
        assert_eq!(
            answer.rows[0],
            vec![Value::Null, Value::Int64(0)],
            "{what} must report no total and a count of none for a context that has no chunks"
        );
    }
}
