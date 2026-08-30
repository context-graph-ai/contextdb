#![cfg(feature = "test-seams")]
//! A cursor's memory ceiling covers the runs it has not reached yet.
//!
//! A predicate listing many values names one run of the index per value. A
//! cursor opened over it keeps the runs it still owes for as long as it stays
//! open -- that is real memory, held on the operator's behalf between fetches,
//! and the operator's declared read-memory ceiling is the only thing standing
//! between a long list and a process that grows without an answer.
//!
//! A cursor charged only for the values the predicate named, and not for the
//! run bounds it derived from them and is holding, is admitted under a ceiling
//! it is already over. The operator who set that ceiling has no other lever:
//! nothing else in the read contract bounds a paused cursor.
//!
//! So what an open cursor holds for a listed value it has not reached is
//! inside the ceiling that admitted it.

use contextdb_core::Value;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;

/// Bytes of listed value. Long enough that copies of it are unmistakable
/// against the fixed cost of a cursor.
const VALUE_BYTES: usize = 256;
/// Rows stored under the first listed value. More than one page, so a cursor
/// over either list is still open after its first page.
const STORED_MATCHES: i64 = 4;
/// Values the short list names.
const SHORT_LIST: usize = 2;
/// Values the long list names. Only the first names a stored row; the rest
/// are runs the cursor owes and never reaches a row through, so both lists
/// answer with exactly the same rows.
const LONG_LIST: usize = 2_048;
/// Runs the long list owes beyond what the short list owes.
const EXTRA_RUNS: usize = LONG_LIST - SHORT_LIST;
/// The index the reads below reach their rows through. Its second column is
/// what keeps a listed value from naming a whole key, so the read walks the
/// run each value names rather than probing the key.
const DECLARED_INDEX: &str = "listed_rows_key_idx";

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These reads are synchronous; the immediately-completing future
        // satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

fn limits_with_memory(memory: u64) -> ReadLimits {
    ReadLimits {
        result_rows: 4_096,
        result_bytes: 64 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 1_000_000,
        memory,
        cursor_page_rows: 1,
        cursor_page_bytes: 16 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn roomy() -> ReadLimits {
    limits_with_memory(512 * 1024 * 1024)
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// One listed value, padded to a fixed width so a list's byte size is
/// exactly the count times that width.
fn listed_value(ordinal: usize) -> String {
    let stem = format!("listed-key-{ordinal:08}");
    format!("{stem}{}", "-".repeat(VALUE_BYTES - stem.len()))
}

/// The one listed value that names a stored row.
fn stored_value() -> String {
    listed_value(0)
}

/// A table with a two-column index. The second column is why a listed value
/// names a run rather than a whole key.
fn listed_rows() -> Arc<Database> {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE listed_rows (id INTEGER PRIMARY KEY, k TEXT, secondary INTEGER, \
         payload TEXT)",
        &empty(),
    )
    .expect("create the listed-row table");
    db.execute(
        &format!("CREATE INDEX {DECLARED_INDEX} ON listed_rows(k, secondary)"),
        &empty(),
    )
    .expect("declare the two-column index the reads below walk");
    // Several rows under the FIRST listed value, so a one-row page leaves the
    // cursor open with rows still to deliver and every later run still owed.
    // A cursor that has already delivered everything holds nothing, and would
    // report an all-zero breakdown that no assertion could distinguish from a
    // settlement that left the runs out.
    for ordinal in 0..STORED_MATCHES {
        db.execute(
            "INSERT INTO listed_rows (id, k, secondary, payload) \
             VALUES ($id, $k, $secondary, 'matched row')",
            &[
                ("id".to_owned(), Value::Int64(ordinal)),
                ("k".to_owned(), Value::Text(stored_value())),
                ("secondary".to_owned(), Value::Int64(ordinal)),
            ]
            .into_iter()
            .collect(),
        )
        .expect("store a row the first listed value reaches");
    }
    Arc::new(db)
}

/// `SELECT ... WHERE k IN (...)` over `count` listed values.
fn in_list_sql(count: usize) -> String {
    let listed = (0..count)
        .map(|ordinal| format!("'{}'", listed_value(ordinal)))
        .collect::<Vec<_>>()
        .join(", ");
    // No ORDER BY: an ordered read walks the index for its ORDERING and keeps
    // the listed values as a residual filter, which is a different shape with
    // no runs to owe. The predicate alone is what makes each listed value name
    // a run.
    format!("SELECT id FROM listed_rows WHERE k IN ({listed})")
}

fn request(sql: &str, limits: ReadLimits) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, HashMap::new(), limits, Arc::new(FrozenClock))
}

/// The temporary memory the kernel charged this cursor for while opening it --
/// the figure the declared ceiling is compared against -- and the rows its
/// first page answered with.
fn charge_for_an_open_cursor(db: &Arc<Database>, sql: &str) -> (u64, usize) {
    let mut opened = bounded::open_cursor(Arc::clone(db), &request(sql, roomy()))
        .expect("a listed-value cursor must open under a roomy ceiling");
    let charged = opened.telemetry.peak_temporary_bytes;
    let rows = opened.first_page.rows.len();
    opened.cursor.close().expect("the cursor closes");
    (charged, rows)
}

/// Bytes the extra runs the long list owes are made of: each listed value the
/// cursor has not reached is held as both the start and the end of the run
/// that value names.
fn extra_run_bytes() -> u64 {
    u64::try_from(EXTRA_RUNS * VALUE_BYTES * 2).expect("fixture bytes fit a u64")
}

/// Fixed room for everything a cursor holds that is not a listed value.
const FIXED_SLACK: u64 = 64 * 1024;

#[test]
fn a_listed_value_read_walks_the_run_each_value_names() {
    let db = listed_rows();
    let outcome = bounded::execute(&db, &request(&in_list_sql(SHORT_LIST), roomy()))
        .expect("a listed-value read must be served");

    // Everything below is about a read that walks a run per listed value.
    // Proving the route first is what keeps the rest from reporting a clean
    // bill on a branch it never entered.
    assert_eq!(
        outcome.result.trace.physical_plan, "IndexScan",
        "a listed-value predicate is answered through the declared index"
    );
    assert_eq!(
        outcome.result.trace.index_used.as_deref(),
        Some(DECLARED_INDEX),
        "the read names the index that answered it"
    );
    assert_eq!(
        outcome.result.rows.len(),
        STORED_MATCHES as usize,
        "one listed value names every stored row, and the rest name nothing"
    );
}

#[test]
fn an_open_cursor_is_charged_for_every_run_it_still_owes() {
    let db = listed_rows();
    let (short_charge, short_rows) = charge_for_an_open_cursor(&db, &in_list_sql(SHORT_LIST));
    let (long_charge, long_rows) = charge_for_an_open_cursor(&db, &in_list_sql(LONG_LIST));
    println!(
        "OBSERVED charge: {SHORT_LIST} listed {short_charge} bytes, {LONG_LIST} listed \
         {long_charge} bytes, per listed value {}",
        (long_charge - short_charge) as f64 / EXTRA_RUNS as f64
    );

    assert_eq!(
        (short_rows, long_rows),
        (1, 1),
        "both lists answer with the same single row, so the only difference between them is \
         the runs the longer one still owes"
    );

    let grew_by = long_charge.saturating_sub(short_charge);
    let owed = extra_run_bytes();
    assert!(
        grew_by >= owed,
        "a cursor over {LONG_LIST} listed values was charged {long_charge} bytes against \
         {short_charge} for the same answer over {SHORT_LIST}, a growth of {grew_by} bytes; the \
         {EXTRA_RUNS} runs it has not reached are {owed} bytes of run bounds it is holding until \
         it is closed, so the ceiling that admitted it does not cover what it holds"
    );
}

#[test]
fn a_ceiling_that_does_not_cover_the_owed_runs_refuses_the_cursor() {
    let db = listed_rows();
    let (short_charge, _) = charge_for_an_open_cursor(&db, &in_list_sql(SHORT_LIST));
    let sql = in_list_sql(LONG_LIST);
    let owed = extra_run_bytes();

    // Generous room for the predicate's own copy of every listed value, and
    // none for what the cursor holds for the runs those values name.
    let too_tight = short_charge + owed + FIXED_SLACK;
    let refused = bounded::open_cursor(
        Arc::clone(&db),
        &request(&sql, limits_with_memory(too_tight)),
    )
    .err()
    .unwrap_or_else(|| {
        panic!(
            "a cursor owing {EXTRA_RUNS} runs of {owed} bytes opened under a \
                 {too_tight}-byte ceiling that leaves no room for them"
        )
    });
    let bounded::TestError::Refused(failure) = refused else {
        panic!("a crossed ceiling is a typed refusal, got {refused:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "a cursor that cannot be held inside the declared ceiling is refused for that ceiling: \
         {failure:?}"
    );
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone() else {
        panic!("the refusal carries its typed detail, got {failure:?}");
    };
    assert_eq!(
        detail.limit,
        ReadFailureLimit::Memory,
        "the crossed ceiling is the read's memory ceiling: {detail:?}"
    );
    assert_eq!(
        detail.value, too_tight,
        "the refusal names the ceiling that was crossed, not the bytes the cursor wanted"
    );

    // The complement: a ceiling the size of what this cursor is actually
    // charged serves it, so the refusal above is about the ceiling and not
    // about the length of the list. The size is measured rather than
    // predicted -- what a cursor holds is the engine's to say, and the pin is
    // about the ceiling covering it, not about its exact value.
    let (long_charge, _) = charge_for_an_open_cursor(&db, &sql);
    let generous = long_charge + FIXED_SLACK;
    let mut opened = bounded::open_cursor(
        Arc::clone(&db),
        &request(&sql, limits_with_memory(generous)),
    )
    .expect("a ceiling with room for the runs it owes serves the same cursor");
    assert_eq!(
        opened.first_page.rows.len(),
        1,
        "the served cursor answers with the one row a listed value names"
    );
    opened.cursor.close().expect("the cursor closes");
}

/// The parts an open cursor names must add up to the figure it is settled to.
///
/// The growth guard above cannot tell whether the run bounds specifically are
/// inside the settlement -- a cursor over many listed values is charged for
/// several things that grow with the list, and their sum clears any bar drawn
/// from the run bounds alone. This is the question asked directly: the cursor
/// says what it holds part by part, and a part it holds that is outside the
/// total is a part the operator's ceiling never sees.
#[test]
fn an_open_cursors_settlement_includes_the_runs_it_still_owes() {
    let db = listed_rows();
    let mut opened =
        bounded::open_cursor(Arc::clone(&db), &request(&in_list_sql(LONG_LIST), roomy()))
            .expect("a listed-value cursor must open under a roomy ceiling");
    let held = opened
        .cursor
        .continuation_bytes()
        .expect("an open cursor reports what it is holding");
    opened.cursor.close().expect("the cursor closes");
    println!(
        "OBSERVED continuation: total {} named {:?} unnamed {}",
        held.total, held.named, held.unnamed
    );

    // The cursor really is holding the runs it has not reached, and it names
    // them -- otherwise the identity below would hold vacuously.
    let owed = extra_run_bytes();
    let pending =
        u64::try_from(held.part(bounded::PENDING_INDEX_RUNS)).expect("reported bytes fit a u64");
    assert!(
        pending >= owed,
        "an open cursor over {LONG_LIST} listed values reports {pending} bytes held for the \
         runs it has not reached; the {EXTRA_RUNS} it still owes are {owed} bytes of run bounds \
         it cannot answer another fetch without"
    );

    let named: usize = held.named.iter().map(|(_, bytes)| bytes).sum();
    assert_eq!(
        named + held.unnamed,
        held.total,
        "the parts an open cursor names add up to the figure its ceiling is compared against; \
         {named} bytes named and {} unnamed against a total of {}, so {} bytes of what this \
         cursor holds are outside the figure it is settled to",
        held.unnamed,
        held.total,
        (named + held.unnamed).saturating_sub(held.total)
    );
}
