//! Telling tied indexes apart is read work, and it is paid for like any read.
//!
//! When several indexes bind equally many equality columns, the planner reads
//! each one's bound range in lockstep to choose the smaller. Those reads are
//! index entries the statement examines, so they draw on the same ceilings as
//! the scan that follows: the WORK limit, the active-time deadline and the
//! read memory limit. A read that answers must have been able to afford every
//! entry it examined, the probes' included; a read that cannot afford them is
//! refused typed, before it allocates what it was refused.
//!
//! The fixture has six indexes on `(a, bK)`. A WHERE binding `a` and every
//! `bK` ties all six; only `b1`'s range is small, so the lockstep probe reads
//! about six times that range while the scan reads it once.

use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    ReadLimits,
};
use contextdb_core::{Error, Value};
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering};
use uuid::Uuid;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

struct PeakTrackingAllocator;

fn note_growth(delta: isize) {
    let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakTrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_growth(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTrackingAllocator = PeakTrackingAllocator;

/// The ceiling a typed limit refusal names.
fn limit_of(failure: &ReadFailure) -> Option<ReadFailureLimit> {
    if failure.kind() == ReadFailureKind::OwnerLimitExceeded
        && let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone()
    {
        Some(detail.limit)
    } else {
        None
    }
}

const ROWS: i64 = 2_000;
/// Rows sharing one `b1` value: the smallest tied range.
const SMALL_RANGE: i64 = 20;
const TIED: usize = 6;

fn roomy() -> ReadLimits {
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

fn seeded(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the store");
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER NOT NULL, b1 INTEGER NOT NULL, \
         b2 INTEGER NOT NULL, b3 INTEGER NOT NULL, b4 INTEGER NOT NULL, b5 INTEGER NOT NULL, \
         b6 INTEGER NOT NULL, c2 INTEGER NOT NULL, c3 INTEGER NOT NULL, \
         c4 INTEGER NOT NULL, c5 INTEGER NOT NULL, c6 INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .expect("declare the table");
    for k in 1..=TIED {
        db.execute(
            &format!("CREATE INDEX i{k} ON t (a, b{k})"),
            &HashMap::new(),
        )
        .expect("declare a tied index");
    }
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(500) {
        let tx = db.begin().expect("seed transaction");
        for n in chunk {
            let mut row = HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(Uuid::from_u128(0x71ed << 64 | *n as u128)),
                ),
                ("a".to_string(), Value::Int64(1)),
                ("b1".to_string(), Value::Int64(n % (ROWS / SMALL_RANGE))),
            ]);
            for k in 2..=TIED {
                row.insert(format!("b{k}"), Value::Int64(0));
                row.insert(format!("c{k}"), Value::Int64(0));
            }
            db.insert_row(tx, "t", row).expect("seed one row");
        }
        db.commit(tx).expect("commit the seed chunk");
    }
    db
}

fn query() -> String {
    let mut sql = "SELECT id FROM t WHERE a = 1 AND b1 = 7".to_string();
    for k in 2..=TIED {
        sql.push_str(&format!(" AND b{k} = 0"));
    }
    sql
}

fn work_ceilings() -> impl Iterator<Item = u64> {
    (1..=40).map(|step| step * 10)
}

/// Every answered read examined no more entries than its WORK limit allows.
fn assert_work_bounds_every_answer(
    route: &str,
    run: &dyn Fn(ReadLimits) -> contextdb_core::Result<contextdb_engine::QueryResult>,
) {
    let mut answered_at = None;
    for work in work_ceilings() {
        let limits = ReadLimits { work, ..roomy() };
        match run(limits) {
            Ok(result) => {
                assert_eq!(result.rows.len(), SMALL_RANGE as usize);
                assert!(
                    result.trace.rows_examined <= work,
                    "{route}: answered under WORK {work} having examined {} entries",
                    result.trace.rows_examined
                );
                answered_at.get_or_insert(work);
            }
            Err(Error::ReadFailure(failure)) => {
                assert_eq!(
                    limit_of(&failure),
                    Some(ReadFailureLimit::Work),
                    "{route}: {failure:?}"
                );
            }
            Err(other) => panic!("{route}: a WORK ceiling refuses typed, not {other:?}"),
        }
    }
    assert!(answered_at.is_some(), "{route}: some ceiling answers");
}

#[test]
fn tied_index_probes_are_charged_against_the_work_limit_on_every_route() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("tied.db");
    let db = seeded(&path);
    assert_work_bounds_every_answer("writer", &|limits| {
        db.read_session(limits)?.execute(&query(), &HashMap::new())
    });
    db.close().expect("close the writer");
    assert_work_bounds_every_answer("file", &|limits| {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                limits,
                ..ReadSessionOptions::default()
            },
        )?
        .execute(&query(), &HashMap::new())
    });
}

/// A clock that moves on every look and counts the looks.
#[derive(Clone)]
struct CountingClock {
    now_ms: Arc<AtomicU64>,
}

impl DeadlineClock for CountingClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.fetch_add(1, Ordering::SeqCst)
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

#[test]
fn tied_index_probes_check_the_deadline_before_each_entry() {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = seeded(&root.path().join("tied.db"));
    let mut answered = false;
    for active_ms in (4..=16).map(|power| 1u64 << power) {
        let clock = CountingClock {
            now_ms: Arc::new(AtomicU64::new(0)),
        };
        let looks = clock.now_ms.clone();
        let limits = ReadLimits {
            active_ms,
            ..roomy()
        };
        let session = db
            .read_session_with_clock_for_test(limits, Arc::new(clock))
            .expect("open a bounded read");
        let opened = looks.load(Ordering::SeqCst);
        match session.execute(&query(), &HashMap::new()) {
            Ok(result) => {
                let looked = looks.load(Ordering::SeqCst) - opened;
                assert!(
                    looked >= result.trace.rows_examined,
                    "answered under a {active_ms} ms deadline having examined {} entries while \
                     consulting the clock {looked} times: some entries never checked it",
                    result.trace.rows_examined
                );
                answered = true;
            }
            Err(Error::ReadFailure(failure)) => {
                assert_eq!(
                    limit_of(&failure),
                    Some(ReadFailureLimit::ActiveMs),
                    "{failure:?}"
                );
            }
            Err(other) => panic!("a deadline refuses typed, not {other:?}"),
        }
    }
    assert!(answered, "some deadline answers");
}

/// Long IN-list bounds on every tied column make each probe's coerced pick
/// and expanded runs large. The baseline statement has the same lists on
/// unindexed columns, so it parses the same text and has one index
/// candidate. Under the smallest read memory limit the baseline answers in,
/// the tied statement is refused typed before its probes allocate, or grows
/// the process no more than the baseline did: probes that allocated their
/// runs unadmitted would add several megabytes.
#[test]
fn tied_index_probes_admit_their_memory_before_allocating() {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = seeded(&root.path().join("tied.db"));
    const SLACK: isize = 1024 * 1024;
    let list = (0..8_000i64)
        .map(|n| (n * 1_000_003).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let statement = |column: char| {
        let mut sql = format!("SELECT id FROM t WHERE a = 1 AND b1 IN ({list})");
        for k in 2..=TIED {
            sql.push_str(&format!(" AND {column}{k} IN ({list})"));
        }
        sql
    };
    let run = |sql: &str, memory: u64| {
        let session = db
            .read_session(ReadLimits { memory, ..roomy() })
            .expect("open a bounded read");
        let before = LIVE_BYTES.load(Ordering::Relaxed);
        PEAK_BYTES.store(before, Ordering::Relaxed);
        let outcome = session.execute(sql, &HashMap::new());
        let peak = PEAK_BYTES.load(Ordering::Relaxed) - before;
        (outcome, peak)
    };
    let typed_memory_refusal =
        |outcome: &contextdb_core::Result<contextdb_engine::QueryResult>| match outcome {
            Err(Error::ReadFailure(failure)) => limit_of(failure) == Some(ReadFailureLimit::Memory),
            Err(Error::MemoryBudgetExceeded { .. }) => true,
            _ => false,
        };
    let baseline = statement('c');
    let (memory, baseline_peak) = (16..=28)
        .map(|power| 1u64 << power)
        .find_map(|memory| match run(&baseline, memory) {
            (Ok(_), peak) => Some((memory, peak)),
            (outcome, _) => {
                assert!(
                    typed_memory_refusal(&outcome),
                    "the baseline refuses typed: {:?}",
                    outcome.err()
                );
                None
            }
        })
        .expect("the baseline answers under some memory limit");
    let (outcome, peak) = run(&statement('b'), memory);
    match &outcome {
        Ok(_) => assert!(
            peak <= baseline_peak + SLACK,
            "the tied statement answered under {memory} bytes having grown the process by \
             {peak} bytes against the baseline's {baseline_peak}: its probes allocated \
             unadmitted"
        ),
        refused => assert!(
            typed_memory_refusal(refused),
            "a memory ceiling refuses typed, not {:?}",
            refused.as_ref().err()
        ),
    }
    assert!(
        peak <= baseline_peak + SLACK,
        "the tied statement grew the process by {peak} bytes against the baseline's \
         {baseline_peak} under {memory} bytes, even though it was refused"
    );
}
