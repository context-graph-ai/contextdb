#![allow(dead_code)] // Existing compatibility and test seam shape.
//! A retained ordered run is physical work: building it cannot pass a caller's
//! work ceiling, and a cursor whose working window is evicted continues from
//! the posting it last proved.
//!
//! The contract these tests hold the route to is *boundedness*, not a
//! particular rebuild. A reader walking an authenticated index range keeps only
//! a bounded, admitted working window of proven postings; when that window is
//! evicted it re-proves one window from its last proven key, never the range.
//! So: a result-limited read succeeds when the whole matching range does not
//! fit the budget, total physical proof and index work grows linearly with the
//! range under sustained eviction, and interleaved cursors evicting each other
//! still each receive their whole range exactly once.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadFailureDetail, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectFileReaderError, DirectReaderConfig, ReadWorkCounters, ReadWorkObservationScope,
    ReadWorkPhase, arm_verified_ordered_run_eviction_for_test, open_for_test,
};
use contextdb_engine::persistence::read_persistence_test_scaffold::take_verified_ordered_run_capacity_displacements_for_test;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 16,
        result_bytes: 1024 * 1024,
        work: 1_000,
        active_ms: 1_000,
        memory: 8 * 1024 * 1024,
        cursor_page_rows: 16,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 1_000,
        cursor_lifetime_ms: 10_000,
    }
}

fn params() -> HashMap<String, Value> {
    HashMap::from([("query".to_owned(), Value::Vector(vec![1.0, 0.0]))])
}

fn statement() -> &'static str {
    "SELECT id FROM verified_runs WHERE bucket = 1 ORDER BY embedding <=> $query LIMIT 5"
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("create durable verified-run fixture");
    db.execute(
        "CREATE TABLE verified_runs (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    )
    .expect("create vector candidate fixture");
    db.execute(
        "CREATE INDEX verified_runs_bucket ON verified_runs (bucket)",
        &HashMap::new(),
    )
    .expect("create retained candidate index");
    for id in 0..5_i64 {
        db.execute(
            "INSERT INTO verified_runs VALUES ($id, 1, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                (
                    "embedding".to_owned(),
                    Value::Vector(vec![1.0 - id as f32 / 10.0, id as f32 / 10.0]),
                ),
            ]),
        )
        .expect("insert retained candidate posting");
    }
    db.close()
        .expect("close durable fixture before direct read");
}

fn open(
    path: &std::path::Path,
    limits: ReadLimits,
) -> contextdb_engine::direct_file_reader::test_seams::DirectFileReader {
    open_for_test(path, DirectReaderConfig::new(limits, Arc::new(FrozenClock)))
        .expect("open retained direct reader")
}

/// A reader that declares a session memory limit, which is what gives the
/// authenticated run cache a real capacity to run out of. Without a declared
/// limit the cache's share is the unbounded default and no eviction pressure
/// exists to measure.
fn open_declaring_memory(
    path: &std::path::Path,
    limits: ReadLimits,
    memory_limit: usize,
) -> contextdb_engine::direct_file_reader::test_seams::DirectFileReader {
    let mut config = DirectReaderConfig::new(limits, Arc::new(FrozenClock));
    config.memory_limit = Some(memory_limit);
    open_for_test(path, config).expect("open retained direct reader")
}

/// The session memory these measurements declare. The run cache's share of it
/// is `limit / 16`, so the whole matching range of the wide fixtures below is
/// several times what one build may hold.
const DECLARED_SESSION_MEMORY: usize = 32 * 1024 * 1024;

fn ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("expected one integer id, got {other:?}"),
        })
        .collect()
}

#[test]
fn verified_ordered_runs_charge_cold_builds_and_continue_after_forced_eviction() {
    let root = tempfile::tempdir().expect("task-scoped direct fixture");
    let path = root.path().join("verified-runs.redb");
    seed(&path);

    let cold = open(&path, limits());
    let cold_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let expected = cold
        .execute(statement(), &params())
        .expect("cold direct vector query completes");
    let cold_work = cold_scope.counters();
    drop(cold_scope);
    assert!(
        cold_work.decoded_rows >= 5,
        "the cold retained build performs authenticated physical row work: {cold_work:?}"
    );
    assert_eq!(ids(&expected.result), vec![0, 1, 2, 3, 4]);
    let warm_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let warm = cold
        .execute(statement(), &params())
        .expect("cached direct vector query completes");
    let warm_work = warm_scope.counters();
    drop(warm_scope);
    assert_eq!(warm.result.rows, expected.result.rows);
    assert_eq!(
        cold_work.decoded_rows,
        expected.result.rows.len() as u64,
        "the cold range proof does not decode posting row bodies: {cold_work:?}"
    );
    assert_eq!(warm_work.decoded_rows, cold_work.decoded_rows);
    assert!(
        warm_work.hashed_bytes < cold_work.hashed_bytes,
        "the cached run reuses authenticated posting pages: cold={cold_work:?}, warm={warm_work:?}"
    );
    assert!(
        warm.result.trace.rows_examined >= 5,
        "cached postings still count as examined work"
    );
    drop(cold);

    let mut tiny_limits = limits();
    tiny_limits.work = 1;
    let tiny = open(&path, tiny_limits);
    let tiny_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let refused = match tiny.execute(statement(), &params()) {
        Ok(_) => panic!("one work unit cannot build the five-posting retained run"),
        Err(error) => error,
    };
    let tiny_work = tiny_scope.counters();
    drop(tiny_scope);
    let DirectFileReaderError::ReadFailure(refusal) = refused else {
        panic!("tiny cold build must report the existing typed work refusal");
    };
    assert!(matches!(
        refusal.detail(),
        ReadFailureDetail::OwnerLimitExceeded(detail) if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Work
    ));
    assert!(
        tiny_work.decoded_rows < cold_work.decoded_rows,
        "the tiny limit stops the cold build before it reads the whole run: tiny={tiny_work:?}, cold={cold_work:?}"
    );
    drop(tiny);

    // Sustained eviction: the run cache is emptied after *every* delivered
    // cached posting, so the cursor has to continue from its last proved key
    // five times over.
    let resumed = open(&path, limits());
    arm_verified_ordered_run_eviction_for_test(&resumed, 1);
    let resumed_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let actual = resumed
        .execute(statement(), &params())
        .expect("the forced cache eviction continues through the retained source");
    let resumed_work = resumed_scope.counters();
    drop(resumed_scope);
    arm_verified_ordered_run_eviction_for_test(&resumed, 0);
    assert_eq!(actual.result.rows, expected.result.rows);
    assert_eq!(ids(&actual.result), vec![0, 1, 2, 3, 4]);
    // Each identity is delivered exactly once, from the authenticated walk, and
    // never from the physical index: the resume decodes no row body the cold
    // build did not.
    assert_eq!(
        resumed_work.decoded_rows, cold_work.decoded_rows,
        "an authenticated continuation decodes no row body the cold build did not: \
         cold={cold_work:?}, resumed={resumed_work:?}"
    );
    // Continuing is not free - it re-proves the window it lost - but it is
    // bounded: continuing from the last proved key four times over must not
    // cost anything like four whole cold builds. A continuation that rebuilt
    // the range from its start would.
    assert!(
        resumed_work.hashed_bytes < cold_work.hashed_bytes.saturating_mul(4),
        "a continuation re-proves a bounded window, not the range: \
         cold={cold_work:?}, resumed={resumed_work:?}"
    );
    assert!(
        warm_work.hashed_bytes < cold_work.hashed_bytes,
        "the cached repeat is the cheap case this is measured against: \
         cold={cold_work:?}, warm={warm_work:?}"
    );

    // The extra work is charged against the caller's ceiling, not taken for
    // free. The ceiling is not a number this test picks: it is the smallest one
    // a cold build fits in, found here, and the same ceiling must then refuse a
    // read that also has to re-prove after eviction.
    let smallest_ceiling = |evicted: bool| -> Option<u64> {
        let mut ceiling = 1;
        while ceiling <= limits().work {
            let mut probe = limits();
            probe.work = ceiling;
            let reader = open(&path, probe);
            if evicted {
                arm_verified_ordered_run_eviction_for_test(&reader, 1);
            }
            let answer = reader.execute(statement(), &params());
            arm_verified_ordered_run_eviction_for_test(&reader, 0);
            if let Ok(answer) = answer {
                assert_eq!(ids(&answer.result), vec![0, 1, 2, 3, 4]);
                return Some(ceiling);
            }
            ceiling *= 2;
        }
        None
    };
    let cold_ceiling = smallest_ceiling(false).expect("a cold build fits some work ceiling");
    let resumed_ceiling = smallest_ceiling(true).expect("a continuing read fits some work ceiling");
    assert!(
        resumed_ceiling > cold_ceiling,
        "re-proving a window after eviction costs charged work the cold build did not: \
         cold_ceiling={cold_ceiling}, resumed_ceiling={resumed_ceiling}"
    );

    let mut at_cold_ceiling = limits();
    at_cold_ceiling.work = cold_ceiling;
    let starved = open(&path, at_cold_ceiling);
    arm_verified_ordered_run_eviction_for_test(&starved, 1);
    let refused = starved
        .execute(statement(), &params())
        .err()
        .expect("the ceiling that fits one build cannot also pay for a re-proof");
    arm_verified_ordered_run_eviction_for_test(&starved, 0);
    let DirectFileReaderError::ReadFailure(refusal) = refused else {
        panic!("a starved continuation reports the existing typed work refusal");
    };
    assert!(matches!(
        refusal.detail(),
        ReadFailureDetail::OwnerLimitExceeded(detail)
            if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Work
    ));

    eprintln!(
        "verified run work: cold={cold_work:?} warm={warm_work:?} resumed={resumed_work:?}; \
         examined cold={} warm={} resumed={}; work ceiling cold={cold_ceiling} \
         resumed={resumed_ceiling}",
        expected.result.trace.rows_examined,
        warm.result.trace.rows_examined,
        actual.result.trace.rows_examined
    );
}

// ---------------------------------------------------------------------------
// Boundedness: the working window, not the range
// ---------------------------------------------------------------------------

/// Index keys wide enough that a working window holds a countable number of
/// them, so "the window is bounded" is a measurement rather than an assertion
/// about a constant. Every row carries the same label, so one equality
/// predicate names one run of `rows` postings.
const WIDE_LABEL_BYTES: usize = 8 * 1024;

/// A narrower key for the cursor fixture below. It still makes a working window
/// a countable number of postings while keeping each delivered row small.
const CURSOR_LABEL_BYTES: usize = 1024;

fn wide_label() -> String {
    "w".repeat(WIDE_LABEL_BYTES)
}

fn seed_wide(path: &std::path::Path, rows: i64) {
    seed_labelled(path, rows, WIDE_LABEL_BYTES, 'w', 0);
}

/// One run per label character, `rows` rows each, ids numbered consecutively so
/// each run's answer is a distinct contiguous block.
fn seed_wide_groups(path: &std::path::Path, rows: i64, groups: &[char]) {
    for (index, group) in groups.iter().enumerate() {
        seed_labelled(path, rows, WIDE_LABEL_BYTES, *group, index as i64 * rows);
    }
}

fn group_params(group: char) -> HashMap<String, Value> {
    HashMap::from([
        ("query".to_owned(), Value::Vector(vec![1.0, 0.0])),
        (
            "label".to_owned(),
            Value::Text(group.to_string().repeat(WIDE_LABEL_BYTES)),
        ),
    ])
}

fn seed_labelled(path: &std::path::Path, rows: i64, label_bytes: usize, fill: char, first_id: i64) {
    let db = Database::open(path).expect("create durable wide-run fixture");
    let _ = db.execute(
        "CREATE TABLE wide_runs (id INTEGER PRIMARY KEY, label TEXT NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    );
    let _ = db.execute(
        "CREATE INDEX wide_runs_label ON wide_runs (label)",
        &HashMap::new(),
    );
    let label = fill.to_string().repeat(label_bytes);
    for id in first_id..first_id + rows {
        db.execute(
            "INSERT INTO wide_runs VALUES ($id, $label, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("label".to_owned(), Value::Text(label.clone())),
                (
                    "embedding".to_owned(),
                    Value::Vector(vec![1.0 - id as f32 / 4096.0, id as f32 / 4096.0]),
                ),
            ]),
        )
        .expect("insert wide candidate posting");
    }
    db.close().expect("close wide fixture before direct read");
}

fn wide_params() -> HashMap<String, Value> {
    group_params('w')
}

fn labelled_params(label_bytes: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("query".to_owned(), Value::Vector(vec![1.0, 0.0])),
        ("label".to_owned(), Value::Text("w".repeat(label_bytes))),
    ])
}

fn wide_statement(limit: usize) -> String {
    format!(
        "SELECT id FROM wide_runs WHERE label = $label ORDER BY embedding <=> $query LIMIT {limit}"
    )
}

fn wide_limits(rows: usize) -> ReadLimits {
    ReadLimits {
        result_rows: (rows as u64).max(16),
        result_bytes: 64 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 600_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 16,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 600_000,
    }
}

/// A cursor page carries no per-page query trace, so the paged route is
/// measured by the authenticated-page counters alone.
#[derive(Debug, Clone, Copy)]
struct PagedMeasurement {
    rows: usize,
    counters: ReadWorkCounters,
}

#[derive(Debug, Clone, Copy)]
struct ScaleMeasurement {
    rows: usize,
    counters: ReadWorkCounters,
    examined: u64,
}

/// Walk the whole range with the run cache emptied after every delivered
/// posting: the harshest sustained eviction there is. What is measured is the
/// authenticated page bytes the proof walk hashed and the physical source
/// touches the read charged for.
fn measure_scale(rows: usize) -> ScaleMeasurement {
    let root = tempfile::tempdir().expect("task-scoped wide fixture");
    let path = root.path().join(format!("wide-{rows}.redb"));
    seed_wide(&path, rows as i64);
    let reader = open_declaring_memory(&path, wide_limits(rows), DECLARED_SESSION_MEMORY);
    arm_verified_ordered_run_eviction_for_test(&reader, 1);
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = reader
        .execute(&wide_statement(rows), &wide_params())
        .expect("the whole range is delivered under sustained eviction");
    let counters = scope.counters();
    drop(scope);
    arm_verified_ordered_run_eviction_for_test(&reader, 0);
    assert_eq!(
        answer.result.rows.len(),
        rows,
        "every row of the range is delivered exactly once"
    );
    let mut delivered = ids(&answer.result);
    delivered.sort_unstable();
    assert_eq!(
        delivered,
        (0..rows as i64).collect::<Vec<_>>(),
        "no identity is dropped or repeated across window boundaries"
    );
    ScaleMeasurement {
        rows,
        counters,
        examined: answer.result.trace.rows_examined,
    }
}

/// Physical proof and index work over an authenticated range must grow with the
/// range, not with its square, under real eviction. A route that re-proves the
/// whole remaining range on every eviction doubles its work when the range
/// doubles *and* pays that cost once per row: four times the work for twice the
/// range. A bounded working window pays a fixed re-proof per eviction, so twice
/// the range is about twice the work.
#[test]
fn authenticated_range_work_grows_linearly_with_the_range_under_sustained_eviction() {
    let small = measure_scale(128);
    let large = measure_scale(256);

    let hashed_ratio =
        large.counters.hashed_bytes as f64 / small.counters.hashed_bytes.max(1) as f64;
    let examined_ratio = large.examined as f64 / small.examined.max(1) as f64;
    eprintln!(
        "bounded window scaling: small={small:?} large={large:?} \
         hashed_ratio={hashed_ratio:.2} examined_ratio={examined_ratio:.2}"
    );

    // Quadratic growth for a doubled range is a factor of four. Linear growth
    // is a factor of two. The midpoint separates them with room for the fixed
    // per-window descent that a bounded window legitimately repeats.
    assert!(
        hashed_ratio < 3.0,
        "authenticated page work grows with the range, not with its square: \
         small={small:?} large={large:?} ratio={hashed_ratio:.2}"
    );
    assert!(
        examined_ratio < 3.0,
        "charged physical source work grows with the range, not with its square: \
         small={small:?} large={large:?} ratio={examined_ratio:.2}"
    );
    // And the work really is proportional to the range: a route that stopped
    // proving would not grow at all.
    assert!(
        hashed_ratio > 1.5,
        "twice the range still costs about twice the authenticated work: ratio={hashed_ratio:.2}"
    );
}

/// The promise this repair exists for: a result-limited read must succeed when
/// the whole matching range cannot be held inside the declared budget, provided
/// one working window and the result can be. Before, the route materialised
/// every authenticated posting of the range before returning one row and
/// refused when they did not all fit.
#[test]
fn a_result_limited_read_succeeds_when_the_whole_matching_range_exceeds_the_budget() {
    let rows = 256usize;
    let root = tempfile::tempdir().expect("task-scoped wide fixture");
    let path = root.path().join("wide-limited.redb");
    seed_wide(&path, rows as i64);

    // The whole range's authenticated postings are about `rows * 8 KiB`; this
    // session's run-cache share of its declared memory is a fraction of that.
    let limits = wide_limits(16);
    let reader = open_declaring_memory(&path, limits, DECLARED_SESSION_MEMORY);
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = reader
        .execute(&wide_statement(3), &wide_params())
        .expect("a result-limited read is answered from a bounded working window");
    let counters = scope.counters();
    drop(scope);
    assert_eq!(answer.result.rows.len(), 3);
    eprintln!(
        "result-limited read over {rows} wide postings: {counters:?} examined={}",
        answer.result.trace.rows_examined
    );
    // The answer is authenticated, so it costs proof work, and it costs far
    // less than proving the whole range would.
    assert!(
        counters.hashed_bytes > 0,
        "the delivered identities are authenticated: {counters:?}"
    );
}

/// Two PAGED CURSORS sharing one reader share one authenticated run cache. They
/// walk DIFFERENT runs, so their working windows are different cache entries
/// and, with a declared session memory whose cache share is one build
/// reservation, each cursor's next window evicts the other's. The two cursors
/// are opened and drained at the same time, so their range walks genuinely
/// overlap and every continuation meets a REAL capacity eviction - no test
/// seam. Both must still receive their whole range, in order, exactly once,
/// for work that grows with the range rather than with its square.
///
/// Session memory whose cache share (`limit / 16`) holds one working window
/// but not two, so two live paged cursors displace each other by capacity.
const PAGED_CONTENTION_SESSION_MEMORY: usize = 16 * 1024 * 1024;

/// When true, the contended path drains one cursor completely before opening
/// the other. That is the sequential execution the displacement assertion
/// must reject; keep it false.
const FORCE_PAGED_DRAINS_SEQUENTIAL: bool = false;

/// Returns the interleaved measurement and, beside it, the same two cursors
/// drained one after the other as the undisturbed control, plus the number of
/// capacity displacements observed while both cursors were live.
fn measure_paged_scale(rows: usize) -> (PagedMeasurement, PagedMeasurement, usize) {
    let root = tempfile::tempdir().expect("task-scoped wide fixture");
    let path = root.path().join(format!("paged-{rows}.redb"));
    seed_wide_groups(&path, rows as i64, &['a', 'b']);

    let mut limits = wide_limits(rows);
    limits.cursor_page_rows = 8;
    let reader = open_declaring_memory(&path, limits, PAGED_CONTENTION_SESSION_MEMORY);
    let page = NonZeroUsize::new(8).expect("a non-zero page");
    let expected =
        |group: usize| ((group * rows) as i64..((group + 1) * rows) as i64).collect::<Vec<_>>();
    let check = |group: usize, ids: &[i64], what: &str| {
        assert_eq!(
            ids.len(),
            rows,
            "{what} cursor {group} delivered {} rows, not {rows}: nothing dropped, nothing twice",
            ids.len()
        );
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            expected(group),
            "{what} cursor {group} receives exactly its range, each id once"
        );
    };

    let drain_group = |group: char| {
        let mut opened = reader
            .open_cursor(&wide_statement(rows), &group_params(group), page)
            .expect("open a paged cursor");
        let ids = drain_cursor(&mut opened, page);
        opened.cursor.close().expect("a drained cursor closes");
        ids
    };

    // Control: the same two cursors, one after the other. Neither is ever
    // part-way through its range while the other builds.
    let control_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    for (group, fill) in ['a', 'b'].into_iter().enumerate() {
        check(group, &drain_group(fill), "sequential");
    }
    let control = control_scope.counters();
    drop(control_scope);

    let _ = take_verified_ordered_run_capacity_displacements_for_test();
    let observed = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let (delivered, displacements) = if FORCE_PAGED_DRAINS_SEQUENTIAL {
        // Sequential drains never occupy two windows at once, so overlap
        // displacement is zero even if each drain self-evicts.
        let ids = ['a', 'b'].map(drain_group);
        (ids, 0)
    } else {
        // Both cursors open and consume one page before either continues, then
        // subsequent fetches alternate. Displacement is counted only while both
        // are live.
        let mut first = reader
            .open_cursor(&wide_statement(rows), &group_params('a'), page)
            .expect("open the first paged cursor");
        let mut second = reader
            .open_cursor(&wide_statement(rows), &group_params('b'), page)
            .expect("open the second paged cursor");
        let mut first_ids = cursor_page_ids(&first.first_page.page.rows);
        let mut second_ids = cursor_page_ids(&second.first_page.page.rows);
        let mut first_more = first.first_page.page.has_more;
        let mut second_more = second.first_page.page.has_more;
        while first_more || second_more {
            if first_more {
                let fetched = first
                    .cursor
                    .fetch(Some(page))
                    .expect("the first paged cursor fetches its next page");
                first_ids.extend(cursor_page_ids(&fetched.page.rows));
                first_more = fetched.page.has_more;
            }
            if second_more {
                let fetched = second
                    .cursor
                    .fetch(Some(page))
                    .expect("the second paged cursor fetches its next page");
                second_ids.extend(cursor_page_ids(&fetched.page.rows));
                second_more = fetched.page.has_more;
            }
        }
        first.cursor.close().expect("the first paged cursor closes");
        second
            .cursor
            .close()
            .expect("the second paged cursor closes");
        let overlap = take_verified_ordered_run_capacity_displacements_for_test();
        ([first_ids, second_ids], overlap)
    };
    let walk = observed.counters();
    drop(observed);
    for (group, ids) in delivered.iter().enumerate() {
        check(group, ids, "interleaved");
    }
    (
        PagedMeasurement {
            rows,
            counters: control,
        },
        PagedMeasurement {
            rows,
            counters: walk,
        },
        displacements,
    )
}

#[test]
fn interleaved_paged_cursors_under_real_cache_capacity_eviction_stay_complete_and_linear() {
    // Both scales have to be genuinely contended for the pair to measure what
    // contention costs. A build admits what it needs now rather than a fixed
    // multiple of it, so the smaller scale has to be large enough that two
    // interleaved cursors really do displace each other's windows.
    let (small_control, small, small_displacements) = measure_paged_scale(128);
    let (large_control, large, large_displacements) = measure_paged_scale(256);
    let hashed_ratio =
        large.counters.hashed_bytes as f64 / small.counters.hashed_bytes.max(1) as f64;
    let control_ratio = large_control.counters.hashed_bytes as f64
        / small_control.counters.hashed_bytes.max(1) as f64;
    eprintln!(
        "paged-cursor capacity eviction: small_control={small_control:?} small={small:?} \
         large_control={large_control:?} large={large:?} hashed_ratio={hashed_ratio:.2} \
         control_ratio={control_ratio:.2} small_displacements={small_displacements} \
         large_displacements={large_displacements}"
    );
    assert!(
        small_displacements >= 1,
        "128-row contended cursors must displace at least one cached window by capacity: \
         displacements={small_displacements}"
    );
    assert!(
        large_displacements >= 1,
        "256-row contended cursors must displace at least one cached window by capacity: \
         displacements={large_displacements}"
    );
    assert!(
        control_ratio > 1.5 && control_ratio < 3.0,
        "undisturbed sequential drains still grow with the range: control_ratio={control_ratio:.2}"
    );
    assert!(
        hashed_ratio < 3.0,
        "interleaved paged-cursor authenticated work grows with the range, not with its square: \
         small={small:?} large={large:?} ratio={hashed_ratio:.2}"
    );
    assert!(
        hashed_ratio > 1.5,
        "twice the range still costs about twice the authenticated work: ratio={hashed_ratio:.2}"
    );
    // Contention costs bounded extra work, not unbounded: two cursors evicting
    // each other stay inside a small multiple of two undisturbed drains. A
    // route that re-proved a range from its start whenever its window was taken
    // could not.
    for (control, contended) in [(&small_control, &small), (&large_control, &large)] {
        assert!(
            contended.counters.hashed_bytes <= control.counters.hashed_bytes.saturating_mul(2),
            "contending paged cursors stay inside a bounded multiple of undisturbed ones: \
             control={control:?} contended={contended:?}"
        );
    }
}

/// Two live cursors over the same reader share one authenticated run cache.
/// With its contents emptied after every delivered posting, each cursor's range
/// walk has to continue from the key it last proved, repeatedly, while the
/// other cursor is alive beside it - and both must still receive their whole
/// range, exactly once. Real capacity eviction between two range walks is
/// measured separately, above, by two concurrent statements.
///
/// The fixture is deliberately small and narrow: what it is holding to account
/// is the continuation across an eviction, not the size of the range, and the
/// wide, larger ranges are covered by the paged-cursor tests above.
fn seed_narrow_runs(path: &std::path::Path, rows: i64, buckets: i64) {
    let db = Database::open(path).expect("create durable narrow-run fixture");
    db.execute(
        "CREATE TABLE narrow_runs (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    )
    .expect("create narrow candidate fixture");
    db.execute(
        "CREATE INDEX narrow_runs_bucket ON narrow_runs (bucket)",
        &HashMap::new(),
    )
    .expect("create narrow candidate index");
    for bucket in 0..buckets {
        for id in 0..rows {
            let id = bucket * rows + id;
            db.execute(
                "INSERT INTO narrow_runs VALUES ($id, $bucket, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("bucket".to_owned(), Value::Int64(bucket)),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vec![1.0 - id as f32 / 4096.0, id as f32 / 4096.0]),
                    ),
                ]),
            )
            .expect("insert narrow candidate posting");
        }
    }
    db.close().expect("close narrow fixture before direct read");
}

fn narrow_params(bucket: i64) -> HashMap<String, Value> {
    HashMap::from([
        ("query".to_owned(), Value::Vector(vec![1.0, 0.0])),
        ("bucket".to_owned(), Value::Int64(bucket)),
    ])
}

fn narrow_statement(limit: usize) -> String {
    format!(
        "SELECT id FROM narrow_runs WHERE bucket = $bucket ORDER BY embedding <=> $query LIMIT {limit}"
    )
}

#[test]
fn interleaved_cursors_continue_correctly_when_their_windows_are_evicted() {
    // Small, because of the pre-existing cursor defect named above: a vector
    // candidate set beyond a few dozen rows trips the executor's own cursor
    // memory accounting, which this repair does not touch.
    let rows = 32usize;
    let root = tempfile::tempdir().expect("task-scoped cursor fixture");
    let path = root.path().join("interleaved-cursors.redb");
    seed_narrow_runs(&path, rows as i64, 2);

    let mut limits = wide_limits(rows);
    limits.cursor_page_rows = 8;
    let reader = open_declaring_memory(&path, limits, DECLARED_SESSION_MEMORY);
    let page = NonZeroUsize::new(8).expect("a non-zero page");

    let drain =
        |cursors: &mut Vec<contextdb_engine::direct_file_reader::test_seams::DirectCursorOpen>| {
            let mut collected = cursors
                .iter()
                .map(|opened| {
                    opened
                        .first_page
                        .page
                        .try_clone(
                            reader
                                .request_read_memory_for_test()
                                .expect("admit the fixture page copy reader"),
                        )
                        .expect("share the admitted first page")
                        .into_public()
                        .rows
                })
                .collect::<Vec<_>>();
            let mut more = cursors
                .iter()
                .map(|opened| opened.first_page.page.has_more)
                .collect::<Vec<_>>();
            while more.iter().any(|has_more| *has_more) {
                for (index, opened) in cursors.iter_mut().enumerate() {
                    if !more[index] {
                        continue;
                    }
                    let fetched = opened
                        .cursor
                        .fetch(Some(page))
                        .expect("a cursor continues after its window is evicted");
                    let terminal = fetched.page.into_public();
                    collected[index].extend(terminal.rows);
                    more[index] = terminal.has_more;
                }
            }
            collected
        };

    let open_interleaved = || {
        vec![
            reader
                .open_cursor(&narrow_statement(rows), &narrow_params(0), page)
                .expect("open the first interleaved cursor"),
            reader
                .open_cursor(&narrow_statement(rows), &narrow_params(1), page)
                .expect("open the second interleaved cursor"),
        ]
    };

    // Control: the same two interleaved cursors with nothing evicting their
    // windows. Comparing like with like - the reader is already hydrated and
    // both runs are walked - so the difference below is the continuation and
    // nothing else.
    let control_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let mut undisturbed = open_interleaved();
    let undisturbed_rows = drain(&mut undisturbed);
    let control = control_scope.counters();
    drop(control_scope);
    drop(undisturbed);

    // The two cursors now share the cache while its contents are emptied after
    // every delivered posting, so each range walk continues from its last
    // proved key over and over while the other cursor is alive beside it.
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    arm_verified_ordered_run_eviction_for_test(&reader, 1);
    let mut interleaved = open_interleaved();
    let interleaved_rows = drain(&mut interleaved);
    arm_verified_ordered_run_eviction_for_test(&reader, 0);
    let counters = scope.counters();
    drop(scope);

    let delivered = |rows_seen: &Vec<Vec<Value>>| {
        rows_seen
            .iter()
            .map(|row| match row.as_slice() {
                [Value::Int64(id)] => *id,
                other => panic!("expected one integer id, got {other:?}"),
            })
            .collect::<Vec<_>>()
    };
    for (bucket, rows_seen) in undisturbed_rows
        .iter()
        .chain(interleaved_rows.iter())
        .enumerate()
    {
        let bucket = (bucket % 2) as i64;
        let mut seen = delivered(rows_seen);
        seen.sort_unstable();
        assert_eq!(
            seen,
            (bucket * rows as i64..(bucket + 1) * rows as i64).collect::<Vec<_>>(),
            "every cursor receives its whole range exactly once"
        );
    }
    // And in the same order it would have received them undisturbed: a
    // continuation that re-delivered or skipped across a window boundary, or
    // that finished from the physical index, could not reproduce this.
    for (undisturbed, evicted) in undisturbed_rows.iter().zip(interleaved_rows.iter()) {
        assert_eq!(
            delivered(evicted),
            delivered(undisturbed),
            "eviction does not change what a cursor delivers or the order it delivers it in"
        );
    }
    eprintln!("interleaved cursors under eviction: undisturbed={control:?} evicted={counters:?}");
}

// ---------------------------------------------------------------------------
// Paged cursors over the authenticated candidate route
// ---------------------------------------------------------------------------

/// Drain one open cursor page by page, returning every id it delivered in the
/// order it delivered them.
fn drain_cursor(
    opened: &mut contextdb_engine::direct_file_reader::test_seams::DirectCursorOpen,
    page: NonZeroUsize,
) -> Vec<i64> {
    let mut delivered = cursor_page_ids(&opened.first_page.page.rows);
    let mut more = opened.first_page.page.has_more;
    while more {
        let fetched = opened
            .cursor
            .fetch(Some(page))
            .expect("a paged cursor fetches its next page");
        delivered.extend(cursor_page_ids(&fetched.page.rows));
        more = fetched.page.has_more;
    }
    delivered
}

fn cursor_page_ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("expected one integer id, got {other:?}"),
        })
        .collect()
}

/// A paged cursor over the authenticated-index candidate route must hand back
/// every byte it took exactly once, against the reservation that took it.
///
/// The search's own growing state - its candidate ids, its scores, its ranked
/// and output queues - lives across fetches, and the cursor's per-page
/// settlement has to be able to see it. When it could not, the settlement gave
/// those bytes away while the search was still holding them, and the fetch that
/// finally released them asked the request for more than it had left.
#[test]
fn a_paged_cursor_over_the_authenticated_candidate_route_returns_its_whole_range() {
    let rows = 64usize;
    let root = tempfile::tempdir().expect("task-scoped wide fixture");
    let path = root.path().join("paged-wide.redb");
    seed_wide(&path, rows as i64);

    let mut limits = wide_limits(rows);
    limits.cursor_page_rows = 8;
    let reader = open_declaring_memory(&path, limits, DECLARED_SESSION_MEMORY);
    let page = NonZeroUsize::new(8).expect("a non-zero page");

    let mut opened = reader
        .open_cursor(&wide_statement(rows), &wide_params(), page)
        .expect("open a paged cursor over the authenticated candidate route");
    let delivered = drain_cursor(&mut opened, page);

    assert_eq!(
        delivered.len(),
        rows,
        "the cursor delivers its whole range, with no duplicates"
    );
    let mut sorted = delivered.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted,
        (0..rows as i64).collect::<Vec<_>>(),
        "exactly the ids of the range, each once"
    );
    opened.cursor.close().expect("the drained cursor closes");
}
