//! A maintenance cycle decides compaction without reading the store. Whether
//! it prunes nothing or prunes expired rows, its decision input is the
//! allocator's maintained free-page ratio, so the work of a cycle stays the
//! same whether the store holds ten thousand or fifty thousand rows, and
//! nothing store-sized runs while the cycle holds the pruning guard. A
//! freshly opened store adopts the totals its last clean close saved, even
//! when that close trimmed the file. An explicit diagnostic still gets the
//! exact statistics ratio on demand.
use contextdb_core::Wallclock;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const NOW: u64 = 1_800_000_000_000;
const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;
const RETAIN_MILLIS: u64 = 3650 * DAY_MILLIS;
const TABLES: [&str; 3] = ["retained_events", "passages", "links"];
/// Retained rows are written in this many cohorts, one day apart, so
/// successive cycles each find a new cohort expired.
const COHORTS: u64 = 3;
const ROWS_PER_STATEMENT: usize = 500;

fn open(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}

fn insert_rows(db: &Database, table: &str, ids: std::ops::Range<usize>) {
    let p = HashMap::new();
    let mut start = ids.start;
    while start < ids.end {
        let end = (start + ROWS_PER_STATEMENT).min(ids.end);
        let mut sql = format!("INSERT INTO {table} VALUES ");
        for id in start..end {
            if id > start {
                sql.push_str(", ");
            }
            write!(
                sql,
                "({id}, 'row {id:08} of {table} carrying a fixed-width body')"
            )
            .unwrap();
        }
        db.execute(&sql, &p).unwrap();
        start = end;
    }
}

/// `rows` rows spread over three tables, one of which declares a retention
/// window. Its rows are written in day-apart cohorts from `NOW` onward.
fn seed(path: &std::path::Path, rows: usize, clock: &AtomicU64) {
    let db = open(path);
    let p = HashMap::new();
    db.execute(
        "CREATE TABLE retained_events (id INT PRIMARY KEY, body TEXT) RETAIN 3650 DAYS",
        &p,
    )
    .unwrap();
    db.execute("CREATE TABLE passages (id INT PRIMARY KEY, body TEXT)", &p)
        .unwrap();
    db.execute("CREATE TABLE links (id INT PRIMARY KEY, body TEXT)", &p)
        .unwrap();
    let per_table = rows / TABLES.len();
    let per_cohort = per_table.div_ceil(COHORTS as usize);
    for cohort in 0..COHORTS {
        clock.store(NOW + cohort * DAY_MILLIS, Ordering::SeqCst);
        let start = (cohort as usize * per_cohort).min(per_table);
        let end = (start + per_cohort).min(per_table);
        insert_rows(&db, "retained_events", start..end);
    }
    clock.store(NOW, Ordering::SeqCst);
    insert_rows(&db, "passages", 0..per_table);
    insert_rows(&db, "links", 0..per_table);
    // Leave the file tight, as a long-running store settles after its own
    // compaction, so no cycle below has a compaction to start.
    db.compact_now().unwrap();
    db.close().unwrap();
}

/// Statistics work and allocator rebuilds observed on this thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Work {
    captures: u64,
    pages: u64,
    pages_under_pruning_guard: u64,
    recounts: u64,
}

fn work(db: &Database) -> Work {
    let (pages, pages_under_pruning_guard) = db.__storage_statistics_pages_walked_for_test();
    Work {
        captures: Database::__storage_statistics_captures_for_test(),
        pages,
        pages_under_pruning_guard,
        recounts: Database::__allocator_free_total_recounts_for_test(),
    }
}

fn since(before: Work, after: Work) -> Work {
    Work {
        captures: after.captures - before.captures,
        pages: after.pages - before.pages,
        pages_under_pruning_guard: after.pages_under_pruning_guard
            - before.pages_under_pruning_guard,
        recounts: after.recounts - before.recounts,
    }
}

/// Open the store, counting the rebuilds and statistics captures the open
/// itself did on this thread.
fn counted_open(path: &std::path::Path) -> (Database, u64, u64) {
    let recounts = Database::__allocator_free_total_recounts_for_test();
    let captures = Database::__storage_statistics_captures_for_test();
    let db = open(path);
    (
        db,
        Database::__allocator_free_total_recounts_for_test() - recounts,
        Database::__storage_statistics_captures_for_test() - captures,
    )
}

/// The maintained ratio, checked against an independent recount.
fn agreed_ratio(db: &Database) -> f64 {
    let (maintained, recounted) = db.__allocator_free_page_ratios_for_test().unwrap();
    assert_eq!(
        maintained,
        Some(recounted),
        "the maintained allocator totals equal an independent bitmap recount"
    );
    recounted
}

struct Observation {
    open_recounts: u64,
    open_captures: u64,
    cycles: Vec<Work>,
    pruned_rows: Vec<u64>,
    pruning_ratios: Vec<(f64, f64)>,
    reopen_recounts: Vec<u64>,
    on_demand_pages: u64,
    on_demand_ratio: f64,
    warm_cycle_micros: u128,
    warm_cycle_compacted: bool,
}

fn observe(rows: usize, clock: &AtomicU64) -> Observation {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("growth.redb");
    seed(&path, rows, clock);
    clock.store(NOW, Ordering::SeqCst);
    let p = HashMap::new();

    // A fresh open, as each import process opens the store. Everything from
    // here counts, open included, and no diagnostic runs first.
    let (db, open_recounts, open_captures) = counted_open(&path);
    let mut cycles = Vec::new();
    let mut pruned_rows = Vec::new();
    let mut pruning_ratios = Vec::new();

    // The first cycle after the open, then a warm one. Neither prunes.
    let before = work(&db);
    let first = db.run_maintenance_cycle().unwrap();
    cycles.push(since(before, work(&db)));
    assert_eq!(first.pruning.pruned_rows, 0, "no row reaches its window");

    let before = work(&db);
    let started = std::time::Instant::now();
    let warm = db.run_maintenance_cycle().unwrap();
    let warm_cycle_micros = started.elapsed().as_micros();
    cycles.push(since(before, work(&db)));
    assert_eq!(warm.pruning.pruned_rows, 0, "no row reaches its window");

    // Each cohort ages out in turn; each cycle prunes it and decides on the
    // free-page ratio the store had just before it pruned.
    for cohort in 0..COHORTS {
        clock.store(
            NOW + cohort * DAY_MILLIS + RETAIN_MILLIS + 1,
            Ordering::SeqCst,
        );
        let ratio_before = agreed_ratio(&db);
        let before = work(&db);
        let report = db.run_maintenance_cycle().unwrap();
        cycles.push(since(before, work(&db)));
        pruned_rows.push(report.pruning.pruned_rows);
        pruning_ratios.push((report.pruning.fragmentation_before, ratio_before));
        agreed_ratio(&db);
    }

    // The diagnostic a caller explicitly asks for still walks and is exact.
    let (before, _) = db.__storage_statistics_pages_walked_for_test();
    let on_demand_ratio = db.__storage_fragmentation_ratio_for_test().unwrap().unwrap();
    let on_demand_pages = db.__storage_statistics_pages_walked_for_test().0 - before;
    db.close().unwrap();
    drop(db);

    // Three more short processes, each deleting rows so its clean close has
    // trailing free space to trim. Every open adopts the saved totals.
    let mut reopen_recounts = Vec::new();
    let per_table = rows / TABLES.len();
    for process in 0..3 {
        let (db, recounts, _) = counted_open(&path);
        reopen_recounts.push(recounts);
        agreed_ratio(&db);
        let start = per_table * process / 4;
        let end = per_table * (process + 1) / 4;
        db.execute(
            &format!("DELETE FROM links WHERE id >= {start} AND id < {end}"),
            &p,
        )
        .unwrap();
        agreed_ratio(&db);
        db.close().unwrap();
    }
    let (db, recounts, _) = counted_open(&path);
    reopen_recounts.push(recounts);
    agreed_ratio(&db);
    db.close().unwrap();

    Observation {
        open_recounts,
        open_captures,
        cycles,
        pruned_rows,
        pruning_ratios,
        reopen_recounts,
        on_demand_pages,
        on_demand_ratio,
        warm_cycle_micros,
        warm_cycle_compacted: warm.pruning.compacted || warm.compaction.ran,
    }
}

#[test]
fn a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics_as_the_store_grows() {
    let clock = Arc::new(AtomicU64::new(NOW));
    let reader = clock.clone();
    let _clock = Wallclock::test_clock_guard(move || reader.load(Ordering::SeqCst));
    let small = observe(10_000, &clock);
    let large = observe(50_000, &clock);
    for (rows, observation) in [(10_000, &small), (50_000, &large)] {
        println!(
            "rows={rows} open_recounts={} open_captures={} cycles={:?} pruned_rows={:?} \
             pruning_ratios={:?} reopen_recounts={:?} on_demand_pages={} on_demand_ratio={} \
             warm_cycle_micros={}",
            observation.open_recounts,
            observation.open_captures,
            observation.cycles,
            observation.pruned_rows,
            observation.pruning_ratios,
            observation.reopen_recounts,
            observation.on_demand_pages,
            observation.on_demand_ratio,
            observation.warm_cycle_micros,
        );
    }
    assert!(
        large.on_demand_pages > small.on_demand_pages,
        "the explicit diagnostic walks the whole store, so it sees the larger store: {} vs {}",
        large.on_demand_pages,
        small.on_demand_pages,
    );
    for observation in [&small, &large] {
        assert!((0.0..=1.0).contains(&observation.on_demand_ratio));
        assert_eq!(
            (observation.open_recounts, observation.open_captures),
            (0, 0),
            "a fresh open adopts the saved allocator totals and captures no statistics"
        );
        for (cycle, work) in observation.cycles.iter().enumerate() {
            assert_eq!(
                *work,
                Work::default(),
                "cycle {cycle} (first, warm, then one per expiring cohort) reads no \
                 statistics, walks no page under the pruning guard, rebuilds no totals"
            );
        }
        assert!(
            observation.reopen_recounts.iter().all(|recounts| *recounts == 0),
            "every reopen after a trimming clean close adopts the saved totals: {:?}",
            observation.reopen_recounts
        );
        assert!(!observation.warm_cycle_compacted);
        assert!(
            observation.pruned_rows.iter().all(|rows| *rows > 0),
            "each cohort's expired rows are pruned: {:?}",
            observation.pruned_rows
        );
        for (reported, before) in &observation.pruning_ratios {
            assert_eq!(
                reported, before,
                "a pruning cycle reports the allocator free-page ratio the store had \
                 just before it pruned"
            );
        }
    }
    // Secondary, load-tolerant: the same empty cycle costs about the same at
    // five times the history. The 5 ms floor absorbs scheduler noise. The
    // deterministic counters above are the gate.
    let floor = 5_000;
    assert!(
        large.warm_cycle_micros.max(floor) < 2 * small.warm_cycle_micros.max(floor),
        "an empty cycle's wall time must not grow with the store: {} us vs {} us",
        large.warm_cycle_micros,
        small.warm_cycle_micros,
    );
}
