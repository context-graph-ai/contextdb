//! A maintenance cycle with nothing to prune takes no compaction decision, so
//! it must not walk the store's pages to compute a fragmentation ratio it would
//! discard. The work of such a cycle stays the same whether the store holds
//! ten thousand or fifty thousand rows; an explicit diagnostic still gets the
//! exact ratio on demand.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const NOW: u64 = 1_800_000_000_000;
const RETAIN_MILLIS: u64 = 3650 * 24 * 60 * 60 * 1000;
const TABLES: [&str; 3] = ["retained_events", "passages", "links"];
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

/// `rows` rows spread over three tables, one of which declares a retention
/// window no row reaches, so every cycle runs the pruning pass and prunes
/// nothing.
fn seed(path: &std::path::Path, rows: usize) {
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
    for table in TABLES {
        let mut start = 0;
        while start < per_table {
            let end = (start + ROWS_PER_STATEMENT).min(per_table);
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
    // Leave the file tight, as a long-running store settles after its own
    // compaction, so no cycle below has a compaction to start.
    db.compact_now().unwrap();
    db.close().unwrap();
}

struct Observation {
    on_demand_pages: u64,
    on_demand_ratio: f64,
    first_cycle_pages: u64,
    warm_cycle_pages: u64,
    pages_under_pruning_guard: u64,
    warm_cycle_micros: u128,
    warm_cycle_compacted: bool,
    pruned_rows: u64,
    pruning_ratio: f64,
    pruning_pages_under_pruning_guard: u64,
}

fn observe(rows: usize, clock: &AtomicU64) -> Observation {
    clock.store(NOW, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("growth.redb");
    seed(&path, rows);
    // A fresh open, as each import process opens the store.
    let db = open(&path);
    let p = HashMap::new();
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM passages", &p).unwrap().rows,
        vec![vec![Value::Int64((rows / TABLES.len()) as i64)]]
    );

    // The first cycle of a fresh open, then a warm one. Neither prunes.
    let (before, before_guarded) = db.__storage_statistics_pages_walked_for_test();
    let first = db.run_maintenance_cycle().unwrap();
    let (after_first, _) = db.__storage_statistics_pages_walked_for_test();
    let first_cycle_pages = after_first - before;
    assert_eq!(first.pruning.pruned_rows, 0, "no row reaches its window");

    let started = std::time::Instant::now();
    let warm = db.run_maintenance_cycle().unwrap();
    let warm_cycle_micros = started.elapsed().as_micros();
    let (after_warm, after_guarded) = db.__storage_statistics_pages_walked_for_test();
    let warm_cycle_pages = after_warm - after_first;
    assert_eq!(warm.pruning.pruned_rows, 0, "no row reaches its window");

    // The diagnostic a caller explicitly asks for still walks and is exact.
    let (before, _) = db.__storage_statistics_pages_walked_for_test();
    let on_demand_ratio = db.__storage_fragmentation_ratio_for_test().unwrap().unwrap();
    let on_demand_pages = db.__storage_statistics_pages_walked_for_test().0 - before;

    // Once the retained rows age out, the cycle that prunes them takes its
    // decision on the exact pre-prune ratio, still never under the guard.
    clock.store(NOW + RETAIN_MILLIS + 1, Ordering::SeqCst);
    let (_, before_guarded) = db.__storage_statistics_pages_walked_for_test();
    let pruning = db.run_maintenance_cycle().unwrap();
    let (_, after_pruning_guarded) = db.__storage_statistics_pages_walked_for_test();
    db.close().unwrap();
    Observation {
        on_demand_pages,
        on_demand_ratio,
        first_cycle_pages,
        warm_cycle_pages,
        pages_under_pruning_guard: after_guarded - before_guarded,
        warm_cycle_micros,
        warm_cycle_compacted: warm.pruning.compacted || warm.compaction.ran,
        pruned_rows: pruning.pruning.pruned_rows,
        pruning_ratio: pruning.pruning.fragmentation_before,
        pruning_pages_under_pruning_guard: after_pruning_guarded - before_guarded,
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
            "rows={rows} on_demand_pages={} on_demand_ratio={} first_cycle_pages={} \
             warm_cycle_pages={} pages_under_pruning_guard={} warm_cycle_micros={}",
            observation.on_demand_pages,
            observation.on_demand_ratio,
            observation.first_cycle_pages,
            observation.warm_cycle_pages,
            observation.pages_under_pruning_guard,
            observation.warm_cycle_micros,
        );
    }
    assert!(
        large.on_demand_pages > small.on_demand_pages,
        "the explicit diagnostic walks the whole store, so it sees the larger store: {} vs {}",
        large.on_demand_pages,
        small.on_demand_pages,
    );
    assert!((0.0..=1.0).contains(&small.on_demand_ratio));
    assert!((0.0..=1.0).contains(&large.on_demand_ratio));
    // The import's vector-tail persistence waits on the pruning guard, so
    // no store-proportional walk may happen while a cycle holds it.
    assert_eq!(
        (small.pages_under_pruning_guard, large.pages_under_pruning_guard),
        (0, 0),
        "no statistics page is walked while the pruning guard is held"
    );
    assert_eq!(
        (small.warm_cycle_pages, large.warm_cycle_pages),
        (0, 0),
        "a cycle that prunes nothing and has no compaction due walks no statistics pages"
    );
    assert!(!small.warm_cycle_compacted && !large.warm_cycle_compacted);
    for observation in [&small, &large] {
        // Retention examines a fixed window per cycle, so one cycle may
        // prune only part of the expired rows.
        assert!(observation.pruned_rows > 0, "expired retained rows are pruned");
        assert_eq!(
            observation.pruning_ratio, observation.on_demand_ratio,
            "a pruning cycle reports the exact ratio the store had before it pruned"
        );
        assert_eq!(
            observation.pruning_pages_under_pruning_guard, 0,
            "a pruning cycle measures the store with the pruning guard released"
        );
    }
    // Secondary, load-tolerant: the same empty cycle costs about the same at
    // five times the history. The 5 ms floor absorbs scheduler noise.
    let floor = 5_000;
    assert!(
        large.warm_cycle_micros.max(floor) < 2 * small.warm_cycle_micros.max(floor),
        "an empty cycle's wall time must not grow with the store: {} us vs {} us",
        large.warm_cycle_micros,
        small.warm_cycle_micros,
    );
}
