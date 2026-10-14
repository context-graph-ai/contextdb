//! A maintenance cycle with nothing to prune takes no compaction decision, so
//! it must not walk the store's pages to compute a fragmentation ratio it would
//! discard. The work of such a cycle stays the same whether the store holds
//! ten thousand or fifty thousand rows; an explicit diagnostic still gets the
//! exact ratio on demand.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::fmt::Write as _;

const NOW: u64 = 1_800_000_000_000;
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
    db.close().unwrap();
}

struct Observation {
    on_demand_pages: u64,
    on_demand_ratio: f64,
    first_cycle_pages: u64,
    warm_cycle_pages: u64,
    warm_cycle_micros: u128,
}

fn observe(rows: usize) -> Observation {
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

    let before = db.__storage_statistics_pages_walked_for_test();
    let first = db.run_maintenance_cycle().unwrap();
    let first_cycle_pages = db.__storage_statistics_pages_walked_for_test() - before;
    assert_eq!(first.pruning.pruned_rows, 0, "no row reaches its window");

    let before = db.__storage_statistics_pages_walked_for_test();
    let started = std::time::Instant::now();
    let warm = db.run_maintenance_cycle().unwrap();
    let warm_cycle_micros = started.elapsed().as_micros();
    let warm_cycle_pages = db.__storage_statistics_pages_walked_for_test() - before;
    assert_eq!(warm.pruning.pruned_rows, 0, "no row reaches its window");
    assert!(!warm.pruning.compacted);
    assert!(!warm.compaction.ran);

    // The diagnostic a caller explicitly asks for still walks and is exact.
    let before = db.__storage_statistics_pages_walked_for_test();
    let on_demand_ratio = db.__storage_fragmentation_ratio_for_test().unwrap().unwrap();
    let on_demand_pages = db.__storage_statistics_pages_walked_for_test() - before;
    db.close().unwrap();
    Observation {
        on_demand_pages,
        on_demand_ratio,
        first_cycle_pages,
        warm_cycle_pages,
        warm_cycle_micros,
    }
}

#[test]
fn a_maintenance_cycle_that_prunes_nothing_walks_no_storage_statistics_as_the_store_grows() {
    let _clock = Wallclock::test_clock_guard(|| NOW);
    let small = observe(10_000);
    let large = observe(50_000);
    for (rows, observation) in [(10_000, &small), (50_000, &large)] {
        println!(
            "rows={rows} on_demand_pages={} on_demand_ratio={} first_cycle_pages={} \
             warm_cycle_pages={} warm_cycle_micros={}",
            observation.on_demand_pages,
            observation.on_demand_ratio,
            observation.first_cycle_pages,
            observation.warm_cycle_pages,
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
    assert_eq!(
        (small.warm_cycle_pages, large.warm_cycle_pages),
        (0, 0),
        "a cycle that prunes nothing and has no compaction due walks no statistics pages"
    );
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
