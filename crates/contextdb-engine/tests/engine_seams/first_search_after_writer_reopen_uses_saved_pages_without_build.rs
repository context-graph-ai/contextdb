//! Acceptance: "A fresh process searches the maintained 100k store from saved index state, with
//! zero query graph-build work and a complete answer. Measure open, first search in that process,
//! and warm search separately."
//!
//! This deterministic engine fixture checks writable reopen; the separate consumer workload
//! supplies the required 100k scale measurement.

use contextdb_core::{Value, Wallclock};
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_vector::hnsw::{HnswBuildObserver, with_hnsw_build_observer};
use std::collections::HashMap;
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const CHILD_PATH: &str = "CONTEXTDB_WRITER_REOPEN_SEARCH_CHILD_PATH";
const ROWS: i64 = 128;

#[derive(Default)]
struct BuildCounter(AtomicU64);
impl HnswBuildObserver for BuildCounter {
    fn before_hnsw_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn first_search_after_writer_reopen_uses_saved_pages_without_build() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let builds = Arc::new(BuildCounter::default());
        let (opened, open_work) = observe(|| {
            with_hnsw_build_observer(builds.clone(), || {
                Database::open_with_options(
                    &path,
                    DatabaseOpenOptions {
                        maintenance_policy: MaintenancePolicy::CallerDriven,
                        ..Default::default()
                    },
                )
            })
        });
        let database = opened.unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            builds.0.load(Ordering::SeqCst),
            0,
            "writer reopen does not construct a query graph"
        );
        assert_eq!(
            open_work.saved_graph_loads, 0,
            "open leaves saved generation pages lazy"
        );
        let sql = "SELECT id FROM saved_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 128";
        let params = HashMap::from([("query".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let (first, first_work) =
            observe(|| with_hnsw_build_observer(builds.clone(), || database.execute(sql, &params)));
        let first = first.unwrap();
        assert_eq!(
            first.rows,
            (1..=ROWS)
                .map(|id| vec![Value::Int64(id)])
                .collect::<Vec<_>>(),
            "first indexed answer includes every saved vector in the selected scope"
        );
        assert_eq!(
            builds.0.load(Ordering::SeqCst),
            0,
            "first writable search performs zero graph-build work"
        );
        assert!(
            first_work.saved_graph_loads > 0 && first_work.saved_graph_bytes > 0,
            "first search actually loads persisted generation pages: {first_work:?}"
        );
        let (warm, warm_work) =
            observe(|| with_hnsw_build_observer(builds.clone(), || database.execute(sql, &params)));
        assert_eq!(warm.unwrap().rows, first.rows);
        assert_eq!(
            builds.0.load(Ordering::SeqCst),
            0,
            "warm search performs zero graph-build work"
        );
        assert_eq!(
            warm_work.saved_graph_loads, 0,
            "warm search reuses the loaded saved generation"
        );
        eprintln!(
            "writer search receipts: open={open_work:?}; first={first_work:?}; warm={warm_work:?}"
        );
        database.close().unwrap();
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("saved-search.redb");
    let database = Database::open(&path).unwrap();
    database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    database.execute("CREATE TABLE saved_items (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, payload TEXT, embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED) HISTORY ALL", &HashMap::new()).unwrap();
    for id in 1..=ROWS {
        database
            .execute(
                "INSERT INTO saved_items VALUES ($id, 1, 'saved', $vector)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    (
                        "vector".to_owned(),
                        Value::Vector(vec![1.0, id as f32 / ROWS as f32, 0.0]),
                    ),
                ]),
            )
            .unwrap();
        database
            .execute(
                "UPDATE saved_items SET payload = 'current' WHERE id = $id",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .unwrap();
    }
    let builds = Arc::new(BuildCounter::default());
    let mut ready = false;
    for _ in 0..128 {
        let report =
            with_hnsw_build_observer(builds.clone(), || database.run_maintenance_cycle()).unwrap();
        if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "maintenance publishes the saved generation before any search"
    );
    assert!(
        builds.0.load(Ordering::SeqCst) > 0,
        "the build observer first witnesses real maintenance construction"
    );
    database.close().unwrap();
    drop(database);
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "first_search_after_writer_reopen_uses_saved_pages_without_build::first_search_after_writer_reopen_uses_saved_pages_without_build", "--nocapture"])
        .env(CHILD_PATH, &path).output().unwrap();
    assert!(
        child.status.success(),
        "fresh writable search child failed:\n{}\n{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    assert!(
        String::from_utf8_lossy(&child.stdout).contains("1 passed"),
        "the fresh process must execute the named test, not silently match zero tests"
    );
    eprintln!("{}", String::from_utf8_lossy(&child.stderr));
}
