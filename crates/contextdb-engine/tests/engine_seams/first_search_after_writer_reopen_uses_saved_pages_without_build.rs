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
#[cfg(unix)]
const RECEIPT_READER_CHILD: &str = "CONTEXTDB_WRITER_REOPEN_SEARCH_RECEIPT_READER";
#[cfg(unix)]
const RECEIPT_PATH: &str = "CONTEXTDB_TEST_SEAMS_RECEIPT_PATH";

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
        #[cfg(unix)]
        if std::env::var_os(RECEIPT_READER_CHILD).is_some() {
            let database = Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    maintenance_policy: MaintenancePolicy::CallerDriven,
                    ..Default::default()
                },
            )
            .unwrap();
            // This is the ordinary ConsumerReader query/cursor route to the
            // live writer, so scoring runs on the owner's request worker.
            let reader = contextdb_engine::ReadSession::open_owner_only(
                &path,
                contextdb_engine::ReadSessionOptions {
                    limits: contextdb_core::read_contract::ReadLimits {
                        cursor_page_rows: 16,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(reader.route(), contextdb_engine::ReadRoute::Owner);
            let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
            let expected = (1..=ROWS)
                .map(|id| vec![Value::Int64(id)])
                .collect::<Vec<_>>();
            println!("OWNER_CALLER_THREAD {:?}", std::thread::current().id());
            for mode in ["INDEXED", "INDEXED", "EXACT"] {
                let before = database
                    .vector_store_for_test()
                    .passive_activity_counters_for_test();
                let sql = format!(
                    "SELECT id FROM saved_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 128"
                );
                assert_eq!(
                    reader.execute(&sql, &params).unwrap().rows,
                    expected,
                    "fresh, warm and exact reader requests all return every saved identity"
                );
                let after = database
                    .vector_store_for_test()
                    .passive_activity_counters_for_test();
                assert_eq!(
                    after.hnsw_builds, before.hnsw_builds,
                    "per-store atomic lifecycle counts observe all worker threads"
                );
                assert_eq!(
                    after.hnsw_repairs_or_compactions,
                    before.hnsw_repairs_or_compactions
                );
            }
            drop(reader);
            database.close().unwrap();
            return;
        }
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
    #[cfg(unix)]
    {
        let receipts = directory.path().join("reader-search-receipts.jsonl");
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "first_search_after_writer_reopen_uses_saved_pages_without_build::first_search_after_writer_reopen_uses_saved_pages_without_build", "--nocapture"])
            .env(CHILD_PATH, &path).env(RECEIPT_READER_CHILD, "1").env(RECEIPT_PATH, &receipts)
            .output().unwrap();
        assert!(
            child.status.success(),
            "fresh owner-reader child failed:\n{}\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
        assert!(String::from_utf8_lossy(&child.stdout).contains("1 passed"));
        let stdout = String::from_utf8_lossy(&child.stdout);
        let caller_thread = stdout
            .lines()
            .find_map(|line| line.strip_prefix("OWNER_CALLER_THREAD "))
            .expect("actual child caller identity");
        let text =
            std::fs::read_to_string(&receipts).expect("actual reader kernel writes receipts");
        let rows = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|row| row["kind"] == "vector_search")
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            3,
            "each scoring phase records once across all cursor pages, including exact fallback"
        );
        for row in &rows {
            assert_ne!(
                row["thread_id"].as_str(),
                Some(caller_thread),
                "the receipt is emitted by the real owner kernel worker, not the test caller"
            );
            assert_eq!(
                row["graph_insertions_observed"].as_u64(),
                Some(0),
                "actual worker scoring must not perform incremental graph insertion; missing instrumentation is never treated as zero"
            );
            assert_eq!(
                row["graph_builds_observed"].as_u64(),
                Some(0),
                "reopened/warm/exact actual worker scoring performs no graph construction: {row}"
            );
            assert!(
                row["candidates"]
                    .as_u64()
                    .is_some_and(|count| count >= ROWS as u64),
                "the actual scoring observer sees the candidates that returned all saved identities: {row}"
            );
            assert!(
                row["elapsed_us"].as_u64().is_some(),
                "receipt includes measured elapsed work without a time-based test oracle"
            );
        }
        assert!(
            rows.windows(2)
                .all(|pair| pair[0]["search_ordinal"].as_u64().unwrap()
                    < pair[1]["search_ordinal"].as_u64().unwrap()),
            "fresh, warm and exact work have distinct increasing ordinals"
        );
        eprintln!("actual owner-reader receipts: {rows:?}");
    }
}
