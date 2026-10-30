//! Independent replacement for the thread-identity oracle. Both identities
//! come from the same engine-owned factory; no protected file is edited.
#![cfg(all(unix, feature = "test-seams"))]
#![expect(
    clippy::result_large_err,
    reason = "Public typed errors keep inline refusal details."
)]
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::writer_query_test_seams::current_owned_worker_thread_id_for_test;
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, ReadRoute, ReadSession, ReadSessionOptions,
};
use std::{collections::HashMap, path::Path, process::Command, sync::Arc};

const CHILD_STORE: &str = "INDEX_PORTS_WORKER_ID_CHILD_STORE";
const CALLER_ID_FILE: &str = "INDEX_PORTS_WORKER_ID_CALLER_FILE";
const MUTATION: &str = "INDEX_PORTS_WORKER_ID_MUTATION";
const RECEIPTS: &str = "CONTEXTDB_TEST_SEAMS_RECEIPT_PATH";
const ROWS: i64 = 128;
#[derive(Clone, Copy)]
struct FrozenClock;
impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn child(path: &Path) {
    let db = open(path);
    let reader = ReadSession::open_owner_only(
        path,
        ReadSessionOptions {
            limits: ReadLimits {
                cursor_page_rows: 16,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(reader.route(), ReadRoute::Owner);
    let caller = current_owned_worker_thread_id_for_test();
    assert_eq!(
        caller,
        current_owned_worker_thread_id_for_test(),
        "the authentic caller identity is stable on this thread"
    );
    std::fs::write(
        std::env::var_os(CALLER_ID_FILE).unwrap(),
        serde_json::to_vec(&caller).unwrap(),
    )
    .unwrap();
    let expected = (1..=ROWS)
        .map(|id| vec![Value::Int64(id)])
        .collect::<Vec<_>>();
    let params = HashMap::from([("q".into(), Value::Vector(vec![1., 0., 0.]))]);
    for mode in ["INDEXED", "INDEXED", "EXACT"] {
        let before = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        let sql = format!(
            "SELECT id FROM saved_worker_items WHERE scope = 1 ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT {ROWS}"
        );
        let result = if std::env::var_os(MUTATION).is_some() {
            // Planted route mutation: execute the SAME real synchronous bounded
            // scoring kernel on this actual caller instead of dispatching the
            // owner-reader request. No receipt, ID or answer is fabricated.
            let request = bounded::BoundedReadRequest::new(
                &sql,
                params.clone(),
                ReadLimits {
                    cursor_page_rows: 16,
                    ..Default::default()
                },
                Arc::new(FrozenClock),
            );
            bounded::execute(&db, &request)
                .expect("mutant real caller scoring completes")
                .result
        } else {
            reader
                .execute(&sql, &params)
                .expect("owned worker scoring completes")
        };
        assert_eq!(
            result.rows(),
            expected,
            "every reopened/warm/exact query returns every saved row"
        );
        let after = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        assert_eq!(
            after.hnsw_builds, before.hnsw_builds,
            "queries never build the maintained index"
        );
        assert_eq!(
            after.hnsw_repairs_or_compactions,
            before.hnsw_repairs_or_compactions
        );
    }
    drop(reader);
    db.close().unwrap();
}

#[test]
fn first_saved_search_scores_on_the_owned_worker_in_one_identity_namespace() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_STORE) {
        child(Path::new(&path));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("worker-id.redb");
    let db = open(&path);
    db.execute(&format!("CREATE TABLE saved_worker_items (id INT PRIMARY KEY, scope INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = {ROWS}.00)) HISTORY ALL"), &HashMap::new()).unwrap();
    let tx = db.begin_or_panic();
    for id in 1..=ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO saved_worker_items VALUES ($id, 1, $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "v".into(),
                    Value::Vector(vec![1., id as f32 / ROWS as f32, 0.]),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    loop {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(
            report.vector.first_failure.is_none(),
            "maintenance refusal: {:?}",
            report.vector
        );
        if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
            break;
        }
    }
    db.close().unwrap();
    drop(db);
    let caller_file = root.path().join("caller.json");
    let receipts = root.path().join("worker-receipts.jsonl");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "first_saved_search_scores_on_the_owned_worker_in_one_identity_namespace",
            "--nocapture",
        ])
        .env(CHILD_STORE, &path)
        .env(CALLER_ID_FILE, &caller_file)
        .env(RECEIPTS, &receipts)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fresh writer/owner reader child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let caller: String = serde_json::from_slice(&std::fs::read(&caller_file).unwrap()).unwrap();
    let text =
        std::fs::read_to_string(&receipts).expect("actual bounded scoring emits its receipt");
    let rows = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|row| row["kind"] == "vector_search")
        .collect::<Vec<_>>();
    assert_eq!(
        rows.len(),
        3,
        "fresh, warm and exact scoring each emit once across all cursor pages"
    );
    for (phase, row) in rows.iter().enumerate() {
        let scorer = row["thread_id"]
            .as_str()
            .expect("actual scorer identity is present");
        println!("authentic_thread_identity phase={phase} caller={caller} scorer={scorer}");
        assert_ne!(
            scorer, caller,
            "real scoring must run on the owned worker, not the caller; BOTH identities use owned_worker::current_thread_id"
        );
        assert_eq!(row["graph_builds_observed"].as_u64(), Some(0));
        assert_eq!(row["graph_insertions_observed"].as_u64(), Some(0));
    }
    assert!(
        rows.windows(2)
            .all(|pair| pair[0]["search_ordinal"].as_u64().unwrap()
                < pair[1]["search_ordinal"].as_u64().unwrap())
    );
}
