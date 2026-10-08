//! Acceptance statement "continuous use during bookkeeping": reads keep answering while a schema
//! replay or migration is applied to the store they read.
//!
//! Two scenarios on the frozen schema-replay fixture (the 102-declaration 100-table FK chain beside
//! the resident 768-dimension vector table): an identical-schema push+pull replay through the
//! authenticated hub, and the largest affected-set migration (one root alteration whose closure is
//! all 100 tables). Three closed-loop readers run against the receiving store throughout:
//! `find` (top-ten vector search over the resident table), `show` (primary-key lookup of one
//! resident row), and `status` (read-only maintenance/lsn/table inventory).
//!
//! The test asserts only exact structural facts: every query succeeded without a retry or refusal,
//! every find answered ten rows and every show exactly its row, each reader kind overlapped the
//! applied work at least once, the resident dataset is byte-for-byte unchanged, and the schemas
//! converged. It never asserts elapsed time. Per-query durations (idle baseline and overlapped
//! separately) and the route evidence are written to the JSON receipt at `CONTEXTDB_SO_RECEIPT`;
//! the 200 ms find p95 and 50 ms show p95 bars are evaluated from it by the campaign cell.
//!
//! Parameters: CONTEXTDB_SO_RESIDENT_ROWS (10000), CONTEXTDB_SO_WARMUP_QUERIES (20),
//! CONTEXTDB_SO_RECEIPT (path; default is a temp file whose path is printed).

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;
use contextdb_core::Value;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use support::*;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer, got {value:?}"))
    })
}

fn resident_vector(id: i64) -> Vec<f32> {
    let mut values = vec![0.0; 768];
    values[0] = id as f32;
    values[1] = id as f32 * 0.25;
    values
}

fn declare_one_hundred_tables(edge: &Database) {
    for ordinal in 0usize..100 {
        let name = format!("schema_{ordinal:03}");
        let ddl = match ordinal.checked_sub(1).map(|p| format!("schema_{p:03}")) {
            Some(parent) => format!(
                "CREATE TABLE {name} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES {parent}(id), body TEXT, FOREIGN KEY (parent_id) REFERENCES {parent}(id)) SYNC OFF"
            ),
            None => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF"),
        };
        edge.execute(&ddl, &empty())
            .expect("declare one source table");
    }
    edge.execute(
        "CREATE EVENT TYPE schema_099_insert WHEN INSERT ON schema_099",
        &empty(),
    )
    .expect("author the final event type");
    edge.execute(
        "CREATE TRIGGER schema_099_insert_trigger ON schema_099 WHEN INSERT INCLUDING SYNC",
        &empty(),
    )
    .expect("author the final including-sync trigger");
}

fn resident_store(path: &std::path::Path, rows: u64) -> Arc<Database> {
    let db = Arc::new(Database::open(path).expect("file-backed resident receiver"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE resident_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(768)) SYNC TWO WAY",
        &empty(),
    )
    .expect("create the resident vector table");
    let tx = db.begin().expect("begin the resident seed transaction");
    for id in 0..rows as i64 {
        db.execute_in_tx(
            tx,
            "INSERT INTO resident_vectors VALUES ($id, $embedding)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("embedding".to_string(), Value::Vector(resident_vector(id))),
            ]),
        )
        .expect("seed one resident row");
    }
    db.commit(tx).expect("commit the resident seed");
    db
}

fn resident_digest(db: &Database, rows: u64) -> Vec<Vec<Value>> {
    let answer = db
        .execute(
            "SELECT id, embedding FROM resident_vectors ORDER BY id",
            &empty(),
        )
        .expect("resident data remains readable")
        .rows;
    assert_eq!(
        answer.len() as u64,
        rows,
        "never shrink the resident dataset"
    );
    answer
}

#[derive(Default)]
struct KindLog {
    idle_ns: Vec<u64>,
    overlapped_ns: Vec<u64>,
    refusals: u64,
    wrong_answers: u64,
    routes: std::collections::BTreeMap<String, u64>,
}

/// `epoch` advances when the applied work begins and again when it ends, so a query that spans
/// either boundary is seen as overlapping it.
struct Active(AtomicBool, AtomicU64);

fn reader(
    kind: &'static str,
    db: &Database,
    rows: u64,
    active: &Active,
    stop: &AtomicBool,
    log: &Mutex<KindLog>,
    completed: &AtomicU64,
) {
    let mut ordinal = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let id = (ordinal.wrapping_mul(2_654_435_761) % rows) as i64;
        ordinal += 1;
        let at_start = active.0.load(Ordering::Acquire);
        let epoch_start = active.1.load(Ordering::Acquire);
        let begun = Instant::now();
        let (ok, route) = match kind {
            "find" => {
                match db.execute(
                    "SELECT id FROM resident_vectors ORDER BY embedding <=> $q LIMIT 10",
                    &HashMap::from([("q".to_string(), Value::Vector(resident_vector(id)))]),
                ) {
                    Ok(result) => (
                        Some(result.rows.len() == 10.min(rows as usize)),
                        Some(format!(
                            "{:?}",
                            result.trace.vector_search.as_ref().and_then(|d| d.route)
                        )),
                    ),
                    Err(_) => (None, None),
                }
            }
            "show" => match db.execute(
                "SELECT id, embedding FROM resident_vectors WHERE id = $id",
                &HashMap::from([("id".to_string(), Value::Int64(id))]),
            ) {
                Ok(result) => (
                    Some(
                        result.rows.len() == 1
                            && result.rows[0][0] == Value::Int64(id)
                            && result.rows[0][1] == Value::Vector(resident_vector(id)),
                    ),
                    None,
                ),
                Err(_) => (None, None),
            },
            _ => {
                let status = db.maintenance_status();
                let lsn = db.current_lsn();
                let tables = db.table_names();
                (
                    Some(status.active_maintenance_loops == 0 && lsn.0 > 0 && !tables.is_empty()),
                    None,
                )
            }
        };
        let elapsed = begun.elapsed().as_nanos() as u64;
        let overlapped = at_start || active.1.load(Ordering::Acquire) != epoch_start;
        {
            let mut log = log.lock().unwrap();
            match ok {
                None => log.refusals += 1,
                Some(false) => log.wrong_answers += 1,
                Some(true) => {}
            }
            if let Some(route) = route {
                *log.routes.entry(route).or_default() += 1;
            }
            if overlapped {
                log.overlapped_ns.push(elapsed);
            } else {
                log.idle_ns.push(elapsed);
            }
        }
        completed.fetch_add(1, Ordering::AcqRel);
    }
}

fn percentile(values: &[u64], p: f64) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    if sorted.is_empty() {
        return 0;
    }
    sorted[(((sorted.len() as f64) * p).ceil() as usize).clamp(1, sorted.len()) - 1]
}

/// Runs the three readers on `db` closed-loop, drives `work` once the readers are warm, and
/// returns each kind's log. Warm-up is awaited on completed-query counters, never a sleep.
async fn overlapped<F: Future>(db: &Arc<Database>, rows: u64, work: F) -> serde_json::Value {
    let warmup = env_u64("CONTEXTDB_SO_WARMUP_QUERIES", 20);
    let active = Active(AtomicBool::new(false), AtomicU64::new(0));
    let stop = AtomicBool::new(false);
    let kinds = ["find", "show", "status"];
    let logs: Vec<Mutex<KindLog>> = kinds.iter().map(|_| Mutex::default()).collect();
    let counters: Vec<AtomicU64> = kinds.iter().map(|_| AtomicU64::new(0)).collect();
    std::thread::scope(|scope| {
        for (index, kind) in kinds.iter().enumerate() {
            let (db, active, stop, log, counter) =
                (db, &active, &stop, &logs[index], &counters[index]);
            scope.spawn(move || reader(kind, db, rows, active, stop, log, counter));
        }
        while counters.iter().any(|c| c.load(Ordering::Acquire) < warmup) {
            std::thread::yield_now();
        }
        active.1.fetch_add(1, Ordering::AcqRel);
        active.0.store(true, Ordering::Release);
        let begun = Instant::now();
        let _ = futures_block(work);
        let work_ns = begun.elapsed().as_nanos() as u64;
        active.0.store(false, Ordering::Release);
        active.1.fetch_add(1, Ordering::AcqRel);
        stop.store(true, Ordering::Release);
        WORK_NS.store(work_ns, Ordering::Release);
    });
    let mut out = serde_json::Map::new();
    for (index, kind) in kinds.iter().enumerate() {
        let log = logs[index].lock().unwrap();
        assert_eq!(
            log.refusals, 0,
            "{kind}: zero refusals or retries while the replay ran"
        );
        assert_eq!(
            log.wrong_answers, 0,
            "{kind}: every answer is exact while the replay ran"
        );
        assert!(
            !log.overlapped_ns.is_empty(),
            "{kind}: at least one query overlapped the applied work"
        );
        out.insert(
            (*kind).to_string(),
            serde_json::json!({
                "overlapped_queries": log.overlapped_ns.len(),
                "idle_queries": log.idle_ns.len(),
                "overlapped_p50_ns": percentile(&log.overlapped_ns, 0.50),
                "overlapped_p95_ns": percentile(&log.overlapped_ns, 0.95),
                "overlapped_max_ns": log.overlapped_ns.iter().max(),
                "idle_p95_ns": percentile(&log.idle_ns, 0.95),
                "route_counts": log.routes,
                "refusals": log.refusals,
                "overlapped_durations_ns": log.overlapped_ns,
                "idle_durations_ns": log.idle_ns,
            }),
        );
    }
    out.insert(
        "work_ns".into(),
        serde_json::json!(WORK_NS.load(Ordering::Acquire)),
    );
    serde_json::Value::Object(out)
}

static WORK_NS: AtomicU64 = AtomicU64::new(0);

/// Drives the future to completion on the ambient multi-thread runtime from a scope thread.
fn futures_block<F: Future>(work: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(work))
}

async fn identical_schema_replay(rows: u64, directory: &std::path::Path) -> serde_json::Value {
    let receiver = resident_store(&directory.join("hub.db"), rows);
    let hub = Hub::start(receiver.clone(), "schema-overlap-identical").await;
    let first = Arc::new(Database::open(directory.join("first.db")).unwrap());
    first.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let second = Arc::new(Database::open(directory.join("second.db")).unwrap());
    second.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let a = hub.client(first.clone());
    let b = hub.client(second.clone());
    hub.bind(&a).await;
    hub.bind(&b).await;
    a.pull_default()
        .await
        .expect("setup pull of the resident table");
    b.pull_default()
        .await
        .expect("setup pull of the resident table");
    for db in [&receiver, &first, &second] {
        declare_one_hundred_tables(db);
        initialize_fixture_callbacks(db);
    }
    let before = resident_digest(&receiver, rows);
    let receipt = overlapped(&receiver, rows, async {
        for client in [&a, &b] {
            client.push().await.expect("identical-schema push");
            client.pull_default().await.expect("identical-schema pull");
        }
    })
    .await;
    assert_eq!(schema(&first), schema(&receiver));
    assert_eq!(schema(&second), schema(&receiver));
    assert_eq!(
        resident_digest(&receiver, rows),
        before,
        "resident rows and vectors unchanged"
    );
    drop(a);
    drop(b);
    hub.stop().await;
    receipt
}

async fn largest_affected_migration(rows: u64, directory: &std::path::Path) -> serde_json::Value {
    let receiver = resident_store(&directory.join("migrating.db"), rows);
    let source = Database::open_memory();
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    declare_one_hundred_tables(&receiver);
    let calls = largest::prepare_existing_rows_and_vectors(&receiver);
    declare_one_hundred_tables(&source);
    let _source_calls = largest::prepare_existing_rows_and_vectors(&source);
    let resident_before = resident_digest(&receiver, rows);
    let before = largest::rows_and_vectors(&receiver);
    let migration = largest::author_migration(&source);
    assert_eq!(
        migration.ddl.len(),
        1,
        "one root alteration includes its 99 FK dependents"
    );
    let receipt = overlapped(&receiver, rows, async {
        receiver
            .apply_changes(
                migration,
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .expect("dependency-complete root alteration applies");
    })
    .await;
    assert_eq!(
        receiver
            .execute("SELECT revised FROM schema_000", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Null]],
        "the authored root alteration is installed"
    );
    assert_eq!(largest::rows_and_vectors(&receiver), before);
    assert_eq!(resident_digest(&receiver, rows), resident_before);
    largest::assert_constraints_and_callback(&receiver, &calls);
    receipt
}

#[cfg(feature = "nightly-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn find_show_status_stay_fast_while_schema_replays() {
    let rows = env_u64("CONTEXTDB_SO_RESIDENT_ROWS", 10_000);
    let root = tempfile::tempdir().expect("fixture directory");
    let receipt_path = std::env::var("CONTEXTDB_SO_RECEIPT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| root.path().join("schema-overlap-receipt.json"));
    let identical = identical_schema_replay(rows, root.path()).await;
    let migration = largest_affected_migration(rows, root.path()).await;
    let receipt = serde_json::json!({
        "schema": "schema-overlap-receipt/1",
        "parameters": { "resident_rows": rows, "tables": 100 },
        "identical_schema_replay": identical,
        "largest_affected_migration": migration,
    });
    std::fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt).unwrap())
        .expect("write the receipt");
}
