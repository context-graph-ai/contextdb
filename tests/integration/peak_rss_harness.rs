use contextdb_core::{Direction, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;

const PEAK_RSS_SUBPROCESS_ENV: &str = "CONTEXTDB_PEAK_RSS_SUBPROCESS";
const PEAK_RSS_DB_PATH_ENV: &str = "CONTEXTDB_PEAK_RSS_DB_PATH";

/// I ran a representative persisted mixed workload in a child process, and peak RSS stayed
/// within the edge-device envelope instead of growing without bound.
#[test]
fn a10_05_peak_rss_within_2gb() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("peak-rss.db");

    let mut child = Command::new(env::current_exe().expect("current integration test binary"))
        .args([
            "--exact",
            "peak_rss_harness::peak_rss_workload_subprocess",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PEAK_RSS_SUBPROCESS_ENV, "1")
        .env(PEAK_RSS_DB_PATH_ENV, &db_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("peak RSS harness should spawn");

    let pid = child.id();
    let start = Instant::now();
    let timeout = Duration::from_secs(120);
    let mut peak_rss_kib = 0usize;

    while start.elapsed() < timeout {
        if let Some(rss_kib) = linux_rss_kib(pid) {
            peak_rss_kib = peak_rss_kib.max(rss_kib);
        }
        if child.try_wait().expect("poll peak RSS harness").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let output = if child
        .try_wait()
        .expect("final peak RSS harness poll")
        .is_none()
    {
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("collect timed out harness output")
    } else {
        child.wait_with_output().expect("collect harness output")
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "peak RSS harness failed before verification\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("WORKLOAD_DONE"),
        "peak RSS harness never reported workload completion\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        peak_rss_kib > 0,
        "rss sampler did not observe child process memory usage"
    );

    let peak_rss_bytes = peak_rss_kib.saturating_mul(1024);
    let budget_bytes = 2usize * 1024 * 1024 * 1024;
    assert!(
        peak_rss_bytes <= budget_bytes,
        "peak RSS exceeded 2 GiB budget: observed={} KiB\nstdout:\n{stdout}\nstderr:\n{stderr}",
        peak_rss_kib
    );
}

#[test]
fn peak_rss_workload_subprocess() {
    if env::var_os(PEAK_RSS_SUBPROCESS_ENV).is_none() {
        return;
    }
    let db_path = PathBuf::from(env::var_os(PEAK_RSS_DB_PATH_ENV).expect("db path env"));
    run_peak_rss_workload(&db_path).expect("peak RSS workload should complete");
}

fn run_peak_rss_workload(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("WORKLOAD_START");

    let db = Database::open(db_path)?;
    db.execute(
        "CREATE TABLE rss_items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(16))",
        &HashMap::new(),
    )?;

    let mut chain = Vec::<Uuid>::new();
    let mut batches = 0usize;
    for batch in 0..8usize {
        let tx = db.begin_or_panic();
        for i in 0..250usize {
            let ordinal = batch * 250 + i;
            let node_id = Uuid::from_u128(ordinal as u128 + 1);
            let row_id = db.insert_row(
                tx,
                "rss_items",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(node_id)),
                    (
                        "name".to_string(),
                        Value::Text(format!("rss-item-{ordinal:04}")),
                    ),
                ]),
            )?;
            let vector = (0..16usize)
                .map(|j| ((ordinal + j) % 17) as f32 / 17.0)
                .collect::<Vec<_>>();
            db.insert_vector(
                tx,
                VectorIndexRef::new("rss_items", "embedding"),
                row_id,
                vector,
            )?;
            if let Some(&previous) = chain.last() {
                db.insert_edge(
                    tx,
                    previous,
                    node_id,
                    "DEPENDS_ON".to_string(),
                    HashMap::new(),
                )?;
            }
            chain.push(node_id);
        }
        db.commit(tx)?;
        batches += 1;
    }

    db.close()?;
    let reopened = Database::open(db_path)?;
    let snapshot = reopened.snapshot();
    let rows = reopened.scan("rss_items", snapshot)?;
    assert_eq!(rows.len(), 2000, "all relational rows must survive reopen");

    let bfs = reopened.query_bfs(Uuid::from_u128(1), None, Direction::Outgoing, 2, snapshot)?;
    assert!(
        !bfs.nodes.is_empty(),
        "graph traversal after reopen must return reachable nodes"
    );

    let vector_results = reopened.query_vector(
        VectorIndexRef::new("rss_items", "embedding"),
        &[0.0; 16],
        5,
        None,
        snapshot,
    )?;
    assert!(
        !vector_results.is_empty(),
        "vector query after reopen must return persisted rows"
    );

    println!(
        "WORKLOAD_DONE batches={batches} rows={} graph_nodes={} vector_hits={}",
        rows.len(),
        bfs.nodes.len(),
        vector_results.len()
    );
    Ok(())
}

fn linux_rss_kib(pid: u32) -> Option<usize> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse::<usize>().ok()
    })
}
