//! Pull progress counter (D1b, owner-ruled 2026-08-06).
//!
//! Today a long `.sync pull` after a refused push re-reads the hub's history
//! page by page with no observable signal: `.sync status` shows a frozen
//! `Pull watermark` the whole time (it only moves once the pull fully
//! completes), and the pull's own result JSON says nothing about how much
//! work it did. An agent (or a human) watching that number cannot tell a
//! healthy multi-page catch-up from a hang.
//!
//! The fix is one counter, `pull_pages_read`, with two observation points:
//! `.sync status --json` and the `.sync pull` result JSON both carry it, and
//! it strictly increases as pages are read. These tests pin both surfaces.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncServer};
use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

/// One more row than two full pull pages (`PULL_PAGE_SIZE` is 500 in
/// `contextdb-engine::sync_client`), so a fresh edge's first pull must issue
/// at least 3 page requests to fully catch up.
const HUB_ROW_COUNT: usize = 1001;

struct RunningHub {
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("hub stop bound")
            .expect("hub task join");
    }
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("bounded real-Iroh operation")
}

async fn start_hub(root: &std::path::Path, tenant: &str, rows: usize) -> RunningHub {
    let identity = root.join("hub.db.fabric-identity.key");
    let fabric_identity =
        Arc::new(FabricIdentity::load_or_generate(&identity).expect("load real Iroh hub identity"));
    let (ticket, node_id, transport) = {
        let endpoint = within(IrohServer::bind(&format!(
            "iroh:?identity={}",
            identity.display()
        )))
        .await
        .expect("bind real Iroh hub");
        (endpoint.ticket(), endpoint.node_id(), endpoint.transport())
    };
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open hub database"));
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("hub schema");
    for i in 0..rows {
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("body".to_string(), Value::Text(format!("row-{i}"))),
            ]),
        )
        .expect("hub seed row");
    }
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db.clone(),
            transport,
            TenantId::from(tenant),
            node_id.clone(),
            fabric_identity,
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        let server = server.clone();
        async move { server.run_until(stop).await }
    });
    RunningHub { ticket, stop, task }
}

fn cli(path: &std::path::Path, hub: &RunningHub, tenant: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    command
        .arg(path)
        .arg("--write")
        .arg("--json")
        .args(["--sync-endpoint", &hub.ticket])
        .args(["--tenant-id", tenant]);
    command
}

fn run(mut command: Command, input: &str) -> (Option<i32>, String, String) {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb CLI");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn find_doc<'a>(docs: &'a [serde_json::Value], key: &str) -> Option<&'a serde_json::Value> {
    docs.iter().find(|d| d.get(key).is_some())
}

fn parse_lines(text: &str) -> Vec<serde_json::Value> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_status_json_carries_a_monotonically_advancing_pull_pages_read_field() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-pullprogress-status-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "acme", HUB_ROW_COUNT).await;
    let edge_path = dir.path().join("edge.db");

    // One session, one dial: `.sync status`, `.sync pull`, `.sync status`
    // back to back, so the baseline/after comparison is not confounded by a
    // fresh transport dial on every command.
    let (code, stdout, stderr) = run(
        cli(&edge_path, &hub, "acme"),
        ".sync status\n.sync pull\n.sync status\n",
    );
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = parse_lines(&stdout);
    let status_docs: Vec<&serde_json::Value> =
        docs.iter().filter(|d| d.get("sync").is_some()).collect();
    assert_eq!(
        status_docs.len(),
        2,
        "expected two `.sync status` documents (before and after the pull): {stdout}"
    );
    let baseline_pages = status_docs[0]["sync"]["pull_pages_read"]
        .as_u64()
        .unwrap_or_else(|| {
            panic!(
                "`.sync status --json` must carry a numeric `pull_pages_read` field so an \
                 agent can tell a healthy multi-page catch-up from a hang by polling it \
                 twice; got {}",
                status_docs[0]
            )
        });
    let after_pages = status_docs[1]["sync"]["pull_pages_read"]
        .as_u64()
        .unwrap_or_else(|| {
            panic!(
                "`.sync status --json` must still carry `pull_pages_read` after a pull: {}",
                status_docs[1]
            )
        });

    assert!(
        after_pages > baseline_pages,
        "pull_pages_read must strictly advance after a pull that needed multiple pages \
         ({HUB_ROW_COUNT} rows, page size 500): baseline={baseline_pages}, after={after_pages}"
    );
    assert!(
        after_pages >= 3,
        "{HUB_ROW_COUNT} rows at 500/page must take at least 3 page reads to fully catch up; \
         got pull_pages_read={after_pages}"
    );

    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_pull_result_json_carries_the_page_count_it_read() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-pullprogress-result-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "acme", HUB_ROW_COUNT).await;
    let edge_path = dir.path().join("edge.db");

    let (code, stdout, stderr) = run(cli(&edge_path, &hub, "acme"), ".sync pull\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = parse_lines(&stdout);
    let pull_doc = find_doc(&docs, "sync_pull")
        .unwrap_or_else(|| panic!(".sync pull --json must emit a `sync_pull` document: {stdout}"));
    let pages = pull_doc["sync_pull"]["pull_pages_read"]
        .as_u64()
        .unwrap_or_else(|| {
            panic!(
                "the pull result JSON must carry `pull_pages_read` alongside applied_rows/\
                 skipped_rows so a script sees liveness without a second `.sync status` call: \
                 {pull_doc}"
            )
        });
    assert!(
        pages >= 3,
        "a pull over {HUB_ROW_COUNT} rows at 500/page must report at least 3 pages read; \
         got {pages}"
    );

    hub.stop().await;
}
