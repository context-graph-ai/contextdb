//! CLI pull liveness (owner-folded correction, 2026-08-06) — the ruled D1
//! promise ("you can tell a long `.sync pull` is alive, not stuck") is
//! structurally unmet as shipped: the interactive-only progress printer
//! (`pull_with_interactive_progress`, `repl.rs`) checks
//! `std::io::stdin().is_terminal()` before it ever prints a line, so a
//! SCRIPTED session (stdin piped, exactly what these tests and every
//! non-interactive caller look like) gets NO liveness signal at all during a
//! long pull — only the cumulative `pull_pages_read` counter once the whole
//! pull has already finished. The `repl.rs:1134`-area doc comment claiming an
//! agent can poll `.sync status --json` twice mid-pull is false: `.sync pull`
//! blocks the one CLI session for its whole duration, and no second process
//! can open the same store while it runs.
//!
//! Folded contract:
//! (a) a long-running `.sync pull` STREAMS machine-readable progress —
//!     periodic JSON Lines notices on stderr, shape
//!     `{"sync_pull_progress":{"pages_read":N}}` — emitted even when stdout
//!     is not a terminal;
//! (b) `.sync status --json` gains `pull_in_progress` (bool);
//! (c) the pull result JSON carries the CURRENT pull's page count, distinct
//!     from the cumulative `pull_pages_read` counter, so a caller can tell
//!     "this pull did nothing" from "this pull did a lot" without having to
//!     diff two cumulative reads itself.
//!
//! Discipline: no sleeps, no elapsed-time assertions — every assertion here
//! is on stderr/JSON content and counts, never on how long anything took.

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

fn parse_lines(text: &str) -> Vec<serde_json::Value> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// (a) stderr streams progress notices even when stdout is not a terminal.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scripted_multi_page_pull_streams_progress_notices_on_stderr() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-pull-liveness-stderr-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "acme", HUB_ROW_COUNT).await;
    let edge_path = dir.path().join("edge.db");

    // This process's stdin/stdout/stderr are ALL piped (Stdio::piped()) —
    // exactly the "stdout is not a terminal" case the promise must still
    // cover.
    let (code, stdout, stderr) = run(cli(&edge_path, &hub, "acme"), ".sync pull\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let progress_notices: Vec<u64> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line.trim()).ok())
        .filter_map(|doc| doc.get("sync_pull_progress")?.get("pages_read")?.as_u64())
        .collect();

    assert!(
        progress_notices.len() >= 2,
        "a pull needing at least 3 page requests ({HUB_ROW_COUNT} rows at 500/page) must \
         stream multiple periodic sync_pull_progress notices to stderr even when stdout is \
         piped (non-interactive), not just a final summary line; got {} notices in stderr:\n{stderr}",
        progress_notices.len()
    );
    assert!(
        progress_notices.windows(2).all(|pair| pair[0] <= pair[1]),
        "streamed pages_read values must be non-decreasing: {progress_notices:?}"
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// (b) `.sync status --json` gains a `pull_in_progress` boolean field.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_status_json_carries_a_pull_in_progress_field() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-pull-liveness-inprogress-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "acme", HUB_ROW_COUNT).await;
    let edge_path = dir.path().join("edge.db");

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
    for (label, doc) in [("before", status_docs[0]), ("after", status_docs[1])] {
        assert!(
            doc["sync"]["pull_in_progress"].is_boolean(),
            "`.sync status --json` must carry a boolean `pull_in_progress` field ({label} \
             the pull); got {doc}"
        );
    }
    // Both reads happen sequentially, outside any pull (the CLI's own
    // session blocks for the whole pull duration — a status read in the
    // SAME script can only ever observe before/after, never during), so
    // both must read false here; a library consumer sharing the same
    // SyncClient handle across threads is what can observe `true`
    // (see the engine-level pull_in_progress test).
    assert_eq!(
        status_docs[0]["sync"]["pull_in_progress"], false,
        "no pull has started yet: {}",
        status_docs[0]
    );
    assert_eq!(
        status_docs[1]["sync"]["pull_in_progress"], false,
        "the pull already finished by the time this status ran: {}",
        status_docs[1]
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// (c) the pull result JSON carries the CURRENT pull's page count, distinct
// from the cumulative pull_pages_read counter.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_pull_reports_zero_pages_for_this_pull_while_the_cumulative_counter_holds() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-pull-liveness-current-count-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "acme", HUB_ROW_COUNT).await;
    let edge_path = dir.path().join("edge.db");

    let (code, stdout, stderr) = run(cli(&edge_path, &hub, "acme"), ".sync pull\n.sync pull\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = parse_lines(&stdout);
    let pull_docs: Vec<&serde_json::Value> = docs
        .iter()
        .filter(|d| d.get("sync_pull").is_some())
        .collect();
    assert_eq!(
        pull_docs.len(),
        2,
        "expected two sync_pull result documents: {stdout}"
    );

    let first = &pull_docs[0]["sync_pull"];
    let second = &pull_docs[1]["sync_pull"];

    let first_current = first["pull_pages_read_this_pull"]
        .as_u64()
        .unwrap_or_else(|| {
            panic!(
                "the pull result JSON must carry a `pull_pages_read_this_pull` field distinct \
             from the cumulative `pull_pages_read` counter, so a caller can read THIS \
             pull's page count without diffing two cumulative reads itself; got {first}"
            )
        });
    let second_current = second["pull_pages_read_this_pull"]
        .as_u64()
        .expect("second pull result must also carry pull_pages_read_this_pull");
    let first_cumulative = first["pull_pages_read"]
        .as_u64()
        .expect("cumulative field on first pull");
    let second_cumulative = second["pull_pages_read"]
        .as_u64()
        .expect("cumulative field on second pull");

    assert!(
        first_current >= 3,
        "the first pull needed at least 3 pages ({HUB_ROW_COUNT} rows at 500/page): {first}"
    );
    assert_eq!(
        second_current, 0,
        "the second pull found nothing new to fetch, so its OWN page count must read 0, \
         distinct from the cumulative counter which must hold steady: {second}"
    );
    assert_eq!(
        first_cumulative, second_cumulative,
        "the cumulative pull_pages_read counter must not move on a no-op second pull: \
         first={first_cumulative}, second={second_cumulative}"
    );

    hub.stop().await;
}
