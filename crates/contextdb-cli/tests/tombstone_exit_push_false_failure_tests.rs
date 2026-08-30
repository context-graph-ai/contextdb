//! Tombstone exit-push false failure (recipe: `skills/sync/SKILL.md` §7).
//!
//! Promise: after a delete has synced everywhere and every store agrees
//! (`COUNT` reads 0 on all machines), quitting the CLI on any machine
//! succeeds — exit 0 means "your session ended clean."
//!
//! Reality: once an edge has pulled a tombstone, the VERY NEXT fresh process
//! on that same store — even one that runs nothing but a `SELECT`, no
//! `.sync` command at all — still attempts its unconditional final-push-on-
//! exit, because the persisted push watermark is still behind the pulled
//! tombstone's LSN. That re-offers the tombstone to the hub, whose replay
//! guard refuses it (`class:sync`, "strict received row ... replays a
//! lineage terminated by an accepted delete"), and the CLI exits 1 even
//! though the store state is fully correct. Any script, agent, or grader
//! that trusts the exit code sees a false failure on a healthy converged
//! store.

use contextdb_core::TenantId;
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncServer};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded real-Iroh operation")
}

async fn start_hub(root: &std::path::Path, tenant: &str) -> RunningHub {
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
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
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

fn row_count(stdout: &str) -> Option<i64> {
    // A successful ordinary SELECT is one namespaced result document carrying
    // its rows, not a bare array.
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|document| document.get("result")?.get("rows")?.as_array().cloned())
        .flatten()
        .find_map(|row| row.get("n").and_then(|n| n.as_i64()))
}

const RECORD_ID: &str = "cccccccc-3333-4ccc-8ccc-cccccccccccc";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_restart_immediately_after_pulling_a_tombstone_exits_clean() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-tombstone-exitpush-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), "lifecycle").await;
    let edge_x = dir.path().join("edge-x.db");
    let edge_y = dir.path().join("edge-y.db");

    // Edge X creates + pushes a row.
    let (code, stdout, stderr) = run(
        cli(&edge_x, &hub, "lifecycle"),
        &format!(
            "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST;\n\
             INSERT INTO records (id, body) VALUES ('{RECORD_ID}', 'will be deleted');\n\
             .sync push\n"
        ),
    );
    assert_eq!(
        code,
        Some(0),
        "edge X's initial push must exit clean; stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Edge Y pulls the row.
    let (code, stdout, stderr) = run(
        cli(&edge_y, &hub, "lifecycle"),
        ".sync pull\nSELECT COUNT(*) AS n FROM records;\n",
    );
    assert_eq!(
        code,
        Some(0),
        "edge Y's initial pull must exit clean; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&stdout),
        Some(1),
        "edge Y must have the row before the delete"
    );

    // Edge X deletes and pushes the tombstone.
    let (code, stdout, stderr) = run(
        cli(&edge_x, &hub, "lifecycle"),
        &format!("DELETE FROM records WHERE id = '{RECORD_ID}';\n.sync push\n"),
    );
    assert_eq!(
        code,
        Some(0),
        "edge X's delete push must exit clean; stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // Edge Y pulls the tombstone — this session itself is expected to (and,
    // per the skill's live recipe, does) exit clean.
    let (code, stdout, stderr) = run(
        cli(&edge_y, &hub, "lifecycle"),
        ".sync pull\nSELECT COUNT(*) AS n FROM records;\n",
    );
    assert_eq!(
        code,
        Some(0),
        "edge Y's pull of the tombstone must exit clean; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&stdout),
        Some(0),
        "edge Y must have converged to 0 rows"
    );

    // THE BUG: a fresh process on edge Y, running nothing but a SELECT (no
    // `.sync` command at all), still attempts its unconditional
    // final-push-on-exit — which re-offers the tombstone this store already
    // pulled and gets refused by the hub's replay guard.
    let (code, stdout, stderr) = run(
        cli(&edge_y, &hub, "lifecycle"),
        "SELECT COUNT(*) AS n FROM records;\n",
    );
    assert_eq!(
        code,
        Some(0),
        "a fresh process on a store that has already fully converged (COUNT 0 \
         on every machine) must exit 0 — quitting must not re-offer a \
         tombstone this store already pulled and then fail on the hub's own \
         replay refusal of it; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&stdout),
        Some(0),
        "the store must still read as converged regardless of the exit-push outcome"
    );

    hub.stop().await;
}
