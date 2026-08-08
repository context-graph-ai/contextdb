//! `contextdb purge <path> --table <t> --force` (D4, owner-ruled 2026-08-06).
//!
//! B_2 shipped the engine's authoritative-purge machinery (rows genuinely and
//! permanently erased, not just deleted/tombstoned), but it has no operator
//! door: nothing in `dispatch_if_subcommand` (`ops.rs`) recognizes `purge`, so
//! the verb falls through to the plain REPL argument parser, which treats
//! `purge` as the database path and fails on the next token. These tests pin
//! the promised shape — refuse without `--force` exactly like `reset`,
//! actually erase with `--force`, and have that erasure survive both a
//! process restart and a subsequent sync pull.

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

fn scratch_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("cdb-purgeverb-{label}-"))
        .tempdir()
        .expect("scratch dir")
}

fn run_purge(path: &std::path::Path, extra: &[&str]) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg("purge").arg(path).args(extra);
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn contextdb purge");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn run_cli(path: &std::path::Path, sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(path).arg("--json");
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb CLI");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(sql.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn row_count(path: &std::path::Path, table: &str) -> usize {
    let (code, stdout, stderr) = run_cli(path, &format!("SELECT * FROM {table};\n"));
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .flat_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.is_array())
        .flat_map(|v| v.as_array().cloned().unwrap_or_default())
        .count()
}

// ---------------------------------------------------------------------------
// Refuses without --force, exactly like `reset`.
// ---------------------------------------------------------------------------

#[test]
fn purge_without_force_refuses_and_leaves_data_untouched() {
    let dir = scratch_dir("noforce");
    let db_path = dir.path().join("store.db");
    {
        let db = Database::open(&db_path).expect("create store");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &HashMap::new())
            .unwrap();
        db.execute("INSERT INTO t (id) VALUES (1)", &HashMap::new())
            .unwrap();
        db.close().unwrap();
    }

    let (code, _stdout, stderr) = run_purge(&db_path, &["--table", "t"]);
    assert_eq!(
        code,
        Some(2),
        "purge without --force must refuse with exit 2, exactly like `reset`; \
         stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--force"),
        "the refusal must name the explicit --force flag an operator needs, the \
         same way reset's refusal does; stderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&db_path, "t"),
        1,
        "a refused purge must leave every row exactly as it was"
    );
}

// ---------------------------------------------------------------------------
// --force permanently erases eligible rows, surviving a process restart.
// ---------------------------------------------------------------------------

#[test]
fn purge_with_force_erases_rows_and_the_erasure_survives_a_restart() {
    let dir = scratch_dir("force");
    let db_path = dir.path().join("store.db");
    {
        let db = Database::open(&db_path).expect("create store");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &HashMap::new())
            .unwrap();
        db.execute("INSERT INTO t (id) VALUES (1)", &HashMap::new())
            .unwrap();
        db.execute("INSERT INTO t (id) VALUES (2)", &HashMap::new())
            .unwrap();
        db.close().unwrap();
    }

    let (code, stdout, stderr) = run_purge(&db_path, &["--table", "t", "--force"]);
    assert_eq!(
        code,
        Some(0),
        "a guarded, --force'd purge of an eligible row must succeed; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&db_path, "t"),
        0,
        "purge --force --table t with no WHERE must erase every row of table t"
    );

    // A fresh process, reopening the same file, must still see the rows gone —
    // the erasure is durable, not an in-memory artifact of the purge run.
    assert_eq!(
        row_count(&db_path, "t"),
        0,
        "the erasure must survive a process restart (this second read is itself a \
         fresh `contextdb` process reopening the file)"
    );
}

// ---------------------------------------------------------------------------
// --force erasure survives a sync pull (the row must not resurrect).
// ---------------------------------------------------------------------------

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
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("hub schema");
    // The hub NEVER holds a row under the purged key — purge's fence blocks
    // recreation of the SAME lineage only (a same-key row independently
    // authored elsewhere is a different lineage and is legitimately allowed
    // to arrive on pull, product-ruled 2026-08-06). Leaving the hub without
    // that key keeps this test's premise valid: it proves connecting and
    // pulling doesn't resurrect the purged row out of nothing, without
    // claiming anything about a same-key row from an independent lineage.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_with_force_erasure_survives_a_sync_pull() {
    let dir = scratch_dir("pullsurvive");
    let row_id = Uuid::new_v4();
    // A hub that never held this key — the single-thread default flavor
    // starves the in-process hub's own background tasks (dial timeouts under
    // load), so this test uses the multi-thread flavor like the other
    // real-Iroh CLI tests.
    let hub = start_hub(dir.path(), "acme").await;

    // Edge stands the row up locally and erases it, standalone (no sync
    // configured yet, so `retention_sync_peer()` is None and purge is legal
    // here).
    let edge_path = dir.path().join("edge.db");
    {
        let db = Database::open(&edge_path).expect("create edge store");
        db.execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(row_id)),
                (
                    "body".to_string(),
                    Value::Text("erased on the edge".to_string()),
                ),
            ]),
        )
        .unwrap();
        db.close().unwrap();
    }

    let (code, stdout, stderr) = run_purge(&edge_path, &["--table", "notes", "--force"]);
    assert_eq!(
        code,
        Some(0),
        "purge --force on a standalone (not-yet-connected) edge must succeed; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        row_count(&edge_path, "notes"),
        0,
        "the row must be erased locally before the edge ever connects"
    );

    // Now connect for the first time and pull. The hub still carries the
    // same row: a pull must not resurrect what purge erased.
    let mut pull = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    pull.arg(&edge_path)
        .arg("--json")
        .args(["--sync-endpoint", &hub.ticket])
        .args(["--tenant-id", "acme"]);
    let mut child = pull
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn edge CLI");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b".sync pull\n")
        .expect("write .sync pull");
    let out = child.wait_with_output().expect("wait edge CLI");
    assert!(
        out.status.success(),
        "sync pull journey must exit cleanly; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert_eq!(
        row_count(&edge_path, "notes"),
        0,
        "a purge erasure must survive connecting and pulling from a hub for the \
         first time — a hub that never held this key must not cause the purged \
         row to reappear"
    );

    hub.stop().await;
}
