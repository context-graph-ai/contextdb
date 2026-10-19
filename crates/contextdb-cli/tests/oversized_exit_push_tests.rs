//! Quitting the CLI pushes a pending oversized unit.
//!
//! Promise (root intent, oversized units): a dependency-complete unit larger
//! than one Iroh frame travels as resumable chunks and commits once at the
//! hub. The CLI's final push on exit is a default path, so it meets the same
//! promise even though it funds its ordinary reads from the session's read
//! memory: reading the pending unit back from the store is engine work funded
//! by the writer budget, not by that 16 MiB read limit.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncServer};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct RunningHub {
    db: Arc<Database>,
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
            db.clone(),
            transport,
            TenantId::from(tenant),
            node_id.clone(),
            fabric_identity,
        )
        .expect("construct SyncServer fixture"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        let server = server.clone();
        async move { server.run_until(stop).await }
    });
    RunningHub {
        db,
        ticket,
        stop,
        task,
    }
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

const TENANT: &str = "oversized-exit-push";
const PARENT_ID: &str = "dddddddd-4444-4ddd-8ddd-dddddddddddd";
const CHILD_ID: &str = "eeeeeeee-5555-4eee-8eee-eeeeeeeeeeee";
const BODY_BYTES: usize = 64 * 1024 * 1024;

fn seed_pending_oversized_unit(path: &std::path::Path) {
    let db = Database::open(path).expect("open edge store to seed it");
    let empty = std::collections::HashMap::new();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &empty,
    )
    .expect("declare parents");
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &empty,
    )
    .expect("declare children");
    db.execute(
        &format!("INSERT INTO parents (id, body) VALUES ('{PARENT_ID}', 'decision')"),
        &empty,
    )
    .expect("insert parent");
    db.execute(
        &format!(
            "INSERT INTO children (id, parent_id, body) VALUES ('{CHILD_ID}', '{PARENT_ID}', 'outcome')"
        ),
        &empty,
    )
    .expect("insert child");
    db.execute(
        &format!("UPDATE parents SET body = $body WHERE id = '{PARENT_ID}'"),
        &std::collections::HashMap::from([(
            "body".to_string(),
            Value::Text("x".repeat(BODY_BYTES)),
        )]),
    )
    .expect("grow the parent past one Iroh frame");
    db.close().expect("close the seeded edge store");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quitting_the_cli_pushes_a_pending_oversized_unit_and_exits_clean() {
    let dir = tempfile::Builder::new()
        .prefix("cdb-oversized-exitpush-")
        .tempdir()
        .expect("scratch dir");
    let hub = start_hub(dir.path(), TENANT).await;
    let edge = dir.path().join("edge.db");
    seed_pending_oversized_unit(&edge);

    // The session runs no `.sync` command: only the final push on exit
    // offers the pending unit, read back from the reopened store.
    let (code, stdout, stderr) = run(
        cli(&edge, &hub, TENANT),
        "SELECT COUNT(*) AS n FROM children;\n",
    );
    assert_eq!(
        code,
        Some(0),
        "the exit push delivers the oversized unit; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let parent_id = Value::Uuid(PARENT_ID.parse().expect("parent UUID"));
    let child_id = Value::Uuid(CHILD_ID.parse().expect("child UUID"));
    // The check reads the 64 MiB row back: a caller read funded explicitly.
    let check_read = contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        512 * 1024 * 1024,
    );
    let parent = hub
        .db
        .point_lookup_with_read_memory_for_test(
            "parents",
            "id",
            &parent_id,
            check_read.child().expect("admit the check read"),
        )
        .expect("hub point lookup")
        .expect("the hub committed the oversized parent");
    assert_eq!(
        parent.values.get("body"),
        Some(&Value::Text("x".repeat(BODY_BYTES))),
        "the hub holds the complete oversized revision"
    );
    assert!(
        hub.db
            .point_lookup("children", "id", &child_id, hub.db.snapshot())
            .expect("hub point lookup")
            .is_some(),
        "the hub committed the dependent child with its parent"
    );
    hub.stop().await;
}
