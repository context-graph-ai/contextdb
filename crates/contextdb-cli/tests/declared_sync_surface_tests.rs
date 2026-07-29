//! Operators can declare sync behavior in DDL; they cannot override it per session.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use contextdb_server::SyncServer;
use contextdb_server::transport::iroh::IrohServer;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn run_cli_at(path: &std::path::Path, input: &str, json: bool) -> (String, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    if json {
        command.arg("--json");
    }
    let mut child = command
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb CLI");
    child
        .stdin
        .as_mut()
        .expect("CLI stdin")
        .write_all(input.as_bytes())
        .expect("write CLI input");
    drop(child.stdin.take());
    let (status, stdout, stderr) = wait_with_bounded_output(child, "simple CLI journey");
    assert!(
        status.success(),
        "simple CLI journey exits cleanly: {status}"
    );
    (stdout, stderr)
}

fn run_cli(input: &str, json: bool) -> (String, String) {
    run_cli_at(std::path::Path::new(":memory:"), input, json)
}

fn assert_no_role_mechanic_words(rendered: &str, surface: &str) {
    let rendered = rendered.to_ascii_lowercase();
    for forbidden in [
        "insertifnotexists",
        "insert_if_not_exists",
        "insert if not exists",
        "serverwins",
        "server_wins",
        "server wins",
        "edgewins",
        "edge_wins",
        "edge wins",
        "latestwins",
        "latest_wins",
        "latest wins",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "{surface} must not expose the role-relative engine word {forbidden}: {rendered}"
        );
    }
}

#[test]
fn removed_sync_commands_are_unknown_and_role_mechanic_words_are_absent() {
    let (help, help_err) = run_cli(".help\n.quit\n", false);
    let help_text = format!("{help}\n{help_err}");
    for forbidden in [
        ".sync policy",
        ".sync direction",
        "InsertIfNotExists",
        "ServerWins",
        "EdgeWins",
        "LatestWins",
    ] {
        assert!(
            !help_text.contains(forbidden),
            "CLI help must not present role mechanics as policy choices: {forbidden}; output: {help_text}"
        );
    }

    for (name, command) in [
        ("policy", ".sync policy notes LatestWins\n.quit\n"),
        ("direction", ".sync direction notes Both\n.quit\n"),
    ] {
        let (output, error) = run_cli(command, false);
        let rendered = format!("{output}\n{error}").to_ascii_lowercase();
        assert!(
            rendered.contains("unknown") || rendered.contains("unrecognized"),
            "removed .sync {name} must be independently unknown, not accepted or aliased: {rendered}"
        );
    }
}

#[test]
fn public_sync_output_uses_only_declared_policy_words() {
    let sql = "CREATE TABLE notes (id UUID PRIMARY KEY) SYNC TWO WAY SYNC CONFLICT KEEP FIRST;\n.schema notes\n.quit\n";
    let (output, error) = run_cli(sql, false);
    let rendered = format!("{output}\n{error}");
    assert!(
        rendered.contains("KEEP FIRST"),
        "declared policy must render: {rendered}"
    );
    assert!(
        rendered.contains("SYNC TWO WAY"),
        "declared direction must render: {rendered}"
    );
    for forbidden in ["InsertIfNotExists", "ServerWins", "EdgeWins", "LatestWins"] {
        assert!(
            !rendered.contains(forbidden),
            "public schema output must not expose engine role vocabulary: {forbidden}; output: {rendered}"
        );
    }

    let (json_output, json_error) = run_cli(sql, true);
    assert!(
        json_error.is_empty(),
        "declared JSON output must not fail: {json_error}"
    );
    let document = json_output
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value.get("table").is_some())
        .unwrap_or_else(|| panic!(".schema --json must emit its table document: {json_output}"));
    assert_eq!(document["sync_direction"], "two_way");
    assert_eq!(document["conflict_policy"], "keep_first");
    assert_no_role_mechanic_words(&document.to_string(), "ordinary-table JSON schema");

    let dir = tempfile::tempdir().expect("system-table tempdir");
    let path = dir.path().join("system-tables.db");
    let db = Database::open(&path).expect("open system-table database");
    install_work_ledger_schema(&db).expect("install private work-ledger schema");
    drop(db);
    let (system_output, system_error) = run_cli_at(&path, ".schema work_claims\n.quit\n", false);
    assert_no_role_mechanic_words(
        &format!("{system_output}\n{system_error}"),
        "system-table human schema",
    );
    let (system_json, system_json_error) = run_cli_at(&path, ".schema work_claims\n.quit\n", true);
    assert!(
        system_json_error.is_empty(),
        "system-table JSON schema must not fail: {system_json_error}"
    );
    assert_no_role_mechanic_words(&system_json, "system-table JSON schema");
}

struct RunningHub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("bounded real-Iroh operation")
}

async fn start_hub(root: &std::path::Path, tenant: &str) -> RunningHub {
    let identity = root.join("hub.db.fabric-identity.key");
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
    let server = Arc::new(SyncServer::with_authenticated_transport_for_test(
        db.clone(),
        transport,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = stop.clone();
        let server = server.clone();
        async move { server.run_until(stop).await }
    });
    RunningHub {
        db,
        ticket,
        node_id,
        stop,
        task,
    }
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

fn insert(db: &Database, id: Uuid, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert hub winner");
}

fn body(db: &Database, id: Uuid) -> Option<String> {
    let rows = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect("select body")
        .rows;
    rows.first().and_then(|row| match row.first() {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    })
}

fn preseed_offline_edge(path: &std::path::Path, id: Uuid, loser: &str) {
    let db = Database::open(path).expect("open offline edge database");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("offline edge schema");
    insert(&db, id, loser);
}

fn delete_offline_edge_row(path: &std::path::Path, id: Uuid) {
    let db = Database::open(path).expect("reopen pulled edge offline");
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("offline edge delete");
}

fn wait_for_exit(child: &mut Child, name: &str) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().expect("poll child") {
            Some(status) => return status,
            None if Instant::now() < deadline => std::thread::yield_now(),
            None => {
                child.kill().expect("stop overdue child");
                let _ = child.wait();
                panic!("{name} did not exit within 30 seconds");
            }
        }
    }
}

type ChildOutput = (&'static str, Option<String>);

fn start_line_reader<R: Read + Send + 'static>(
    reader: R,
    stream: &'static str,
    tx: mpsc::Sender<ChildOutput>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let line = line.unwrap_or_else(|error| format!("<{stream} read error: {error}>"));
            if tx.send((stream, Some(line))).is_err() {
                return;
            }
        }
        let _ = tx.send((stream, None));
    })
}

fn start_output_readers(
    child: &mut Child,
) -> (
    mpsc::Receiver<ChildOutput>,
    Vec<std::thread::JoinHandle<()>>,
) {
    let (tx, rx) = mpsc::channel();
    let stdout = start_line_reader(
        child.stdout.take().expect("child stdout"),
        "stdout",
        tx.clone(),
    );
    let stderr = start_line_reader(child.stderr.take().expect("child stderr"), "stderr", tx);
    (rx, vec![stdout, stderr])
}

fn collect_reader_output(
    rx: &mpsc::Receiver<ChildOutput>,
    readers: Vec<std::thread::JoinHandle<()>>,
    mut closed: usize,
    name: &str,
) -> (String, String) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    while closed < 2 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (stream, line) = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("{name} output readers did not close within 30 seconds"));
        match (stream, line) {
            ("stdout", Some(line)) => stdout.push(line),
            ("stderr", Some(line)) => stderr.push(line),
            (_, None) => closed += 1,
            (other, Some(line)) => panic!("unknown child stream {other}: {line}"),
        }
    }
    for reader in readers {
        reader.join().expect("child output reader join");
    }
    (stdout.join("\n"), stderr.join("\n"))
}

fn wait_with_bounded_output(mut child: Child, name: &str) -> (ExitStatus, String, String) {
    let (rx, readers) = start_output_readers(&mut child);
    let status = wait_for_exit(&mut child, name);
    let (stdout, stderr) = collect_reader_output(&rx, readers, 0, name);
    (status, stdout, stderr)
}

fn cli(path: &std::path::Path, hub: &RunningHub, tenant: &str, debounce_ms: u64) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    command
        .arg(path)
        .arg("--json")
        .args(["--sync-endpoint", &hub.ticket])
        .args(["--tenant-id", tenant])
        .args(["--sync-debounce-ms", &debounce_ms.to_string()]);
    command
}

fn json_leaves<'a>(document: &'a serde_json::Value, leaves: &mut Vec<&'a serde_json::Value>) {
    match document {
        serde_json::Value::Array(values) => {
            for value in values {
                json_leaves(value, leaves);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                json_leaves(value, leaves);
            }
        }
        leaf => leaves.push(leaf),
    }
}

fn refusal_event<'a>(
    lines: impl IntoIterator<Item = &'a str>,
    id: Uuid,
    hub: &RunningHub,
    position: u64,
    mutation_kind: &str,
) -> serde_json::Value {
    let id = id.to_string();
    let author = hub.node_id.as_str();
    let lines = lines.into_iter().collect::<Vec<_>>();
    let document = lines
        .iter()
        .copied()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|document| {
            let mut leaves = Vec::new();
            json_leaves(document, &mut leaves);
            let rendered = document.to_string().to_ascii_lowercase();
            (rendered.contains("conflict") || rendered.contains("refus"))
                && leaves
                    .iter()
                    .any(|leaf| leaf.as_str() == Some("notes"))
                && leaves
                    .iter()
                    .any(|leaf| leaf.as_str() == Some(id.as_str()))
                && leaves
                    .iter()
                    .any(|leaf| leaf.as_str() == Some(mutation_kind))
                && leaves
                    .iter()
                    .any(|leaf| leaf.as_str() == Some(author))
                && leaves.iter().any(|leaf| leaf.as_u64() == Some(position))
        })
        .unwrap_or_else(|| {
            panic!(
                "one JSON conflict/refusal event must carry the exact table, natural key, mutation kind, winning author, and hub position; output: {lines:#?}"
            )
        });
    assert_no_role_mechanic_words(&document.to_string(), "manual refusal event");
    document
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_push_and_auto_sync_render_complete_refusal_diagnostic() {
    let root = tempfile::tempdir().expect("tempdir");
    let tenant = "cli-refusal";
    let hub = start_hub(root.path(), tenant).await;
    for _ in 0..12 {
        insert(&hub.db, Uuid::new_v4(), "position decoy");
    }
    let manual_id = Uuid::new_v4();
    insert(&hub.db, manual_id, "hub winner");
    let manual_position = hub.db.current_lsn().0;
    let manual_path = root.path().join("manual-edge.db");
    preseed_offline_edge(&manual_path, manual_id, "edge loser");
    let manual = cli(&manual_path, &hub, tenant, 60_000)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn manual CLI");
    let manual_sql = ".sync push\n.quit\n";
    let mut manual = manual;
    manual
        .stdin
        .as_mut()
        .expect("manual stdin")
        .write_all(manual_sql.as_bytes())
        .expect("write manual journey");
    drop(manual.stdin.take());
    let (manual_status, manual_stdout, manual_stderr) =
        wait_with_bounded_output(manual, "manual push CLI");
    let _manual_status = manual_status;
    let _manual_event = refusal_event(
        manual_stdout.lines().chain(manual_stderr.lines()),
        manual_id,
        &hub,
        manual_position,
        "edit",
    );
    let manual_db = Database::open(&manual_path).expect("reopen manual edge");
    assert_eq!(body(&hub.db, manual_id).as_deref(), Some("hub winner"));
    assert_eq!(body(&manual_db, manual_id).as_deref(), Some("hub winner"));

    let auto_id = Uuid::new_v4();
    insert(&hub.db, auto_id, "second hub winner");
    let auto_position = hub.db.current_lsn().0;
    let auto_path = root.path().join("auto-edge.db");
    let mut pull = cli(&auto_path, &hub, tenant, 60_000)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn real pull CLI");
    pull.stdin
        .as_mut()
        .expect("pull stdin")
        .write_all(b".sync pull\n.quit\n")
        .expect("pull hub winner to edge");
    drop(pull.stdin.take());
    let (pull_status, _pull_stdout, pull_stderr) =
        wait_with_bounded_output(pull, "auto setup pull CLI");
    assert!(
        pull_status.success(),
        "auto setup pull must exit cleanly: {pull_status}; {pull_stderr}"
    );
    let pulled_edge = Database::open(&auto_path).expect("inspect pulled edge");
    assert_eq!(
        body(&pulled_edge, auto_id).as_deref(),
        Some("second hub winner")
    );
    drop(pulled_edge);
    delete_offline_edge_row(&auto_path, auto_id);
    let mut auto = cli(&auto_path, &hub, tenant, 0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn auto-sync CLI");
    let (report_rx, readers) = start_output_readers(&mut auto);
    let notification_id = Uuid::new_v4();
    let auto_sql = format!(
        "INSERT INTO notes (id, body) VALUES ('{notification_id}', 'post-launch notification');\n"
    );
    let mut auto_stdin = auto.stdin.take().expect("auto stdin");
    auto_stdin
        .write_all(auto_sql.as_bytes())
        .expect("write auto trigger DML");
    auto_stdin.flush().expect("flush auto trigger DML");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut auto_reports = Vec::new();
    let mut auto_closed = 0;
    let auto_id_string = auto_id.to_string();
    let auto_author = hub.node_id.as_str();
    let auto_event = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (stream, line) = report_rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("bounded actual auto-sync report event: {auto_reports:?}"));
        let Some(line) = line else {
            auto_closed += 1;
            assert!(
                auto_closed < 2,
                "auto CLI closed both report pipes before the refusal: {auto_reports:?}"
            );
            auto_reports.push(format!("{stream}: <closed>"));
            continue;
        };
        auto_reports.push(format!("{stream}: {line}"));
        if let Some(document) = std::iter::once(line.as_str())
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|document| {
                let mut leaves = Vec::new();
                json_leaves(document, &mut leaves);
                let rendered = document.to_string().to_ascii_lowercase();
                (rendered.contains("conflict") || rendered.contains("refus"))
                    && leaves.iter().any(|leaf| leaf.as_str() == Some("notes"))
                    && leaves
                        .iter()
                        .any(|leaf| leaf.as_str() == Some(auto_id_string.as_str()))
                    && leaves.iter().any(|leaf| leaf.as_str() == Some("delete"))
                    && leaves.iter().any(|leaf| leaf.as_str() == Some(auto_author))
                    && leaves
                        .iter()
                        .any(|leaf| leaf.as_u64() == Some(auto_position))
            })
        {
            break document;
        }
    };
    assert_no_role_mechanic_words(&auto_event.to_string(), "automatic refusal event");
    auto_stdin.write_all(b".quit\n").expect("quit auto CLI");
    drop(auto_stdin);
    let status = wait_for_exit(&mut auto, "auto CLI");
    assert!(status.success(), "auto CLI exits cleanly: {status}");
    let _ = collect_reader_output(&report_rx, readers, auto_closed, "auto CLI");
    let auto_db = Database::open(&auto_path).expect("reopen auto edge");
    assert_eq!(body(&hub.db, auto_id).as_deref(), Some("second hub winner"));
    assert_eq!(
        body(&auto_db, auto_id).as_deref(),
        Some("second hub winner")
    );
    hub.stop().await;
}
