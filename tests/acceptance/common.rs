#![allow(dead_code)]

use contextdb_core::{Direction, Error, Lsn, Value, VersionedRow};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::{MessageType, PullRequest, PullResponse, decode, encode};
use contextdb_server::subjects::pull_subject;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, LazyLock, Once, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

static BUILD_RELEASE_BINARIES: Once = Once::new();
static NATS_TEST_LOCK: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(1)));

pub(crate) struct NatsFixture {
    _lock: OwnedSemaphorePermit,
    _container: ContainerAsync<GenericImage>,
    pub(crate) nats_url: String,
    pub(crate) ws_url: String,
}

pub(crate) fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn target_dir() -> PathBuf {
    workspace_root().join("target").join("release")
}

pub(crate) fn cli_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-cli");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

pub(crate) fn server_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-server");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

pub(crate) fn ensure_release_binaries() {
    BUILD_RELEASE_BINARIES.call_once(|| {
        let status = Command::new("cargo")
            .current_dir(workspace_root())
            .args([
                "build",
                "--release",
                "-p",
                "contextdb-cli",
                "-p",
                "contextdb-server",
            ])
            .status()
            .expect("release build command should start");
        assert!(
            status.success(),
            "release build for CLI/server should succeed"
        );
    });
}

pub(crate) fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub(crate) fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

pub(crate) fn values(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    params(pairs)
}

pub(crate) fn output_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub(crate) fn run_cli_script(db_path: &Path, extra_args: &[&str], script: &str) -> Output {
    ensure_release_binaries();
    let mut command = Command::new(cli_bin());
    command.arg(db_path);
    command.args(extra_args);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn().expect("CLI should spawn");
    match child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(script.as_bytes())
    {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::BrokenPipe => {}
        Err(err) => panic!("write script to CLI: {err}"),
    }
    child.wait_with_output().expect("CLI should finish")
}

pub(crate) fn run_cli_script_allow_startup_failure(
    db_path: &Path,
    extra_args: &[&str],
    script: &str,
) -> Output {
    run_cli_script_allow_startup_failure_with_timeout(
        db_path,
        extra_args,
        script,
        Duration::from_secs(30),
    )
}

pub(crate) fn run_cli_script_allow_startup_failure_with_timeout(
    db_path: &Path,
    extra_args: &[&str],
    script: &str,
    timeout: Duration,
) -> Output {
    ensure_release_binaries();
    let mut command = Command::new(cli_bin());
    command.arg(db_path);
    command.args(extra_args);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn().expect("CLI should spawn");
    let stdin = child.stdin.take().expect("stdin pipe");
    let script = script.as_bytes().to_vec();
    let writer = thread::spawn(move || {
        let mut stdin = stdin;
        stdin.write_all(&script)
    });
    let output = wait_for_child_output(child, timeout, "CLI should finish");
    match writer.join().expect("stdin writer thread should join") {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::BrokenPipe => {}
        Err(err) => panic!("write script to CLI: {err}"),
    }
    output
}

pub(crate) fn run_cli_script_from_file(
    db_path: &Path,
    extra_args: &[&str],
    script_path: &Path,
) -> Output {
    ensure_release_binaries();
    let input = File::open(script_path).expect("script file should open");
    Command::new(cli_bin())
        .arg(db_path)
        .args(extra_args)
        .stdin(Stdio::from(input))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("CLI should run")
}

pub(crate) fn spawn_cli(db_path: &Path, extra_args: &[&str]) -> Child {
    ensure_release_binaries();
    Command::new(cli_bin())
        .arg(db_path)
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn")
}

pub(crate) fn spawn_server(db_path: &Path, tenant_id: &str, nats_url: &str) -> Child {
    ensure_release_binaries();
    Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            tenant_id,
            "--nats-url",
            nats_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("server should spawn")
}

pub(crate) fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

pub(crate) fn write_child_stdin(child: &mut Child, script: &str) {
    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(script.as_bytes())
        .expect("write child stdin");
}

pub(crate) fn wait_for_child_stdout_contains(
    child: &mut Child,
    needle: &str,
    timeout: Duration,
) -> String {
    let stdout = child.stdout.take().expect("stdout pipe");
    let (tx, rx) = mpsc::channel();
    let needle = needle.to_string();
    let reader_needle = needle.clone();

    thread::spawn(move || {
        let mut stdout = stdout;
        let mut chunk = [0_u8; 1024];
        let mut seen = Vec::new();
        loop {
            match stdout.read(&mut chunk) {
                Ok(0) => {
                    let _ = tx.send(output_string(&seen));
                    break;
                }
                Ok(read) => {
                    seen.extend_from_slice(&chunk[..read]);
                    let rendered = output_string(&seen);
                    if rendered.contains(&reader_needle) {
                        let _ = tx.send(rendered);
                        break;
                    }
                }
                Err(err) => {
                    let _ = tx.send(format!(
                        "stdout read error: {err}; partial={}",
                        output_string(&seen)
                    ));
                    break;
                }
            }
        }
    });

    rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!("timed out waiting for child stdout to contain {needle:?}");
    })
}

pub(crate) async fn start_nats() -> NatsFixture {
    let lock = NATS_TEST_LOCK
        .clone()
        .acquire_owned()
        .await
        .expect("NATS fixture lock should not close");
    let conf = workspace_root()
        .join("crates/contextdb-engine/tests/nats.conf")
        .to_string_lossy()
        .into_owned();
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container: ContainerAsync<GenericImage> =
        request.start().await.expect("NATS container should start");
    let nats_port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS port should be mapped");
    let ws_port = container
        .get_host_port_ipv4(9222.tcp())
        .await
        .expect("NATS websocket port should be mapped");
    NatsFixture {
        _lock: lock,
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
        ws_url: format!("ws://127.0.0.1:{ws_port}"),
    }
}

pub(crate) async fn wait_for_sync_server_ready(
    nats_url: &str,
    tenant_id: &str,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let client = match async_nats::connect(nats_url).await {
            Ok(client) => client,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let inbox = client.new_inbox();
        let mut inbox_sub = match client.subscribe(inbox.clone()).await {
            Ok(sub) => sub,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let payload = match encode(
            MessageType::PullRequest,
            &PullRequest {
                since_lsn: Lsn(0),
                max_entries: Some(1),
            },
        ) {
            Ok(payload) => payload,
            Err(_) => return false,
        };

        if client
            .publish_with_reply(pull_subject(tenant_id), inbox.clone(), payload.into())
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let response = tokio::time::timeout(Duration::from_millis(500), inbox_sub.next()).await;
        match response {
            Ok(Some(msg)) if msg.status == Some(async_nats::StatusCode::NO_RESPONDERS) => {}
            Ok(Some(msg)) if msg.status.is_some() => {}
            Ok(Some(msg)) => {
                if let Ok(envelope) = decode(&msg.payload)
                    && matches!(envelope.message_type, MessageType::PullResponse)
                    && rmp_serde::from_slice::<PullResponse>(&envelope.payload).is_ok()
                {
                    return true;
                }
            }
            Ok(None) | Err(_) => {}
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

pub(crate) fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn wait_for_child_output(mut child: Child, timeout: Duration, context: &str) -> Output {
    let start = Instant::now();
    loop {
        match child.try_wait().expect("CLI status poll") {
            Some(_) => return child.wait_with_output().expect(context),
            None => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    return child
                        .wait_with_output()
                        .expect("CLI should finish after timeout kill");
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

pub(crate) fn temp_db_file(tmp: &TempDir, name: &str) -> PathBuf {
    tmp.path().join(name)
}

pub(crate) fn count_rows(db: &Database, table: &str) -> usize {
    db.scan(table, db.snapshot())
        .expect("scan should succeed")
        .len()
}

pub(crate) fn count_rows_from_file(db_path: &Path, table: &str) -> usize {
    let db = Database::open(db_path).expect("db should open");
    let count = count_rows(&db, table);
    db.close().expect("db close should succeed");
    count
}

pub(crate) fn try_count_rows_from_file(
    db_path: &Path,
    table: &str,
) -> Result<Option<usize>, Error> {
    let db = Database::open(db_path)?;
    let count = match db.scan(table, db.snapshot()) {
        Ok(rows) => Some(rows.len()),
        Err(Error::TableNotFound(_)) => None,
        Err(err) => {
            let _ = db.close();
            return Err(err);
        }
    };
    db.close()?;
    Ok(count)
}

pub(crate) fn query_count(db: &Database, sql: &str) -> i64 {
    let result = db
        .execute(sql, &empty_params())
        .expect("query should succeed");
    extract_i64(&result, 0, 0)
}

pub(crate) fn extract_i64(result: &QueryResult, row: usize, col: usize) -> i64 {
    match &result.rows[row][col] {
        Value::Int64(value) => value.to_owned(),
        other => panic!("expected integer cell, got {other:?}"),
    }
}

pub(crate) fn extract_f64(result: &QueryResult, row: usize, col: usize) -> f64 {
    match &result.rows[row][col] {
        Value::Float64(value) => value.to_owned(),
        other => panic!("expected float cell, got {other:?}"),
    }
}

pub(crate) fn text_value<'a>(row: &'a VersionedRow, key: &str) -> &'a str {
    row.values
        .get(key)
        .and_then(Value::as_text)
        .expect("text value")
}

pub(crate) fn setup_simple_sensor_db(db: &Database) {
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)",
        &empty_params(),
    )
    .expect("create sensors table");
}

pub(crate) fn insert_sensor_rows(db: &Database, count: usize) {
    for idx in 0..count {
        db.execute(
            "INSERT INTO sensors (id, name, reading) VALUES ($id, $name, $reading)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(format!("temp-{idx}"))),
                ("reading", Value::Float64(idx as f64 + 0.5)),
            ]),
        )
        .expect("insert sensor row");
    }
}

pub(crate) fn setup_graph_entities(db: &Database, ids: &[Uuid]) {
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities table");
    let tx = db.begin();
    for (index, id) in ids.iter().enumerate() {
        db.insert_row(
            tx,
            "entities",
            values(vec![
                ("id", Value::Uuid(*id)),
                ("name", Value::Text(format!("entity-{index}"))),
            ]),
        )
        .expect("insert entity");
    }
    db.commit(tx).expect("commit entities");
}

pub(crate) fn setup_vector_table(db: &Database, dimension: usize) {
    db.execute(
        &format!("CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR({dimension}))"),
        &empty_params(),
    )
    .expect("create vector table");
}

pub(crate) fn insert_embedding(db: &Database, id: Uuid, vector: Vec<f32>) -> i64 {
    let tx = db.begin();
    let row_id = db
        .insert_row(tx, "embeddings", values(vec![("id", Value::Uuid(id))]))
        .expect("insert embedding row");
    db.insert_vector(tx, row_id, vector)
        .expect("insert embedding vector");
    db.commit(tx).expect("commit embedding row");
    row_id.0 as i64
}

pub(crate) fn bfs_ids(db: &Database, start: Uuid) -> Vec<Uuid> {
    db.query_bfs(start, None, Direction::Outgoing, 5, db.snapshot())
        .expect("bfs should succeed")
        .nodes
        .iter()
        .map(|node| node.id)
        .collect()
}
