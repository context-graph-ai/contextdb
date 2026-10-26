use contextdb_core::{Direction, Error, Value, VectorIndexRef, VersionedRow};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::transport::iroh::IrohServer;
use std::collections::HashMap;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::LazyLock;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

static SYNC_TEST_LOCK: LazyLock<Arc<tokio::sync::Semaphore>> =
    LazyLock::new(|| Arc::new(tokio::sync::Semaphore::new(1)));

pub(crate) struct SyncFixture {
    _lock: OwnedSemaphorePermit,
    _root: TempDir,
    pub(crate) bind_spec: String,
    pub(crate) ticket: String,
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn workspace_root() -> PathBuf {
    crate::binary_path::workspace_root()
}

pub(crate) fn cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_contextdb") {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_contextdb") {
        return PathBuf::from(path);
    }
    crate::binary_path::resolve_workspace_binary("contextdb")
}

pub(crate) fn server_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_contextdb-server") {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_contextdb-server") {
        return PathBuf::from(path);
    }
    crate::binary_path::resolve_workspace_binary("contextdb-server")
}

// Binary resolution (the newest-mtime-of-debug/release rule, plus the
// CONTEXTDB_TEST_BIN_PROFILE override) lives once in tests/support/binary_path.rs,
// shared with tests/integration/cli_spawn.rs -- see that module's doc comment
// for the reasoning.

pub(crate) fn ensure_release_binaries() {
    let cli = cli_bin();
    let server = server_bin();
    assert!(
        cli.exists() && server.exists(),
        "contextdb CLI/server binaries must be built before acceptance tests run; looked for {} and {}. Run `cargo build -p contextdb-cli -p contextdb-server` (add --release for the release profile) before invoking CLI/server acceptance tests directly.",
        cli.display(),
        server.display()
    );
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
pub(crate) fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
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

/// Own a server until it is reaped, including readiness assertion unwinds.
pub(crate) struct ServerChild {
    child: Option<Child>,
}

impl ServerChild {
    pub(crate) fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }
}

impl std::ops::Deref for ServerChild {
    type Target = Child;

    fn deref(&self) -> &Child {
        self.child.as_ref().expect("server child is still owned")
    }
}

impl std::ops::DerefMut for ServerChild {
    fn deref_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("server child is still owned")
    }
}

impl Drop for ServerChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The kernel also kills the server if nextest terminates its parent test:
/// destructors cannot run after a killed or timed-out test process.
pub(crate) fn server_command() -> Command {
    with_parent_death(Command::new(server_bin()))
}

fn with_parent_death(mut command: Command) -> Command {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let parent = std::process::id() as libc::pid_t;
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // Cover a parent exit between fork and installing the signal.
                if libc::getppid() != parent {
                    return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
                }
                Ok(())
            });
        }
    }
    command
}

pub(crate) fn spawn_server(db_path: &Path, tenant_id: &str, bind_spec: &str) -> ServerChild {
    ensure_release_binaries();
    let mut child = ServerChild::new(
        server_command()
            .args([
                "--db-path",
                db_path.to_str().expect("utf-8 db path"),
                "--tenant-id",
                tenant_id,
                "--sync-endpoint",
                bind_spec,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("server should spawn"),
    );
    let ready =
        wait_for_child_stdout_contains(&mut child, "enrollment ticket:", Duration::from_secs(15));
    assert!(
        ready.contains("enrollment ticket:"),
        "server must publish its enrollment ticket after binding; stdout={ready}"
    );
    child
}

pub(crate) fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
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
        let mut signaled = false;
        loop {
            match stdout.read(&mut chunk) {
                Ok(0) => {
                    if !signaled {
                        let _ = tx.send(output_string(&seen));
                    }
                    break;
                }
                Ok(read) => {
                    if signaled {
                        continue;
                    }
                    seen.extend_from_slice(&chunk[..read]);
                    let rendered = output_string(&seen);
                    if rendered.contains(&reader_needle) {
                        let _ = tx.send(rendered);
                        signaled = true;
                        seen.clear();
                    }
                }
                Err(err) => {
                    if !signaled {
                        let _ = tx.send(format!(
                            "stdout read error: {err}; partial={}",
                            output_string(&seen)
                        ));
                    }
                    break;
                }
            }
        }
    });

    rx.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!("timed out waiting for child stdout to contain {needle:?}");
    })
}

pub(crate) async fn start_sync_fixture() -> SyncFixture {
    let lock = SYNC_TEST_LOCK
        .clone()
        .acquire_owned()
        .await
        .expect("sync fixture lock should not close");
    let root = TempDir::new().expect("sync fixture tempdir");
    let identity = root.path().join("hub-identity.key");
    let bind_spec = format!("iroh:?identity={}", identity.display());
    let endpoint = IrohServer::bind(&bind_spec)
        .await
        .expect("reserve the sync fixture endpoint");
    let ticket = endpoint.ticket();
    endpoint.close().await;
    SyncFixture {
        _lock: lock,
        _root: root,
        bind_spec,
        ticket,
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
fn wait_for_child_exit(child: &mut Child, timeout: Duration, context: &str) {
    let start = Instant::now();
    loop {
        match child.try_wait().expect(context) {
            Some(_) => return,
            None => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    child.wait().expect("child exits after timeout kill");
                    return;
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn wait_for_child_output(mut child: Child, timeout: Duration, context: &str) -> Output {
    wait_for_child_exit(&mut child, timeout, context);
    child.wait_with_output().expect(context)
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn wait_for_server_output(
    mut child: ServerChild,
    timeout: Duration,
    context: &str,
) -> Output {
    wait_for_child_exit(&mut child, timeout, context);
    // Only transfer the process out of its guard after it is already reaped.
    child
        .child
        .take()
        .expect("server child")
        .wait_with_output()
        .expect(context)
}

pub(crate) fn temp_db_file(tmp: &TempDir, name: &str) -> PathBuf {
    tmp.path().join(name)
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
pub(crate) fn count_rows(db: &Database, table: &str) -> usize {
    db.scan(table, db.snapshot())
        .expect("scan should succeed")
        .len()
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance, acceptance_sync"
)]
pub(crate) fn count_rows_from_file(db_path: &Path, table: &str) -> usize {
    let db = Database::open(db_path).expect("db should open");
    let count = count_rows(&db, table);
    db.close().expect("db close should succeed");
    count
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn query_count(db: &Database, sql: &str) -> i64 {
    let result = db
        .execute(sql, &empty_params())
        .expect("query should succeed");
    extract_i64(&result, 0, 0)
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn extract_i64(result: &QueryResult, row: usize, col: usize) -> i64 {
    match &result.rows()[row][col] {
        Value::Int64(value) => value.to_owned(),
        other => panic!("expected integer cell, got {other:?}"),
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn extract_f64(result: &QueryResult, row: usize, col: usize) -> f64 {
    match &result.rows()[row][col] {
        Value::Float64(value) => value.to_owned(),
        other => panic!("expected float cell, got {other:?}"),
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn text_value<'a>(row: &'a VersionedRow, key: &str) -> &'a str {
    row.values
        .get(key)
        .and_then(Value::as_text)
        .expect("text value")
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn setup_simple_sensor_db(db: &Database) {
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, reading REAL)",
        &empty_params(),
    )
    .expect("create sensors table");
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn setup_graph_entities(db: &Database, ids: &[Uuid]) {
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities table");
    let tx = db.begin_or_panic();
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn setup_vector_table(db: &Database, dimension: usize) {
    db.execute(
        &format!("CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR({dimension}))"),
        &empty_params(),
    )
    .expect("create vector table");
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by acceptance"
)]
pub(crate) fn insert_embedding(db: &Database, id: Uuid, vector: Vec<f32>) -> i64 {
    let tx = db.begin_or_panic();
    let row_id = db
        .insert_row(tx, "embeddings", values(vec![("id", Value::Uuid(id))]))
        .expect("insert embedding row");
    db.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("embeddings", "embedding"),
        row_id,
        vector,
    )
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

#[cfg(target_os = "linux")]
#[test]
fn a_server_fixture_cannot_survive_an_unwinding_or_killed_test() {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    const CHILD: &str = "CONTEXTDB_SERVER_FIXTURE_LIFETIME_CHILD";
    fn spawn(mode: &str) -> ServerChild {
        ServerChild::new(
            with_parent_death(Command::new(
                std::env::current_exe().expect("test executable"),
            ))
            .args([
                "--exact",
                "common::a_server_fixture_cannot_survive_an_unwinding_or_killed_test",
                "--nocapture",
            ])
            .env(CHILD, mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn guarded fixture child"),
        )
    }
    match std::env::var(CHILD).ok().as_deref() {
        Some("child") => {
            println!("server fixture child ready");
            std::io::stdout().flush().expect("publish readiness");
            loop {
                std::thread::park();
            }
        }
        Some("parent") => {
            let mut child = spawn("child");
            let ready = wait_for_child_stdout_contains(
                &mut child,
                "server fixture child ready",
                Duration::from_secs(15),
            );
            assert!(ready.contains("server fixture child ready"));
            println!("server fixture descendant={}", child.id());
            std::io::stdout()
                .flush()
                .expect("publish descendant identity");
            // The test process will be killed while this child remains live.
            loop {
                std::thread::park();
            }
        }
        None => {}
        Some(mode) => panic!("unknown fixture mode {mode}"),
    }
    let mut child = spawn("child");
    let ready = wait_for_child_stdout_contains(
        &mut child,
        "server fixture child ready",
        Duration::from_secs(15),
    );
    assert!(ready.contains("server fixture child ready"));
    let pid = child.id() as libc::pid_t;
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _child = child;
        panic!("exercise fixture cleanup during unwind");
    }));
    assert!(unwound.is_err());
    let mut status = 0;
    assert_eq!(
        unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD),
        "the guard killed and reaped the child during unwind"
    );

    // Adopt the descendant so this test also reaps it after its parent dies.
    assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) }, 0);
    let mut parent = spawn("parent");
    let ready = wait_for_child_stdout_contains(
        &mut parent,
        "server fixture descendant=",
        Duration::from_secs(15),
    );
    let descendant: libc::pid_t = ready
        .lines()
        .find_map(|line| {
            line.split_once("server fixture descendant=")
                .map(|(_, pid)| pid)
        })
        .expect("parent reported its live descendant")
        .parse()
        .expect("descendant pid");
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, descendant, 0) } as i32;
    assert!(
        fd >= 0,
        "open the exact live descendant: {}",
        std::io::Error::last_os_error()
    );
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    parent.kill().expect("simulate a killed test process");
    parent.wait().expect("reap the killed test process");
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let exited = unsafe { libc::poll(&mut poll, 1, 15_000) };
    // Even a failed assertion cannot leave this fixture running on the host.
    unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            libc::SIGKILL,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        );
    }
    assert_eq!(
        unsafe { libc::waitpid(descendant, &mut status, 0) },
        descendant
    );
    assert_eq!(
        exited, 1,
        "the kernel ended the child when its test parent died"
    );
    assert!(libc::WIFSIGNALED(status));
    assert_eq!(libc::WTERMSIG(status), libc::SIGKILL);
}
