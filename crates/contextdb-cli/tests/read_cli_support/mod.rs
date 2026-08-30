//! Shared scaffolding for the CLI read-surface journey tests.
//!
//! Every helper here spawns the real `contextdb` binary. The binary is resolved
//! ONLY through the compile-time `CARGO_BIN_EXE_contextdb` variable Cargo sets
//! for integration test targets — never by searching a target directory or
//! comparing file modification times, which would silently test a stale build.
//!
//! Synchronization rules these helpers obey, so the journeys stay deterministic:
//!
//! * A test waits for an OUTPUT EVENT (a marker in the child's stream), never
//!   for a duration. `WATCHDOG` is a failsafe that turns a hang into a readable
//!   failure; no assertion is derived from elapsed time.
//! * Nothing sleeps to "let the child get ready". Readiness is proven by the
//!   child publishing a document.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde_json::Value;

/// Failsafe for a hung child. Never an assertion about how long work takes.
pub const WATCHDOG: Duration = Duration::from_secs(120);

/// Environment names the ratified contract says are either bootstrap-only or
/// gone entirely. Every helper clears all of them so an operator's shell can
/// never leak into a journey.
pub const CONTEXTDB_ENV_NAMES: &[&str] = &[
    "CONTEXTDB_DB_PATH",
    "CONTEXTDB_OWNER_READ_RUNTIME_DIR",
    "CONTEXTDB_SYNC_ENDPOINT",
    "CONTEXTDB_TENANT_ID",
    "CONTEXTDB_MEMORY_LIMIT",
    "CONTEXTDB_DISK_LIMIT",
    "CONTEXTDB_SYNC_DEBOUNCE_MS",
];

pub fn cli_binary() -> &'static str {
    env!("CARGO_BIN_EXE_contextdb")
}

fn base_command() -> Command {
    let mut cmd = Command::new(cli_binary());
    for name in CONTEXTDB_ENV_NAMES {
        cmd.env_remove(name);
    }
    cmd
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Outcome {
    pub fn describe(&self) -> String {
        format!(
            "exit={:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.code, self.stdout, self.stderr
        )
    }

    /// Every non-empty stdout line parsed as JSON. Panics naming the offending
    /// line, which is itself the JSON-Lines purity contract.
    pub fn stdout_docs(&self) -> Vec<Value> {
        strict_json_lines(&self.stdout, "stdout")
    }

    /// The JSON documents present on stderr. Lenient by design: the purity
    /// promise applies to stdout, and a stderr-purity journey asserts stderr
    /// separately rather than making every other test depend on it.
    pub fn stderr_docs(&self) -> Vec<Value> {
        lenient_json_lines(&self.stderr)
    }

    pub fn errors(&self) -> Vec<Value> {
        inner_objects(&self.stderr_docs(), "error")
    }

    pub fn notices(&self) -> Vec<Value> {
        inner_objects(&self.stderr_docs(), "notice")
    }

    pub fn error_kinds(&self) -> Vec<String> {
        self.errors().iter().filter_map(detail_kind).collect()
    }

    pub fn route_notices(&self) -> Vec<Value> {
        self.notices()
            .into_iter()
            .filter(|n| detail_kind(n).as_deref() == Some("read_route"))
            .collect()
    }
}

pub fn detail_kind(envelope: &Value) -> Option<String> {
    envelope
        .get("detail")?
        .get("kind")?
        .as_str()
        .map(str::to_owned)
}

pub fn strict_json_lines(text: &str, stream: &str) -> Vec<Value> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line.trim()).unwrap_or_else(|err| {
                panic!(
                    "under --json every {stream} line must be one complete JSON document, \
                     but {line:?} failed to parse: {err}\nfull {stream}:\n{text}"
                )
            })
        })
        .collect()
}

pub fn lenient_json_lines(text: &str) -> Vec<Value> {
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .collect()
}

pub fn inner_objects(docs: &[Value], key: &str) -> Vec<Value> {
    docs.iter().filter_map(|d| d.get(key).cloned()).collect()
}

pub fn documents_named(docs: &[Value], key: &str) -> Vec<Value> {
    inner_objects(docs, key)
}

pub fn document_named(docs: &[Value], key: &str) -> Option<Value> {
    docs.iter().find_map(|d| d.get(key).cloned())
}

pub fn expect_document(docs: &[Value], key: &str, context: &str) -> Value {
    document_named(docs, key).unwrap_or_else(|| {
        panic!("{context}: expected one document whose top-level key is {key:?}, saw: {docs:?}")
    })
}

pub fn rows_of(document: &Value) -> Vec<Value> {
    document
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Run a non-interactive (piped stdin) session to completion.
pub fn run(args: &[&str], stdin_text: &str) -> Outcome {
    run_with_env(args, &[], stdin_text)
}

pub fn run_with_env(args: &[&str], env: &[(&str, &str)], stdin_text: &str) -> Outcome {
    let mut cmd = base_command();
    cmd.args(args);
    for (name, value) in env {
        cmd.env(name, value);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb");
    child
        .stdin
        .as_mut()
        .expect("child stdin")
        .write_all(stdin_text.as_bytes())
        .expect("write child stdin");
    let output = child.wait_with_output().expect("wait for contextdb");
    Outcome {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// A store folder plus the database path inside it.
pub struct Store {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
}

impl Store {
    pub fn path_str(&self) -> &str {
        self.path.to_str().expect("utf-8 store path")
    }

    pub fn folder(&self) -> &Path {
        self.dir.path()
    }

    pub fn contents(&self) -> BTreeMap<String, Vec<u8>> {
        folder_contents(self.dir.path())
    }
}

/// A folder that holds no store yet, plus the path a store WOULD occupy.
pub fn absent_store() -> Store {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("journey.db");
    Store { dir, path }
}

/// Create a fixture store by running the ratified writing invocation.
///
/// Creation goes through `--write` deliberately: the contract is that a bare
/// path never creates anything, so a fixture that creates one without the flag
/// would be testing a surface the contract removes.
pub fn store_with(sql: &str) -> Store {
    let store = absent_store();
    let path = store.path.clone();
    let outcome = run(&[path.to_str().expect("utf-8 store path"), "--write"], sql);
    assert_eq!(
        outcome.code,
        Some(0),
        "creating a fixture store must succeed through `contextdb <path> --write`, \
         the invocation the contract makes responsible for creation.\n{}",
        outcome.describe()
    );
    store
}

pub fn folder_contents(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut found = BTreeMap::new();
    collect_folder(dir, dir, &mut found);
    found
}

fn collect_folder(root: &Path, current: &Path, into: &mut BTreeMap<String, Vec<u8>>) {
    let entries = match std::fs::read_dir(current) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_folder(root, &path, into);
        } else {
            let name = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path).unwrap_or_default();
            into.insert(name, bytes);
        }
    }
}

/// SQL that fills `table` with `count` rows whose payloads are distinct and
/// carry no restatement of any expected answer.
pub fn seed_rows(table: &str, count: usize) -> String {
    let mut sql = String::new();
    let mut written = 0usize;
    while written < count {
        let batch = std::cmp::min(100, count - written);
        sql.push_str(&format!("INSERT INTO {table} (id, label) VALUES "));
        for offset in 0..batch {
            let id = written + offset + 1;
            if offset > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({id}, 'row-{id:06}')"));
        }
        sql.push_str(";\n");
        written += batch;
    }
    sql
}

pub fn create_seeded_table(table: &str, count: usize) -> String {
    format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT);\n{}",
        seed_rows(table, count)
    )
}

// ---------------------------------------------------------------------------
// Interactive sessions
// ---------------------------------------------------------------------------

/// An interactive session driven through a real terminal.
///
/// A terminal is required because the contract makes the prompt, the start
/// banner, and Ctrl-C statement cancellation interactive-only behavior; a piped
/// child cannot exhibit them by definition. `script(1)` supplies the pseudo
/// terminal, so the binary under test sees exactly what a person's terminal
/// gives it. Terminal echo means the transcript contains typed input as well as
/// output, which is why assertions look for markers rather than whole lines.
pub struct Terminal {
    child: Child,
    stdin: ChildStdin,
    transcript: Arc<(Mutex<String>, Condvar)>,
}

pub fn terminal(args: &[&str]) -> Terminal {
    let mut invocation = shell_quote(cli_binary());
    for arg in args {
        invocation.push(' ');
        invocation.push_str(&shell_quote(arg));
    }

    let mut cmd = Command::new("script");
    for name in CONTEXTDB_ENV_NAMES {
        cmd.env_remove(name);
    }
    // The fixture promises a line-editing terminal, independent of the shell
    // that launched `cargo test`. Headless runners commonly export TERM=dumb,
    // which makes rustyline take its unsupported-terminal fallback and turns
    // these PTY journeys into tests of the runner environment instead.
    cmd.env("TERM", "xterm");
    // -q silences script's own banner, -e returns the child's exit status,
    // -f flushes each write so a marker becomes observable as it is produced.
    cmd.arg("-q")
        .arg("-e")
        .arg("-f")
        .arg("-c")
        .arg(&invocation)
        .arg("/dev/null");

    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn an interactive contextdb session through script(1)");

    let stdin = child.stdin.take().expect("terminal stdin");
    let transcript = Arc::new((Mutex::new(String::new()), Condvar::new()));

    for stream in [
        child.stdout.take().map(StreamHandle::Out),
        child.stderr.take().map(StreamHandle::Err),
    ]
    .into_iter()
    .flatten()
    {
        let sink = Arc::clone(&transcript);
        std::thread::spawn(move || {
            let mut reader: Box<dyn Read + Send> = match stream {
                StreamHandle::Out(out) => Box::new(out),
                StreamHandle::Err(err) => Box::new(err),
            };
            let mut buffer = [0u8; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        let text = String::from_utf8_lossy(&buffer[..read]).into_owned();
                        let (lock, signal) = &*sink;
                        let mut held = lock.lock().expect("transcript lock");
                        held.push_str(&text.replace('\r', ""));
                        signal.notify_all();
                    }
                }
            }
            let (_, signal) = &*sink;
            signal.notify_all();
        });
    }

    Terminal {
        child,
        stdin,
        transcript,
    }
}

enum StreamHandle {
    Out(std::process::ChildStdout),
    Err(std::process::ChildStderr),
}

impl Terminal {
    pub fn transcript(&self) -> String {
        self.transcript.0.lock().expect("transcript lock").clone()
    }

    pub fn send(&mut self, text: &str) {
        self.stdin
            .write_all(text.as_bytes())
            .expect("write to the terminal session");
        self.stdin.flush().expect("flush terminal session input");
    }

    pub fn send_line(&mut self, line: &str) {
        self.send(&format!("{line}\n"));
    }

    /// Interrupt: the byte a terminal's line discipline turns into SIGINT for
    /// the foreground session, i.e. what pressing Ctrl-C does.
    pub fn interrupt(&mut self) {
        self.send("\x03");
    }

    /// Block until `marker` appears in the transcript. Waits on an output
    /// event; the watchdog only converts a hang into a readable failure.
    pub fn wait_for(&self, marker: &str) -> String {
        self.wait_for_count(marker, 1)
    }

    /// Block until `marker` has appeared at least `occurrences` times.
    pub fn wait_for_count(&self, marker: &str, occurrences: usize) -> String {
        let (lock, signal) = &*self.transcript;
        let mut held = lock.lock().expect("transcript lock");
        loop {
            if held.matches(marker).count() >= occurrences {
                return held.clone();
            }
            let (next, timeout) = signal
                .wait_timeout(held, WATCHDOG)
                .expect("transcript condvar");
            held = next;
            if timeout.timed_out() {
                panic!(
                    "interactive session never produced {marker:?} \
                     ({occurrences} occurrence(s) expected). transcript so far:\n{held}"
                );
            }
        }
    }

    pub fn contains(&self, marker: &str) -> bool {
        self.transcript().contains(marker)
    }

    pub fn occurrences(&self, marker: &str) -> usize {
        self.transcript().matches(marker).count()
    }

    /// End the session the way a person does and collect its exit code.
    pub fn quit(mut self) -> (Option<i32>, String) {
        self.send_line(".quit");
        self.finish()
    }

    pub fn finish(mut self) -> (Option<i32>, String) {
        drop(self.stdin);
        let sink = Arc::clone(&self.transcript);
        let status = self.child.wait().expect("wait for terminal session");
        let text = sink.0.lock().expect("transcript lock").clone();
        (status.code(), text)
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

// ---------------------------------------------------------------------------
// Sessions that keep running while the test does something else
// ---------------------------------------------------------------------------

/// A piped session held open in the background: its stdin stays connected, its
/// output accumulates as it is produced, and the test drives it statement by
/// statement. Used for the journeys where two processes must overlap, and for
/// the ones that act on a session WHILE a statement is running.
pub struct Background {
    child: Child,
    stdin: Option<ChildStdin>,
    transcript: Arc<(Mutex<String>, Condvar)>,
    pub path: PathBuf,
}

pub fn background(args: &[&str]) -> Background {
    let mut cmd = base_command();
    cmd.args(args);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn a background contextdb session");

    let stdin = child.stdin.take().expect("background stdin");
    let transcript = Arc::new((Mutex::new(String::new()), Condvar::new()));
    for stream in [
        child.stdout.take().map(StreamHandle::Out),
        child.stderr.take().map(StreamHandle::Err),
    ]
    .into_iter()
    .flatten()
    {
        let sink = Arc::clone(&transcript);
        std::thread::spawn(move || {
            let mut reader: Box<dyn Read + Send> = match stream {
                StreamHandle::Out(out) => Box::new(out),
                StreamHandle::Err(err) => Box::new(err),
            };
            let mut buffer = [0u8; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        let text = String::from_utf8_lossy(&buffer[..read]).into_owned();
                        let (lock, signal) = &*sink;
                        let mut held = lock.lock().expect("background transcript lock");
                        held.push_str(&text);
                        signal.notify_all();
                    }
                }
            }
            let (_, signal) = &*sink;
            signal.notify_all();
        });
    }

    Background {
        child,
        stdin: Some(stdin),
        transcript,
        path: PathBuf::new(),
    }
}

impl Background {
    pub fn transcript(&self) -> String {
        self.transcript
            .0
            .lock()
            .expect("background transcript")
            .clone()
    }

    pub fn documents(&self) -> Vec<Value> {
        lenient_json_lines(&self.transcript())
    }

    pub fn send(&mut self, text: &str) {
        let stdin = self.stdin.as_mut().expect("background stdin");
        stdin
            .write_all(text.as_bytes())
            .expect("write to the background session");
        stdin.flush().expect("flush background session input");
    }

    pub fn send_line(&mut self, line: &str) {
        self.send(&format!("{line}\n"));
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Deliver the signal a terminal's Ctrl-C sends, to a session that has no
    /// terminal — the journey where interruption must still end the process.
    pub fn interrupt(&self) {
        let status = Command::new("kill")
            .arg("-INT")
            .arg(self.pid().to_string())
            .status()
            .expect("send SIGINT");
        assert!(status.success(), "failed to signal the background session");
    }

    /// Block until `marker` appears. Waits on an output event; the watchdog only
    /// turns a hang into a readable failure.
    pub fn wait_for(&self, marker: &str) -> String {
        let (lock, signal) = &*self.transcript;
        let mut held = lock.lock().expect("background transcript");
        loop {
            if held.contains(marker) {
                return held.clone();
            }
            let (next, timeout) = signal
                .wait_timeout(held, WATCHDOG)
                .expect("background transcript condvar");
            held = next;
            if timeout.timed_out() {
                panic!("the background session never produced {marker:?}. transcript:\n{held}");
            }
        }
    }

    pub fn close_input(&mut self) {
        self.stdin.take();
    }

    pub fn finish(mut self) -> (Option<i32>, String) {
        self.stdin.take();
        let status = self.child.wait().expect("wait for the background session");
        (status.code(), self.transcript())
    }

    /// Stop the session abruptly — the journey where a process disappears.
    pub fn kill(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a writer that holds `path` open, and return once the writer has PROVEN
/// it is up by publishing its owner-status document — an event, not a wait.
///
/// Cross-process is the point: an in-process stand-in cannot prove the owner
/// route, so every owner journey in this estate uses a real second process.
pub fn live_owner(path: &Path, setup_sql: &str) -> Background {
    live_owner_with(path, &[], setup_sql)
}

/// The same live owner, started with explicit owner-service policy flags.
pub fn live_owner_with(path: &Path, extra: &[&str], setup_sql: &str) -> Background {
    let mut args = vec![
        path.to_str().expect("utf-8 store path"),
        "--write",
        "--json",
    ];
    args.extend_from_slice(extra);
    let mut owner = background(&args);
    owner.path = path.to_path_buf();
    if !setup_sql.trim().is_empty() {
        owner.send(setup_sql);
    }
    owner.send_line(".owner status");
    owner.wait_for("\"owner\"");
    owner
}
