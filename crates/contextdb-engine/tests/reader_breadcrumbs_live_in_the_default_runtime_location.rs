#![cfg(all(unix, feature = "test-seams"))]
//! A direct reader writes itself down where every writer of this store looks,
//! whatever runtime directory that reader was pointed at.
//!
//! Two different things share the name "runtime directory", and conflating them
//! costs an operator the one diagnosis they need.
//!
//! The first is the owner CHANNEL. A container, a packaged service, or a Home
//! Assistant add-on has no platform runtime location, so it states one, and both
//! sides of that deployment have to agree on it or the reader dials a pathname
//! the writer never bound. That is what the supplied root exists for, and it is
//! not touched here.
//!
//! The second is the reader BREADCRUMB -- the note a hydrating reader leaves so
//! that a writer refused by it can say who to go and look at. Its readers and
//! its writers are not one deployment. A person runs `contextdb` against a store
//! and, wanting the live owner, passes the deployment's runtime root; a
//! supervisor restarts the writer with nothing but the store path. If the
//! breadcrumb followed the reader's flag, the writer would scan the platform
//! default, find nobody, and tell the operator "N direct readers are hydrating
//! this store" with no name in it -- while the reader that is blocking them is
//! sitting in a directory the writer was never told about. The two sides
//! disagree exactly when the flag is the thing that differs between them.
//!
//! So the breadcrumb location is the DEFAULT per-user runtime location, always,
//! and the `--owner-read-runtime-dir` / `CONTEXTDB_OWNER_READ_RUNTIME_DIR`
//! override does not move it. One location, no flag for the two sides to
//! disagree on.
//!
//! The default location is the process environment's, so the two journeys that
//! turn on it run real child processes with a real `XDG_RUNTIME_DIR`, the way a
//! deployed reader and a deployed writer actually resolve it.

use contextdb_core::read_contract::{
    HeldByReadersDetail, OwnerReadCancellation, ReadFailureDetail, ReadFailureKind, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::persistence::read_persistence_test_scaffold::arm_read_image_hydration_pause_for_test;
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, OwnerRequestHandler, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::Arc;

const CHILD_ROLE_ENV: &str = "CONTEXTDB_BREADCRUMB_LOCATION_CHILD_ROLE";
const CHILD_STORE_ENV: &str = "CONTEXTDB_BREADCRUMB_LOCATION_STORE";
const CHILD_OVERRIDE_ENV: &str = "CONTEXTDB_BREADCRUMB_LOCATION_OVERRIDE_ROOT";
const HYDRATION_HELD: &str = "READ_IMAGE_HYDRATION_HELD=";
const READER_FINISHED: &str = "BREADCRUMB_READER_FINISHED";
const PROBE: &str = "BREADCRUMB_WRITER_PROBE=";
const PROBE_BREADCRUMB: &str = "BREADCRUMB_WRITER_NAMED=";
const RELEASE: &str = "release";

fn child_role() -> Option<String> {
    std::env::var(CHILD_ROLE_ENV).ok()
}

fn child_store() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_STORE_ENV).expect("the child receives a store path"))
}

fn child_override_root() -> PathBuf {
    PathBuf::from(
        std::env::var_os(CHILD_OVERRIDE_ENV).expect("the child receives an override runtime root"),
    )
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(text: &str) -> Vec<u8> {
    assert!(
        text.len().is_multiple_of(2),
        "hex payload has an odd length: {text}"
    );
    (0..text.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&text[index..index + 2], 16).expect("hex payload decodes to bytes")
        })
        .collect()
}

fn secure_root(inside: &Path, name: &str) -> PathBuf {
    let root = inside.join(name);
    std::fs::create_dir(&root).expect("create a runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the runtime root");
    std::fs::canonicalize(&root).expect("resolve the root the way the production resolver will")
}

/// Enough committed bytes that loading the image reports progress at least
/// once, which is where a reader can be parked while it holds the store.
const WIDE_ROWS: usize = 8;
const WIDE_ROW_BYTES: usize = 512 * 1024;

fn seeded_store(directory: &Path, name: &str) -> PathBuf {
    let path = directory.join(name);
    let database = Database::open(&path).expect("claim a store to seed");
    database
        .execute(
            "CREATE TABLE held (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create the held table");
    for row in 0..WIDE_ROWS {
        database
            .execute(
                "INSERT INTO held (id, body) VALUES ($id, $body)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row as i64)),
                    ("body".to_owned(), Value::Text("h".repeat(WIDE_ROW_BYTES))),
                ]),
            )
            .expect("seed one wide row");
    }
    database.close().expect("release the seeded store");
    path
}

// ---------------------------------------------------------------------------
// Children. A deployed reader and a deployed writer are separate processes with
// their own environments, which is the whole point of these journeys.
// ---------------------------------------------------------------------------

/// A direct reader that was pointed at an override runtime root, parked inside
/// its load with the store held.
#[test]
fn breadcrumb_override_reader_child() {
    if child_role().as_deref() != Some("override-reader") {
        return;
    }
    arm_read_image_hydration_pause_for_test();
    let session = ReadSession::open_in_runtime_dir(
        child_store(),
        ReadSessionOptions::default(),
        Some(child_override_root()),
    )
    .expect("an idle store is readable");
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "a direct reader is what holds the store"
    );
    drop(session);
    println!("{READER_FINISHED}");
    std::io::stdout().flush().expect("flush the reader marker");
}

/// A writer started with nothing but the store path, the way a supervisor
/// restarts one.
#[test]
fn breadcrumb_default_writer_probe_child() {
    if child_role().as_deref() != Some("default-writer-probe") {
        return;
    }
    match Database::open(child_store()) {
        Ok(database) => {
            database.close().expect("close the successful writer probe");
            println!("{PROBE}acquired");
        }
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByReaders => {
            let ReadFailureDetail::HeldByReaders(HeldByReadersDetail {
                observed_direct_readers,
                verified_readers,
            }) = failure.detail()
            else {
                panic!("held-by-readers must carry its specialized typed detail");
            };
            println!("{PROBE}reader_refusal:{observed_direct_readers}");
            for breadcrumb in verified_readers {
                println!(
                    "{PROBE_BREADCRUMB}{}:{}",
                    breadcrumb.process_id,
                    encode_hex(breadcrumb.process_name.as_bytes())
                );
            }
        }
        Err(Error::ReadFailure(failure)) => println!("{PROBE}other_typed:{:?}", failure.kind()),
        Err(other) => println!("{PROBE}other:{}", encode_hex(other.to_string().as_bytes())),
    }
    println!("{PROBE}end");
    std::io::stdout().flush().expect("flush the probe result");
}

// ---------------------------------------------------------------------------
// Parent-side harness.
// ---------------------------------------------------------------------------

struct HeldReader {
    child: Child,
    stdout: BufReader<ChildStdout>,
    finished: bool,
}

impl HeldReader {
    fn process_id(&self) -> u32 {
        self.child.id()
    }

    /// Block until the child reports that it is holding the store, and say
    /// where it wrote itself down.
    fn wait_until_holding(&mut self) -> PathBuf {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read the held reader's output");
            assert!(read != 0, "the reader exited before it ever held the store",);
            let Some(index) = line.find(HYDRATION_HELD) else {
                continue;
            };
            let value = line[index + HYDRATION_HELD.len()..].trim();
            assert_ne!(
                value, "unverified",
                "the reader could not identify itself, so this journey cannot say where it wrote \
                 itself down",
            );
            let encoded = value
                .split(':')
                .next()
                .expect("the hold marker carries a breadcrumb path");
            return PathBuf::from(
                String::from_utf8(decode_hex(encoded))
                    .expect("the breadcrumb path is valid UTF-8 here"),
            );
        }
    }

    fn release(mut self) {
        let stdin = self
            .child
            .stdin
            .as_mut()
            .expect("the reader's release pipe");
        writeln!(stdin, "{RELEASE}").expect("release the held reader");
        stdin.flush().expect("flush the release");
        let mut line = String::new();
        let mut finished = false;
        while self
            .stdout
            .read_line(&mut line)
            .expect("read the reader's completion")
            != 0
        {
            if line.contains(READER_FINISHED) {
                finished = true;
                break;
            }
            line.clear();
        }
        let status = self.child.wait().expect("wait for the held reader");
        self.finished = true;
        assert!(
            finished && status.success(),
            "the reader did not finish cleanly: marker={finished}, status={status}",
        );
    }
}

impl Drop for HeldReader {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Start a direct reader that is TOLD to use the override runtime root, with
/// the default per-user runtime location supplied through the environment
/// exactly as a deployed process receives it.
fn spawn_reader_pointed_at_the_override(
    store: &Path,
    default_root: &Path,
    override_root: &Path,
) -> HeldReader {
    let mut child = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("breadcrumb_override_reader_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "override-reader")
        .env(CHILD_STORE_ENV, store)
        .env(CHILD_OVERRIDE_ENV, override_root)
        .env("XDG_RUNTIME_DIR", default_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start the reader pointed at the override root");
    let stdout = child.stdout.take().expect("reader stdout");
    HeldReader {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

/// What a writer started with nothing but the store path saw.
struct WriterProbe {
    outcome: String,
    named: Vec<(u32, String)>,
}

fn probe_writer_with_no_override(store: &Path, default_root: &Path) -> WriterProbe {
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("breadcrumb_default_writer_probe_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "default-writer-probe")
        .env(CHILD_STORE_ENV, store)
        .env("XDG_RUNTIME_DIR", default_root)
        .env_remove("CONTEXTDB_OWNER_READ_RUNTIME_DIR")
        .stderr(Stdio::inherit())
        .output()
        .expect("run the writer probe");
    let text = String::from_utf8(output.stdout).expect("probe output is UTF-8");
    let mut outcome = None;
    let mut named = Vec::new();
    for line in text.lines() {
        if let Some(index) = line.find(PROBE_BREADCRUMB) {
            let value = line[index + PROBE_BREADCRUMB.len()..].trim();
            let (id, name) = value
                .split_once(':')
                .expect("a named reader carries its process id and name");
            named.push((
                id.parse::<u32>().expect("a named reader's process id"),
                String::from_utf8(decode_hex(name)).expect("a named reader's process name"),
            ));
        } else if let Some(index) = line.find(PROBE) {
            let value = line[index + PROBE.len()..].trim();
            if value != "end" {
                outcome = Some(value.to_owned());
            }
        }
    }
    WriterProbe {
        outcome: outcome.unwrap_or_else(|| panic!("the writer probe said nothing:\n{text}")),
        named,
    }
}

// ---------------------------------------------------------------------------
// The journeys.
// ---------------------------------------------------------------------------

/// A reader told to use an override runtime root still writes itself down in
/// the default per-user location.
#[test]
fn a_reader_given_an_override_publishes_in_the_default_runtime_location() {
    let directory = tempfile::TempDir::new().expect("task-scoped breadcrumb-location directory");
    let default_root = secure_root(directory.path(), "default-runtime");
    let override_root = secure_root(directory.path(), "override-runtime");
    let store = seeded_store(directory.path(), "breadcrumb-location.db");

    let mut reader = spawn_reader_pointed_at_the_override(&store, &default_root, &override_root);
    let breadcrumb = reader.wait_until_holding();

    assert!(
        breadcrumb.starts_with(&default_root),
        "the reader wrote itself down at {}, outside the default per-user runtime location {}. \
         A writer restarted with nothing but the store path never looks there.",
        breadcrumb.display(),
        default_root.display(),
    );
    assert!(
        !breadcrumb.starts_with(&override_root),
        "the reader wrote itself down inside the override root {}. The override names where the \
         owner CHANNEL lives; it is not where a reader is looked for.",
        override_root.display(),
    );

    reader.release();
}

/// The writer that was told nothing is the one an operator restarts, and it
/// must be able to say WHO is holding the store it cannot take.
#[test]
fn a_writer_given_no_override_names_the_reader_that_is_blocking_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped breadcrumb-location directory");
    let default_root = secure_root(directory.path(), "default-runtime");
    let override_root = secure_root(directory.path(), "override-runtime");
    let store = seeded_store(directory.path(), "named-by-the-writer.db");

    let mut reader = spawn_reader_pointed_at_the_override(&store, &default_root, &override_root);
    let _ = reader.wait_until_holding();

    let probe = probe_writer_with_no_override(&store, &default_root);
    assert!(
        probe.outcome.starts_with("reader_refusal:"),
        "a store a reader is hydrating refuses a writer as reader contention, not as {}",
        probe.outcome,
    );
    let reader_process = reader.process_id();
    assert!(
        probe.named.iter().any(|(id, _)| *id == reader_process),
        "the writer was refused by a reader it could not name: it reported {:?}, and the reader \
         holding the store is process {reader_process}. An operator is left with a refusal that \
         tells them to go and find something, and nothing to find it by.",
        probe.named,
    );

    reader.release();
}

/// The same reader, asked the other way round: nothing about it appears in the
/// override root, so a deployment's channel directory never fills up with notes
/// about readers that belong somewhere else.
#[test]
fn an_override_root_never_collects_reader_breadcrumbs() {
    let directory = tempfile::TempDir::new().expect("task-scoped breadcrumb-location directory");
    let default_root = secure_root(directory.path(), "default-runtime");
    let override_root = secure_root(directory.path(), "override-runtime");
    let store = seeded_store(directory.path(), "override-empty.db");

    let mut reader = spawn_reader_pointed_at_the_override(&store, &default_root, &override_root);
    let _ = reader.wait_until_holding();

    assert_eq!(
        breadcrumbs_under(&override_root),
        Vec::<PathBuf>::new(),
        "the override root holds the owner channel and nothing else",
    );
    assert_ne!(
        breadcrumbs_under(&default_root),
        Vec::<PathBuf>::new(),
        "the default per-user runtime location is where the reader is found",
    );

    reader.release();
}

/// The other half of the contract, and the half that must not be weakened: the
/// owner CHANNEL still binds and is dialled through the supplied root. A
/// packaged deployment that states its runtime directory keeps a writer its
/// readers can reach.
#[test]
fn the_owner_channel_still_binds_and_is_dialled_through_the_supplied_root() {
    let directory = tempfile::TempDir::new().expect("task-scoped breadcrumb-location directory");
    let default_root = secure_root(directory.path(), "default-runtime");
    let override_root = secure_root(directory.path(), "override-runtime");
    let store = seeded_store(directory.path(), "channel-through-override.db");

    let owner = Database::open_with_options(
        &store,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(override_root.clone()),
                handler: Some(Arc::new(EchoHandler)),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("a packaged writer serves on the root it was given");

    let session = ReadSession::open_in_runtime_dir(
        &store,
        ReadSessionOptions::default(),
        Some(override_root.clone()),
    )
    .expect("a reader given the same root reaches the owner");
    assert_eq!(
        session.route(),
        ReadRoute::Owner,
        "the supplied root is where both sides of a packaged deployment meet",
    );
    drop(session);

    assert_ne!(
        channels_under(&override_root),
        Vec::<PathBuf>::new(),
        "the channel lives in the root the deployment stated",
    );
    assert_eq!(
        channels_under(&default_root),
        Vec::<PathBuf>::new(),
        "a stated root means the channel is NOT in the platform default",
    );

    owner.close().expect("close the packaged writer");
}

struct EchoHandler;

impl OwnerRequestHandler for EchoHandler {
    fn handle(
        &self,
        _namespace: &str,
        request: &[u8],
        _cancellation: &OwnerReadCancellation,
    ) -> contextdb_core::Result<Vec<u8>> {
        Ok(request.to_vec())
    }
}

fn breadcrumbs_under(root: &Path) -> Vec<PathBuf> {
    collect(root, &|path, metadata| {
        metadata.file_type().is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "reader")
    })
}

fn channels_under(root: &Path) -> Vec<PathBuf> {
    collect(root, &|_, metadata| metadata.file_type().is_socket())
}

fn collect(root: &Path, keep: &dyn Fn(&Path, &std::fs::Metadata) -> bool) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(root) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_dir() {
            found.extend(collect(&path, keep));
        } else if keep(&path, &metadata) {
            found.push(path);
        }
    }
    found.sort();
    found
}
