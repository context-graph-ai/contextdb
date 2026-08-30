//! A second process refused because a writer holds the store must be handed
//! the holder's process identity as DATA it can render, not a sentence it has
//! to parse.
//!
//! Reader contention already works this way: `HeldByReaders` carries
//! `HeldByReadersDetail` with each reader's process id. Writer contention does
//! not — the holding process number is formatted into a
//! `ReadFailureDetail::Reason` string, so a consumer that wants to say "stop
//! process 4321" in its own words has to go looking for it inside prose.
//!
//! The surface these arms compile against, and the one the engine implements:
//!
//! ```ignore
//! pub struct HeldByWriterDetail {
//!     pub process_id: Option<u64>,
//!     pub store_path: String,
//! }
//!
//! pub enum ReadFailureDetail {
//!     // ... every existing variant keeps its position ...
//!     HeldByWriter(HeldByWriterDetail),   // appended LAST
//! }
//! ```
//!
//! `store_path` rides along with the process id deliberately. The refusal an
//! operator reads today names the store as well as the holder, and an
//! acceptance journey asserts that it does; a detail carrying only the process
//! number would quietly shorten that sentence, which is a behaviour change,
//! not a refactor. Carrying both keeps the human sentence whole while making
//! each half addressable.
//!
//! The pairing is one-directional: this detail belongs only to the
//! writer-contention refusal, but that refusal may still carry the ordinary
//! detail vocabulary, so no already-encoded refusal changes shape.

use contextdb_core::read_contract::{
    HeldByWriterDetail, ReadFailure, ReadFailureConstructionError, ReadFailureDetail,
    ReadFailureKind,
};
use contextdb_engine::Database;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const HOLDER_ROLE_ENV: &str = "CONTEXTDB_WRITER_HOLDER_ROLE";
const HOLDER_PATH_ENV: &str = "CONTEXTDB_WRITER_HOLDER_STORE_PATH";
const HOLDER_OPENED: &str = "WRITER_HOLDER_OPENED";
const HOLDER_RELEASE: &str = "release";

fn holder_store_path() -> PathBuf {
    PathBuf::from(std::env::var(HOLDER_PATH_ENV).expect("holder child store path"))
}

fn companion_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

fn announce_and_wait_for_release() {
    println!("{HOLDER_OPENED}");
    std::io::stdout()
        .flush()
        .expect("flush the holder-opened marker");
    let mut command = String::new();
    std::io::stdin()
        .read_line(&mut command)
        .expect("read the holder release command");
    assert_eq!(command.trim(), HOLDER_RELEASE);
}

/// A holder that takes the store the ordinary way, so its companion record is
/// published and names it.
#[test]
fn published_writer_holder_child() {
    if std::env::var(HOLDER_ROLE_ENV).ok().as_deref() != Some("published-writer") {
        return;
    }
    let database = Database::open(holder_store_path()).expect("the holder child takes the store");
    announce_and_wait_for_release();
    database.close().expect("the holder child closes cleanly");
}

/// A holder that owns the store's exclusive companion lock without ever
/// publishing a record into it — the state a writer is in before it has said
/// anything about itself, and the state that leaves a contender with no
/// process number to report.
#[test]
fn unpublished_companion_holder_child() {
    if std::env::var(HOLDER_ROLE_ENV).ok().as_deref() != Some("unpublished-companion") {
        return;
    }
    let companion = companion_path(&holder_store_path());
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&companion)
        .unwrap_or_else(|error| panic!("create companion {}: {error}", companion.display()));
    #[cfg(unix)]
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .expect("keep the companion owner-only");
    fs2::FileExt::try_lock_exclusive(&file).expect("hold the companion exclusively");
    announce_and_wait_for_release();
    fs2::FileExt::unlock(&file).expect("release the companion");
}

struct HoldingProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    released: bool,
}

impl HoldingProcess {
    fn process_id(&self) -> u64 {
        u64::from(self.child.id())
    }

    fn release(&mut self) {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = writeln!(stdin, "{HOLDER_RELEASE}");
            let _ = stdin.flush();
        }
        let _ = self.child.wait();
        self.released = true;
    }
}

impl Drop for HoldingProcess {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_holder(role: &str, child_test: &str, path: &Path) -> HoldingProcess {
    let mut child = Command::new(std::env::current_exe().expect("current engine test binary"))
        .arg("--exact")
        .arg(child_test)
        .arg("--nocapture")
        .env(HOLDER_ROLE_ENV, role)
        .env(HOLDER_PATH_ENV, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start the holding process");
    let stdout = child.stdout.take().expect("holding process stdout");
    let stdin = child.stdin.take().expect("holding process stdin");
    let mut reader = BufReader::new(stdout);
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .expect("read the holding process marker");
        assert_ne!(
            read, 0,
            "the holding process ended before it took the store"
        );
        if line.contains(HOLDER_OPENED) {
            break;
        }
    }
    HoldingProcess {
        child,
        stdin: Some(stdin),
        released: false,
    }
}

fn writer_refusal_detail(path: &Path) -> (HeldByWriterDetail, String) {
    match Database::open(path) {
        Ok(database) => {
            let _ = database.close();
            panic!("a store another process holds must not open here");
        }
        Err(contextdb_core::Error::ReadFailure(failure)) => {
            assert_eq!(failure.kind(), ReadFailureKind::HeldByWriter);
            let sentence = failure.to_string();
            match failure.detail() {
                ReadFailureDetail::HeldByWriter(detail) => (detail.clone(), sentence),
                other => {
                    panic!("writer contention must carry its own structured detail, got {other:?}")
                }
            }
        }
        Err(other) => panic!("expected a typed writer-contention refusal, got {other:?}"),
    }
}

#[test]
fn a_refused_opener_reads_the_holding_process_number_out_of_the_refusal() {
    let directory = tempfile::TempDir::new().expect("task-scoped writer-holder directory");
    let path = directory.path().join("writer-holder.db");
    let mut holder = spawn_holder("published-writer", "published_writer_holder_child", &path);

    let (detail, sentence) = writer_refusal_detail(&path);
    assert_eq!(
        detail.process_id,
        Some(holder.process_id()),
        "the refusal must name the process that actually holds the store",
    );
    assert!(
        detail.store_path.ends_with("writer-holder.db"),
        "the refusal must name the store it is about: {}",
        detail.store_path,
    );
    // The words an operator reads are unchanged: both halves of the sentence
    // are still there, they are simply addressable now.
    assert!(
        sentence.contains(&format!("process {}", holder.process_id())),
        "the human sentence must still name the holding process: {sentence}",
    );
    assert!(
        sentence.contains(&detail.store_path),
        "the human sentence must still name the store: {sentence}",
    );

    holder.release();
}

#[test]
fn a_holder_that_has_published_nothing_leaves_the_process_number_unset() {
    let directory = tempfile::TempDir::new().expect("task-scoped unpublished-holder directory");
    let path = directory.path().join("unpublished-holder.db");
    let mut holder = spawn_holder(
        "unpublished-companion",
        "unpublished_companion_holder_child",
        &path,
    );

    let (detail, sentence) = writer_refusal_detail(&path);
    assert_eq!(
        detail.process_id, None,
        "a holder that published no record cannot be named, and must not be invented",
    );
    assert!(
        detail.store_path.ends_with("unpublished-holder.db"),
        "the refusal still names the store it is about: {}",
        detail.store_path,
    );
    assert!(
        sentence.contains(&detail.store_path),
        "the human sentence must still name the store: {sentence}",
    );

    holder.release();
}

#[test]
fn the_writer_holder_detail_belongs_only_to_the_writer_contention_refusal() {
    let detail = HeldByWriterDetail {
        process_id: Some(4321),
        store_path: "/store/held.db".to_owned(),
    };
    assert!(
        ReadFailure::new(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(detail.clone()),
        )
        .is_ok(),
    );
    for kind in [
        ReadFailureKind::HeldByReaders,
        ReadFailureKind::OwnerNotRunning,
        ReadFailureKind::StoreNotFound,
    ] {
        assert_eq!(
            ReadFailure::new(kind, ReadFailureDetail::HeldByWriter(detail.clone())),
            Err(ReadFailureConstructionError::KindDetailMismatch),
        );
    }
    // The pairing runs one way only: writer contention keeps the ordinary
    // detail vocabulary, so every refusal already on the wire keeps its shape.
    assert!(
        ReadFailure::new(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::Reason {
                reason: "a writer owns the store".to_owned(),
            },
        )
        .is_ok(),
    );
    assert!(ReadFailure::new(ReadFailureKind::HeldByWriter, ReadFailureDetail::None).is_ok(),);
}
