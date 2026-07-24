//! `contextdb-cli reset` (explicit destructive flag, recreates a
//! wedged/corrupt root) and `contextdb-cli repair` (reports what is
//! salvageable, conservative — never modifies).
//!
//! `make_truncated_store` reproduces the same real corruption shape as
//! `corrupt_store_open_tests.rs` (a valid store, truncated after close so
//! the on-disk page layout is inconsistent) rather than inventing a new
//! one — the shape that trips redb's own internal panic, per that file's
//! header.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};

fn scratch_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("cdb-resetrepair-{label}-"))
        .tempdir()
        .expect("scratch dir")
}

/// A valid store first (writes the format marker), then truncated after
/// close so the on-disk page layout is inconsistent — the shape
/// `corrupt_store_open_tests.rs` uses to reproduce redb's internal
/// page_manager panic on open.
fn make_truncated_store(path: &Path) {
    {
        let db = contextdb_engine::Database::open(path).expect("create valid store");
        db.close().expect("close");
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("reopen store to corrupt it");
    file.set_len(2000).expect("truncate store");
    file.sync_all().unwrap();
}

fn read_bytes(path: &Path) -> Vec<u8> {
    let mut buf = Vec::new();
    std::fs::File::open(path)
        .unwrap_or_else(|err| panic!("open {}: {err}", path.display()))
        .read_to_end(&mut buf)
        .expect("read file");
    buf
}

fn run(
    subcommand: &str,
    extra: &[&str],
    path: &Path,
) -> (std::process::ExitStatus, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(subcommand).arg(path).args(extra);
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|err| panic!("spawn contextdb-cli {subcommand}: {err}"));
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

// ---------------------------------------------------------------------------
// repair: conservative, reports salvageability, never modifies.
// ---------------------------------------------------------------------------

#[test]
fn repair_reports_salvageability_without_modifying_the_corrupt_store() {
    let dir = scratch_dir("repair");
    let db_path = dir.path().join("corrupt.db");
    make_truncated_store(&db_path);
    let before = read_bytes(&db_path);

    let (_status, stdout, stderr) = run("repair", &[], &db_path);

    assert_eq!(
        read_bytes(&db_path),
        before,
        "repair is conservative: it must NEVER modify the store it inspects"
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("corrupt.db"),
        "repair's report must name the file it inspected. combined output:\n{combined}"
    );
    assert!(
        combined.to_lowercase().contains("salvage")
            || combined.to_lowercase().contains("corrupt")
            || combined.to_lowercase().contains("truncat"),
        "repair's report must classify what it found (salvageable / corrupt / truncated), \
         not just fail generically. combined output:\n{combined}"
    );
}

// ---------------------------------------------------------------------------
// reset without --force: refuses, never destroys data.
// ---------------------------------------------------------------------------

#[test]
fn reset_without_force_flag_refuses_and_leaves_the_store_untouched() {
    let dir = scratch_dir("reset-no-force");
    let db_path = dir.path().join("wedged.db");
    make_truncated_store(&db_path);
    let before = read_bytes(&db_path);

    let (status, _stdout, stderr) = run("reset", &[], &db_path);

    assert!(
        !status.success(),
        "reset without --force must refuse, not silently recreate the store. stderr:\n{stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("force"),
        "the refusal must name the --force flag it is waiting for. stderr:\n{stderr}"
    );
    assert_eq!(
        read_bytes(&db_path),
        before,
        "a refused reset must never destroy the existing (however corrupt) data"
    );
}

// ---------------------------------------------------------------------------
// reset --force: recreates a working store.
// ---------------------------------------------------------------------------

#[test]
fn reset_with_force_flag_recreates_a_working_store() {
    let dir = scratch_dir("reset-force");
    let db_path = dir.path().join("wedged.db");
    make_truncated_store(&db_path);

    let (status, _stdout, stderr) = run("reset", &["--force"], &db_path);

    assert!(
        status.success(),
        "reset --force must succeed on a corrupt store. stderr:\n{stderr}"
    );

    let db = contextdb_engine::Database::open(&db_path).unwrap_or_else(|err| {
        panic!("reset --force must leave a store that opens cleanly, got: {err}")
    });
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, body TEXT)",
        &Default::default(),
    )
    .expect("a freshly reset store must accept writes");
}

// ---------------------------------------------------------------------------
// Exit codes conform to the shipped 4-value table
// (contextdb_server::exit_codes): usage errors are EXIT_USAGE (2), a
// definitive run failure is EXIT_ERROR (1).
// ---------------------------------------------------------------------------

#[test]
fn reset_without_force_uses_the_usage_exit_code() {
    let dir = scratch_dir("reset-exit-code");
    let db_path = dir.path().join("wedged.db");
    make_truncated_store(&db_path);

    let (status, _stdout, _stderr) = run("reset", &[], &db_path);
    assert_eq!(
        status.code(),
        Some(contextdb_server::exit_codes::EXIT_USAGE),
        "an incomplete flag combination (reset without --force) is a usage error per the \
         shipped exit-code table, not a run failure"
    );
}
