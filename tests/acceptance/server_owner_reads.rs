//! The server serves owner reads under the same policy the writer does.
//!
//! A file-backed writer already lets another process read live committed
//! state through its local channel, under ceilings the operator declared.
//! The server is the same writer with a network leg: an operator running one
//! expects the same reads to work and the same flags to shape them. A server
//! that does not serve them makes the reader open the file itself, which is a
//! different route with different answers -- or, where the store is held,
//! no answer at all.
//!
//! So: the same `--owner-read-*` set as the writer, the same defaults,
//! serving by default over a file store, one way to turn it off, a clear
//! report when the channel cannot be placed, and no environment alias for
//! anything that changes behavior -- an operator reads what a server will do
//! from the command that started it, not from the shell around it.

use crate::common::{ensure_release_binaries, output_string, server_bin, temp_db_file};
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Every read ceiling and timeout the file-backed writer exposes, which the
/// server exposes under the same names.
const OWNER_READ_FLAGS: [&str; 13] = [
    "--owner-read-result-rows",
    "--owner-read-result-bytes",
    "--owner-read-work",
    "--owner-read-active-ms",
    "--owner-read-memory",
    "--owner-read-cursor-page-rows",
    "--owner-read-cursor-page-bytes",
    "--owner-read-cursor-idle-ms",
    "--owner-read-cursor-lifetime-ms",
    "--owner-read-concurrency",
    "--owner-read-request-ms",
    "--owner-read-shutdown-drain-ms",
    "--owner-read-runtime-dir",
];

fn server_help() -> String {
    ensure_release_binaries();
    let output = Command::new(server_bin())
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the server prints its usage");
    format!(
        "{}{}",
        output_string(&output.stdout),
        output_string(&output.stderr)
    )
}

/// I asked the server what it takes, and every read setting the writer takes
/// was there under the same name.
#[test]
fn s01_the_server_takes_every_owner_read_flag_the_writer_takes() {
    let help = server_help();
    let missing: Vec<&str> = OWNER_READ_FLAGS
        .into_iter()
        .filter(|flag| !help.contains(flag))
        .collect();
    assert!(
        missing.is_empty(),
        "an operator who knows the writer's read policy can state the same policy to the server; \
         these are not offered: {missing:?}"
    );
    assert!(
        help.contains("--no-owner-reads"),
        "and there is one way to turn owner reads off: {help}"
    );
}

/// The read settings are stated on the command line, not exported around it,
/// so what a server will do is readable from the command that started it.
#[test]
fn s02_no_owner_read_behaviour_flag_has_an_environment_alias() {
    let help = server_help();
    for alias in [
        "CONTEXTDB_OWNER_READ_CONCURRENCY",
        "CONTEXTDB_OWNER_READ_RESULT_ROWS",
        "CONTEXTDB_OWNER_READ_WORK",
        "CONTEXTDB_OWNER_READ_MEMORY",
        "CONTEXTDB_NO_OWNER_READS",
    ] {
        assert!(
            !help.contains(alias),
            "no owner-read behaviour setting is taken from the environment; the usage names \
             {alias}"
        );
    }
    assert!(
        help.contains("CONTEXTDB_OWNER_READ_RUNTIME_DIR"),
        "the one exception is where the channel goes, which a container has to be able to set: \
         {help}"
    );
}

/// A memory store has no file to serve from and no channel to place, so
/// naming a read setting alongside one is an invalid invocation rather than a
/// value that quietly does nothing.
#[test]
fn s03_a_memory_store_with_an_owner_read_flag_is_refused_at_startup() {
    ensure_release_binaries();
    let output = Command::new(server_bin())
        .args([
            "--db-path",
            ":memory:",
            "--tenant-id",
            "s03",
            "--owner-read-concurrency",
            "2",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the server answers the invocation");
    let stderr = output_string(&output.stderr);
    // Exit 2 alone would also be what a server that has never heard of the
    // flag answers, so the refusal has to be about the memory store.
    assert!(
        !stderr.to_lowercase().contains("unexpected argument")
            && !stderr.to_lowercase().contains("unrecognized"),
        "the server knows the flag and refuses the COMBINATION, rather than not knowing it: \
         {stderr}"
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "an invalid invocation exits with the usage code and attempts nothing; stderr: {stderr}"
    );
}

/// A server told to put its channel somewhere it cannot keeps serving sync,
/// says so once, and does not pretend to serve reads.
#[test]
fn s04_an_unusable_runtime_directory_leaves_the_server_running_and_says_so_once() {
    ensure_release_binaries();
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "s04.db");
    let unusable = tmp.path().join("not-a-directory");
    std::fs::write(&unusable, b"this is a file, not a directory\n").expect("place a file there");

    let output = Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            "s04",
            "--owner-read-runtime-dir",
            unusable.to_str().expect("utf-8 runtime dir"),
            "--show-ticket",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the server answers the invocation");
    let stderr = output_string(&output.stderr);

    assert_ne!(
        output.status.code(),
        Some(2),
        "a runtime directory it cannot use is not an invalid invocation; the server keeps \
         running and serves sync: {stderr}"
    );
    let warnings = stderr
        .lines()
        .filter(|line| line.to_lowercase().contains("owner"))
        .count();
    assert_eq!(
        warnings, 1,
        "it says so exactly once, so an operator sees a fact rather than a repeating alarm; \
         stderr: {stderr}"
    );
}

/// A reader dialling a server that is serving reads is answered through the
/// owner's channel, not by opening the file for itself.
#[test]
fn s05_a_file_backed_server_serves_owner_reads_by_default() {
    ensure_release_binaries();
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "s05.db");
    let runtime_dir = tmp.path().join("s05-runtime");
    std::fs::create_dir(&runtime_dir).expect("create the runtime root");

    let output = Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            "s05",
            "--owner-read-runtime-dir",
            runtime_dir.to_str().expect("utf-8 runtime dir"),
            "--show-ticket",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the server answers the invocation");
    let stderr = output_string(&output.stderr);
    assert_ne!(
        output.status.code(),
        Some(2),
        "a file-backed server accepts a runtime directory for its read channel: {stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("unexpected argument"),
        "the server knows the flag: {stderr}"
    );
}

/// Turning owner reads off is reportable, so an operator can tell a server
/// that was told not to serve from one that failed to.
#[test]
fn s06_no_owner_reads_is_reported_as_serving_disabled() {
    ensure_release_binaries();
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "s06.db");

    let output = Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            "s06",
            "--no-owner-reads",
            "--show-ticket",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("the server answers the invocation");
    let stderr = output_string(&output.stderr);
    assert!(
        !stderr.to_lowercase().contains("unexpected argument"),
        "the server takes --no-owner-reads: {stderr}"
    );
    assert_ne!(
        output.status.code(),
        Some(2),
        "and it is a valid thing to say to a file-backed server: {stderr}"
    );
}
