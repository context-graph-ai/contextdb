//! Opening a corrupted/truncated store from the CLI must yield a handled,
//! actionable error — never leak an internal redb panic backtrace to stderr.
//!
//! Reproduces the usability transcript (Job 8 / USR-17 & USR-9): a truncated
//! `.db` opened via the CLI printed a raw redb panic line
//! (`panicked at .../redb-3.1.1/.../page_manager.rs:237`) to the user's
//! terminal before the handled `Error:` sentence. The internal crate path is
//! an implementation leak; the user must instead see an actionable error.

use std::process::{Command, Stdio};

/// Build a real file-backed store, then truncate it so the on-disk format is
/// corrupt — this is the shape that made redb panic during open.
fn make_truncated_store(path: &std::path::Path) {
    {
        // A valid store first (writes the format marker), then dropped so its
        // lock is released before we corrupt the file.
        let db = contextdb_engine::Database::open(path).expect("create valid store");
        drop(db);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("reopen store to corrupt it");
    // Keep the valid redb magic/header prefix but cut the file short so the
    // internal page structure is inconsistent — this is the shape that trips
    // redb's internal page_manager assertion (a panic, not a clean Err),
    // faithfully reproducing the transcript.
    file.set_len(2000).expect("truncate store");
    file.sync_all().unwrap();
}

#[test]
fn opening_a_corrupt_store_does_not_leak_an_internal_panic_backtrace() {
    let dir = std::env::temp_dir().join(format!(
        "cdb-corrupt-open-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("corrupt.db");
    make_truncated_store(&db_path);

    let output = Command::new(env!("CARGO_BIN_EXE_contextdb-cli"))
        .arg(&db_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn contextdb-cli");

    let stderr = String::from_utf8_lossy(&output.stderr);

    let _ = std::fs::remove_dir_all(&dir);

    // The CLI must fail to open (non-zero exit) — the store really is corrupt.
    assert!(
        !output.status.success(),
        "opening a corrupt store must fail, got success. stderr:\n{stderr}"
    );

    // No internal implementation leak may reach the user's terminal.
    for leak in ["panicked at", "redb", "page_manager", "assertion", ".rs:"] {
        assert!(
            !stderr.contains(leak),
            "stderr leaked internal marker {leak:?} to the user. stderr:\n{stderr}"
        );
    }

    // The handled, actionable error must name the file and offer a next step.
    assert!(
        stderr.contains("failed to open database") && stderr.contains("corrupt.db"),
        "handled error must name the file. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("corrupt") || stderr.contains("truncated"),
        "handled error must classify the store as corrupt. stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("restore") || stderr.contains("remove the file"),
        "handled error must offer an actionable recovery next step. stderr:\n{stderr}"
    );
}
