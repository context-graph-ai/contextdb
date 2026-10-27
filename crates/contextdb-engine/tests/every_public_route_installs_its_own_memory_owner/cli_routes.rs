//! CLI operational commands. Their typed result is the process exit code;
//! on a healthy store with well-formed arguments each must exit 0.

use super::fixture::Store;
use super::{Outcome, Row, call};
use contextdb_engine::vector_read_memory::DeclaredReadMemory;
use std::io::Write;

const READER_LIMIT: u64 = 64 * 1024 * 1024;

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn factory() -> contextdb_core::Result<(
    contextdb_core::read_memory::ReadMemoryOwner,
    contextdb_engine::vector_read_memory::DeclaredReadRoot,
)> {
    let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)?;
    Ok((root.child()?, root))
}

fn exit(code: Option<i32>) -> Result<(), String> {
    match code {
        Some(0) => Ok(()),
        other => Err(format!("the command exited with {other:?}")),
    }
}

fn strict(outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Refused(reason) => Outcome::Unexplained(reason),
        other => other,
    }
}

fn snapshot_args(dir: &std::path::Path, source: &std::path::Path) -> Vec<String> {
    vec![
        "export".to_string(),
        source.display().to_string(),
        dir.join("cli-export.db").display().to_string(),
    ]
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_cli::ops::run_operational",
            drive: || -> Outcome {
                let (dir, path) = Store::closed();
                let args = snapshot_args(dir.path(), &path);
                strict(call(|| {
                    exit(contextdb_cli::ops::run_operational(
                        "snapshot", &args, factory,
                    ))
                }))
            },
        },
        Row {
            route: "contextdb_cli::ops::dispatch_if_subcommand",
            drive: || -> Outcome {
                let (_dir, path) = Store::closed();
                let args = vec![
                    "contextdb".to_string(),
                    "inspect".to_string(),
                    "sync-apply-state".to_string(),
                    path.display().to_string(),
                ];
                strict(call(|| {
                    exit(contextdb_cli::ops::dispatch_if_subcommand(&args, factory))
                }))
            },
        },
        Row {
            // The REPL reads its statements from standard input; this row
            // points file descriptor 0 at a pipe holding a short script for
            // the length of the call. Each test runs in its own process
            // under nextest, and nothing else in this binary reads stdin.
            route: "contextdb_cli::run",
            drive: || -> Outcome {
                let store = Store::open();
                let root = DeclaredReadMemory::from_reader_limit(READER_LIMIT)
                    .expect("fixture reader root");
                let session = contextdb_cli::Session::writing(
                    store.db.clone(),
                    contextdb_core::read_contract::ReadLimits::default(),
                    root,
                )
                .expect("fixture CLI session");
                let script = b"SELECT id, body FROM notes ORDER BY id;\nSELECT id, bucket FROM breadth_rows ORDER BY id;\n";
                let _stdin = StdinFrom::script(script);
                strict(call(|| {
                    exit(Some(contextdb_cli::run(
                        &session,
                        None,
                        None,
                        None,
                        contextdb_cli::OutputOptions { json: false },
                    )))
                }))
            },
        },
    ]
}

/// File descriptor 0 redirected to a pipe that holds `script` and then
/// ends; the original descriptor is restored on drop.
struct StdinFrom {
    saved: libc::c_int,
}

impl StdinFrom {
    fn script(script: &[u8]) -> Self {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: `fds` is a valid two-element array for pipe(2).
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0, "fixture pipe");
        // SAFETY: the write end was just created and is owned here.
        let mut writer = unsafe { <std::fs::File as std::os::fd::FromRawFd>::from_raw_fd(fds[1]) };
        writer
            .write_all(script)
            .expect("fixture script fits the pipe");
        drop(writer);
        // SAFETY: duplicates standard input, then replaces it with the read
        // end of the pipe this function owns.
        let saved = unsafe { libc::dup(0) };
        assert!(saved >= 0, "fixture saves stdin");
        assert_eq!(
            unsafe { libc::dup2(fds[0], 0) },
            0,
            "fixture redirects stdin"
        );
        unsafe { libc::close(fds[0]) };
        Self { saved }
    }
}

impl Drop for StdinFrom {
    fn drop(&mut self) {
        // SAFETY: restores the descriptor saved in `script`.
        unsafe {
            libc::dup2(self.saved, 0);
            libc::close(self.saved);
        }
    }
}
