//! CLI operational commands. Their typed result is the process exit code;
//! on a healthy store with well-formed arguments each must exit 0.

use super::fixture::Store;
use super::{Outcome, Row, call};
use contextdb_engine::vector_read_memory::DeclaredReadMemory;

const READER_LIMIT: u64 = 64 * 1024 * 1024;

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
                strict(call(|| exit(contextdb_cli::ops::run_operational("snapshot", &args, factory))))
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
                strict(call(|| exit(contextdb_cli::ops::dispatch_if_subcommand(&args, factory))))
            },
        },
    ]
}
