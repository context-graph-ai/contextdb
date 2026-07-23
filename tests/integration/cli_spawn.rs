use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::LazyLock;

// Binary resolution (the newest-mtime-of-debug/release rule, plus the
// CONTEXTDB_TEST_BIN_PROFILE override) lives once in tests/support/binary_path.rs,
// shared with tests/acceptance/common.rs -- see that module's doc comment for
// the reasoning.
static CLI_BIN: LazyLock<PathBuf> = LazyLock::new(resolve_cli_bin);

pub(crate) fn spawn_cli(db_path: impl AsRef<Path>, extra_args: &[&str]) -> Child {
    cli_command(db_path.as_ref(), extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn")
}

pub(crate) fn spawn_cli_no_stdin(db_path: impl AsRef<Path>, extra_args: &[&str]) -> Child {
    cli_command(db_path.as_ref(), extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn")
}

pub(crate) fn cli_bin() -> PathBuf {
    CLI_BIN.clone()
}

fn resolve_cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_contextdb-cli") {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_contextdb-cli") {
        return PathBuf::from(path);
    }
    crate::binary_path::resolve_workspace_binary("contextdb-cli")
}

fn cli_command(db_path: &Path, extra_args: &[&str]) -> Command {
    let mut command = Command::new(cli_bin());
    command.env_remove("CARGO_BIN_EXE_contextdb-cli");
    command.arg(db_path).args(extra_args);
    command
}
