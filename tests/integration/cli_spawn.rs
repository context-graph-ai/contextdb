use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

pub(crate) fn spawn_cli(db_path: impl AsRef<Path>, extra_args: &[&str]) -> Child {
    cargo_run_command(db_path.as_ref(), extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn through cargo run")
}

pub(crate) fn spawn_cli_no_stdin(db_path: impl AsRef<Path>, extra_args: &[&str]) -> Child {
    cargo_run_command(db_path.as_ref(), extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn through cargo run")
}

pub(crate) fn cli_bin() -> PathBuf {
    PathBuf::from("cargo")
}

fn cargo_run_command(db_path: &Path, extra_args: &[&str]) -> Command {
    let mut command = Command::new("cargo");
    command
        .args(["run", "-p", "contextdb-cli", "--"])
        .arg(db_path)
        .args(extra_args)
        .current_dir(workspace_root());
    command
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
}
