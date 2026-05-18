use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Once;

static BUILD_RELEASE_CLI: Once = Once::new();

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
    let mut path = workspace_root()
        .join("target")
        .join("release")
        .join("contextdb-cli");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

fn ensure_release_cli() {
    BUILD_RELEASE_CLI.call_once(|| {
        let status = Command::new("cargo")
            .current_dir(workspace_root())
            .args(["build", "--release", "-p", "contextdb-cli"])
            .status()
            .expect("release CLI build command should start");
        assert!(status.success(), "release CLI build should succeed");
    });
}

fn cli_command(db_path: &Path, extra_args: &[&str]) -> Command {
    ensure_release_cli();
    let mut command = Command::new(cli_bin());
    command.arg(db_path).args(extra_args);
    command
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}
