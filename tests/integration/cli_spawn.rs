use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::LazyLock;

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
    let profile_dir = profile_dir();
    let primary = profile_dir.join(cli_binary_name());
    if primary.exists() {
        return primary;
    }

    let alternate_profile = if profile_dir.ends_with("debug") {
        "release"
    } else {
        "debug"
    };
    let alternate = workspace_root()
        .join("target")
        .join(alternate_profile)
        .join(cli_binary_name());
    if alternate.exists() {
        return alternate;
    }

    if let Some(path) = option_env!("CARGO_BIN_EXE_contextdb-cli") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path;
        }
    }

    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_contextdb-cli") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path;
        }
    }

    panic!(
        "contextdb-cli binary was not built before the integration test ran; looked for {} and {}",
        primary.display(),
        alternate.display()
    );
}

fn cli_command(db_path: &Path, extra_args: &[&str]) -> Command {
    let mut command = Command::new(cli_bin());
    command.env_remove("CARGO_BIN_EXE_contextdb-cli");
    command.arg(db_path).args(extra_args);
    command
}

fn cli_binary_name() -> &'static str {
    if cfg!(windows) {
        "contextdb-cli.exe"
    } else {
        "contextdb-cli"
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn profile_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().and_then(Path::parent).map(Path::to_path_buf))
        .expect("test executable should live under target/<profile>/deps")
}
