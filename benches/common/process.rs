use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};

static BUILD_RELEASE_BINARIES: Once = Once::new();

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

pub fn target_dir() -> PathBuf {
    workspace_root().join("target").join("release")
}

pub fn cli_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-cli");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

pub fn server_bin() -> PathBuf {
    let mut path = target_dir().join("contextdb-server");
    if cfg!(windows) {
        path.set_extension("exe");
    }
    path
}

pub fn ensure_release_binaries() {
    BUILD_RELEASE_BINARIES.call_once(|| {
        if cli_bin().is_file() && server_bin().is_file() {
            return;
        }
        let status = Command::new("cargo")
            .current_dir(workspace_root())
            .args([
                "build",
                "--release",
                "-p",
                "contextdb-cli",
                "-p",
                "contextdb-server",
            ])
            .status()
            .expect("release build command should start");
        assert!(status.success(), "release build should succeed");
    });
}

pub fn run_cli_script(db_path: &Path, args: &[&str], script: &str) -> Output {
    ensure_release_binaries();
    let mut child = Command::new(cli_bin())
        .arg(db_path)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should spawn");
    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(script.as_bytes())
        .expect("write script");
    child.wait_with_output().expect("CLI should finish")
}

pub fn spawn_server(db_path: &Path, tenant_id: &str, nats_url: &str) -> Child {
    ensure_release_binaries();
    Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 path"),
            "--tenant-id",
            tenant_id,
            "--nats-url",
            nats_url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("server should spawn")
}

pub fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_server_with_barrier(
    db_path: &Path,
    tenant_id: &str,
    nats_url: &str,
    barrier_path: &Path,
    release_path: &Path,
    min_rows: usize,
    stderr_path: &Path,
) -> Child {
    ensure_release_binaries();
    let stderr_file = std::fs::File::create(stderr_path).expect("server stderr");
    Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 path"),
            "--tenant-id",
            tenant_id,
            "--nats-url",
            nats_url,
        ])
        .env("CONTEXTDB_TEST_PUSH_BARRIER_MIN_ROWS", min_rows.to_string())
        .env("CONTEXTDB_TEST_PUSH_BARRIER_FILE", barrier_path)
        .env("CONTEXTDB_TEST_PUSH_RELEASE_FILE", release_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .expect("server should spawn")
}

pub fn wait_for_child_output(mut child: Child, timeout: Duration, context: &str) -> Output {
    let start = Instant::now();
    loop {
        match child.try_wait().expect("child status poll") {
            Some(_) => return child.wait_with_output().expect(context),
            None => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    return child
                        .wait_with_output()
                        .expect("child should finish after timeout kill");
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
