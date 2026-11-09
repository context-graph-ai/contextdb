//! Runs one library test alone in a child of this test binary.
//!
//! Reader joins and leaves are serialized on the user's platform runtime
//! root, which every contextdb process of that user shares; a test that pins
//! an owner cursor or reclaims idle registries is refused while any other
//! process joins or leaves it. A child with a private root has no such
//! neighbour, so its outcome depends on its own source alone.

use std::ffi::OsStr;

/// Names the test a child process was started to run.
const CHILD_TEST: &str = "CONTEXTDB_TEST_CHILD";

/// Re-run the calling test, alone, in a child whose platform runtime root
/// (`XDG_RUNTIME_DIR`) is a fresh private directory, with `env` added, and
/// assert that the child ran exactly that test and it passed. Returns `true`
/// in the parent, which then returns at once, and `false` in the child, which
/// runs the test body.
pub(crate) fn rerun_alone(module_path: &str, test: &str, env: &[(&str, &OsStr)]) -> bool {
    let module = module_path
        .split_once("::")
        .map_or(module_path, |(_, rest)| rest);
    let name = format!("{module}::{test}");
    if std::env::var_os(CHILD_TEST).is_some_and(|running| running == name.as_str()) {
        return false;
    }
    // Beside the user's own runtime root, so the private root keeps its file
    // system (normally a memory file system) and its fsyncs cost the same.
    let parent = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .filter(|parent| parent.is_dir())
        .unwrap_or_else(std::env::temp_dir);
    let runtime = tempfile::Builder::new()
        .prefix("contextdb-test-")
        .tempdir_in(parent)
        .expect("a private runtime root");
    let mut child = std::process::Command::new(std::env::current_exe().expect("this test binary"));
    child
        .args(["--exact", &name, "--nocapture", "--test-threads=1"])
        .env(CHILD_TEST, &name)
        .env("XDG_RUNTIME_DIR", runtime.path())
        .envs(env.iter().copied());
    // The child dies with the test thread that started it, so a test the
    // harness terminates leaves no process behind.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;
        // SAFETY: prctl is async-signal-safe and touches no Rust state.
        unsafe {
            child.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
    }
    let output = child.output().expect("run the test in its child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed;"),
        "{name} in its child: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
