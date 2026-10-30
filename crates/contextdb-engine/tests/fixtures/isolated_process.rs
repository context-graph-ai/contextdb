//! Process-wide observations need a process owned by their one journey.

/// Return true in the parent after the exact test has passed in its own
/// process, or false in that child so it runs the original journey.
/// Sibling tests still run concurrently, each with independent OS resources,
/// allocator state and instrumentation. No retry or serial harness is used.
pub fn run_in_own_process(module: &str, test: &str) -> bool {
    const CHILD: &str = "CONTEXTDB_ISOLATED_OBSERVATION_TEST";
    let module = module.split_once("::").map(|(_, rest)| rest).unwrap_or("");
    let filter = if module.is_empty() {
        test.to_owned()
    } else {
        format!("{module}::{test}")
    };
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new(&filter)) {
        return false;
    }
    let output = std::process::Command::new(
        std::env::current_exe().expect("locate this journey's test binary"),
    )
    .args(["--exact", &filter, "--nocapture"])
    .env(CHILD, &filter)
    .output()
    .expect("run the journey in its own process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed;"),
        "isolated journey {filter} failed ({:?}):\n{stdout}\n{stderr}",
        output.status,
    );
    true
}
