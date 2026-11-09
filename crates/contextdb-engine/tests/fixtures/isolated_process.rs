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
        "isolated journey {filter} failed ({}).\n{}\n\n--- the child's stdout ---\n{stdout}\n\
         --- the child's stderr ---\n{stderr}",
        output.status,
        failure_cause(&filter, &stdout, &stderr),
    );
    true
}

/// Every panic the child printed, each as its `panicked at <file>:<line>:<column>:`
/// header followed by the message it carried, so the parent's failure names the
/// child's real cause first instead of leaving it somewhere in the raw output.
fn child_panics(output: &str) -> Vec<String> {
    let mut panics = Vec::new();
    let mut lines = output.lines().peekable();
    while let Some(header) = lines.next() {
        if !header.contains(" panicked at ") {
            continue;
        }
        let mut panic = vec![header.trim_end()];
        while let Some(next) = lines.peek() {
            if next.starts_with("note: ")
                || next.starts_with("stack backtrace:")
                || next.contains(" panicked at ")
            {
                break;
            }
            panic.push(next.trim_end());
            lines.next();
        }
        panics.push(panic.join("\n").trim_end().to_owned());
    }
    panics
}

/// What went wrong in the child, named from its own output.
fn failure_cause(filter: &str, stdout: &str, stderr: &str) -> String {
    let mut panics = child_panics(stderr);
    panics.extend(child_panics(stdout));
    if !panics.is_empty() {
        return format!("the child's own panic:\n{}", panics.join("\n\n"));
    }
    if stdout.contains("running 0 tests") {
        return format!("the child ran no test: the exact filter `{filter}` matched nothing");
    }
    "the child printed no panic: it exited before reporting a passing result".to_owned()
}
