use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// I ran a representative persisted mixed workload in a child process, and peak RSS stayed
/// within the edge-device envelope instead of growing without bound.
#[test]
fn a10_05_peak_rss_within_2gb() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("peak-rss.db");
    let harness_bin = build_peak_rss_harness();

    let mut child = Command::new(harness_bin)
        .arg(&db_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("peak RSS harness should spawn");

    let pid = child.id();
    let start = Instant::now();
    let timeout = Duration::from_secs(120);
    let mut peak_rss_kib = 0usize;

    while start.elapsed() < timeout {
        if let Some(rss_kib) = linux_rss_kib(pid) {
            peak_rss_kib = peak_rss_kib.max(rss_kib);
        }
        if child.try_wait().expect("poll peak RSS harness").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let output = if child
        .try_wait()
        .expect("final peak RSS harness poll")
        .is_none()
    {
        let _ = child.kill();
        child
            .wait_with_output()
            .expect("collect timed out harness output")
    } else {
        child.wait_with_output().expect("collect harness output")
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "peak RSS harness failed before verification\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("WORKLOAD_DONE"),
        "peak RSS harness never reported workload completion\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        peak_rss_kib > 0,
        "rss sampler did not observe child process memory usage"
    );

    let peak_rss_bytes = peak_rss_kib.saturating_mul(1024);
    let budget_bytes = 2usize * 1024 * 1024 * 1024;
    assert!(
        peak_rss_bytes <= budget_bytes,
        "peak RSS exceeded 2 GiB budget: observed={} KiB\nstdout:\n{stdout}\nstderr:\n{stderr}",
        peak_rss_kib
    );
}

fn linux_rss_kib(pid: u32) -> Option<usize> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse::<usize>().ok()
    })
}

fn build_peak_rss_harness() -> std::path::PathBuf {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let status = Command::new("cargo")
        .current_dir(workspace_root)
        .args([
            "build",
            "-p",
            "contextdb-engine",
            "--example",
            "peak_rss_harness",
        ])
        .status()
        .expect("peak RSS harness build should start");
    assert!(
        status.success(),
        "peak RSS harness example build must succeed"
    );

    workspace_root
        .join("target")
        .join("debug")
        .join("examples")
        .join("peak_rss_harness")
}
