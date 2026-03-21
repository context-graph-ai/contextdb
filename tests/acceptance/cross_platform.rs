use super::common::*;
use std::process::Command;

#[test]
fn f49_build_on_macos_apple_silicon() {
    assert!(false, "requires special infrastructure");
}

#[test]
fn f50_build_on_windows() {
    assert!(false, "requires special infrastructure");
}

#[test]
fn f51_run_contextdb_in_ci_for_application_tests() {
    let readme = std::fs::read_to_string(workspace_root().join("README.md")).expect("read README");
    assert!(readme.contains("contextdb-engine") || readme.contains("cargo test --workspace"));
    let output = Command::new("cargo")
        .current_dir(workspace_root())
        .args([
            "test",
            "-p",
            "contextdb-engine",
            "--test",
            "integration",
            "--no-run",
        ])
        .output()
        .expect("cargo test --no-run");
    assert!(output.status.success());
}
