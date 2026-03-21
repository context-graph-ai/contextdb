use super::common::*;
use std::process::Command;

/// I ran cargo build on an M1/M2 Mac, and it compiled and the tests passed.
#[test]
fn f49_build_on_macos_apple_silicon() {
    assert!(false, "requires special infrastructure");
}

/// I ran cargo build on Windows, and it compiled and the tests passed.
#[test]
fn f50_build_on_windows() {
    assert!(false, "requires special infrastructure");
}

/// I added contextdb as a dependency in CI, and I could compile and run integration tests without special setup beyond cargo.
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
