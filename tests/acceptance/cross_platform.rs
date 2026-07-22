use super::common::*;

/// I ran cargo build on an M1/M2 Mac, and it compiled and the tests passed.
#[test]
#[ignore = "validated by CI cross-compile workflow (aarch64-apple-darwin)"]
fn f49_build_on_macos_apple_silicon() {}

/// I ran cargo build on Windows, and it compiled and the tests passed.
#[test]
#[ignore = "no Windows CI target yet"]
fn f50_build_on_windows() {}

/// I can identify the CI-facing engine crate and integration test entrypoint without special setup beyond cargo.
#[test]
fn f51_run_contextdb_in_ci_for_application_tests() {
    let readme = std::fs::read_to_string(workspace_root().join("README.md")).expect("read README");
    assert!(readme.contains("contextdb-engine") || readme.contains("cargo test --workspace"));
    let engine_manifest =
        std::fs::read_to_string(workspace_root().join("crates/contextdb-engine/Cargo.toml"))
            .expect("read engine manifest");
    assert!(engine_manifest.contains("name = \"contextdb-engine\""));
    assert!(workspace_root().join("tests/integration.rs").exists());
}
