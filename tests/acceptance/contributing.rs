use super::common::*;
use std::fs;
use std::process::Command;

#[test]
fn f54_contributing_md_exists_and_is_accurate() {
    let path = workspace_root().join("CONTRIBUTING.md");
    let text = fs::read_to_string(&path).expect("CONTRIBUTING.md should exist");
    assert!(text.contains("cargo fmt --all --check"));
    assert!(text.contains("cargo clippy --workspace --all-targets -- -D warnings"));
    assert!(text.contains("cargo test"));
    assert!(text.contains("crates/"));
}

#[test]
fn f55_ignored_tests_are_explained_to_contributors() {
    let output = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["test", "--test", "acceptance", "--", "--list"])
        .output()
        .expect("cargo test --list");
    assert!(output.status.success());
    let combined = format!(
        "{}{}",
        output_string(&output.stdout),
        output_string(&output.stderr)
    );
    assert!(!combined.contains("#[ignore = \"\"]"));
    assert!(!combined.contains("TODO"));
}
