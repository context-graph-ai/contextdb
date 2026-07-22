use super::common::*;
use std::fs;
use std::path::Path;

/// I opened CONTRIBUTING.md, and it told me the exact verification commands (fmt, clippy, test) and the crate layout so I know how to submit a PR.
#[test]
fn f54_contributing_md_exists_and_is_accurate() {
    let path = workspace_root().join("CONTRIBUTING.md");
    let text = fs::read_to_string(&path).expect("CONTRIBUTING.md should exist");
    assert!(text.contains("cargo fmt --all --check"));
    assert!(text.contains("cargo clippy --workspace --all-targets -- -D warnings"));
    assert!(text.contains("cargo test"));
    assert!(text.contains("crates/"));
}

/// I inspected the test sources, and every ignored test had a reason — none were blank TODOs that leave me guessing.
#[test]
fn f55_ignored_tests_are_explained_to_contributors() {
    let mut ignored = Vec::new();
    collect_ignore_attributes(&workspace_root().join("tests"), &mut ignored);
    assert!(
        !ignored.is_empty(),
        "source scan should find the documented ignored cross-platform tests"
    );
    for (path, line_no, line) in ignored {
        assert!(
            line.contains("#[ignore = \"") && !line.contains("#[ignore = \"\"]"),
            "{}:{} ignored test must include a non-empty reason: {}",
            path.display(),
            line_no,
            line
        );
        assert!(
            !line.contains("TODO"),
            "{}:{} ignored test reason must not be TODO: {}",
            path.display(),
            line_no,
            line
        );
    }
}

fn collect_ignore_attributes(dir: &Path, ignored: &mut Vec<(std::path::PathBuf, usize, String)>) {
    for entry in fs::read_dir(dir).expect("read tests dir") {
        let entry = entry.expect("read tests entry");
        let path = entry.path();
        if path.is_dir() {
            collect_ignore_attributes(&path, ignored);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read test source");
        for (index, line) in source.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[ignore") {
                ignored.push((path.clone(), index + 1, trimmed.to_string()));
            }
        }
    }
}
