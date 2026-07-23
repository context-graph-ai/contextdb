//! Source-scan guard for the exit-code contract. The example binaries under
//! `examples/` are demo callers that themselves spawn/drive long-running
//! processes; running them from a test would spawn `cargo run` inside
//! `cargo test`, which deadlocks on the shared `target/` lock (AGENTS.md:
//! "Never run multiple cargo commands in parallel"). So their exit-code
//! contract is pinned here BY CONSTRUCTION — a scan for a bare integer
//! literal passed to `process::exit` — rather than by executing them; the
//! shared verdict functions (`exit_code_for`, `verdict_exit_code`) get their
//! own unit-test coverage in `exit_codes.rs`.

use std::path::PathBuf;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel_to_manifest: &str) -> String {
    let path = manifest_dir().join(rel_to_manifest);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Every `.rs` file directly inside this crate's `examples/` (non-recursive
/// — the demos are single files today).
fn example_files() -> Vec<PathBuf> {
    let dir = manifest_dir().join("examples");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"))
        .collect();
    out.sort();
    out
}

/// `true` when `haystack` contains `process::exit(` immediately followed
/// (after any ASCII whitespace) by an ASCII digit — a bare integer literal
/// exit code, rather than a named `exit_codes::EXIT_*` constant or a call to
/// `exit_code_for(..)` / `verdict_exit_code(..)`.
fn contains_bare_integer_exit(haystack: &str) -> bool {
    const NEEDLE: &str = "process::exit(";
    let mut idx = 0;
    while let Some(pos) = haystack[idx..].find(NEEDLE) {
        let start = idx + pos + NEEDLE.len();
        let rest = haystack[start..].trim_start();
        if rest.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            return true;
        }
        idx = start;
    }
    false
}

#[test]
fn no_binary_exits_with_a_bare_integer_literal() {
    let mut sources: Vec<(String, String)> = vec![(
        "crates/contextdb-server/src/main.rs".to_string(),
        read("src/main.rs"),
    )];

    let cli_main = manifest_dir()
        .parent()
        .expect("crates/ directory must exist above contextdb-server")
        .join("contextdb-cli/src/main.rs");
    sources.push((
        "crates/contextdb-cli/src/main.rs".to_string(),
        std::fs::read_to_string(&cli_main)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", cli_main.display())),
    ));

    for path in example_files() {
        let label = format!(
            "crates/contextdb-server/examples/{}",
            path.file_name().expect("file name").to_string_lossy()
        );
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        sources.push((label, content));
    }

    let violations: Vec<&str> = sources
        .iter()
        .filter(|(_, content)| contains_bare_integer_exit(content))
        .map(|(label, _)| label.as_str())
        .collect();

    assert!(
        violations.is_empty(),
        "every exit site must name an exit_codes::EXIT_* constant or call \
         exit_code_for/verdict_exit_code, never a bare integer literal; \
         found a bare-literal process::exit( in: {violations:?}"
    );
}

#[test]
fn media_demo_scan_reports_a_marker_hit_as_a_failure() {
    let demo = read("examples/media_transfer_fabric_demo.rs");
    assert!(
        demo.contains("verdict_exit_code("),
        "run_scan_hub must report a marker hit through verdict_exit_code(..) \
         — printing hub_marker_found=true and returning would let `main` \
         exit 0 even when the scan finds a leak"
    );
    assert!(
        !demo.contains("EXIT_OUTCOME_DEVIATION"),
        "EXIT_OUTCOME_DEVIATION must be deleted — exit code 3 means exactly \
         one thing (interrupted-push-unconfirmed) across every binary in the \
         repo, not \"a rogue got through\""
    );
}
