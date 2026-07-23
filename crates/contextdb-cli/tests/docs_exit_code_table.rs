//! Docs binding for the exit-code contract — `docs/cli.md` must carry a
//! `### Exit Codes` section naming exactly the four process exit codes every
//! contextdb binary honors.
//!
//! The values pinned here (0/1/2/3) mirror the
//! `contextdb_server::exit_codes::EXIT_*` constants verbatim. This test
//! intentionally does NOT import that module, so the literal contract is
//! asserted directly instead — this test's job is to fail loud if a code is
//! added to code without docs, or to docs without code.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    // This file lives at crates/contextdb-cli/tests/, so CARGO_MANIFEST_DIR
    // resolves to crates/contextdb-cli — walk up two levels to the workspace
    // root where docs/ lives.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root must be two levels above crate manifest dir")
        .to_path_buf()
}

fn read_doc(rel: &str) -> String {
    let path = workspace_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read doc {}: {e}", path.display()))
}

/// Extract the text of a markdown section bounded by a heading of the given
/// level (2 = `## `, 3 = `### `), from the heading line to the next
/// same-or-higher-level heading (exclusive). `None` when the heading is
/// absent.
fn section(doc: &str, level: usize, heading_text: &str) -> Option<String> {
    let prefix = "#".repeat(level) + " ";
    let mut in_section = false;
    let mut out = String::new();
    for line in doc.lines() {
        if !in_section {
            if line.starts_with(&prefix)
                && line[prefix.len()..].trim_start().starts_with(heading_text)
            {
                in_section = true;
                out.push_str(line);
                out.push('\n');
            }
        } else {
            let stop = (1..=level).any(|l| line.starts_with(&("#".repeat(l) + " ")));
            if stop {
                break;
            }
            out.push_str(line);
            out.push('\n');
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

/// The leading `| <code> |` cell of a markdown table row, parsed as an
/// integer — `None` for any line that is not such a row.
fn row_leading_code(line: &str) -> Option<i64> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') {
        return None;
    }
    let first_cell = trimmed.trim_matches('|').split('|').next()?;
    first_cell.trim().trim_matches('`').parse::<i64>().ok()
}

/// The frozen four-row exit-code contract:
/// (code, a keyword that must appear on its documented row).
const EXPECTED_CODES: &[(i64, &str)] = &[
    (0, "Success"),
    (1, "Error"),
    (2, "Usage"),
    (3, "unconfirmed"),
];

#[test]
fn docs_exit_code_table_matches_the_constants() {
    let doc = read_doc("docs/cli.md");
    let section = section(&doc, 3, "Exit Codes")
        .expect("docs/cli.md must contain a `### Exit Codes` section");

    let mut found_codes: Vec<i64> = section.lines().filter_map(row_leading_code).collect();
    found_codes.sort_unstable();
    found_codes.dedup();
    assert_eq!(
        found_codes,
        vec![0, 1, 2, 3],
        "the Exit Codes section must document exactly the codes {{0,1,2,3}}, got {found_codes:?}\nsection:\n{section}"
    );

    for (code, keyword) in EXPECTED_CODES {
        let row = section
            .lines()
            .find(|line| row_leading_code(line) == Some(*code))
            .unwrap_or_else(|| panic!("no row documents code {code}\nsection:\n{section}"));
        assert!(
            row.contains(keyword),
            "code {code}'s row must describe it as {keyword:?}, got: {row}"
        );
    }
}
