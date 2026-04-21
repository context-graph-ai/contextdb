// ======== T11 ========

#[test]
fn docs_query_language_lists_txid_column_type() {
    // Runtime read of docs/query-language.md at the workspace root.
    // Walk upward from CARGO_MANIFEST_DIR to locate the workspace root.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut root = manifest_dir.clone();
    let docs_path = loop {
        let candidate = root.join("docs").join("query-language.md");
        if candidate.exists() {
            break candidate;
        }
        if !root.pop() {
            panic!(
                "could not locate docs/query-language.md walking up from {}",
                manifest_dir.display()
            );
        }
    };

    let contents = std::fs::read_to_string(&docs_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", docs_path.display()));

    // Parse the Column Types table. Locate the `## Column Types` heading
    // (case-insensitive) and collect every subsequent line that begins with `|`
    // until a blank line or the next heading.
    let mut in_table = false;
    let mut rows: Vec<String> = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if !in_table {
            if trimmed.eq_ignore_ascii_case("## Column Types")
                || trimmed.eq_ignore_ascii_case("### Column Types")
            {
                in_table = true;
            }
            continue;
        }
        if trimmed.is_empty() {
            // Tables must be contiguous pipe lines; blank ends the table.
            if !rows.is_empty() {
                break;
            }
            continue;
        }
        if trimmed.starts_with('#') {
            break;
        }
        if trimmed.starts_with('|') {
            rows.push(trimmed.to_string());
        }
    }

    assert!(
        !rows.is_empty(),
        "no Column Types markdown table found in {}",
        docs_path.display()
    );

    // Find a row whose first cell (case-insensitive, trimmed) equals "TXID".
    // Skip rows that are header-separator lines (`|---|---|`).
    let txid_row = rows
        .iter()
        .find(|row| {
            let cells: Vec<&str> = row.trim_matches('|').split('|').map(|c| c.trim()).collect();
            if cells.is_empty() {
                return false;
            }
            // Separator lines look like `---`, `:---:`, etc.
            if cells
                .iter()
                .all(|c| c.chars().all(|ch| ch == '-' || ch == ':'))
            {
                return false;
            }
            cells[0].eq_ignore_ascii_case("TXID")
        })
        .unwrap_or_else(|| {
            panic!(
                "Column Types table has no row whose first column is `TXID`. Table rows:\n{}",
                rows.join("\n")
            )
        });

    // The row text must mention `Value::TxId` so readers can find the variant.
    assert!(
        txid_row.contains("Value::TxId"),
        "TXID row must mention `Value::TxId` so readers can locate the variant; got: {txid_row}"
    );
}

// ======== T33 ========

use std::collections::BTreeSet;
use std::path::PathBuf;

use regex::Regex;
use walkdir::WalkDir;

fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().to_path_buf()
}

#[test]
fn timestamp_audit_no_new_txid_shaped_columns() {
    // Whitelist of known-legitimate `<col> TIMESTAMP` occurrences in the tree.
    // Each entry is (relative_path, line_number) of a real wall-clock column declaration.
    // These are not transaction identifiers — they are user-visible timestamps.
    let whitelist: BTreeSet<(String, u32)> = [
        (
            "crates/contextdb-engine/tests/sql_surface_tests.rs".to_string(),
            888u32,
        ),
        ("tests/acceptance/query_surface.rs".to_string(), 686u32),
        ("tests/acceptance/query_surface.rs".to_string(), 1114u32),
        ("tests/integration/retention_tests.rs".to_string(), 1366u32),
        ("tests/integration/retention_tests.rs".to_string(), 1414u32),
    ]
    .into_iter()
    .collect();

    // Anchored regex: three column-name prefixes, each followed by `TIMESTAMP`.
    let re = Regex::new(
        r"\bcreated_at\s+TIMESTAMP\b|\bvalid_from\s+TIMESTAMP\b|\bvalid_to\s+TIMESTAMP\b",
    )
    .unwrap();

    let root = workspace_root();
    let contextdb_root = root.join("contextdb");
    let scan_root = if contextdb_root.exists() {
        contextdb_root
    } else {
        root.clone()
    };

    let mut hits: BTreeSet<(String, u32)> = BTreeSet::new();

    for entry in WalkDir::new(&scan_root).into_iter().filter_map(Result::ok) {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let ext = p.extension().and_then(|s| s.to_str());
        if ext != Some("rs") {
            continue;
        }
        // Skip the audit test itself (which contains the regex as a literal).
        if p.file_name().and_then(|s| s.to_str()) == Some("timestamp_audit.rs") {
            continue;
        }

        let Ok(source) = std::fs::read_to_string(p) else {
            continue;
        };
        for (line_no, line) in source.lines().enumerate() {
            if re.is_match(line) {
                let rel = p
                    .strip_prefix(&root)
                    .unwrap_or(p)
                    .to_string_lossy()
                    .replace('\\', "/");
                hits.insert((rel, (line_no + 1) as u32));
            }
        }
    }

    assert_eq!(
        hits,
        whitelist,
        "TIMESTAMP audit set mismatch: expected exact equality with whitelist. \
         Extra hits (NEW uses of created_at/valid_from/valid_to TIMESTAMP): {:?}. \
         Missing hits (whitelist entries no longer present): {:?}.",
        hits.difference(&whitelist).collect::<Vec<_>>(),
        whitelist.difference(&hits).collect::<Vec<_>>(),
    );
}
