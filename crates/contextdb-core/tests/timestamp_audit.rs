// ======== T33 ========

use std::collections::BTreeSet;

use regex::Regex;
use walkdir::WalkDir;

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

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
        ("tests/acceptance/query_surface.rs".to_string(), 690u32),
        (
            "benches/indexed_scan_filter_entity_list.rs".to_string(),
            15u32,
        ),
        ("tests/acceptance/query_surface.rs".to_string(), 1120u32),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            1994u32,
        ),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            2020u32,
        ),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            2920u32,
        ),
        ("tests/integration/retention_tests.rs".to_string(), 1652u32),
        ("tests/integration/retention_tests.rs".to_string(), 1698u32),
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
