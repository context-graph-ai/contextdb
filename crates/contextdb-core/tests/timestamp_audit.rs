// ======== T33 ========

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use regex::Regex;

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

/// `git ls-files -z` scoped to the whole workspace root (no pathspec), walked
/// directly off the filesystem. Duplicated from the sibling fallback in
/// `crates/contextdb-engine/tests/engine/timeless_source_vocabulary_tests.rs`
/// (cross-crate test-support sharing isn't available); used only when the
/// source tree is not a git repository (e.g. a `git archive` export) --
/// git stays the primary source because it honors `.gitignore`, so this
/// fallback is only reached when git itself cannot answer.
fn walk_tracked_like_paths(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    walk_dir_into(root, root, &mut out);
    out.sort();
    out
}

fn walk_dir_into(dir: &Path, root: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == ".git" || name == "target" || name == ".worktrees" {
            continue;
        }
        if path.is_dir() {
            walk_dir_into(&path, root, out);
        } else if path.is_file()
            && let Ok(rel) = path.strip_prefix(root)
        {
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
}

fn path_is_excluded(rel: &str) -> bool {
    rel.split('/')
        .any(|component| component == ".git" || component == ".worktrees")
}

fn collapse_ws(line: &str) -> String {
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Pin a hit by the CREATE TABLE line with whitespace collapsed, or by the
/// column-declaration token sequence when the line is not a CREATE TABLE.
fn declaration_key(line: &str, re: &Regex) -> String {
    let collapsed = collapse_ws(line);
    let upper = collapsed.to_ascii_uppercase();
    if let Some(start) = upper.find("CREATE TABLE") {
        let from_create = &collapsed[start..];
        let trimmed = from_create
            .trim_end_matches([',', ';'])
            .trim_end_matches(['"', '\''])
            .trim_end();
        collapse_ws(trimmed)
    } else if let Some(m) = re.find(line) {
        collapse_ws(m.as_str())
    } else {
        collapsed
    }
}

fn tracked_paths(scan_root: &Path) -> Vec<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(scan_root)
        .args(["ls-files", "-z"])
        // The not-a-repo detection matches git's English message; pin the
        // locale so a translated git cannot change the failure mode.
        .env("LC_ALL", "C")
        .output()
        .expect("list tracked files");
    let rel_paths: Vec<String> = if output.status.success() {
        output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|rel| {
                std::str::from_utf8(rel)
                    .expect("UTF-8 tracked path")
                    .replace('\\', "/")
            })
            .collect()
    } else {
        // Not every source tree that must pass this gate is a git checkout --
        // a `git archive` export or source tarball has no `.git` directory.
        // Fall back to a plain filesystem walk over the same scope; any
        // OTHER git failure (corrupt repo, permissions, ...) still panics.
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("not a git repository"),
            "git ls-files failed: {output:?}"
        );
        walk_tracked_like_paths(scan_root)
    };
    // An empty candidate set means the scan is vacuous -- e.g. the source
    // tree was unpacked inside an unrelated git repository, where ls-files
    // matches nothing yet exits 0. A green audit must mean files were
    // actually read.
    assert!(
        !rel_paths.is_empty(),
        "timestamp audit found no tracked files to scan under {}",
        scan_root.display()
    );
    rel_paths
}

#[test]
fn timestamp_audit_no_new_txid_shaped_columns() {
    // Whitelist of known-legitimate `<col> TIMESTAMP` occurrences in the tree.
    // Each entry is (relative_path, normalized declaration text) of a real
    // wall-clock column declaration. These are not transaction identifiers —
    // they are user-visible timestamps.
    let whitelist: BTreeMap<(String, String), usize> = [
        (
            "crates/contextdb-engine/tests/sql_surface/sql_surface_tests.rs".to_string(),
            "CREATE TABLE events (id UUID PRIMARY KEY, created_at TIMESTAMP DEFAULT NOW())"
                .to_string(),
        ),
        (
            "tests/acceptance/query_surface.rs".to_string(),
            "CREATE TABLE messages (id UUID PRIMARY KEY, conversation_id UUID, body TEXT, embedding VECTOR(3), created_at TIMESTAMP)"
                .to_string(),
        ),
        (
            "benches/indexed_scan_filter_entity_list.rs".to_string(),
            "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT, name TEXT, created_at TIMESTAMP)"
                .to_string(),
        ),
        (
            "tests/acceptance/query_surface.rs".to_string(),
            "CREATE TABLE messages (id UUID PRIMARY KEY, created_at TIMESTAMP, embedding VECTOR(3))"
                .to_string(),
        ),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP)".to_string(),
        ),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP)".to_string(),
        ),
        (
            "tests/integration/indexed_scan_filter_tests.rs".to_string(),
            "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT, name TEXT, created_at TIMESTAMP)"
                .to_string(),
        ),
        (
            "tests/integration/retention_tests.rs".to_string(),
            "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP EXPIRES) RETAIN 1 SECONDS"
                .to_string(),
        ),
        (
            "tests/integration/retention_tests.rs".to_string(),
            "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP EXPIRES) RETAIN 1 SECONDS"
                .to_string(),
        ),
    ]
    .into_iter()
    .fold(BTreeMap::new(), |mut counts, key| {
        *counts.entry(key).or_insert(0) += 1;
        counts
    });

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

    let mut hits: BTreeMap<(String, String), usize> = BTreeMap::new();

    for rel in tracked_paths(&scan_root) {
        if path_is_excluded(&rel) {
            continue;
        }
        if !rel.ends_with(".rs") {
            continue;
        }
        // Skip the audit test itself (which contains the regex as a literal).
        if rel.rsplit('/').next() == Some("timestamp_audit.rs") {
            continue;
        }

        let path = scan_root.join(&rel);
        if !path.is_file() {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in source.lines() {
            if re.is_match(line) {
                *hits
                    .entry((rel.clone(), declaration_key(line, &re)))
                    .or_insert(0) += 1;
            }
        }
    }

    let extra: Vec<_> = hits
        .iter()
        .filter(|(key, count)| whitelist.get(*key) != Some(*count))
        .collect();
    let missing: Vec<_> = whitelist
        .iter()
        .filter(|(key, count)| hits.get(*key) != Some(*count))
        .collect();
    assert_eq!(
        hits, whitelist,
        "TIMESTAMP audit mismatch: each whitelisted declaration must occur exactly as many times as listed. \
         Extra or more frequent hits (NEW uses of created_at/valid_from/valid_to TIMESTAMP): {extra:?}. \
         Missing or less frequent hits (whitelist entries no longer present): {missing:?}.",
    );
}
