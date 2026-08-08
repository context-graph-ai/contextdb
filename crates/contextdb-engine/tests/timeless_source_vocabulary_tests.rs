//! Tracked implementation prose must describe durable behavior, not a past
//! execution session.  The one user-facing CLI phrase below is deliberately
//! excluded: it names the current command invocation rather than project work.

use std::path::PathBuf;
use std::process::Command;

fn historical_phrases() -> Vec<String> {
    vec![
        ["this", "run"].join(" "),
        ["this", "round"].join(" "),
        ["separately", "tracked", "gap"].join(" "),
        ["cold", "review", "#4"].join(" "),
        ["Finding", "1"].join(" "),
        ["Finding", "2"].join(" "),
        ["own", "commit"].join(" "),
        ["deferral", "ledger"].join("-"),
        ["fix", "round"].join(" "),
        ["owner", "ruling"].join(" "),
        ["review", "must-fix"].join(" "),
    ]
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

/// `git ls-files` scoped to the same three top-level directories, walked
/// directly off the filesystem. Used only when the source tree is not a git
/// repository (e.g. a `git archive` export) -- git stays the primary source
/// because it honors `.gitignore`, so this fallback is only reached when git
/// itself cannot answer.
fn walk_tracked_like_paths(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    for top in ["crates", "tests", "docs"] {
        let dir = root.join(top);
        if dir.is_dir() {
            walk_dir_into(&dir, root, &mut out);
        }
    }
    out.sort();
    out
}

fn walk_dir_into(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') || name == "target" {
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

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn has_token_boundary(bytes: &[u8], index: usize) -> bool {
    bytes
        .get(index)
        .is_none_or(|byte| !is_identifier_byte(*byte))
}

fn execution_token_end(bytes: &[u8], start: usize) -> Option<usize> {
    if !bytes.get(start).is_some_and(u8::is_ascii_alphabetic) {
        return None;
    }
    let mut cursor = start + 1;
    if bytes.get(cursor) == Some(&b'_') {
        cursor += 1;
    }
    let digits_start = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
        cursor += 1;
    }
    (cursor > digits_start).then_some(cursor)
}

fn contains_campaign_test_filename(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for index in 0..bytes.len() {
        if index > 0 && is_identifier_byte(bytes[index - 1]) {
            continue;
        }
        let Some(prefix_len) = [b"run_".as_slice(), b"run-".as_slice()]
            .iter()
            .find_map(|prefix| bytes[index..].starts_with(prefix).then_some(prefix.len()))
        else {
            continue;
        };
        let Some(token_end) = execution_token_end(bytes, index + prefix_len) else {
            continue;
        };
        let mut filename_end = token_end;
        while bytes
            .get(filename_end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-' | b'.'))
        {
            filename_end += 1;
        }
        if lower[index..filename_end].ends_with(".rs") {
            return true;
        }
    }
    false
}

fn contains_campaign_prose(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for index in 0..bytes.len() {
        if !bytes[index..].starts_with(b"run")
            || (index > 0 && is_identifier_byte(bytes[index - 1]))
        {
            continue;
        }
        let Some(separator) = bytes.get(index + 3) else {
            continue;
        };
        if !matches!(separator, b' ' | b'_' | b'-') {
            continue;
        }
        let mut token_start = index + 4;
        if *separator == b' ' {
            while bytes.get(token_start).is_some_and(u8::is_ascii_whitespace) {
                token_start += 1;
            }
        }
        let Some(token_end) = execution_token_end(bytes, token_start) else {
            continue;
        };
        if has_token_boundary(bytes, token_end) {
            return true;
        }
    }
    false
}

fn contains_uppercase_round_tag(value: &str) -> bool {
    let bytes = value.as_bytes();
    for index in 0..bytes.len() {
        if bytes[index] != b'R' || (index > 0 && is_identifier_byte(bytes[index - 1])) {
            continue;
        }
        let mut cursor = index + 1;
        let digits_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor == digits_start {
            continue;
        }
        if bytes.get(cursor) == Some(&b':') {
            return true;
        }
        if bytes.get(cursor) == Some(&b'\'')
            && bytes
                .get(cursor + 1)
                .is_some_and(|byte| matches!(*byte, b's' | b'S'))
            && has_token_boundary(bytes, cursor + 2)
        {
            return true;
        }
        if bytes.get(cursor) != Some(&b'/') || bytes.get(cursor + 1) != Some(&b'R') {
            continue;
        }
        cursor += 2;
        let second_digits_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor > second_digits_start && has_token_boundary(bytes, cursor) {
            return true;
        }
    }
    false
}

/// Case-sensitive whole-word match on a reviewer/agent/model name. Case-
/// sensitive so a lowercase, unrelated use of the same letters (a crate
/// named for a Pacific island time zone, a source label naming a chat CLI
/// in fixture data) is not mistaken for the capitalized reviewer identity.
/// Each name is assembled from split fragments so this dictionary is not
/// itself flagged when the audit below scans its own source file.
fn contains_reviewer_name(value: &str) -> bool {
    let names: [String; 6] = [
        ["Cod", "ex"].concat(),
        ["Op", "us"].concat(),
        ["Cla", "ude"].concat(),
        ["Sonn", "et"].concat(),
        ["Hai", "ku"].concat(),
        ["G", "PT"].concat(),
    ];
    let bytes = value.as_bytes();
    names.iter().any(|name| {
        value.match_indices(name.as_str()).any(|(start, matched)| {
            let end = start + matched.len();
            (start == 0 || !is_identifier_byte(bytes[start - 1])) && has_token_boundary(bytes, end)
        })
    })
}

/// A spelled-out, hyphenated, capitalized execution-round reference (a
/// capital "Round" immediately followed by a hyphen and digits) -- the
/// spelling this repo actually uses for that vocabulary. Scoped to the
/// hyphenated form only: a space-separated capitalized round reference
/// collides with legitimate, unrelated uses elsewhere in the tree (a smoke
/// test's own numbered scenario stages), which are not execution-session
/// vocabulary.
fn contains_spelled_out_round_tag(value: &str) -> bool {
    let bytes = value.as_bytes();
    for (start, _) in value.match_indices("Round-") {
        if start > 0 && is_identifier_byte(bytes[start - 1]) {
            continue;
        }
        let mut cursor = start + "Round-".len();
        let digits_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        if cursor > digits_start && has_token_boundary(bytes, cursor) {
            return true;
        }
    }
    false
}

#[test]
fn tracked_implementation_prose_has_no_execution_session_vocabulary() {
    let root = repo_root();
    let historical_phrases = historical_phrases();
    let invocation_phrase = ["this", "run"].join(" ");
    let invocation_failure_phrase = ["failure", "of", "this", "run"].join(" ");
    let commit_phrase = ["own", "commit"].join(" ");
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "crates", "tests", "docs"])
        // The not-a-repo detection matches git's English message; pin the
        // locale so a translated git cannot change the failure mode.
        .env("LC_ALL", "C")
        .output()
        .expect("list tracked implementation files");
    let rel_paths: Vec<String> = if output.status.success() {
        output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|rel| {
                std::str::from_utf8(rel)
                    .expect("UTF-8 tracked path")
                    .to_string()
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
        walk_tracked_like_paths(&root)
    };
    // An empty candidate set means the scan is vacuous — e.g. the source tree
    // was unpacked inside an unrelated git repository, where ls-files matches
    // nothing yet exits 0. A green audit must mean files were actually read.
    assert!(
        !rel_paths.is_empty(),
        "vocabulary audit found no files to scan under crates/tests/docs"
    );

    let mut hits = Vec::new();
    for rel in &rel_paths {
        let rel = rel.as_str();
        if contains_campaign_test_filename(rel) || contains_campaign_prose(rel) {
            hits.push(format!(
                "{rel}: execution-campaign vocabulary in tracked path"
            ));
        }
        let scoped = rel.contains("/src/")
            || rel.contains("/tests/")
            || rel.starts_with("tests/")
            || rel.starts_with("docs/");
        if !scoped || !(rel.ends_with(".rs") || rel.ends_with(".md")) {
            continue;
        }
        let path = root.join(rel);
        if !path.is_file() {
            continue;
        }
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read tracked implementation file {rel}: {error}"));
        for (line_number, line) in body.lines().enumerate() {
            let lower = line.to_ascii_lowercase();
            for phrase in &historical_phrases {
                let phrase_lower = phrase.to_ascii_lowercase();
                let Some(offset) = lower.find(&phrase_lower) else {
                    continue;
                };
                let before = lower.as_bytes().get(offset.wrapping_sub(1));
                let after = lower.as_bytes().get(offset + phrase_lower.len());
                if before.is_some_and(u8::is_ascii_alphanumeric)
                    || after.is_some_and(u8::is_ascii_alphanumeric)
                {
                    continue;
                }
                // The one allowed CLI wording names the invocation currently
                // being parsed.  Durable own-commit vocabulary is allowed
                // only for the database ordering statements that say what
                // the commit LSN/position/committed state means.
                let durable_product_vocabulary = if phrase == &invocation_phrase {
                    rel == "crates/contextdb-cli/src/main.rs"
                        && lower.contains(invocation_failure_phrase.as_str())
                } else if phrase == &commit_phrase {
                    !lower.contains("re-aimed")
                        && !lower.contains("sanctioned flip")
                        && !lower.contains("see its subject")
                        && [
                            "lsn",
                            "position",
                            "committed",
                            "arrival",
                            "atomic",
                            "prune",
                            "trim",
                        ]
                        .iter()
                        .any(|term| lower.contains(term))
                } else {
                    false
                };
                if !durable_product_vocabulary {
                    hits.push(format!("{rel}:{}: {phrase}: {line}", line_number + 1));
                }
            }
            if contains_campaign_test_filename(line) {
                hits.push(format!(
                    "{rel}:{}: campaign-shaped test filename: {line}",
                    line_number + 1
                ));
            }
            if contains_campaign_prose(line) {
                hits.push(format!(
                    "{rel}:{}: execution-campaign prose: {line}",
                    line_number + 1
                ));
            }
            if contains_uppercase_round_tag(line) {
                hits.push(format!(
                    "{rel}:{}: execution-round tag: {line}",
                    line_number + 1
                ));
            }
            if contains_reviewer_name(line) {
                hits.push(format!(
                    "{rel}:{}: reviewer/agent/model name: {line}",
                    line_number + 1
                ));
            }
            if contains_spelled_out_round_tag(line) {
                hits.push(format!(
                    "{rel}:{}: spelled-out execution-round tag: {line}",
                    line_number + 1
                ));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "implementation-history vocabulary must be removed from tracked production/tests/docs:\n{}",
        hits.join("\n")
    );
}

/// The filesystem-walk fallback only stands in for `git ls-files` when git is
/// unavailable, so it must find at least everything git finds. Confirms scan
/// parity (walk is a superset-or-equal of the git-tracked set) on this repo,
/// where both sources are available to compare.
#[test]
fn filesystem_walk_fallback_is_a_superset_of_git_tracked_paths() {
    let root = repo_root();
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "crates", "tests", "docs"])
        // The not-a-repo detection matches git's English message; pin the
        // locale so a translated git cannot change the failure mode.
        .env("LC_ALL", "C")
        .output()
        .expect("list tracked implementation files");
    if !output.status.success() {
        // No git repository here either -- nothing to compare against.
        return;
    }

    let git_paths: std::collections::BTreeSet<String> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|rel| {
            std::str::from_utf8(rel)
                .expect("UTF-8 tracked path")
                .to_string()
        })
        .collect();
    let walk_paths: std::collections::BTreeSet<String> =
        walk_tracked_like_paths(&root).into_iter().collect();

    let missing: Vec<&String> = git_paths.difference(&walk_paths).collect();
    assert!(
        missing.is_empty(),
        "filesystem walk fallback missed git-tracked paths that a git-free export must still scan: {missing:?}"
    );
}
