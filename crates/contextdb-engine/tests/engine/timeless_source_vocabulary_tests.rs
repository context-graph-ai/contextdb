//! Tracked source, tests, tooling, and documentation must describe durable
//! behavior, not a past execution session, the planning documents behind it,
//! or the build workspace that produced it: a promise is stated in product
//! terms rather than cited by clause number or checklist letter.  The one
//! user-facing CLI phrase below is deliberately excluded: it names the current
//! command invocation rather than project work.

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

/// Phrases that cite a planning document, a build workspace, or a past
/// execution instead of stating the behavior itself. Assembled from
/// fragments so this file does not flag its own dictionary.
fn planning_citation_phrases() -> Vec<String> {
    vec![
        ["approved", "intent"].join(" "),
        ["vector-search", "intent"].join(" "),
        ["root", "intent"].join(" "),
        ["of", "the", "intent"].join(" "),
        ["intent", "statement"].join(" "),
        ["design", "brief"].join(" "),
        ["the", "brief's"].join(" "),
        ["this", "brief"].join(" "),
        ["per", "the", "brief"].join(" "),
        ["receipts", "and", "failures"].join(" "),
        ["decimal", "ceiling"].join("-"),
        ["execution", "lane"].join("-"),
        ["execution", "lane"].join(" "),
        ["this", "lane"].join(" "),
        ["the", "lane's"].join(" "),
        ["lane", "target"].join(" "),
        ["fixture", "lane"].join(" "),
        ["concurrent", "lane"].join(" "),
        [".claude", "plans"].join("/"),
    ]
}

/// Top-level paths whose tracked text is product source, tests, tooling, or
/// user documentation.
const SCANNED_TOP_LEVEL: [&str; 10] = [
    "crates",
    "tests",
    "docs",
    "skills",
    "scripts",
    "AGENTS.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "README.md",
    ".gitignore",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

/// Whether a tracked path's text is scanned: crate sources, tests and
/// documentation; every skill and script file; the top-level and
/// per-crate documentation; and ignore files, which name every directory the
/// repository expects to exist beside its sources. Examples and benchmarks
/// are programs whose output names their own current invocation.
fn is_scanned_text(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    if name == ".gitignore" {
        return true;
    }
    let text = [".rs", ".md", ".py", ".sh"]
        .iter()
        .any(|extension| name.ends_with(extension));
    let code_scope = rel.contains("/src/")
        || rel.contains("/tests/")
        || rel.starts_with("tests/")
        || rel.starts_with("docs/")
        || rel.starts_with("skills/")
        || rel.starts_with("scripts/");
    let documentation =
        name.ends_with(".md") && (!rel.contains('/') || rel.split('/').count() == 3);
    text && (code_scope || documentation)
}

/// `git ls-files` over the same scanned top-level paths, walked directly off
/// the filesystem. Used only when the source tree is not a git repository
/// (e.g. a `git archive` export) -- git stays the primary source because it
/// honors `.gitignore`, so this fallback is only reached when git itself
/// cannot answer.
fn walk_tracked_like_paths(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    for top in SCANNED_TOP_LEVEL {
        let path = root.join(top);
        if path.is_dir() {
            walk_dir_into(&path, root, &mut out);
        } else if path.is_file() {
            out.push(top.to_string());
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
        if (name.starts_with('.') && name != ".gitignore") || name == "target" {
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

/// A numbered clause of a planning document ("statement" or "statements"
/// followed by a number), cited in place of the promise it stands for.
fn contains_numbered_statement_citation(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    lower.match_indices("statement").any(|(start, matched)| {
        if start > 0 && is_identifier_byte(bytes[start - 1]) {
            return false;
        }
        let mut cursor = start + matched.len();
        if bytes.get(cursor) == Some(&b's') {
            cursor += 1;
        }
        let spaces_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| *byte == b' ') {
            cursor += 1;
        }
        cursor > spaces_start && bytes.get(cursor).is_some_and(u8::is_ascii_digit)
    })
}

/// A numbered section of a planning document ("section" followed by a
/// dotted number such as 6.2).
fn contains_numbered_section_citation(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    lower.match_indices("section ").any(|(start, matched)| {
        if start > 0 && is_identifier_byte(bytes[start - 1]) {
            return false;
        }
        let mut cursor = start + matched.len();
        let major = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            cursor += 1;
        }
        cursor > major
            && bytes.get(cursor) == Some(&b'.')
            && bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit)
    })
}

/// A lettered checklist label ("Assertion" followed by a capital letter)
/// standing in for the behavior the assertion pins.
fn contains_lettered_assertion_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    let label = ["Assert", "ion "].concat();
    value.match_indices(label.as_str()).any(|(start, matched)| {
        let letter = start + matched.len();
        (start == 0 || !is_identifier_byte(bytes[start - 1]))
            && bytes.get(letter).is_some_and(u8::is_ascii_uppercase)
            && has_token_boundary(bytes, letter + 1)
    })
}

/// A hyphenated correction- or review-round number.
fn contains_hyphenated_round_id(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    ["correction-", "review-"].iter().any(|prefix| {
        lower.match_indices(prefix).any(|(start, matched)| {
            if start > 0 && is_identifier_byte(bytes[start - 1]) {
                return false;
            }
            let mut cursor = start + matched.len();
            let digits_start = cursor;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            cursor > digits_start && has_token_boundary(bytes, cursor)
        })
    })
}

fn contains_phrase(lower: &str, phrase_lower: &str) -> bool {
    let bytes = lower.as_bytes();
    lower.match_indices(phrase_lower).any(|(start, matched)| {
        let before = start.checked_sub(1).and_then(|index| bytes.get(index));
        let after = bytes.get(start + matched.len());
        !before.is_some_and(u8::is_ascii_alphanumeric)
            && !after.is_some_and(u8::is_ascii_alphanumeric)
    })
}

#[test]
fn planning_citation_detectors_match_citations_and_spare_product_prose() {
    assert!(contains_numbered_statement_citation(
        &["the refusal (statement", " 7) stays typed"].concat()
    ));
    assert!(contains_numbered_statement_citation(
        &["Statement", " 17a: purge"].concat()
    ));
    assert!(!contains_numbered_statement_citation(
        "a prepared statement binds parameters"
    ));
    assert!(contains_numbered_statement_citation(
        &["Statement", "s 9/13: slots"].concat()
    ));
    assert!(!contains_numbered_statement_citation("statementsx 3"));
    assert!(contains_numbered_section_citation(
        &["an open question (section", " 6.2)"].concat()
    ));
    assert!(!contains_numbered_section_citation("the section 6 header"));
    let label = ["Assert", "ion G"].concat();
    assert!(contains_lettered_assertion_label(&format!(
        "/// {label} -- refusal"
    )));
    assert!(!contains_lettered_assertion_label("Assertion failed"));
    assert!(contains_hyphenated_round_id(
        &["see ", "correction", "-15/"].concat()
    ));
    assert!(!contains_hyphenated_round_id(
        "a review-ready preview-2 build"
    ));
    let phrase = ["approved", "intent"].join(" ");
    assert!(contains_phrase(&format!("the {phrase}, clause"), &phrase));
    assert!(!contains_phrase(&format!("the {phrase}s"), &phrase));
    let lane = ["this", "lane"].join(" ");
    assert!(!contains_phrase(
        "the trailing purge lane keeps its slot",
        &lane
    ));
}

#[test]
fn tracked_implementation_prose_has_no_execution_session_vocabulary() {
    let root = repo_root();
    let historical_phrases = historical_phrases();
    let planning_phrases: Vec<String> = planning_citation_phrases()
        .iter()
        .map(|phrase| phrase.to_ascii_lowercase())
        .collect();
    let invocation_phrase = ["this", "run"].join(" ");
    let invocation_failure_phrase = ["failure", "of", "this", "run"].join(" ");
    let commit_phrase = ["own", "commit"].join(" ");
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "--"])
        .args(SCANNED_TOP_LEVEL)
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
        "vocabulary audit found no tracked files to scan"
    );

    let mut hits = Vec::new();
    for rel in &rel_paths {
        let rel = rel.as_str();
        if contains_campaign_test_filename(rel) || contains_campaign_prose(rel) {
            hits.push(format!(
                "{rel}: execution-campaign vocabulary in tracked path"
            ));
        }
        if !is_scanned_text(rel) {
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
            for phrase in &planning_phrases {
                if contains_phrase(&lower, phrase) {
                    hits.push(format!(
                        "{rel}:{}: planning or build-workspace citation `{phrase}`: {line}",
                        line_number + 1
                    ));
                }
            }
            if contains_numbered_statement_citation(line) {
                hits.push(format!(
                    "{rel}:{}: numbered planning-statement citation: {line}",
                    line_number + 1
                ));
            }
            if contains_numbered_section_citation(line) {
                hits.push(format!(
                    "{rel}:{}: numbered planning-section citation: {line}",
                    line_number + 1
                ));
            }
            if contains_lettered_assertion_label(line) {
                hits.push(format!(
                    "{rel}:{}: lettered assertion label: {line}",
                    line_number + 1
                ));
            }
            if contains_hyphenated_round_id(line) {
                hits.push(format!(
                    "{rel}:{}: correction/review round id: {line}",
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
        .args(["ls-files", "-z", "--"])
        .args(SCANNED_TOP_LEVEL)
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

    // A required owner-directed deletion can remain in Git's index while the
    // path is intentionally absent from a candidate filesystem. Such a path
    // is not scannable in an archive/export and must not make parity fail.
    let missing: Vec<&String> = git_paths
        .difference(&walk_paths)
        .filter(|path| root.join(path).exists())
        .collect();
    assert!(
        missing.is_empty(),
        "filesystem walk fallback missed git-tracked paths that a git-free export must still scan: {missing:?}"
    );
}
