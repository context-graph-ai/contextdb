//! Docs audits: runtime checks that shipped documentation stays in lockstep
//! with the type system. Relocated from timestamp_audit.rs (test-estate round):
//! this is a DOCS audit, not a timestamp-column audit.

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

// ======== Documentation claim bindings ========
//
// A published sentence that states behavior names the test that fails if the
// sentence stops being true, with an inline tag written after the claim:
//
//     <!-- enforced by: file_stem::test_fn, file_stem::test_fn -->
//
// Three entry forms are accepted:
// - `file_stem::test_fn` — `crates/*/tests/<file_stem>.rs`,
//   `crates/*/tests/<file_stem>/main.rs`, or `crates/*/tests/*/<file_stem>.rs`
//   defines a test `fn test_fn`;
// - `<path>.rs::test_fn` — a repository-relative test file (for example
//   `tests/integration/hnsw_tests.rs::h04_hnsw_recall_is_at_least_ninety_five_percent`)
//   defines a test `fn test_fn`;
// - `<crate>::<file_stem>` — the whole test target
//   `crates/<crate>/tests/<file_stem>.rs` enforces the claim.
//
// A claim with no enforcing test carries no `enforced by:` tag at all.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::{rust_scope_text, workspace_root};

const BINDING_TAG: &str = "enforced by:";

/// Every H2/H3 section of the capability reference pages whose heading names a
/// vector, partition, maintenance, purge, or restart capability. The list is
/// explicit so the sections that must carry a binding are named in one place;
/// `documented_vector_capabilities_carry_bindings` also fails when a matching
/// heading appears in one of these pages without being listed here.
const CAPABILITY_SECTIONS: &[(&str, &str)] = &[
    ("docs/architecture.md", "Vector (`contextdb-vector`)"),
    (
        "docs/architecture.md",
        "Vector generations, restart, and repair",
    ),
    (
        "docs/architecture.md",
        "Maintenance (retention + version cleanup)",
    ),
    (
        "docs/query-language.md",
        "PROPAGATE ON STATE ... EXCLUDE VECTOR",
    ),
    (
        "docs/query-language.md",
        "Graph + Vector: Neighborhood Similarity Search",
    ),
    ("docs/query-language.md", "Vector Similarity Search"),
    (
        "docs/query-language.md",
        "Partition scope, filters, and one answer",
    ),
    ("README.md", "Two-Vector Walkthrough"),
    (
        "skills/vector-search/SKILL.md",
        "Search by an existing row's vector",
    ),
    (
        "skills/vector-search/SKILL.md",
        "The hybrid query — graph narrows, vector ranks",
    ),
];

/// The reference pages whose capability sections must bind their claims. Also
/// covers the two published pages readers land on first — the root README and
/// the vector-search skill recipe — so a page that loses every binding on a
/// covered section fails here, not just the interior `docs/` reference pages.
const CAPABILITY_PAGES: &[&str] = &[
    "docs/architecture.md",
    "docs/capability-index.md",
    "docs/query-language.md",
    "README.md",
    "skills/vector-search/SKILL.md",
];

/// Heading words that mark a section as stating vector-lifecycle behavior.
const CAPABILITY_WORDS: &[&str] = &["vector", "partition", "maintenance", "purge", "restart"];

/// Floor on the total number of `enforced by:` entries a page may carry,
/// counted across the whole file (not just its capability sections). Set to
/// the count at the time this floor was introduced. A page dropping below its
/// floor means bindings were deleted, not just moved or reworded; lower a
/// count only by an explicit edit to this table once the page's claims
/// genuinely shrink.
const PAGE_BINDING_FLOORS: &[(&str, usize)] = &[
    ("README.md", 11),
    ("skills/vector-search/SKILL.md", 40),
    ("docs/capability-index.md", 17),
    ("docs/query-language.md", 65),
    ("docs/architecture.md", 72),
    ("docs/getting-started.md", 12),
    ("docs/why-contextdb.md", 4),
    ("docs/cli.md", 129),
    ("skills/using-contextdb/SKILL.md", 4),
];

fn read_document(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn relative_display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn crate_dirs(root: &Path) -> Vec<PathBuf> {
    let crates = root.join("crates");
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&crates)
        .unwrap_or_else(|e| panic!("failed to list {}: {e}", crates.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();
    dirs
}

/// The published documents a binding may live in: the root README, every
/// top-level reference page under `docs/`, every skill recipe, and every crate
/// guide.
fn documents_carrying_bindings(root: &Path) -> Vec<PathBuf> {
    let mut documents = vec![root.join("README.md")];
    let docs = root.join("docs");
    documents.extend(
        std::fs::read_dir(&docs)
            .unwrap_or_else(|e| panic!("failed to list {}: {e}", docs.display()))
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "md")),
    );
    documents.extend(
        walkdir::WalkDir::new(root.join("skills"))
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file() && entry.file_name() == "SKILL.md")
            .map(walkdir::DirEntry::into_path),
    );
    documents.extend(
        crate_dirs(root)
            .into_iter()
            .map(|dir| dir.join("AGENTS.md"))
            .filter(|guide| guide.is_file()),
    );
    documents.sort();
    documents
}

/// Every entry of every `<!-- enforced by: ... -->` comment in `text`, with the
/// 1-based line the comment opens on. A tag that names nothing yields one empty
/// entry so the caller reports it.
fn binding_entries(text: &str) -> Vec<(usize, String)> {
    let mut entries = Vec::new();
    let mut cursor = 0;
    while let Some(found) = text[cursor..].find("<!--") {
        let open = cursor + found + "<!--".len();
        let Some(length) = text[open..].find("-->") else {
            break;
        };
        let comment = text[open..open + length].trim();
        if let Some(list) = comment.strip_prefix(BINDING_TAG) {
            let line = text[..open].matches('\n').count() + 1;
            let named: Vec<String> = list
                .split(',')
                .map(|entry| entry.trim().trim_matches('`').trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect();
            if named.is_empty() {
                entries.push((line, String::new()));
            }
            entries.extend(named.into_iter().map(|entry| (line, entry)));
        }
        cursor = open + length + "-->".len();
    }
    entries
}

fn test_target_files(crate_dir: &Path, file_stem: &str) -> Vec<PathBuf> {
    let tests = crate_dir.join("tests");
    let mut files = Vec::new();
    let direct = tests.join(format!("{file_stem}.rs"));
    if direct.is_file() {
        files.push(direct);
    }
    let nested_main = tests.join(file_stem).join("main.rs");
    if nested_main.is_file() {
        files.push(nested_main);
    }
    if let Ok(entries) = std::fs::read_dir(&tests) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let nested = path.join(format!("{file_stem}.rs"));
                if nested.is_file() {
                    files.push(nested);
                }
            }
        }
    }
    files
}

fn is_test_attribute(attr: &str, guide: bool) -> bool {
    let inner = attr
        .trim()
        .trim_start_matches("#[")
        .trim_end_matches(']')
        .trim();
    let path = inner.split('(').next().unwrap_or(inner).trim();
    path == "test"
        || path == "tokio::test"
        || path == "async_std::test"
        || (!guide && (path.ends_with("::test") || path == "serial" || path.ends_with("::serial")))
}

/// Collect actual test functions, including async tests with `#[tokio::test]`.
/// Other attributes may appear between the test attribute and the function.
fn test_function_names_in_source(source: &str, guide: bool) -> BTreeSet<String> {
    let fn_pattern = regex::Regex::new(r"\bfn\s+([A-Za-z_][A-Za-z_0-9]*)\s*[<(]").unwrap();
    let mut names = BTreeSet::new();
    let mut pending_attrs: Vec<String> = Vec::new();
    let mut attr_accum: Option<String> = None;
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(acc) = attr_accum.as_mut() {
            acc.push(' ');
            acc.push_str(trimmed);
            if trimmed.contains(']') {
                pending_attrs.push(std::mem::take(acc));
                attr_accum = None;
            }
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        if trimmed.starts_with("#[") {
            if trimmed.contains(']') {
                pending_attrs.push(trimmed.to_string());
            } else {
                attr_accum = Some(trimmed.to_string());
            }
            continue;
        }
        if let Some(found) = fn_pattern.captures(trimmed)
            && pending_attrs
                .iter()
                .any(|attr| is_test_attribute(attr, guide))
        {
            names.insert(found[1].to_string());
        }
        pending_attrs.clear();
    }
    names
}

fn test_function_names(file: &Path) -> BTreeSet<String> {
    test_function_names_in_source(&read_document(file), false)
}

/// A plain helper of the same name does not satisfy a binding.
fn defines_fn(file: &Path, name: &str) -> bool {
    test_function_names(file).contains(name)
}

/// Resolves one binding entry to the test that enforces it, or says why it
/// names nothing that exists.
fn resolve_binding(root: &Path, entry: &str) -> Result<(), String> {
    let segments: Vec<&str> = entry.split("::").collect();
    let well_formed = segments.len() == 2
        && segments.iter().all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        });
    if !well_formed {
        return Err("is not `file_stem::test_fn`, `<path>.rs::test_fn`, or \
                    `<crate>::<test_file_stem>`"
            .to_string());
    }
    let (target, name) = (segments[0], segments[1]);

    if target.ends_with(".rs") {
        let file = root.join(target);
        if !file.is_file() {
            return Err(format!("names test file `{target}`, which does not exist"));
        }
        return if defines_fn(&file, name) {
            Ok(())
        } else {
            Err(format!(
                "names `fn {name}`, which `{target}` does not define"
            ))
        };
    }

    let crate_dir = root.join("crates").join(target);
    if crate_dir.is_dir() {
        return if test_target_files(&crate_dir, name).is_empty() {
            Err(format!(
                "names test target `{name}` of crate `{target}`, but \
                 `crates/{target}/tests/{name}.rs` does not exist"
            ))
        } else {
            Ok(())
        };
    }

    let candidates: Vec<PathBuf> = crate_dirs(root)
        .iter()
        .flat_map(|dir| test_target_files(dir, target))
        .collect();
    if candidates.is_empty() {
        return Err(format!(
            "names test file `{target}`, but neither `crates/*/tests/{target}.rs` nor \
             `crates/*/tests/{target}/main.rs` nor `crates/*/tests/*/{target}.rs` exists"
        ));
    }
    if candidates.iter().any(|file| defines_fn(file, name)) {
        Ok(())
    } else {
        Err(format!(
            "names `fn {name}`, which no `{target}` test file defines ({})",
            candidates
                .iter()
                .map(|file| relative_display(root, file))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

#[test]
fn documentation_bindings_reference_existing_tests() {
    let root = workspace_root();
    let mut checked = 0usize;
    let mut dead = Vec::new();
    for document in documents_carrying_bindings(&root) {
        let shown = relative_display(&root, &document);
        for (line, entry) in binding_entries(&read_document(&document)) {
            checked += 1;
            if let Err(why) = resolve_binding(&root, &entry) {
                dead.push(format!("{shown}:{line}: `{entry}` {why}"));
            }
        }
    }
    assert!(
        checked > 0,
        "no `{BINDING_TAG}` bindings were found; the scan is not reading the published documents"
    );
    assert!(
        dead.is_empty(),
        "{} documentation binding(s) name a test that does not exist:\n{}",
        dead.len(),
        dead.join("\n")
    );
}

/// Only backticked citations in the Guard column of the invariant table are
/// citations. Both `file.rs::test` and `file.rs` → `test` are used by the guides.
/// A following `::test` or bare test name inherits the preceding file in its row.
fn guide_guard_citations(text: &str) -> Vec<(usize, Vec<(String, String)>)> {
    let code_span = regex::Regex::new(r"`([^`]+)`").unwrap();
    let mut in_invariants = false;
    let mut citations = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim() == "## Invariants and their guards" {
            in_invariants = true;
            continue;
        }
        if in_invariants && line.starts_with("## ") {
            break;
        }
        if !in_invariants || !line.trim_start().starts_with('|') || line.contains("|---") {
            continue;
        }
        let Some(guard) = line.rsplit('|').nth(1) else {
            continue;
        };
        if guard.trim().starts_with("Guard") {
            continue;
        }
        let mut previous_file = None::<String>;
        let mut arrow_names = false;
        let mut previous_end = 0;
        let mut row = Vec::new();
        for found in code_span.captures_iter(guard) {
            let whole = found.get(0).unwrap();
            let cited = found[1].to_string();
            let between = &guard[previous_end..whole.start()];
            let expanded = if cited.starts_with("::") {
                match &previous_file {
                    Some(file) => format!("{file}{cited}"),
                    None => cited.clone(),
                }
            } else if cited.ends_with(".rs") {
                previous_file = Some(cited.clone());
                arrow_names = false;
                if cited.contains('/') {
                    row.push((cited.clone(), cited));
                }
                previous_end = whole.end();
                continue;
            } else if cited.contains(".rs::") {
                if let Some((file, _)) = cited.split_once(".rs::") {
                    previous_file = Some(format!("{file}.rs"));
                }
                arrow_names = false;
                cited.clone()
            } else if previous_file.is_some()
                && (between.contains('→') || arrow_names)
                && cited
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ':')
            {
                if !arrow_names
                    && between.contains('→')
                    && row
                        .last()
                        .is_some_and(|(_, expanded)| Some(expanded) == previous_file.as_ref())
                {
                    row.pop();
                }
                arrow_names = true;
                format!("{}::{cited}", previous_file.as_ref().unwrap())
            } else {
                previous_end = whole.end();
                continue;
            };
            row.push((cited, expanded));
            previous_end = whole.end();
        }
        citations.push((index + 1, row));
    }
    citations
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    for (i, l) in left.bytes().enumerate() {
        let mut current = vec![i + 1];
        for (j, r) in right.bytes().enumerate() {
            current.push(
                (previous[j + 1] + 1)
                    .min(current[j] + 1)
                    .min(previous[j] + usize::from(l != r)),
            );
        }
        previous = current;
    }
    previous[right.len()]
}

fn nearest_guard_match(root: &Path, crate_dir: &Path, name: &str) -> String {
    [crate_dir.join("src"), crate_dir.join("tests")]
        .iter()
        .flat_map(|dir| {
            walkdir::WalkDir::new(dir)
                .into_iter()
                .filter_map(Result::ok)
        })
        .map(|entry| entry.into_path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .flat_map(|path| {
            guide_test_names(&path)
                .into_iter()
                .map(move |test| (edit_distance(name, &test), path.clone(), test))
        })
        .min_by(|a, b| a.cmp(b))
        .map(|(_, path, test)| format!("{}::{test}", relative_display(root, &path)))
        .unwrap_or_else(|| "no nearby test".to_string())
}

fn immediate_scope(source: &str) -> String {
    let mut depth = 0usize;
    let masked = source
        .bytes()
        .map(|ch| {
            let previous = depth;
            match ch {
                b'{' => depth += 1,
                b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
            if previous > 0 && ch != b'\n' {
                b' '
            } else {
                ch
            }
        })
        .collect();
    String::from_utf8(masked).expect("masking whole scopes preserves UTF-8")
}

fn guide_test_names(file: &Path) -> BTreeSet<String> {
    test_function_names_in_source(&rust_scope_text(&read_document(file)), true)
}

fn module_defines_test(file: &Path, qualified: &str) -> bool {
    let source = rust_scope_text(&read_document(file));
    let mut body = source.as_str();
    let mut parts: Vec<&str> = qualified.split("::").collect();
    let name = parts.pop().unwrap_or("");
    for module in parts {
        let declaration =
            regex::Regex::new(&format!(r"\bmod\s+{}\s*\{{", regex::escape(module))).unwrap();
        let scope = immediate_scope(body);
        let Some(found) = declaration.find(&scope) else {
            return false;
        };
        let open = found.end() - 1;
        let mut depth = 0usize;
        let mut close = None;
        for (offset, ch) in body[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(open + offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = close else { return false };
        body = &body[open + 1..end];
    }
    test_function_names_in_source(&immediate_scope(body), true).contains(name)
}

fn resolve_guide_guard(root: &Path, guide: &Path, citation: &str) -> Result<(), String> {
    let (source, function) = if let Some((source, function)) = citation.split_once(".rs::") {
        (format!("{source}.rs"), Some(function))
    } else if citation.ends_with(".rs") {
        (citation.to_string(), None)
    } else {
        return Err("has no source file and test function".to_string());
    };
    let source_in_crate = if source.starts_with("crates/") {
        source.splitn(3, '/').nth(2).unwrap_or("")
    } else {
        source.as_str()
    };
    if !(source_in_crate.starts_with("src/")
        || source_in_crate.starts_with("tests/")
        || source_in_crate.starts_with("../../tests/"))
    {
        return Err("must cite a crate src/ or tests/ Rust file".to_string());
    }
    let function = function.unwrap_or("");
    let function_name = function.rsplit("::").next().unwrap_or("");
    if (function.is_empty() && citation.contains(".rs::"))
        || !function
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ':')
    {
        return Err("has no valid test function".to_string());
    }
    let file = if source.starts_with("crates/") {
        root.join(&source)
    } else {
        guide.parent().unwrap().join(&source)
    };
    let crate_dir = if source_in_crate.starts_with("../../tests/") {
        root.to_path_buf()
    } else if source.starts_with("crates/") {
        let crate_name = source.split('/').nth(1).unwrap_or("");
        root.join("crates").join(crate_name)
    } else {
        guide.parent().unwrap().to_path_buf()
    };
    if !file.is_file() {
        return Err(format!(
            "names missing source file `{source}`; nearest match: {}",
            nearest_guard_match(root, &crate_dir, function_name)
        ));
    }
    let found = if function.is_empty() {
        !guide_test_names(&file).is_empty()
    } else {
        module_defines_test(&file, function)
    };
    if !found {
        return Err(format!(
            "names no test `{}` in `{source}`; nearest match: {}",
            if function.is_empty() {
                "#[test]"
            } else {
                function
            },
            nearest_guard_match(root, &crate_dir, function_name)
        ));
    }
    Ok(())
}

#[test]
fn crate_guide_invariant_guards_reference_existing_tests() {
    let root = workspace_root();
    let mut guides = 0;
    let mut checked = 0;
    let mut dead = Vec::new();
    for guide in crate_dirs(&root)
        .into_iter()
        .map(|dir| dir.join("AGENTS.md"))
        .filter(|guide| guide.is_file())
    {
        guides += 1;
        let text = read_document(&guide);
        let rows = guide_guard_citations(&text);
        if rows.is_empty() {
            dead.push(format!(
                "{}: invariant guard table missing or empty",
                relative_display(&root, &guide)
            ));
        }
        let mut guide_checked = 0;
        for (line, row) in rows {
            if row.is_empty() {
                let guard = text.lines().nth(line - 1).unwrap_or("");
                let explicit_unguarded = guard.to_ascii_lowercase();
                if !explicit_unguarded.contains("unguarded")
                    && !explicit_unguarded.contains("no rust test")
                {
                    dead.push(format!(
                        "{}:{line}: no test citation in guard row",
                        relative_display(&root, &guide)
                    ));
                }
            }
            for (cited, expanded) in row {
                checked += 1;
                guide_checked += 1;
                if let Err(reason) = resolve_guide_guard(&root, &guide, &expanded) {
                    dead.push(format!(
                        "{}:{line}: `{cited}` {reason}",
                        relative_display(&root, &guide)
                    ));
                }
            }
        }
        if guide_checked == 0 {
            dead.push(format!(
                "{}: no test citations scanned",
                relative_display(&root, &guide)
            ));
        }
    }
    assert!(
        guides > 0 && checked > 0,
        "no crate-guide invariant guard citations were scanned"
    );
    assert!(
        dead.is_empty(),
        "stale crate-guide guard citation(s):\n{}",
        dead.join("\n")
    );
}

#[test]
fn guide_guard_parser_and_resolver_reject_bad_citations() {
    let root = tempfile::tempdir().unwrap();
    let crate_dir = root.path().join("crates/example");
    std::fs::create_dir_all(crate_dir.join("tests")).unwrap();
    let file = crate_dir.join("tests/guards.rs");
    std::fs::write(
        &file,
        "#[test]\nfn good() {}\n#[tokio::test]\nasync fn async_good() {}\nfn helper() {}\n",
    )
    .unwrap();
    std::fs::write(
        crate_dir.join("tests/other.rs"),
        "#[test]\nfn different() {}\n",
    )
    .unwrap();
    let guide = crate_dir.join("AGENTS.md");
    let text = "## Invariants and their guards\n| Invariant | Guard |\n|---|---|\n| Combined | `tests/guards.rs::good`, `::async_good`, `crates/example/tests/guards.rs::good` |\n| Arrow | `tests/guards.rs` → `good`, `async_good` |\n| Source | `src/embedded.rs` → `source_good` |\n| Bad | `tests/guards.rs::helper`, `::goood`, `tests/other.rs::good`, `tests/missing.rs::good`, `tests/typo/guards.rs` → `good`, `tests/other.rs` → `helper` |\n## Where a change lives\n| unrelated | `tests/other.rs::good` |\n";
    std::fs::create_dir_all(crate_dir.join("src")).unwrap();
    std::fs::write(
        crate_dir.join("src/embedded.rs"),
        "#[test]\nfn source_good() {}\nmod inner {\n    #[test]\n    fn module_good() {}\n}\n",
    )
    .unwrap();
    let rows = guide_guard_citations(text);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows.iter().map(|(_, row)| row.len()).sum::<usize>(), 12);
    let citations: Vec<_> = rows.into_iter().flat_map(|(_, row)| row).collect();
    assert_eq!(citations[1].1, "tests/guards.rs::async_good");
    assert_eq!(citations[4].1, "tests/guards.rs::async_good");
    for (_, expanded) in &citations[..6] {
        assert!(resolve_guide_guard(root.path(), &guide, expanded).is_ok());
    }
    for (_, expanded) in &citations[6..] {
        let reason = resolve_guide_guard(root.path(), &guide, expanded).unwrap_err();
        assert!(reason.contains("nearest match:"), "{reason}");
        assert!(
            reason.contains("crates/example/tests/guards.rs::good")
                || reason.contains("crates/example/tests/other.rs::different")
                || reason.contains("crates/example/src/embedded.rs::source_good"),
            "{reason}"
        );
    }
    assert!(resolve_guide_guard(root.path(), &guide, "tests/guards.rs").is_ok());
    std::fs::create_dir_all(root.path().join("tests/acceptance")).unwrap();
    std::fs::write(
        root.path().join("tests/acceptance/root.rs"),
        "#[test]\nfn root_good() {}\n",
    )
    .unwrap();
    assert!(
        resolve_guide_guard(
            root.path(),
            &guide,
            "../../tests/acceptance/root.rs::root_good"
        )
        .is_ok()
    );
    assert!(
        resolve_guide_guard(root.path(), &guide, "src/embedded.rs::inner::module_good").is_ok()
    );
    assert!(
        resolve_guide_guard(root.path(), &guide, "src/embedded.rs::other::module_good").is_err()
    );
    let typo = resolve_guide_guard(root.path(), &guide, "tests/typo/guards.rs::good").unwrap_err();
    assert!(
        typo.contains("crates/example/tests/guards.rs::good"),
        "{typo}"
    );
}

#[test]
fn guide_guards_require_test_markers_and_complete_module_paths() {
    let root = tempfile::tempdir().unwrap();
    let crate_dir = root.path().join("crates/example");
    std::fs::create_dir_all(crate_dir.join("src")).unwrap();
    let guide = crate_dir.join("AGENTS.md");
    std::fs::write(
        crate_dir.join("src/guards.rs"),
        r##"
#[serial]
fn serial_helper() {}
#[serial_test::serial]
fn qualified_serial_helper() {}
mod outer {
    const DATA: &str = r#"} mod inner { #[test] fn fake() {} }"#;
    // } mod skipped {
    /* } /* nested comment */ */
    #[test]
    #[serial]
    fn direct() {}
    mod inner {
        #[tokio::test]
        async fn guard() {}
    }
}
"##,
    )
    .unwrap();
    for name in [
        "serial_helper",
        "qualified_serial_helper",
        "direct",
        "guard",
        "outer::guard",
        "inner::guard",
        "outer::fake",
    ] {
        assert!(
            resolve_guide_guard(root.path(), &guide, &format!("src/guards.rs::{name}")).is_err(),
            "accepted {name}"
        );
    }
    for name in ["outer::direct", "outer::inner::guard"] {
        assert!(
            resolve_guide_guard(root.path(), &guide, &format!("src/guards.rs::{name}")).is_ok(),
            "rejected {name}"
        );
    }
    assert!(resolve_guide_guard(root.path(), &guide, "src/guards.rs").is_ok());
    std::fs::write(
        crate_dir.join("src/serial_only.rs"),
        "#[serial]\nfn helper() {}\n",
    )
    .unwrap();
    assert!(resolve_guide_guard(root.path(), &guide, "src/serial_only.rs").is_err());
}

struct Section {
    level: usize,
    heading: String,
    line: usize,
    body: String,
}

/// Markdown sections outside fenced code: each heading with the text up to the
/// next heading of the same or a higher level.
fn sections(text: &str) -> Vec<Section> {
    let lines: Vec<&str> = text.lines().collect();
    let mut headings: Vec<(usize, usize, String)> = Vec::new();
    let mut in_fence = false;
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        let level = line.chars().take_while(|c| *c == '#').count();
        if (1..=6).contains(&level) && line[level..].starts_with(' ') {
            headings.push((index, level, line[level..].trim().to_string()));
        }
    }
    headings
        .iter()
        .enumerate()
        .map(|(position, (index, level, heading))| {
            let end = headings[position + 1..]
                .iter()
                .find(|(_, next_level, _)| next_level <= level)
                .map_or(lines.len(), |(next_index, _, _)| *next_index);
            Section {
                level: *level,
                heading: heading.clone(),
                line: index + 1,
                body: lines[index + 1..end].join("\n"),
            }
        })
        .collect()
}

#[test]
fn documented_vector_capabilities_carry_bindings() {
    let root = workspace_root();
    let mut found: BTreeSet<(String, String)> = BTreeSet::new();
    let mut unbound = Vec::new();
    for page in CAPABILITY_PAGES {
        for section in sections(&read_document(&root.join(page))) {
            if !matches!(section.level, 2 | 3) {
                continue;
            }
            let heading = section.heading.to_lowercase();
            if !CAPABILITY_WORDS.iter().any(|word| heading.contains(word)) {
                continue;
            }
            found.insert(((*page).to_string(), section.heading.clone()));
            if binding_entries(&section.body).is_empty() {
                unbound.push(format!(
                    "{page}:{}: section `{}` binds none of its claims to a test",
                    section.line, section.heading
                ));
            }
        }
    }
    let listed: BTreeSet<(String, String)> = CAPABILITY_SECTIONS
        .iter()
        .map(|(page, heading)| ((*page).to_string(), (*heading).to_string()))
        .collect();
    assert_eq!(
        found,
        listed,
        "the capability section list no longer matches the pages. Matching headings not \
         listed: {:?}. Listed headings no longer present: {:?}.",
        found.difference(&listed).collect::<Vec<_>>(),
        listed.difference(&found).collect::<Vec<_>>(),
    );
    assert!(
        unbound.is_empty(),
        "{} capability section(s) state behavior with no `{BINDING_TAG}` binding:\n{}",
        unbound.len(),
        unbound.join("\n")
    );
}

/// A page can keep at least one `enforced by:` tag per capability section
/// while still losing every OTHER binding on the page (bindings in prose
/// outside the selected sections, or in a section whose heading no longer
/// matches a capability word). That is still readers losing the proof a
/// documented behavior is tested, so cap it independently of section
/// coverage: each page in `PAGE_BINDING_FLOORS` must keep at least as many
/// total `enforced by:` entries as it had when the floor was set.
#[test]
fn capability_pages_do_not_drop_below_their_binding_floor() {
    let root = workspace_root();
    let mut short = Vec::new();
    for (page, floor) in PAGE_BINDING_FLOORS {
        let count = binding_entries(&read_document(&root.join(page))).len();
        if count < *floor {
            short.push(format!(
                "{page}: has {count} `{BINDING_TAG}` entries, below its floor of {floor}"
            ));
        }
    }
    assert!(
        short.is_empty(),
        "{} page(s) lost documentation bindings below their floor:\n{}",
        short.len(),
        short.join("\n")
    );
}
