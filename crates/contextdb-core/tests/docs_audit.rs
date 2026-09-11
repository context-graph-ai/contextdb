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
// - `file_stem::test_fn` — `crates/*/tests/<file_stem>.rs` or
//   `crates/*/tests/<file_stem>/main.rs` defines `fn test_fn`;
// - `<path>.rs::test_fn` — a repository-relative test file (for example
//   `tests/integration/hnsw_tests.rs::h04_hnsw_recall_is_at_least_ninety_five_percent`)
//   defines `fn test_fn`;
// - `<crate>::<file_stem>` — the whole test target
//   `crates/<crate>/tests/<file_stem>.rs` enforces the claim.
//
// A claim with no enforcing test carries no `enforced by:` tag at all.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

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
    ("README.md", 15),
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
    [
        tests.join(format!("{file_stem}.rs")),
        tests.join(file_stem).join("main.rs"),
    ]
    .into_iter()
    .filter(|path| path.is_file())
    .collect()
}

fn defines_fn(file: &Path, name: &str) -> bool {
    let pattern = regex::Regex::new(&format!(r"\bfn\s+{}\s*[<(]", regex::escape(name))).unwrap();
    pattern.is_match(&read_document(file))
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
             `crates/*/tests/{target}/main.rs` exists"
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
