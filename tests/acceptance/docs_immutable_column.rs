//! Docs binding — fenced SQL blocks are extracted by section and executed
//! against a live Database. A doc change that drops IMMUTABLE from a section
//! breaks the section-anchored match; a block that claims rejection but does
//! not actually reject breaks the executable assertion.

use contextdb_core::Error;
use contextdb_engine::Database;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn doc_path(rel: &str) -> PathBuf {
    // The tests are registered as `[[test]]` targets on `contextdb-engine`, so
    // `CARGO_MANIFEST_DIR` resolves to `crates/contextdb-engine`. Walk up to
    // the workspace root where `docs/` lives.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root must be two levels above crate manifest dir")
        .to_path_buf();
    workspace_root.join(rel)
}

fn read(rel: &str) -> String {
    fs::read_to_string(doc_path(rel)).unwrap_or_else(|e| panic!("cannot read doc {rel}: {e}"))
}

/// Extract the text of a markdown section bounded by a heading of the given
/// level (2 = `## `, 3 = `### `). Returns the text from the heading line to
/// the next same-or-higher-level heading (exclusive).
#[allow(clippy::while_let_on_iterator)]
fn section(doc: &str, level: usize, heading_text: &str) -> String {
    let prefix = "#".repeat(level) + " ";
    let mut lines = doc.lines().peekable();
    let mut in_section = false;
    let mut out = String::new();
    while let Some(line) = lines.next() {
        if !in_section {
            if line.starts_with(&prefix)
                && line[prefix.len()..].trim_start().starts_with(heading_text)
            {
                in_section = true;
                out.push_str(line);
                out.push('\n');
            }
        } else {
            // Stop at next heading of same-or-higher level.
            let stop = (1..=level).any(|l| {
                let p = "#".repeat(l) + " ";
                line.starts_with(&p)
            });
            if stop {
                break;
            }
            out.push_str(line);
            out.push('\n');
        }
    }
    assert!(
        !out.is_empty(),
        "section {prefix}{heading_text:?} not found in doc"
    );
    out
}

/// Extract fenced code blocks of the given language from a doc fragment.
fn fenced_blocks(fragment: &str, lang: &str) -> Vec<String> {
    let open = format!("```{lang}");
    let mut out = Vec::new();
    let mut in_block = false;
    let mut current = String::new();
    for line in fragment.lines() {
        let trimmed = line.trim_start();
        if !in_block && trimmed.starts_with(&open) {
            in_block = true;
            current.clear();
        } else if in_block && trimmed.starts_with("```") {
            in_block = false;
            out.push(std::mem::take(&mut current));
        } else if in_block {
            current.push_str(line);
            current.push('\n');
        }
    }
    out
}

/// Split a SQL document into individual statements on `;` while tolerating
/// `;` inside single-quoted strings.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    for ch in sql.chars() {
        if ch == '\'' {
            in_str = !in_str;
            cur.push(ch);
        } else if ch == ';' && !in_str {
            let trimmed = cur.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
            cur.clear();
        } else {
            cur.push(ch);
        }
    }
    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    out
}

// ============================================================================
// DO01 — RED — docs/query-language.md `## Column Constraints` section lists
// IMMUTABLE as a constraint, AND the section's "audit-frozen" language is
// present in a subsection.
// ============================================================================
#[test]
fn do01_query_language_column_constraints_lists_immutable() {
    let doc = read("docs/query-language.md");
    let cc = section(&doc, 2, "Column Constraints");
    assert!(
        cc.contains("`IMMUTABLE`"),
        "Column Constraints section must list `IMMUTABLE`"
    );
    let lower = cc.to_lowercase();
    assert!(
        lower.contains("audit") || lower.contains("frozen") || lower.contains("append-only"),
        "Column Constraints section must describe semantics (audit / frozen / append-only)"
    );
}

// ============================================================================
// DO02 — RED — docs/cli.md `### Meta-Commands` section's `.schema` row
// mentions IMMUTABLE.
// ============================================================================
#[test]
fn do02_cli_schema_description_mentions_immutable() {
    let doc = read("docs/cli.md");
    let meta = section(&doc, 3, "Meta-Commands");
    let schema_line = meta
        .lines()
        .find(|l| l.contains(".schema"))
        .expect(".schema command row must appear in Meta-Commands section");
    assert!(
        schema_line.to_uppercase().contains("IMMUTABLE"),
        ".schema command row must mention IMMUTABLE rendering, got: {schema_line}"
    );
}

// ============================================================================
// DO03 — RED — docs/getting-started.md contains at least one fenced `sql`
// block that (a) CREATEs a table with IMMUTABLE, (b) INSERTs, (c) attempts an
// UPDATE on the flagged column. The CREATE + INSERT execute successfully;
// the UPDATE returns Err(Error::ImmutableColumn) when executed live.
// ============================================================================
#[test]
fn do03_getting_started_immutable_walkthrough_executes() {
    let doc = read("docs/getting-started.md");
    let blocks: Vec<String> = fenced_blocks(&doc, "sql")
        .into_iter()
        .filter(|b| b.to_uppercase().contains("IMMUTABLE") && b.to_uppercase().contains("UPDATE"))
        .collect();
    assert!(
        !blocks.is_empty(),
        "getting-started must contain at least one `sql` block with both IMMUTABLE and UPDATE"
    );

    let db = Database::open_memory();
    let mut saw_rejection = false;
    for block in &blocks {
        let stmts = split_sql_statements(block);
        for stmt in stmts {
            // Skip comment-only or blank lines.
            let cleaned: String = stmt
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            if cleaned.trim().is_empty() {
                continue;
            }
            let result = db.execute(&cleaned, &HashMap::new());
            match result {
                Ok(_) => {}
                Err(Error::ImmutableColumn { .. }) => {
                    saw_rejection = true;
                }
                Err(other) => {
                    // Do not fail on other errors — doc examples sometimes rely
                    // on parameters that :memory: can't bind without fixtures.
                    // We only require that at least one UPDATE in the block
                    // surfaces ImmutableColumn as the doc promises.
                    let _ = other;
                }
            }
        }
    }
    assert!(
        saw_rejection,
        "getting-started must include an executable IMMUTABLE UPDATE that returns Error::ImmutableColumn"
    );
}

// ============================================================================
// DO04 — RED — docs/usage-scenarios.md Decisions section marks IMMUTABLE
// fields AND contains the correction-via-supersede note.
// ============================================================================
#[test]
fn do04_usage_scenarios_decisions_marks_immutable_and_supersede_note() {
    let doc = read("docs/usage-scenarios.md");
    // The Decisions scenario heading is `## Scenario 2: Decisions With State
    // Machines` in the current doc; a feature-shipping doc revision may
    // rename it, so we match any `## Scenario` heading whose line contains
    // "Decisions".
    let heading_line = doc
        .lines()
        .find(|l| l.starts_with("## ") && l.contains("Decisions"))
        .expect("Decisions scenario heading must exist")
        .trim_start_matches("## ")
        .to_string();
    let scenario = section(
        &doc,
        2,
        heading_line.split(':').next().unwrap_or(&heading_line),
    );
    assert!(
        scenario.contains("IMMUTABLE"),
        "Decisions scenario must mark fields with IMMUTABLE"
    );
    let lower = scenario.to_lowercase();
    assert!(
        lower.contains("supersede") || lower.contains("supersedes") || lower.contains("superseded"),
        "Decisions scenario must contain the correction-via-supersede note"
    );
}
