//! Docs binding — the `### Auto-Indexes` subsection under `## Indexes` in
//! `docs/query-language.md` must spell out the reserved naming scheme and
//! pin the behavioral claim that user-declared names using the reserved
//! prefix return `ReservedIndexName`.

use contextdb_core::Error;
use contextdb_engine::Database;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn doc_path(rel: &str) -> PathBuf {
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
/// level. Returns text from the heading line to the next same-or-higher-level
/// heading (exclusive).
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

// ============================================================================
// AI_DO01 — docs/query-language.md contains the Auto-Indexes subsection
// naming the __pk_ / __unique_ prefix scheme and the ReservedIndexName error.
// ============================================================================
#[test]
fn ai_do01_auto_indexes_subsection_documents_name_prefix_and_error() {
    let doc = read("docs/query-language.md");
    let auto = section(&doc, 3, "Auto-Indexes");
    assert!(
        auto.contains("__pk_"),
        "Auto-Indexes section must document the __pk_ prefix"
    );
    assert!(
        auto.contains("__unique_"),
        "Auto-Indexes section must document the __unique_ prefix"
    );
    assert!(
        auto.contains("PRIMARY KEY") && auto.contains("UNIQUE"),
        "Auto-Indexes section must mention both PRIMARY KEY and UNIQUE triggers"
    );
    assert!(
        auto.contains("ReservedIndexName"),
        "Auto-Indexes section must cite the ReservedIndexName error"
    );
    assert!(
        auto.contains("EXPLAIN"),
        "Auto-Indexes section must mention EXPLAIN as the consumer-visible surface"
    );
}

// ============================================================================
// AI_DO02 — the documented contract executes live: a CREATE INDEX using the
// reserved prefix returns Error::ReservedIndexName.
// ============================================================================
#[test]
fn ai_do02_reserved_prefix_contract_executes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &HashMap::new(),
    )
    .unwrap();
    let err = db
        .execute("CREATE INDEX __pk_id ON t (id)", &HashMap::new())
        .unwrap_err();
    assert!(
        matches!(err, Error::ReservedIndexName { .. }),
        "reserved __pk_ prefix must return ReservedIndexName, got {err:?}"
    );
    let err2 = db
        .execute("CREATE INDEX __unique_email ON t (email)", &HashMap::new())
        .unwrap_err();
    assert!(
        matches!(err2, Error::ReservedIndexName { .. }),
        "reserved __unique_ prefix must return ReservedIndexName, got {err2:?}"
    );
}
