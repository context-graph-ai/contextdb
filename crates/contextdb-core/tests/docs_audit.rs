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
