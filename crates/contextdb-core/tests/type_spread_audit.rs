// ======== TU1 ========

#[test]
fn type_spread_no_bare_u64_in_identifier_positions() {
    use regex::Regex;
    use std::path::PathBuf;
    use walkdir::WalkDir;

    let crates = [
        "contextdb-core",
        "contextdb-tx",
        "contextdb-engine",
        "contextdb-server",
        "contextdb-vector",
        "contextdb-graph",
        "contextdb-relational",
        "contextdb-parser",
    ];

    // Whitelist is intentionally empty. Any leaked bare u64 in an identifier-shaped
    // parameter position is a production bug — not to be suppressed in this audit.
    let whitelist: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    let fn_sig = Regex::new(r"(?s)fn\s+[A-Za-z_][A-Za-z0-9_]*\s*(?:<[^>]*>)?\s*\(([^)]*)\)")
        .expect("fn signature regex compiles");
    let ident_name = Regex::new(r"(?i)^\s*(?:mut\s+)?(tx|txid|snapshot|snapshot_id|row_id|lsn|since_lsn|new_lsn|watermark|wallclock|created_at)\s*$")
        .expect("identifier name regex compiles");
    let bare_u64 = Regex::new(r"\bu64\b").expect("u64 regex compiles");

    let mut hits: Vec<String> = Vec::new();

    let workspace_root: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate has parent")
        .to_path_buf();

    for cr in &crates {
        let src_dir = workspace_root.join(cr).join("src");
        if !src_dir.exists() {
            continue;
        }
        for entry in WalkDir::new(&src_dir).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|s| s.to_str()) != Some("rs") {
                continue;
            }
            let body = std::fs::read_to_string(entry.path())
                .unwrap_or_else(|e| panic!("read {}: {e}", entry.path().display()));
            for cap in fn_sig.captures_iter(&body) {
                let params = cap.get(1).expect("group 1 present").as_str();
                if params.trim().is_empty() {
                    continue;
                }
                for param in split_toplevel_commas(params) {
                    let (name_part, type_part) = match param.split_once(':') {
                        Some(pair) => pair,
                        None => continue,
                    };
                    if !ident_name.is_match(name_part) {
                        continue;
                    }
                    if !bare_u64.is_match(type_part) {
                        continue;
                    }
                    let hit = format!(
                        "{}: `{}:{}`",
                        entry.path().display(),
                        name_part.trim(),
                        type_part.trim()
                    );
                    if !whitelist.contains(&hit) {
                        hits.push(hit);
                    }
                }
            }
        }
    }

    hits.sort();
    assert!(
        hits.is_empty(),
        "A5 type-spread violation: identifier-shaped parameters still typed as bare u64:\n{}",
        hits.join("\n")
    );
}

fn split_toplevel_commas(s: &str) -> Vec<String> {
    let mut depth_ang = 0i32;
    let mut depth_paren = 0i32;
    let mut current = String::new();
    let mut out = Vec::new();
    for ch in s.chars() {
        match ch {
            '<' => {
                depth_ang += 1;
                current.push(ch);
            }
            '>' => {
                depth_ang -= 1;
                current.push(ch);
            }
            '(' => {
                depth_paren += 1;
                current.push(ch);
            }
            ')' => {
                depth_paren -= 1;
                current.push(ch);
            }
            ',' if depth_ang == 0 && depth_paren == 0 => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}
