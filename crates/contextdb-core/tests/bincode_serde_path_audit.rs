// ======== T8 ========

use std::collections::BTreeSet;
use std::path::PathBuf;

use regex::Regex;
use walkdir::WalkDir;

const SCOPED_CRATES: &[&str] = &[
    "contextdb-core",
    "contextdb-tx",
    "contextdb-engine",
    "contextdb-vector",
    "contextdb-graph",
    "contextdb-relational",
];

fn workspace_root() -> PathBuf {
    // tests run with CARGO_MANIFEST_DIR = crates/contextdb-core; go two up.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().to_path_buf()
}

fn scoped_src_files() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut out = Vec::new();
    for crate_name in SCOPED_CRATES {
        let src = root.join("crates").join(crate_name).join("src");
        if !src.exists() {
            continue;
        }
        for entry in WalkDir::new(&src).into_iter().filter_map(Result::ok) {
            let p = entry.path();
            if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(p.to_path_buf());
            }
        }
    }
    out
}

fn all_cargo_tomls() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut out = vec![root.join("Cargo.toml")];
    for entry in WalkDir::new(root.join("crates"))
        .into_iter()
        .filter_map(Result::ok)
    {
        let p = entry.path();
        if p.is_file() && p.file_name().and_then(|s| s.to_str()) == Some("Cargo.toml") {
            out.push(p.to_path_buf());
        }
    }
    out
}

fn file_derives_bincode(source: &str) -> bool {
    // Heuristic: file either has `use bincode::{...}` (group) mentioning Encode/Decode,
    // or a standalone `use bincode::Encode;` / `use bincode::Decode;`.
    let group_re =
        Regex::new(r"(?m)^\s*use\s+bincode::\{[^}]*\b(Encode|Decode)\b[^}]*\};").unwrap();
    if group_re.is_match(source) {
        return true;
    }
    let single_re = Regex::new(r"(?m)^\s*use\s+bincode::(Encode|Decode)\s*;").unwrap();
    single_re.is_match(source)
}

#[test]
fn bincode_serde_path_audit() {
    // (a) native encode_/decode_ calls that are NOT prefixed with bincode::serde::
    //
    // Anchored regex: match `bincode::(encode|decode)_(to|from)_(vec|slice|std_write|std_read)`
    // at a word boundary. Then reject hits where the immediate lexical prefix is
    // `bincode::serde::`. We anchor on `bincode::` and explicitly exclude the serde
    // sub-namespace with a negative check on the line context.
    let native_re =
        Regex::new(r"bincode::(encode|decode)_(to|from)_(vec|slice|std_write|std_read)\b").unwrap();
    let serde_prefix_re = Regex::new(r"bincode::serde::").unwrap();

    // (b) derive(...) containing Encode or Decode as a bareword, where the file's
    // use-graph resolves that derive to bincode (not rmp_serde etc.).
    let derive_re = Regex::new(r"#\[derive\([^)]*\b(Encode|Decode)\b[^)]*\)").unwrap();

    let mut native_hits: Vec<String> = Vec::new();
    let mut derive_hits: Vec<String> = Vec::new();

    for file in scoped_src_files() {
        let Ok(source) = std::fs::read_to_string(&file) else {
            continue;
        };

        for (line_no, line) in source.lines().enumerate() {
            if native_re.is_match(line) {
                // Allow hits that are bincode::serde::* — that family is the allowed path.
                // Strip any `bincode::serde::...` occurrences on the line before re-checking.
                let cleaned = serde_prefix_re.replace_all(line, "<ALLOWED>");
                if native_re.is_match(&cleaned) {
                    native_hits.push(format!(
                        "{}:{}: {}",
                        file.display(),
                        line_no + 1,
                        line.trim()
                    ));
                }
            }
        }

        if file_derives_bincode(&source) {
            for (line_no, line) in source.lines().enumerate() {
                if derive_re.is_match(line) {
                    derive_hits.push(format!(
                        "{}:{}: {}",
                        file.display(),
                        line_no + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert_eq!(
        native_hits,
        Vec::<String>::new(),
        "bincode native encode_/decode_ calls found outside bincode::serde::* prefix: {native_hits:#?}"
    );
    assert_eq!(
        derive_hits,
        Vec::<String>::new(),
        "bincode::Encode/Decode derive found on a type (use bincode::serde path instead): {derive_hits:#?}"
    );

    // (c) No Cargo.toml enables bincode's `derive` feature.
    let mut feature_hits: Vec<String> = Vec::new();
    for toml_path in all_cargo_tomls() {
        let Ok(toml_src) = std::fs::read_to_string(&toml_path) else {
            continue;
        };
        let parsed: toml::Value = match toml::from_str(&toml_src) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let mut candidates: Vec<&toml::Value> = Vec::new();
        if let Some(deps) = parsed.get("dependencies").and_then(|v| v.get("bincode")) {
            candidates.push(deps);
        }
        if let Some(ws_deps) = parsed
            .get("workspace")
            .and_then(|v| v.get("dependencies"))
            .and_then(|v| v.get("bincode"))
        {
            candidates.push(ws_deps);
        }

        for c in candidates {
            let features = c.get("features").and_then(|v| v.as_array());
            if let Some(feats) = features {
                for f in feats {
                    if f.as_str() == Some("derive") {
                        feature_hits.push(format!(
                            "{}: bincode features include 'derive'",
                            toml_path.display()
                        ));
                    }
                }
            }
        }
    }

    assert_eq!(
        feature_hits,
        Vec::<String>::new(),
        "bincode feature 'derive' enabled in workspace Cargo.toml(s): {feature_hits:#?}"
    );

    // Sanity: the audit actually walked some files. If SCOPED_CRATES is misspelled
    // or layout changes, the test would otherwise trivially pass.
    let scanned: BTreeSet<_> = scoped_src_files().into_iter().collect();
    assert!(
        scanned.len() >= 20,
        "audit scanned suspiciously few files: {}",
        scanned.len()
    );
}
