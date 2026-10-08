//! Shared scaffolding for the source-audit test binaries in this directory
//! (timestamp, atomic-wrapper, bincode-path, test-estate, docs). One definition of
//! "where is the workspace root" and of "which bytes are Rust code" instead of a
//! copy per audit.
#![allow(dead_code)]

use std::path::PathBuf;

/// The workspace root, two levels up from this crate's manifest
/// (`crates/contextdb-core`).
pub fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().to_path_buf()
}

/// The source with every comment, string literal and character literal
/// replaced by spaces (newlines kept), so a scan sees only Rust code at the
/// original byte offsets and line numbers.
pub fn rust_scope_text(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        if bytes[i..].starts_with(b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if bytes[i] == b'r' {
            let mut quote = i + 1;
            while quote < bytes.len() && bytes[quote] == b'#' {
                quote += 1;
            }
            if bytes.get(quote) != Some(&b'"') {
                i += 1;
                continue;
            }
            let hashes = quote - i - 1;
            i = quote + 1;
            while i < bytes.len() {
                if bytes[i] == b'"'
                    && bytes
                        .get(i + 1..i + 1 + hashes)
                        .is_some_and(|tail| tail.iter().all(|b| *b == b'#'))
                {
                    i += 1 + hashes;
                    break;
                }
                i += 1;
            }
        } else if bytes[i] == b'"' || bytes[i] == b'\'' {
            let quote = bytes[i];
            // A lifetime is not a character literal. Character literals end
            // after one UTF-8 scalar or an escape (possibly a Unicode escape).
            if quote == b'\'' {
                let tail = &source[i + 1..];
                let char_len = tail.chars().next().map_or(0, char::len_utf8);
                if !tail.starts_with('\\') && bytes.get(i + 1 + char_len) != Some(&b'\'') {
                    i += 1;
                    continue;
                }
            }
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == quote {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
        } else {
            i += 1;
            continue;
        }
        for byte in &mut masked[start..i] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    }
    String::from_utf8(masked).expect("masking whole literals preserves UTF-8")
}
