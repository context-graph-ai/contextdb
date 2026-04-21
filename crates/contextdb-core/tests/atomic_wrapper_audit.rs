// ======== T34 ========

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

// Forces a hard compile dependency on the wrappers existing publicly.
// If A4 regresses and the wrappers are removed, this test file fails to compile.
#[allow(unused_imports)]
use contextdb_core::{AtomicLsn, AtomicTxId};

use regex::Regex;
use walkdir::WalkDir;

fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().to_path_buf()
}

#[test]
fn atomic_wrapper_only_five_methods_used_on_typed_atomics() {
    const ALLOWED_METHODS: &[&str] = &[
        "load",
        "store",
        "fetch_add",
        "fetch_max",
        "compare_exchange",
    ];
    let allowed: BTreeSet<&str> = ALLOWED_METHODS.iter().copied().collect();

    let root = workspace_root();
    let crates_root = root.join("crates");

    // Anchored regex: field declaration `name: AtomicTxId` or `name: AtomicLsn`.
    // Captures the field name so we can search for `.<name>.<method>(` below.
    let field_re =
        Regex::new(r"(?m)\b([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(AtomicTxId|AtomicLsn)\b").unwrap();

    // Files to skip: the wrapper definition itself.
    let wrapper_def_suffix = "contextdb-core/src/types.rs";

    let mut violations: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut files_scanned = 0usize;
    let mut field_occurrences = 0usize;

    for entry in WalkDir::new(&crates_root)
        .into_iter()
        .filter_map(Result::ok)
    {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        if p.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        let rel = p
            .strip_prefix(&root)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/");

        // Only walk scoped crates (contextdb-*).
        if !rel.starts_with("crates/contextdb-") {
            continue;
        }
        // Skip the wrapper definition file.
        if rel.ends_with(wrapper_def_suffix) {
            continue;
        }
        // Skip this test file so its source regex literals are not false-positives.
        if p.file_name().and_then(|s| s.to_str()) == Some("atomic_wrapper_audit.rs") {
            continue;
        }

        files_scanned += 1;

        let Ok(source) = std::fs::read_to_string(p) else {
            continue;
        };

        // Find every field declaration `name: AtomicTxId` / `name: AtomicLsn` in this file.
        for cap in field_re.captures_iter(&source) {
            let field_name = &cap[1];
            field_occurrences += 1;

            // Anchored method-call regex: `.field_name.method_name(`
            // Word boundaries + literal dot + identifier + literal dot + identifier + open paren.
            let method_call_re = Regex::new(&format!(
                r"\.{0}\.([A-Za-z_][A-Za-z0-9_]*)\s*\(",
                regex::escape(field_name)
            ))
            .unwrap();

            for mcap in method_call_re.captures_iter(&source) {
                let method = &mcap[1];
                if !allowed.contains(method) {
                    violations
                        .entry(rel.clone())
                        .or_default()
                        .insert(format!(".{field_name}.{method}()"));
                }
            }
        }
    }

    assert!(
        files_scanned >= 20,
        "audit walked suspiciously few files ({files_scanned}); path layout may have moved"
    );
    assert!(
        field_occurrences > 0,
        "audit found zero AtomicTxId/AtomicLsn field declarations; wrappers may not be in use"
    );
    assert_eq!(
        violations,
        BTreeMap::<String, BTreeSet<String>>::new(),
        "disallowed AtomicU64 methods invoked on AtomicTxId/AtomicLsn fields: {violations:#?}. \
         Allowed methods: {ALLOWED_METHODS:?}"
    );
}

// ======== TU6 ========

#[test]
fn inline_on_atomic_wrapper_methods_audit() {
    use regex::Regex;

    let types_rs = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("types.rs");
    let body = std::fs::read_to_string(&types_rs)
        .unwrap_or_else(|e| panic!("read {}: {e}", types_rs.display()));

    for wrapper in ["AtomicTxId", "AtomicLsn"] {
        // Locate `impl <wrapper> { ... }` block.
        let block_re = Regex::new(&format!(r"(?ms)impl\s+{wrapper}\s*\{{(?P<body>.*?)^\}}"))
            .expect("impl-block regex compiles");

        let mut block_matches = 0;
        for cap in block_re.captures_iter(&body) {
            block_matches += 1;
            let block = cap.name("body").expect("body group").as_str();

            let expected_methods = [
                "load",
                "store",
                "fetch_add",
                "fetch_max",
                "compare_exchange",
            ];
            for method in expected_methods {
                // Count `#[inline] ... pub fn <method>` occurrences in this impl block.
                let method_re = Regex::new(&format!(
                    r"#\[inline\]\s*(?:\n|\r\n)\s*pub\s+fn\s+{method}\b"
                ))
                .expect("method-with-inline regex compiles");
                let count = method_re.find_iter(block).count();
                assert_eq!(
                    count, 1,
                    "impl {wrapper}: expected exactly one `#[inline]\\npub fn {method}`, found {count}"
                );
            }
        }
        assert_eq!(
            block_matches, 1,
            "expected exactly one `impl {wrapper} {{}}` block"
        );
    }
}
