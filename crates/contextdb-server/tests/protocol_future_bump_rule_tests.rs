//! The protocol document must say when a future wire bump is mandatory.

use std::path::PathBuf;

#[test]
fn architecture_requires_a_protocol_version_bump_for_every_envelope_shape_change() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest.ancestors().nth(2).expect("workspace root");
    let architecture = std::fs::read_to_string(root.join("docs/architecture.md"))
        .expect("read protocol architecture");
    let lower = architecture.to_ascii_lowercase();
    let bytes_rule = lower.contains("sync bytes") && lower.contains("bump");
    let semantics_rule = lower.contains("sync semantics") && lower.contains("bump");
    let unchanged_exclusion = ["sql", "storage", "cli", "maintenance"]
        .iter()
        .all(|term| lower.contains(term))
        && lower.contains("sync unchanged");
    assert!(
        bytes_rule && semantics_rule && unchanged_exclusion,
        "docs/architecture.md must say: bump the protocol for changed sync bytes OR sync semantics; do not bump for SQL, storage, CLI, or maintenance work that leaves sync unchanged"
    );
}
