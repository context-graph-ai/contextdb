//! Statement 18, effective owner-amended scope: custody has one self-contained
//! Unreleased changelog entry and the existing wire-source mirrors remain exact.

use std::path::Path;

// Statement 18: greenfield custody does not advance a protocol constant or
// simulate an old peer on this branch. It records its vocabulary together and
// retains the repository's existing audit of the actual transport mirrors.
#[test]
fn custody_changelog_entry_is_self_contained_and_wire_mirrors_remain_exact() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("contextdb workspace root");
    let changelog = std::fs::read_to_string(root.join("CHANGELOG.md")).expect("read changelog");
    let unreleased = changelog
        .split("## Unreleased")
        .nth(1)
        .and_then(|section| section.split("\n## ").next())
        .expect("Unreleased changelog section");
    assert!(
        unreleased.split("\n- ").any(|entry| {
            [
                "DECLARE TENANT TABLE POLICY",
                "delivery manifest",
                "outcome",
            ]
            .into_iter()
            .all(|vocabulary| entry.contains(vocabulary))
        }),
        "Statement 18: one Unreleased entry names the complete custody vocabulary"
    );

    // Keep the same two exact source pairs as sync_source_mirror_tests, the
    // repository's canonical audit of these wire-carrying transport mirrors.
    for path in ["transport/iroh.rs", "transport/large_request_staging.rs"] {
        assert_eq!(
            std::fs::read(root.join("crates/contextdb-engine/src").join(path))
                .expect("read canonical transport source"),
            std::fs::read(root.join("crates/contextdb-server/src").join(path))
                .expect("read transport audit mirror"),
            "Statement 18: {path} remains an exact wire-source mirror"
        );
    }
}
