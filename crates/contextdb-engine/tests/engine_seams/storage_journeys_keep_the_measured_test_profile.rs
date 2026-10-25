//! Acceptance journeys retain the measured optimisation and checked arithmetic.
//!
//! This reads the manifest, not the effective Cargo configuration. It cannot see
//! .cargo/config.toml or CARGO_PROFILE_TEST_* overrides; debug_assertions is the
//! only compiled-in check. Artifact receipts and the timed journey bind the build.
#[test]
fn storage_journeys_keep_the_measured_test_profile() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest: toml::Value = toml::from_str(
        &std::fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest"),
    )
    .expect("workspace manifest parses");
    let profile = &manifest["profile"]["test"];
    assert!(cfg!(debug_assertions), "test assertions must stay enabled");
    for setting in ["debug-assertions", "overflow-checks"] {
        assert_eq!(
            profile.get(setting).and_then(toml::Value::as_bool),
            Some(true),
            "test profile must explicitly enable {setting}"
        );
    }
    for name in ["contextdb-engine", "contextdb-redb", "contextdb-core"] {
        let package = profile
            .get("package")
            .and_then(|packages| packages.get(name))
            .unwrap_or_else(|| panic!("{name} needs its measured test optimisation"));
        assert_eq!(
            package.get("opt-level").and_then(toml::Value::as_integer),
            Some(1),
            "{name} must keep the measured test opt-level"
        );
        for setting in ["debug-assertions", "overflow-checks"] {
            assert_eq!(
                package.get(setting).unwrap_or(&profile[setting]).as_bool(),
                Some(true),
                "{name} must keep {setting} enabled"
            );
        }
    }
}
