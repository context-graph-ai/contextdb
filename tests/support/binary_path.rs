//! Shared workspace-binary resolution for the acceptance and integration
//! test binaries (`tests/acceptance.rs` and `tests/integration.rs`), both
//! `[[test]]` targets of `contextdb-engine` -- this file is included by
//! `#[path]` into each, so it compiles once per test binary from one
//! physical source instead of being copy-pasted per test root.
#![allow(dead_code)]

include!("binary_path_helpers.rs");

#[cfg(test)]
mod resolve_target_dir_tests {
    //! `target_root()` returned a configured `CARGO_TARGET_DIR` as-is. Cargo
    //! runs a test binary from its package directory, not the workspace
    //! root it was invoked from, so a relative configured value resolved
    //! under the package instead of the workspace root Cargo actually built
    //! into -- existence checks and binary launches then missed the real build or
    //! picked up a stale binary left over in the wrong place. These proofs
    //! exercise the pure resolution rule against synthetic inputs, never the
    //! real `CARGO_TARGET_DIR` -- mutating that for real would race every
    //! other test in this same binary that depends on it for its own
    //! correct binary resolution.
    use super::*;

    #[test]
    fn an_absolute_configured_target_dir_is_used_unchanged() {
        let workspace_root = Path::new("/workspace/contextdb");
        let configured = Path::new("/var/tmp/some-other-target");
        assert_eq!(
            resolve_target_dir(Some(configured), workspace_root),
            configured.to_path_buf(),
            "an absolute CARGO_TARGET_DIR names its own directory outright"
        );
    }

    #[test]
    fn a_relative_configured_target_dir_is_anchored_to_the_workspace_root() {
        let workspace_root = Path::new("/workspace/contextdb");
        let configured = Path::new(".custom-build-dir/target");
        assert_eq!(
            resolve_target_dir(Some(configured), workspace_root),
            workspace_root.join(".custom-build-dir/target"),
            "a relative CARGO_TARGET_DIR must resolve against the workspace root Cargo was \
             invoked from -- the supported invocation base -- not the test binary's own package \
             directory"
        );
    }

    #[test]
    fn no_configured_target_dir_falls_back_to_workspace_root_target() {
        let workspace_root = Path::new("/workspace/contextdb");
        assert_eq!(
            resolve_target_dir(None, workspace_root),
            workspace_root.join("target"),
            "with CARGO_TARGET_DIR unset, the plain build's own target directory is used"
        );
    }
}
