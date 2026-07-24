//! Shared workspace-binary resolution for the acceptance and integration
//! test binaries (`tests/acceptance.rs` and `tests/integration.rs`), both
//! `[[test]]` targets of `contextdb-engine` -- this file is included by
//! `#[path]` into each, so it compiles once per test binary from one
//! physical source instead of being copy-pasted per test root.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::SystemTime;

const BIN_PROFILE_ENV_VAR: &str = "CONTEXTDB_TEST_BIN_PROFILE";

pub(crate) fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

pub(crate) fn binary_name(binary: &str) -> String {
    if cfg!(windows) {
        format!("{binary}.exe")
    } else {
        binary.to_string()
    }
}

/// Resolves `binary` (e.g. `"contextdb"`) to a freshly built workspace
/// binary. `target/debug` and `target/release` can each hold a binary left
/// over from an unrelated earlier build; picking either one blind risks
/// running stale code, which can hide a live regression (false pass) just
/// as easily as it can report one that no longer exists (false fail). The
/// rule: when both profiles have the binary, the one with the newer mtime
/// wins (that is the one a build just produced), and the choice is always
/// printed so a failure is traceable to the exact binary that ran.
/// `CONTEXTDB_TEST_BIN_PROFILE` (`debug` or `release`) pins one profile
/// explicitly and fails loudly if that profile's binary is missing.
pub(crate) fn resolve_workspace_binary(binary: &str) -> PathBuf {
    let name = binary_name(binary);
    let debug = workspace_root().join("target").join("debug").join(&name);
    let release = workspace_root().join("target").join("release").join(&name);

    if let Ok(profile) = std::env::var(BIN_PROFILE_ENV_VAR) {
        let (label, path) = match profile.as_str() {
            "debug" => ("debug", debug),
            "release" => ("release", release),
            other => panic!(
                "{BIN_PROFILE_ENV_VAR}={other:?} is not a recognized profile; use \"debug\" or \"release\""
            ),
        };
        assert!(
            path.exists(),
            "contextdb {binary} binary must be built before tests run ({BIN_PROFILE_ENV_VAR}={profile} requested {}); run `cargo build --profile {profile} -p contextdb-cli -p contextdb-server` first.",
            path.display()
        );
        eprintln!(
            "contextdb test harness: using {label} {binary} at {} ({BIN_PROFILE_ENV_VAR}={profile})",
            path.display()
        );
        return path;
    }

    match (mtime(&debug), mtime(&release)) {
        (Some(debug_mtime), Some(release_mtime)) => {
            let (label, chosen) = if release_mtime > debug_mtime {
                ("release", release)
            } else {
                ("debug", debug)
            };
            eprintln!(
                "contextdb test harness: using {label} {binary} at {} (newer of debug/release)",
                chosen.display()
            );
            chosen
        }
        (Some(_), None) => {
            eprintln!(
                "contextdb test harness: using debug {binary} at {}",
                debug.display()
            );
            debug
        }
        (None, Some(_)) => {
            eprintln!(
                "contextdb test harness: using release {binary} at {}",
                release.display()
            );
            release
        }
        (None, None) => panic!(
            "contextdb {binary} binary must be built before tests run; looked for {} and {}. Run `cargo build -p contextdb-cli -p contextdb-server` (add --release for the release profile) before invoking CLI/server tests directly.",
            debug.display(),
            release.display()
        ),
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    path.metadata().and_then(|meta| meta.modified()).ok()
}
