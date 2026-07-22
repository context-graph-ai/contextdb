//! Shared scaffolding for the source-audit test binaries in this directory
//! (timestamp, atomic-wrapper, bincode-path, test-estate). One definition of
//! "where is the workspace root" instead of a copy per audit.
#![allow(dead_code)]

use std::path::PathBuf;

/// The workspace root, two levels up from this crate's manifest
/// (`crates/contextdb-core`).
pub fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().unwrap().parent().unwrap().to_path_buf()
}
