//! The store companion lock is named by APPENDING `.lock` to the full store
//! filename, never by swapping the store's own extension.
//!
//! Authority: `.claude/plans/contextdb/active/contextdb-read-intent.md`,
//! "Stable companion lock" -- `alpha.db` -> `alpha.db.lock`, never
//! `alpha.lock`. contextdb-core is the ONE source of truth for this name:
//! vigil links contextdb-core unconditionally but contextdb-engine only
//! behind its `fabric` feature, so the helper must live where every
//! consumer of the companion lock name can reach it unconditionally.
//! `contextdb-engine::persistence::store_companion_path` delegates to this
//! one.

use contextdb_core::store_companion_path;
use std::path::{Path, PathBuf};

#[test]
fn store_companion_path_appends_lock_to_a_plain_store_name() {
    assert_eq!(
        store_companion_path(Path::new("/x/alpha.db")),
        PathBuf::from("/x/alpha.db.lock"),
    );
}

#[test]
fn store_companion_path_keeps_the_store_s_own_extension_intact() {
    // The `with_extension` trap: naively swapping the extension would turn
    // `store.contextgraph` into `store.lock`, silently discarding
    // `.contextgraph`. Appending must keep the existing extension whole.
    assert_eq!(
        store_companion_path(Path::new("/x/store.contextgraph")),
        PathBuf::from("/x/store.contextgraph.lock"),
    );
}
