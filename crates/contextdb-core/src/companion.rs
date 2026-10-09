use std::path::{Path, PathBuf};

/// The path of a store's companion lock file.
///
/// The companion lock is named by APPENDING `.lock` to the full store
/// filename, never by replacing the store's own extension: `alpha.db`
/// becomes `alpha.db.lock`, and `store.contextgraph` becomes
/// `store.contextgraph.lock`, keeping the `.contextgraph` extension intact.
/// See the "Stable companion lock" naming in
/// `contextdb-read-intent.md`. This is the one public source of truth for
/// the name, so a consumer never has to re-derive it (a naive
/// `Path::with_extension("lock")` silently discards an existing extension
/// instead of appending to it).
pub fn store_companion_path(store_path: &Path) -> PathBuf {
    let mut name = store_path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

/// The path of a store's holder-presence companion file.
///
/// Named like the companion lock, by APPENDING `.holders` to the full store filename: `alpha.db`
/// becomes `alpha.db.holders`, and `store.contextgraph` becomes `store.contextgraph.holders`. The
/// file is always empty. A process that owns background work beside the store holds an exclusive
/// lock on one byte of it for as long as it owns that work, so another process can tell a live
/// holder from one that has exited. This is the one public source of truth for the name.
pub fn store_holders_path(store_path: &Path) -> PathBuf {
    let mut name = store_path.as_os_str().to_os_string();
    name.push(".holders");
    PathBuf::from(name)
}
