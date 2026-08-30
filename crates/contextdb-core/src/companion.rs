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
