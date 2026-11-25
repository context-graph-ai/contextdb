//! What the vector search tests share beyond `support`: the one way each starts a store whose
//! `docs` is declared and holds no row yet.

use std::path::PathBuf;

use contextdb::Database;

/// A store opened for writing in a fresh folder of this name, its `docs` declared with these
/// clauses after `VECTOR(dimension)`.
pub fn docs(name: &str, dimension: usize, clauses: &str) -> (PathBuf, Database) {
    let folder = crate::support::folder(name);
    let db = crate::support::open(&folder);
    crate::support::declare_docs(&db, dimension, clauses);
    (folder, db)
}
