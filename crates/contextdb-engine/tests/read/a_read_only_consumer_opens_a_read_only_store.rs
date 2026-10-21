//! A store whose files are read-only on disk is still a store a reader reads.
//!
//! A read-only consumer is the whole point of the committed-file route: nobody
//! owns the store, nothing is written, and the caller only wants its rows
//! back. An operator who has made that literal -- sealed the store and the
//! companion beside it read-only, which is what an archived or evidence-held
//! deployment looks like -- is doing exactly what the route is for, and the
//! reader must answer rather than report the route unbuilt.
//!
//! What the reader actually needs from the companion is to READ it: the
//! published record, and the shared side of an advisory byte, both of which a
//! read-only descriptor supplies. It never writes one. The owner's write bit
//! is a writer's requirement, and the privacy of the file -- nothing at all
//! for group or other -- is the part of the mode a reader does care about, so
//! a world-readable companion is still refused.
use super::reader_proof_support as fixture;
use contextdb_core::Value;
use contextdb_core::read_contract::ReadRoute;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

/// The private modes the trust boundary accepts, and the one it must not.
const PRIVATE_READ_ONLY: u32 = 0o400;
const PRIVATE_WRITABLE: u32 = 0o600;

fn seed(path: &Path) {
    let database = Database::open(path).expect("create the sealed fixture");
    database
        .execute(
            "CREATE TABLE sealed_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the sealed table");
    database
        .execute(
            "INSERT INTO sealed_rows (id, payload) VALUES (1, 'sealed')",
            &HashMap::new(),
        )
        .expect("seed one sealed row");
    database.close().expect("release the sealed store");
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .unwrap_or_else(|error| panic!("set {mode:o} on {}: {error}", path.display()));
}

struct Sealed {
    _directory: tempfile::TempDir,
    runtime: std::path::PathBuf,
    store: std::path::PathBuf,
    companion: std::path::PathBuf,
}

fn sealed_store(name: &str) -> Sealed {
    let directory = tempfile::TempDir::new().expect("task-scoped sealed directory");
    let runtime = fixture::runtime(directory.path());
    let store = directory.path().join(name);
    seed(&store);
    let companion = contextdb_engine::persistence::store_companion_path(&store);
    Sealed {
        _directory: directory,
        runtime,
        store,
        companion,
    }
}

fn read_back(sealed: &Sealed) -> contextdb_core::Result<Vec<Vec<Value>>> {
    ReadSession::with_runtime_directory_for_test(&sealed.runtime, || {
        let session = ReadSession::open_with_options(&sealed.store, ReadSessionOptions::default())?;
        assert_eq!(
            session.route(),
            ReadRoute::File,
            "a store nobody owns is read through the committed file"
        );
        Ok(session
            .execute("SELECT payload FROM sealed_rows", &HashMap::new())?
            .rows().to_vec())
    })
}

#[test]
fn a_store_sealed_read_only_on_disk_still_answers_a_read_only_consumer() {
    let sealed = sealed_store("sealed.db");
    for (store_mode, companion_mode, sealing) in [
        (PRIVATE_WRITABLE, PRIVATE_WRITABLE, "nothing sealed"),
        (PRIVATE_READ_ONLY, PRIVATE_WRITABLE, "the store sealed"),
        (PRIVATE_WRITABLE, PRIVATE_READ_ONLY, "the companion sealed"),
        (PRIVATE_READ_ONLY, PRIVATE_READ_ONLY, "both sealed"),
    ] {
        chmod(&sealed.store, store_mode);
        chmod(&sealed.companion, companion_mode);
        let answered = read_back(&sealed);
        assert_eq!(
            answered.unwrap_or_else(|error| panic!(
                "with {sealing} read-only, a read-only consumer was refused: {error:?}"
            )),
            vec![vec![Value::Text("sealed".to_owned())]],
            "with {sealing} read-only the store answered the wrong rows",
        );
        chmod(&sealed.store, PRIVATE_WRITABLE);
        chmod(&sealed.companion, PRIVATE_WRITABLE);
    }
}
