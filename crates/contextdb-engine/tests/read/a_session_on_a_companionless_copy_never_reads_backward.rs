//! A read session on a store copied without its companion never reads
//! backward. Such a copy names no database identity, so the session may adopt
//! the first live writer of that path, but only one whose committed state is at
//! least as new as the image it already read: a writer of an older image put
//! back at the same path must not move the session's snapshot backward.

use contextdb_core::read_contract::{ReadFailureKind, ReadRoute};
use contextdb_core::{Error, Value, store_companion_path};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::path::Path;

fn commit(database: &Database, sql: &str) {
    database.execute(sql, &HashMap::new()).expect("commit");
}

/// Copy the store file alone, leaving its companion behind.
fn copy_without_companion(from: &Path, to: &Path) {
    std::fs::copy(from, to).expect("copy the store file");
    assert!(
        !store_companion_path(to).exists(),
        "the copy has no companion beside it"
    );
}

fn ids(ids: &[i64]) -> Vec<Vec<Value>> {
    ids.iter().map(|id| vec![Value::Int64(*id)]).collect()
}

#[test]
fn a_session_on_a_companionless_copy_never_adopts_a_writer_of_an_older_image() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let source = directory.path().join("source.db");
    let older = directory.path().join("older.db");
    let path = directory.path().join("store.db");

    // The source store first holds row 1, and a copy of that older image is kept.
    let writer = Database::open(&source).expect("open the source store");
    commit(&writer, "CREATE TABLE items (id INTEGER PRIMARY KEY)");
    commit(&writer, "INSERT INTO items VALUES (1)");
    writer.close().expect("close the source store");
    copy_without_companion(&source, &older);

    // It then commits rows 2 and 3, and the session reads a copy of that newer image.
    let writer = Database::open(&source).expect("reopen the source store");
    commit(&writer, "INSERT INTO items VALUES (2)");
    commit(&writer, "INSERT INTO items VALUES (3)");
    writer.close().expect("close the source store");
    copy_without_companion(&source, &path);

    let session = ReadSession::open(&path).expect("open a session on the copy");
    assert_eq!(
        session
            .execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .expect("the session reads the newer copy")
            .rows(),
        ids(&[1, 2, 3]).as_slice()
    );
    assert_eq!(session.route(), ReadRoute::File);

    // The older image is put back at the same path, and a writer opens it.
    let staged = directory.path().join("store.db.restoring");
    copy_without_companion(&older, &staged);
    std::fs::rename(&staged, &path).expect("put the older image at the store's path");
    let older_writer = Database::open(&path).expect("a writer opens the older image");

    match session.execute("SELECT id FROM items ORDER BY id", &HashMap::new()) {
        Ok(answer) => assert_eq!(
            answer.rows(),
            ids(&[1, 2, 3]).as_slice(),
            "the session must not move its snapshot backward to the older image's rows (route \
             now {:?})",
            session.route()
        ),
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::OwnerMismatch => {}
        Err(refusal) => panic!(
            "the session keeps its newer snapshot or refuses the older writer as a different \
             store; it was refused with: {refusal}"
        ),
    }
    drop(session);
    older_writer
        .close()
        .expect("close the older image's writer");
}
