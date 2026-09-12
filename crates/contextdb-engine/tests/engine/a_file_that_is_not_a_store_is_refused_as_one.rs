//! A path that holds something other than a store is refused as a store that
//! cannot be read, not as a capability that was never built.
//!
//! Reading such a file used to answer "bounded read session is not
//! implemented", which is untrue and, worse, leaves the operator nothing to
//! do: the reading session IS implemented, and what is wrong is the file. A
//! writable open of the same path finds exactly the same thing, so a reader
//! now says the same thing about it and adds the one next step that exists.

use contextdb_core::Error;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;

fn write_file(path: &std::path::Path, bytes: &[u8]) {
    std::fs::write(path, bytes).expect("write the fixture file");
}

fn refusal_reason(error: &Error, path: &std::path::Path) -> String {
    match error {
        Error::StoreCorrupted {
            path: named,
            reason,
        } => {
            assert_eq!(
                named,
                &path.display().to_string(),
                "the refusal names the file it is about"
            );
            reason.clone()
        }
        other => panic!(
            "a file that is not a store is refused as a store that cannot be read, got {other:?}"
        ),
    }
}

#[test]
fn opening_a_file_that_is_not_a_store_names_the_fault_and_the_next_step() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    for (name, bytes) in [
        ("prose.db", b"this file is not a store at all".as_slice()),
        ("empty.db", b"".as_slice()),
    ] {
        let path = directory.path().join(name);
        write_file(&path, bytes);

        let refused = ReadSession::open(&path)
            .err()
            .unwrap_or_else(|| panic!("{name} is not a store, so opening it for reading refuses"));
        let reason = refusal_reason(&refused, &path);
        assert!(
            reason.contains("contextdb diagnose"),
            "the refusal ends with something the operator can actually run: {reason}"
        );
        let rendered = refused.to_string();
        assert!(
            !rendered.contains("not implemented"),
            "the reading session IS implemented; what is wrong is the file: {rendered}"
        );
    }
}

#[test]
fn a_writable_open_of_the_same_file_refuses_it_too() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("prose.db");
    write_file(&path, b"this file is not a store at all");

    // The parity that matters: a reader must not be told the file is fine
    // for writers, nor a writer that it is fine for readers. Neither can use
    // it.
    let writable = Database::open(&path);
    assert!(
        writable.is_err(),
        "a writable open of a file that is not a store is refused as well"
    );

    let reading = ReadSession::open(&path);
    assert!(
        reading.is_err(),
        "and so is a reading open of the very same file"
    );
}

#[test]
fn a_real_store_still_opens_for_reading() {
    let directory = tempfile::tempdir().expect("task-scoped store directory");
    let path = directory.path().join("real.db");
    let database = Database::open(&path).expect("create a real store");
    database
        .execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create a table");
    database.close().expect("close the store");

    let session =
        ReadSession::open(&path).expect("a real store opens for reading as it always did");
    session
        .execute("SELECT id FROM kept", &HashMap::new())
        .expect("and answers");
}
