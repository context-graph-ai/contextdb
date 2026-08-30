//! A store that cannot be read is refused when it is opened, not later.
//!
//! Opening a reading session is where a caller finds out whether this store
//! can be read at all: it is the moment they can still choose another store,
//! print a diagnostic, or stop. A session that opens successfully over a
//! corrupt file has told the caller the opposite of the truth, and the
//! corruption then surfaces at whatever statement happens to touch the broken
//! part -- which may be the second one, or the hundredth, long after the
//! caller committed to reading.
//!
//! So the committed image is decoded and judged at open, and a store that
//! cannot produce one is refused there, in the same words a writable open
//! uses for the same file. One file, one verdict, whoever asked.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::Error;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;

/// A store that was valid and is now cut short: the header still looks like a
/// store, the pages behind it do not add up. This is the shape an interrupted
/// copy or a truncated restore leaves behind.
fn make_truncated_store(path: &Path) {
    {
        let database = Database::open(path).expect("create a valid store first");
        database
            .execute(
                "CREATE TABLE kept (id INTEGER PRIMARY KEY, payload TEXT)",
                &HashMap::new(),
            )
            .expect("give the store something to lose");
        drop(database);
    }
    truncate(path);
}

/// The barest corrupt store: created and immediately cut short, with no table
/// ever written. A store with content and a store with none are truncated to
/// the same length but lose different things, so both shapes are read here.
fn make_empty_truncated_store(path: &Path) {
    {
        let database = Database::open(path).expect("create a valid store first");
        drop(database);
    }
    truncate(path);
}

fn truncate(path: &Path) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("reopen the store to corrupt it");
    file.set_len(2_000).expect("cut the store short");
    file.sync_all().expect("flush the truncation");
}

#[test]
fn a_truncated_store_with_nothing_in_it_is_refused_when_a_session_opens_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped corrupt-store directory");
    let path = directory.path().join("truncated-empty.db");
    make_empty_truncated_store(&path);

    // Opened the way the command-line tool opens it: the same door, carrying
    // the same shipped ceilings and deadlines, on the same fixture bytes.
    let refused = ReadSession::open_with_options(&path, ReadSessionOptions::default());
    let error = match refused {
        Ok(session) => panic!(
            "opening a corrupt store answered Ok on the {:?} route -- the caller was told it \
             could read this store",
            session.route()
        ),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::StoreCorrupted { .. }),
        "a store that cannot produce a committed image is refused as corrupt: {error:?}"
    );
}

#[test]
fn a_truncated_store_is_refused_when_a_session_opens_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped corrupt-store directory");
    let path = directory.path().join("truncated.db");
    make_truncated_store(&path);

    let refused = ReadSession::open(&path);
    let error = match refused {
        Ok(session) => panic!(
            "opening a corrupt store answered Ok on the {:?} route -- the caller was told it \
             could read this store",
            session.route()
        ),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::StoreCorrupted { .. }),
        "a store that cannot produce a committed image is refused as corrupt: {error:?}"
    );
}

#[test]
fn a_reader_and_a_writer_refuse_a_truncated_store_in_the_same_words() {
    let directory = tempfile::TempDir::new().expect("task-scoped corrupt-store directory");
    let path = directory.path().join("truncated-parity.db");
    make_truncated_store(&path);

    // The operator who runs into this is told what to do about it. What they
    // are told must not depend on whether they were reading or writing.
    // Neither open hands back something printable, so each refusal is taken
    // by pattern rather than through a helper that would have to render the
    // value it did not get.
    let Err(writable) = Database::open(&path) else {
        panic!("a writable open refuses a corrupt store");
    };
    let Err(reading) = ReadSession::open(&path) else {
        panic!("a reading open refuses a corrupt store");
    };

    assert!(
        matches!(writable, Error::StoreCorrupted { .. }),
        "the writable open refuses it as corrupt: {writable:?}"
    );
    assert!(
        matches!(reading, Error::StoreCorrupted { .. }),
        "the reading open refuses it as corrupt too: {reading:?}"
    );

    // Both name the store, and both end with the same thing to go and do.
    // What each of them found differs -- one reached the pages, one did not --
    // but an operator's next step cannot depend on which door they knocked at.
    let reading = reading.to_string();
    let writable = writable.to_string();
    for rendered in [&reading, &writable] {
        assert!(
            rendered.contains(&path.display().to_string()),
            "the diagnostic names the store it is about: {rendered}"
        );
        assert!(
            rendered.contains("contextdb diagnose"),
            "the diagnostic says what to run next: {rendered}"
        );
        assert!(
            !rendered.contains("contextdb repair"),
            "the diagnostic never names a command this build does not have: {rendered}"
        );
    }
    let next_step = |rendered: &str| {
        rendered
            .rsplit_once(" \u{2014} ")
            .map(|(_, step)| step.to_owned())
            .unwrap_or_else(|| panic!("the diagnostic offers no next step: {rendered}"))
    };
    assert_eq!(
        next_step(&reading),
        next_step(&writable),
        "one corrupt file, one next step, whoever opened it"
    );
}

#[test]
fn a_readable_store_still_opens() {
    let directory = tempfile::TempDir::new().expect("task-scoped corrupt-store directory");
    let path = directory.path().join("intact.db");
    let database = Database::open(&path).expect("create a valid store");
    database
        .execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    database.close().expect("the writer closes cleanly");

    // The refusal above is about stores that cannot be read, not about
    // caution: an intact store opens and answers.
    let session = ReadSession::open(&path).expect("an intact store opens");
    assert_eq!(
        session
            .execute("SELECT id FROM kept", &HashMap::new())
            .expect("the intact store answers")
            .rows
            .len(),
        0
    );
}
