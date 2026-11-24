//! A reader's check for a running writer (`ReadSession::writer_running`) only looks: it answers
//! from the writer's own hold on the store, and it never keeps a writer out.

mod support;

use std::fs::File;
use std::time::UNIX_EPOCH;

use contextdb::DatabaseOpenOptions;

#[test]
fn a_readers_check_for_a_running_writer_looks_at_the_writer_and_never_refuses_one() {
    let folder = support::folder("reader-check");
    support::close(support::open(&folder));
    let reader = support::open_reader(&folder, None);
    assert!(
        !reader.writer_running().unwrap(),
        "no writer has the store: the check says so"
    );

    // A holder of `lock` with no data file yet is no running writer.
    let owner = File::open(folder.join("lock")).unwrap();
    owner.lock().unwrap();
    assert!(
        !reader.writer_running().unwrap(),
        "a holder of `lock` that has not taken its data file is not a running writer"
    );
    owner.unlock().unwrap();

    // The check has finished and holds nothing: a writer with no wait left is not refused.
    let mut options = DatabaseOpenOptions::default();
    options.deadline = Some(UNIX_EPOCH);
    let opened = support::open_with(&folder, options);
    let db = opened.unwrap_or_else(|error| {
        panic!("a writer was refused after a reader's check finished: {error:?}")
    });
    assert!(
        reader.writer_running().unwrap(),
        "the writer that opened is running: the check says so"
    );
    support::close(db);
    assert!(
        !reader.writer_running().unwrap(),
        "the writer closed: the check says no writer"
    );
}
