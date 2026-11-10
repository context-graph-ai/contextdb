//! A store is one folder: made with no configuration, the same after close and reopen, and held
//! for writing by one process at a time.

mod support;

use contextdb::{DatabaseOpenOptions, Error, Value, params};

#[test]
fn a_fresh_store_is_one_folder_and_works_with_no_configuration() {
    let around = support::folder("fresh-store");
    let folder = around.join("notes");
    let db = support::open(&folder);

    // Nothing was declared, so no table is there: not the user's, and none the engine brought.
    assert!(
        db.table_names().is_empty(),
        "a fresh store has the tables {:?}",
        db.table_names()
    );
    match support::refusal(&db, "SELECT * FROM notes") {
        Error::TableNotFound(table) => assert_eq!(table, "notes"),
        other => panic!("a fresh store answered {other:?} when asked for a table"),
    }

    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
    );
    assert_eq!(db.table_names(), ["notes"]);
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params! { "id" => 1_i64, "body" => "buy milk" },
    )
    .expect("insert into a fresh store");
    assert_eq!(
        support::rows(&db, "SELECT id, body FROM notes ORDER BY id"),
        [[Value::Int64(1), Value::Text("buy milk".to_owned())]]
    );

    // The store is the folder it was opened at. Nothing was written beside it.
    assert_eq!(support::listing(&around), ["notes"]);
    assert_eq!(support::listing(&folder), ["data", "lock", "readers"]);
    support::close(db);
}

#[test]
fn what_was_committed_is_there_after_close_and_reopen() {
    let folder = support::folder("close-and-reopen");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=20);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 21..=40);
    tx.commit().expect("commit");
    support::run(&db, "DELETE FROM t WHERE id > 35");
    support::close(db);

    let db = support::open(&folder);
    support::assert_rows_are(&db, 1..=35);

    // The reopened store goes on from there.
    support::insert(&db, 36..=36);
    support::close(db);
    support::assert_rows_are(&support::open(&folder), 1..=36);
}

#[test]
fn a_second_writer_is_refused_at_once_naming_the_holder_and_the_next_open_needs_no_retry() {
    if let Some(role) = support::role() {
        return holder(role);
    }
    let folder = support::folder("second-writer");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);

    let mut holder = support::Child::start(
        "a_second_writer_is_refused_at_once_naming_the_holder_and_the_next_open_needs_no_retry",
        "holder",
        &folder,
    );
    holder.expect("holding");

    // The holder is waiting for a line and will not let go, so an open that returns at all has
    // not waited for it. Refused twice: a refused open takes nothing away from the holder.
    for _ in 0..2 {
        match support::open_with(&folder, DatabaseOpenOptions::default()) {
            Err(Error::DatabaseLocked { holder_pid, path }) => {
                assert_eq!(
                    holder_pid,
                    holder.id(),
                    "the refusal names the process that holds it"
                );
                assert_eq!(path, folder, "the refusal names the store");
            }
            Err(other) => panic!("a second writer was refused as {other:?}"),
            Ok(_) => panic!("a second writer was let in beside the first"),
        }
    }
    holder.tell("commit");
    holder.expect("committed");

    // The holder lets go and keeps running. The very next open gets the store.
    holder.tell("close");
    holder.expect("closed");
    let db = support::open(&folder);
    support::assert_rows_are(&db, 1..=1);
    holder.end();
}

/// Holds the store for writing until it is told to close it.
fn holder(role: support::Role) {
    let db = support::open(&role.folder);
    support::say("holding");
    while let Some(word) = support::hear() {
        match word.as_str() {
            "commit" => {
                support::insert(&db, 1..=1);
                support::say("committed");
            }
            "close" => {
                db.close().expect("close");
                support::say("closed");
            }
            other => panic!("the holder was told {other:?}"),
        }
    }
}
