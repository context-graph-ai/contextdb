//! Transactions: a transaction reads its own writes, leaves nothing when it does not commit,
//! survives a refused statement, and loses cleanly to a transaction that committed first.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{Database, DatabaseOpenOptions, Error, Value};

use support::Call;

#[test]
fn a_transaction_sees_its_own_writes_and_a_dropped_or_rolled_back_one_leaves_nothing() {
    let folder = support::folder("transaction-leaves-nothing");
    // Counts what reaches the data file, so that "leaves nothing" is a number.
    let written = Arc::new(AtomicU64::new(0));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage({
        let written = written.clone();
        move |call| {
            if matches!(call, Call::Write(..) | Call::Sync) {
                written.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    let db = support::open_with(&folder, options).expect("open");
    support::declare(&db);
    support::insert(&db, 1..=1);
    let before = written.load(Ordering::SeqCst);
    let newest = db.committed_watermark();

    // Dropped: it saw its own rows, nobody else did, and nothing reached the file.
    {
        let tx = db.transaction().expect("begin");
        support::insert(&tx, 2..=3);
        support::assert_rows_are(&tx, 1..=3);
        support::assert_rows_are(&db, 1..=1);
        // The transaction is dropped here, uncommitted.
    }
    support::assert_rows_are(&db, 1..=1);
    assert_eq!(
        written.load(Ordering::SeqCst),
        before,
        "a dropped transaction wrote to the file"
    );
    assert_eq!(
        db.committed_watermark(),
        newest,
        "a dropped transaction moved the newest commit"
    );

    // Rolled back: the same.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 2..=3);
    support::run(&tx, "DELETE FROM t WHERE id = 1");
    support::assert_rows_are(&tx, 2..=3);
    tx.rollback();
    support::assert_rows_are(&db, 1..=1);
    assert_eq!(
        written.load(Ordering::SeqCst),
        before,
        "a rollback wrote to the file"
    );
    assert_eq!(
        db.committed_watermark(),
        newest,
        "a rollback moved the newest commit"
    );

    // Committed with nothing in it: the same.
    db.transaction()
        .expect("begin")
        .commit()
        .expect("commit an empty transaction");
    assert_eq!(
        written.load(Ordering::SeqCst),
        before,
        "an empty commit wrote to the file"
    );
    assert_eq!(
        db.committed_watermark(),
        newest,
        "an empty commit moved the newest commit"
    );

    // A transaction that commits something does reach the file, so the count above means it.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 2..=3);
    tx.commit().expect("commit");
    assert!(
        written.load(Ordering::SeqCst) > before,
        "a commit did not reach the file"
    );
    assert!(
        db.committed_watermark() > newest,
        "a commit of rows did not move the newest commit"
    );

    // A statement that finds nothing to change is the same as no statement: run on its own, and
    // as the whole of a transaction that commits.
    let before = written.load(Ordering::SeqCst);
    let newest = db.committed_watermark();
    let nothing = support::run(&db, "DELETE FROM t WHERE id = 404");
    assert_eq!(nothing.rows_affected(), 0);
    assert_eq!(
        db.committed_watermark(),
        newest,
        "a DELETE that matched no row moved the newest commit"
    );
    assert_eq!(
        written.load(Ordering::SeqCst),
        before,
        "a DELETE that matched no row wrote to the file"
    );
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM t WHERE id = 404");
    tx.commit()
        .expect("commit a transaction that changed nothing");
    assert_eq!(
        db.committed_watermark(),
        newest,
        "a transaction whose DELETE matched no row moved the newest commit"
    );
    assert_eq!(
        written.load(Ordering::SeqCst),
        before,
        "a transaction whose DELETE matched no row wrote to the file"
    );
    support::close(db);
    support::assert_rows_are(&support::open(&folder), 1..=3);
}

#[test]
fn of_two_transactions_inserting_one_key_exactly_one_commits() {
    let folder = support::folder("two-transactions-one-key");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
    );

    // Both begin at the same moment and take the same key. The first to commit has it.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(
        &first,
        "INSERT INTO people (id, email) VALUES (1, 'first@example.org')",
    );
    support::run(
        &second,
        "INSERT INTO people (id, email) VALUES (1, 'second@example.org')",
    );
    first
        .commit()
        .expect("the first of two transactions commits");
    match second.commit() {
        Err(Error::UniqueViolation { table, column, .. }) => {
            assert_eq!((table.as_str(), column.as_str()), ("people", "id"));
        }
        other => panic!("the second transaction to take key 1 got {other:?}"),
    }

    // The same for a value of a unique column under two different keys.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(
        &first,
        "INSERT INTO people (id, email) VALUES (2, 'shared@example.org')",
    );
    support::run(
        &second,
        "INSERT INTO people (id, email) VALUES (3, 'shared@example.org')",
    );
    first
        .commit()
        .expect("the first of two transactions commits");
    match second.commit() {
        Err(Error::UniqueViolation { table, column, .. }) => {
            assert_eq!((table.as_str(), column.as_str()), ("people", "email"));
        }
        other => panic!("the second transaction to take the email got {other:?}"),
    }

    // Two that take nothing from each other both commit, though each began before the other
    // committed.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(
        &first,
        "INSERT INTO people (id, email) VALUES (4, 'four@example.org')",
    );
    support::run(
        &second,
        "INSERT INTO people (id, email) VALUES (5, 'five@example.org')",
    );
    first
        .commit()
        .expect("the first of two transactions commits");
    second
        .commit()
        .expect("a transaction that conflicts with nothing commits");

    // A table with no primary key has no key for rows to collide on: both commit, as in
    // Postgres, and both rows are there.
    support::run(&db, "CREATE TABLE notes (body TEXT)");
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(&first, "INSERT INTO notes (body) VALUES ('one')");
    support::run(&second, "INSERT INTO notes (body) VALUES ('two')");
    first
        .commit()
        .expect("the first insert into a table with no key");
    second
        .commit()
        .unwrap_or_else(|error| panic!("the second insert into a table with no key: {error:?}"));
    assert_eq!(
        support::rows(&db, "SELECT body FROM notes ORDER BY body"),
        [
            [Value::Text("one".to_owned())],
            [Value::Text("two".to_owned())]
        ]
    );

    // A transaction that inserts a key and deletes it again leaves the key empty, but another
    // handle took the key meanwhile: its commit is refused as a conflict, and the other handle's
    // row stands among the winners below.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO people VALUES (6, 'six@example.org')");
    support::run(&tx, "DELETE FROM people WHERE id = 6");
    support::run(&db, "INSERT INTO people VALUES (6, 'six@example.org')");
    match tx.commit() {
        Err(Error::ConditionalUpdateConflict { count }) => assert_eq!(count, 1),
        other => panic!("a transaction whose key was taken meanwhile: {other:?}"),
    }

    // The stored rows are exactly the winners'.
    let text = |s: &str| Value::Text(s.to_owned());
    assert_eq!(
        support::rows(&db, "SELECT id, email FROM people ORDER BY id"),
        [
            [Value::Int64(1), text("first@example.org")],
            [Value::Int64(2), text("shared@example.org")],
            [Value::Int64(4), text("four@example.org")],
            [Value::Int64(5), text("five@example.org")],
            [Value::Int64(6), text("six@example.org")],
        ]
    );
}

#[test]
fn of_two_transactions_changing_one_row_the_first_commits_and_the_second_is_refused() {
    let folder = support::folder("two-transactions-one-row");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);

    // Two updates of one row. The second transaction also adds a row, and loses that too.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(&first, "UPDATE t SET tag = 7 WHERE id = 1");
    support::run(&second, "UPDATE t SET tag = 8 WHERE id = 1");
    support::insert(&second, 4..=4);
    first
        .commit()
        .expect("the first of two transactions commits");
    assert_overtaken(
        second.commit(),
        "an update of a row that another commit updated",
    );
    assert_tags(&db, &[(1, 7), (2, 2), (3, 3)], "after two updates of row 1");
    // The refusal is one to try again after: the same statements in a new transaction, which
    // begins after the winner's commit, commit.
    let again = db.transaction().expect("begin");
    support::run(&again, "UPDATE t SET tag = 8 WHERE id = 1");
    support::insert(&again, 4..=4);
    again
        .commit()
        .expect("the refused transaction commits when it is run again");
    assert_tags(
        &db,
        &[(1, 8), (2, 2), (3, 3), (4, 4)],
        "after the second update was run again",
    );

    // An update, then a delete of the same row.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(&first, "UPDATE t SET tag = 9 WHERE id = 2");
    support::run(&second, "DELETE FROM t WHERE id = 2");
    first
        .commit()
        .expect("the first of two transactions commits");
    assert_overtaken(
        second.commit(),
        "a delete of a row that another commit updated",
    );
    assert_tags(
        &db,
        &[(1, 8), (2, 9), (3, 3), (4, 4)],
        "after an update and a delete of row 2",
    );
    let again = db.transaction().expect("begin");
    support::run(&again, "DELETE FROM t WHERE id = 2");
    again
        .commit()
        .expect("the refused delete commits when it is run again");
    assert_tags(
        &db,
        &[(1, 8), (3, 3), (4, 4)],
        "after the delete was run again",
    );

    // A delete, then an update of the same row.
    let first = db.transaction().expect("begin");
    let second = db.transaction().expect("begin");
    support::run(&first, "DELETE FROM t WHERE id = 3");
    support::run(&second, "UPDATE t SET tag = 5 WHERE id = 3");
    first
        .commit()
        .expect("the first of two transactions commits");
    assert_overtaken(
        second.commit(),
        "an update of a row that another commit deleted",
    );
    assert_tags(
        &db,
        &[(1, 8), (4, 4)],
        "after a delete and an update of row 3",
    );
    // Run again, the update finds no row to change, and commits that.
    let again = db.transaction().expect("begin");
    let changed = support::run(&again, "UPDATE t SET tag = 5 WHERE id = 3");
    assert_eq!(changed.rows_affected(), 0);
    again
        .commit()
        .expect("the refused update commits when it is run again");
    assert_tags(
        &db,
        &[(1, 8), (4, 4)],
        "after the update of a deleted row was run again",
    );
}

/// Asserts that a commit was refused because one row it changed was changed by a commit that
/// came first.
fn assert_overtaken(commit: contextdb::Result<()>, what: &str) {
    match commit {
        Err(Error::ConditionalUpdateConflict { count }) => {
            assert_eq!(count, 1, "{what}: the number of rows overtaken");
        }
        other => panic!("{what} got {other:?}"),
    }
}

/// Asserts that `t` holds exactly the rows with these ids, each with this tag in place of the
/// one it was inserted with: by its rows, by its kept count, and by its index on `tag`.
fn assert_tags(db: &Database, wanted: &[(i64, i64)], when: &str) {
    let rows: Vec<Vec<Value>> = wanted
        .iter()
        .map(|&(id, tag)| {
            let mut row = support::row(id);
            row[2] = Value::Int64(tag);
            row
        })
        .collect();
    support::assert_same_rows(
        &support::rows(db, support::SCAN),
        &rows,
        &format!("the rows of t {when}"),
    );
    assert_eq!(
        support::int(db, "SELECT COUNT(*) FROM t"),
        wanted.len() as i64,
        "the kept count of t {when}"
    );
    for tag in 0..10 {
        let ids: Vec<[Value; 1]> = wanted
            .iter()
            .filter(|row| row.1 == tag)
            .map(|row| [Value::Int64(row.0)])
            .collect();
        let found = support::rows(
            db,
            &format!("SELECT id FROM t WHERE tag = {tag} ORDER BY id"),
        );
        support::assert_same_rows(
            &found,
            &ids,
            &format!("the rows the index gives for tag {tag} {when}"),
        );
    }
}

#[test]
fn a_refused_statement_changes_nothing_and_leaves_the_transaction_usable() {
    let folder = support::folder("refused-statement");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=1);

    let tx = db.transaction().expect("begin");
    support::insert(&tx, 2..=2);

    // A key that is taken, by a committed row and by a row of this transaction.
    for taken in [1, 2] {
        let sql = format!("INSERT INTO t (id, name, tag) VALUES ({taken}, 'again', 0)");
        match support::refusal(&tx, &sql) {
            Error::UniqueViolation { table, column, .. } => {
                assert_eq!((table.as_str(), column.as_str()), ("t", "id"));
            }
            other => panic!("a taken key was refused as {other:?}"),
        }
    }
    // No value where one is required, on the way in and in a change to a stored row.
    for sql in [
        "INSERT INTO t (id, name, tag) VALUES (3, NULL, 3)",
        "UPDATE t SET name = NULL WHERE id = 1",
    ] {
        match support::refusal(&tx, sql) {
            Error::ColumnNotNullable { table, column } => {
                assert_eq!((table.as_str(), column.as_str()), ("t", "name"));
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
    // A value of another type than the column's.
    match support::refusal(
        &tx,
        "INSERT INTO t (id, name, tag) VALUES (3, 'row-0000003', 'three')",
    ) {
        Error::ColumnTypeMismatch { table, column, .. } => {
            assert_eq!((table.as_str(), column.as_str()), ("t", "tag"));
        }
        other => panic!("text for an INTEGER column was refused as {other:?}"),
    }
    // One bad row refuses the whole statement: its good first row is not kept.
    let sql = "INSERT INTO t (id, name, tag) VALUES (3, 'row-0000003', 3), (1, 'again', 0)";
    assert!(matches!(
        support::refusal(&tx, sql),
        Error::UniqueViolation { .. }
    ));

    // The transaction holds what it held before the refusals, takes more, and commits.
    support::assert_rows_are(&tx, 1..=2);
    support::insert(&tx, 3..=3);
    tx.commit().expect("commit after refused statements");
    support::assert_rows_are(&db, 1..=3);

    // The same refusals outside a transaction leave the committed rows as they were.
    support::refusal(&db, "INSERT INTO t (id, name, tag) VALUES (1, 'again', 0)");
    support::refusal(&db, "UPDATE t SET name = NULL WHERE id = 1");
    support::assert_rows_are(&db, 1..=3);
}

#[test]
fn a_transaction_that_declares_a_table_may_not_also_write_rows() {
    let folder = support::folder("declare-and-write");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=1);

    let tx = db.transaction().expect("begin");
    support::run(&tx, "CREATE TABLE made (id INTEGER PRIMARY KEY, body TEXT)");
    // Rows are refused, in the table it declared and in one that was there before.
    for sql in [
        "INSERT INTO made (id, body) VALUES (1, 'x')",
        "INSERT INTO t (id, name, tag) VALUES (2, 'row-0000002', 2)",
        "DELETE FROM t WHERE id = 1",
    ] {
        let refusal = support::refusal(&tx, sql);
        assert!(
            matches!(refusal, Error::MixedSchemaAndRowWrite),
            "{sql}\nwas refused as {refusal:?}"
        );
    }
    // It may go on declaring, and its declarations commit together.
    support::run(&tx, "CREATE TABLE made_too (id INTEGER PRIMARY KEY)");
    tx.commit().expect("commit the declarations");

    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM made"), 0);
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM made_too"), 0);
    support::assert_rows_are(&db, 1..=1);
    // Once the declaration is committed, rows are written in the ordinary way.
    support::run(&db, "INSERT INTO made (id, body) VALUES (1, 'x')");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM made"), 1);

    // The other way round: a transaction that has written a row may not then declare a table.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 2..=2);
    let refusal = support::refusal(&tx, "CREATE TABLE late (id INTEGER PRIMARY KEY)");
    assert!(
        matches!(refusal, Error::MixedSchemaAndRowWrite),
        "a declaration after a row was refused as {refusal:?}"
    );
    tx.commit().expect("commit the row");
    support::assert_rows_are(&db, 1..=2);
    assert!(matches!(
        support::refusal(&db, "SELECT * FROM late"),
        Error::TableNotFound(_)
    ));

    // The same for a change and a drop of the table: after a row, each is refused and the row
    // commits with the table as it was; before a row, the drop of the column commits.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 3..=3);
    for sql in ["ALTER TABLE t DROP COLUMN tag CASCADE", "DROP TABLE t"] {
        let refusal = support::refusal(&tx, sql);
        assert!(
            matches!(refusal, Error::MixedSchemaAndRowWrite),
            "{sql}\nafter a row was refused as {refusal:?}"
        );
    }
    tx.commit().expect("commit the row");
    support::assert_rows_are(&db, 1..=3);
    let tx = db.transaction().expect("begin");
    support::run(&tx, "ALTER TABLE t DROP COLUMN tag CASCADE");
    let refusal = support::refusal(&tx, "UPDATE t SET name = 'x' WHERE id = 1");
    assert!(
        matches!(refusal, Error::MixedSchemaAndRowWrite),
        "a row change after a dropped column was refused as {refusal:?}"
    );
    tx.commit().expect("commit the drop");
    assert!(matches!(
        support::refusal(&db, "SELECT tag FROM t"),
        Error::ColumnNotFound { .. }
    ));
}

#[test]
fn a_statement_commits_while_a_cycle_works_out_a_step() {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::time::Duration;

    // 2,000 sealed vectors of 256 numbers in three blocks, and their build begun.
    const DIMENSION: usize = 256;
    let folder = support::folder("commit-beside-a-step");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "AUTO_INDEX_AT 1000");
    support::load_docs(&db, 1..=2000, "a", |n| support::gathered(n, DIMENSION));
    let report = support::cycle(&db);
    assert_eq!(
        (report.maintenance_vectors_done, report.remaining_partitions),
        (2000, 1)
    );
    support::close(db);

    // The storage door holds the first read of 64 KiB or more made once it is armed: a block
    // read by the step the cycle works out. It says so, and waits to be let go.
    let armed = Arc::new(AtomicBool::new(false));
    let (holding, held) = mpsc::channel::<()>();
    let (let_go, go) = mpsc::channel::<()>();
    let go = Mutex::new(go);
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage({
        let armed = armed.clone();
        move |call| {
            let large = matches!(&call, Call::Read(_, bytes) if bytes.len() >= 64 << 10);
            if large && armed.swap(false, Ordering::SeqCst) {
                holding.send(()).expect("the test listens");
                let go = go.lock().expect("one read is held");
                go.recv().expect("the test lets the read go");
            }
        }
    }));
    let db = support::open_with(&folder, options).expect("open");
    let before = db.committed_watermark();
    armed.store(true, Ordering::SeqCst);

    let db = &db;
    let (inserted, in_time, cycled) = std::thread::scope(|scope| {
        let cycle = scope.spawn(|| db.run_maintenance_cycle());
        held.recv_timeout(Duration::from_secs(20))
            .expect("a step of the build reads a block");
        // The cycle is inside its step. An INSERT on another thread returns while it is.
        let (done, returned) = mpsc::channel::<()>();
        let insert = scope.spawn(move || {
            let row = support::gathered(9001, DIMENSION);
            let outcome = support::insert_doc(db, 9001, "a", Some(row));
            done.send(()).expect("the test listens");
            outcome.map(|_| db.committed_watermark())
        });
        let in_time = returned.recv_timeout(Duration::from_secs(20)).is_ok();
        let_go.send(()).expect("the held read listens");
        let inserted = insert.join().expect("the thread of the INSERT");
        (inserted, in_time, cycle.join().expect("the cycle's thread"))
    });
    assert!(
        in_time,
        "the INSERT waited for the step the cycle was working out"
    );
    let after = inserted.expect("an INSERT beside a cycle");
    assert!(after > before, "the INSERT committed");
    cycled.expect("the cycle whose read was held");
    assert_eq!(support::int(db, "SELECT COUNT(*) FROM docs"), 2001);
}
