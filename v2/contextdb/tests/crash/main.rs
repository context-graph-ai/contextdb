//! Kill and reopen: a writer killed at any storage call of a commit leaves the whole commit or
//! none of it, and the store opens again with nothing to mend by hand. That holds for a commit
//! of rows with vectors and for the commit of a maintenance cycle that seals vectors: after
//! either, every search finds exactly the vectors of the rows that are there. It holds for a
//! commit of rows that reference each other, are counted by a kept count and are indexed by
//! values too long for an index entry: the rows, the counts and the entries are there together
//! or not at all. Damage in the file is refused as damage, by name.
//!
//! A kill lands at a point the child names. The child's storage door counts the writes and
//! syncs of its data file; before call number `stop_at` it announces `stopped` and waits there,
//! and the parent kills it.

mod purge;
#[path = "../support/mod.rs"]
mod support;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use contextdb::{Database, DatabaseOpenOptions, Error, ReadSession, Value, WrapStorage, params};

use support::{Call, Child, Run, count, faulting_before_call, for_each_stop, learned};

#[test]
fn a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing() {
    if let Some(role) = support::role() {
        if role.name == "purger" {
            return purge::purging(role);
        }
        return three_commits(role);
    }
    let test = "a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing";
    // Run once to the end to learn how many storage calls three commits make: at least a write
    // and a sync each. Three commits fill both commit slots and write over the first again.
    let (_, calls, _) = learned(test, "writer", &store_with_rows("kill-count", 0), None);
    assert!(calls >= 6, "three commits made {calls} writes and syncs");

    let store = store_with_rows("kill-source", 0);
    let sequel = support::folder("kill-again");
    let mut from = 0;
    for_each_stop(test, "writer", 1..=calls, store, |k, folder, before| {
        // Commits that had returned before the kill.
        let committed = support::last_number(before, "committed").unwrap_or(0) as i64;

        let db = support::open(folder); // it must open: there is nothing to mend by hand
        let rows = support::int(&db, "SELECT COUNT(*) FROM t");
        assert!(
            rows == committed || rows == committed + 1,
            "killed at storage call {k}: {rows} rows are there, {committed} commits had returned"
        );
        support::assert_rows_are(&db, 1..=rows);
        // The store goes on from the state it reopened at.
        support::insert(&db, rows + 1..=rows + 1);
        support::assert_rows_are(&db, 1..=rows + 1);
        if k == calls / 2 {
            fs::copy(folder.join("data"), sequel.join("data")).unwrap();
            from = rows + 1;
        }
    });
    let counted = support::folder("kill-again-count");
    fs::copy(sequel.join("data"), counted.join("data")).unwrap();
    let (_, calls, _) = learned(test, "writer", &counted, None);
    for_each_stop(
        test,
        "writer",
        [1, calls / 2, calls],
        sequel,
        |_, folder, before| {
            let db = support::open(folder);
            let committed = support::last_number(before, "committed").map_or(from, |n| n as i64);
            let rows = support::int(&db, "SELECT COUNT(*) FROM t");
            assert!(rows == committed || rows == committed + 1);
            support::assert_rows_are(&db, 1..=rows);
            db.verify().unwrap();
        },
    );
    purge::kill_workload(test);
}

/// Commits the next three rows, one commit each, announcing `committed <n>` after each one returns, and
/// `calls <total>` once the store is closed.
fn three_commits(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    let first = support::int(&db, "SELECT COUNT(*) FROM t") + 1;
    for n in first..first + 3 {
        support::insert(&db, n..=n);
        support::say(format!("committed {n}"));
    }
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_kill_inside_a_commit_of_many_rows_keeps_rows_index_and_count_together() {
    if let Some(role) = support::role() {
        return one_large_commit(role, 1010);
    }
    let test = "a_kill_inside_a_commit_of_many_rows_keeps_rows_index_and_count_together";
    // Run once to the end to learn which storage calls belong to the large commit.
    let folder = store_with_rows("kill-large-count", 10);
    let (begins, calls, _) = learned(test, "writer", &folder, Some("committing"));
    assert!(
        calls > begins,
        "a commit of a thousand rows made no write and no sync"
    );

    let store = store_with_rows("kill-large-source", 10);
    for_each_stop(
        test,
        "writer",
        begins + 1..=calls,
        store,
        |k, folder, before| {
            let returned = before.iter().any(|line| line == "committed");

            // The store holds the rows from before the commit or the rows from after it, and in
            // either case its rows, its index and its kept count say the same.
            let db = support::open(folder);
            match support::int(&db, "SELECT COUNT(*) FROM t") {
                10 if !returned => support::assert_rows_are(&db, 1..=10),
                1005 => support::assert_rows_are(&db, 6..=1010),
                rows => panic!(
                    "killed at storage call {k}: {rows} rows are there; the commit had {}returned",
                    if returned { "" } else { "not " }
                ),
            }
        },
    );
}

/// In one transaction removes rows 1 to 5 and adds rows 11 to `last`. Announces `committing <n>`
/// with the storage calls made so far when the commit is about to begin, `committed` when it
/// has returned, and `calls <total>` once the store is closed.
fn one_large_commit(role: support::Role, last: i64) {
    let (db, calls) = open_stopping(&role);
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM t WHERE id <= 5");
    support::insert(&tx, 11..=last);
    support::say(format!("committing {}", calls.load(Ordering::SeqCst)));
    tx.commit().expect("commit");
    support::say("committed");
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_damaged_page_is_refused_as_damage_by_name() {
    if let Some(role) = support::role() {
        return killed_writer(role);
    }
    let folder = store_with_rows("damaged-page", 300);
    // The store is one a killed writer left, so no clean close has checked it.
    let mut writer = Child::start(
        "a_damaged_page_is_refused_as_damage_by_name",
        "writer",
        &folder,
    );
    writer.expect("committed");
    writer.kill();
    let wanted: Vec<Vec<Value>> = (1..=310).map(support::row).collect();

    // Through a reader: each page the scan reads is damaged in turn, on its way from the file.
    let on_disk = fs::read(folder.join("data")).expect("read the data file");
    let refused = damage_each_page_of_a_scan(&wanted, support::SCAN, |storage| {
        let mut options = support::reader_options(None);
        options.storage = Some(storage);
        ReadSession::open_with_options(&folder, options)
    });
    assert!(refused > 0, "no damaged page was refused through a reader");
    // A reader that met damage rebuilt nothing: the file is byte for byte what it was.
    assert!(
        fs::read(folder.join("data")).expect("read the data file") == on_disk,
        "a reader that met damage changed the data file"
    );

    // Through the writer: the same.
    let refused = damage_each_page_of_a_scan(&wanted, support::SCAN, |storage| {
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        support::open_with(&folder, options)
    });
    assert!(
        refused > 0,
        "no damaged page was refused through the writer"
    );
    // The file itself was never damaged, and nothing was rebuilt from a damaged read: opened
    // with no damage on the way, the store holds what it held.
    support::assert_rows_are(&support::open(&folder), 1..=310);
}

#[test]
fn a_damaged_page_of_an_indexed_search_is_refused_as_damage_by_name() {
    let folder = support::folder("damaged-vector-page");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "INDEXING_THRESHOLD 100");
    support::load_docs(&db, 1..=300, "a", |n| support::gathered(n, DIMENSION));
    support::maintenance(&db);
    assert_eq!(groups(&db), Value::Int64(17), "the IVF is built");
    support::load_docs(&db, 301..=310, "a", |n| support::gathered(n, DIMENSION));
    let sql = support::by_mode(None, "INDEXED", 10);
    let query = support::gathered(98_101, DIMENSION);
    let wanted = support::search(&db, &sql, &query).unwrap().rows().to_vec();
    support::close(db);
    let reader = |storage| {
        let mut options = support::reader_options(None);
        options.storage = Some(storage);
        ReadSession::open_with_options(&folder, options)
    };
    assert!(
        damage_each_page_of_a_scan(&wanted, &sql, reader) > 0,
        "a reader met no damage"
    );
    let writer = |storage| {
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        support::open_with(&folder, options)
    };
    assert!(
        damage_each_page_of_a_scan(&wanted, &sql, writer) > 0,
        "the writer met no damage"
    );
}

/// Opens the store, adds rows 301 to 310, announces `committed` and holds the store until it is
/// killed.
fn killed_writer(role: support::Role) {
    let db = support::open(&role.folder);
    support::insert(&db, 301..=310);
    support::say("committed");
    support::hear();
}

/// Scans the table once to learn which pages the scan reads, then once more for each of those
/// pages with one byte of that page changed on its way from the file. Every time, the scan must
/// be refused as damage or answer with exactly the right rows; never a wrong answer and never
/// another refusal. Returns how many times it was refused as damage.
fn damage_each_page_of_a_scan<S: Run>(
    wanted: &[Vec<Value>],
    scan: &str,
    open: impl Fn(WrapStorage) -> contextdb::Result<S>,
) -> usize {
    // The offsets the scan reads at, in order.
    let scanning = Arc::new(AtomicBool::new(false));
    let offsets = Arc::new(Mutex::new(BTreeSet::new()));
    let session = open(support::storage({
        let (scanning, offsets) = (scanning.clone(), offsets.clone());
        move |call| {
            if let Call::Read(offset, _) = call
                && scanning.load(Ordering::SeqCst)
            {
                offsets.lock().expect("the list of offsets").insert(offset);
            }
        }
    }))
    .unwrap_or_else(|error| panic!("the undamaged store did not open: {error:?}"));
    scanning.store(true, Ordering::SeqCst);
    let query = params! { "q" => support::gathered(98_101, DIMENSION) };
    let answer = session.run(scan, &query).unwrap();
    support::assert_same_rows(answer.rows(), wanted, "the undamaged scan");
    scanning.store(false, Ordering::SeqCst);
    drop(session);
    let offsets = offsets.lock().expect("the list of offsets").clone();
    assert!(
        !offsets.is_empty(),
        "the scan read nothing through the storage door"
    );

    let mut refused = 0;
    for damaged in offsets {
        let storage = support::storage(move |call| {
            if let Call::Read(offset, bytes) = call
                && offset == damaged
                && let Some(byte) = bytes.get_mut(16)
            {
                *byte ^= 0xFF;
            }
        });
        let answer = open(storage).and_then(|session| session.run(scan, &query));
        match answer {
            Err(Error::StoreCorrupted { .. }) => refused += 1,
            Err(other) => panic!("damage in the page at {damaged} was reported as {other:?}"),
            Ok(result) => support::assert_same_rows(
                result.rows(),
                wanted,
                &format!("the answer with damage in the page at {damaged}"),
            ),
        }
    }
    refused
}

/// Rows of padding in the test below, 800 bytes each: 8 MiB.
const PADDING_ROWS: i64 = 10_000;

#[test]
fn a_commit_on_disk_is_reported_and_served_though_the_read_after_it_fails() {
    let test = "a_commit_on_disk_is_reported_and_served_though_the_read_after_it_fails";
    let folder = if let Some(role) = support::role() {
        role.folder
    } else {
        let mut child = Child::start(test, "faults", &support::folder("read-fails-after-commit"));
        child.until("failed");
        child.tell("resume");
        child.end();
        return;
    };
    let (pause, searching, reads) = support::pausing_at_read(1);
    let paused = Arc::new(std::sync::OnceLock::new());
    let fail_kind = Arc::new(AtomicU64::new(0));
    // The storage door counts syncs. Told a number, it fails every read made once that many
    // syncs have been counted: the reads that follow a commit's arrival on disk.
    let syncs = Arc::new(AtomicU64::new(0));
    let fail_after = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    // When set, every read fails, whatever the syncs: reads that fail before a commit is on disk.
    let fail_now = Arc::new(AtomicBool::new(false));
    let options = || {
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(support::failing_storage({
            let (syncs, fail_after, failed) = (syncs.clone(), fail_after.clone(), failed.clone());
            let (fail_now, fail_kind) = (fail_now.clone(), fail_kind.clone());
            let (pause, paused, searching, data) = (
                pause.clone(),
                paused.clone(),
                searching.clone(),
                folder.join("data"),
            );
            move |call| {
                if matches!(
                    (&call, fail_kind.load(Ordering::SeqCst)),
                    (Call::Write(..), 1) | (Call::Sync, 2)
                ) {
                    return Err(io::Error::other("a write or sync this test makes fail"));
                }
                match call {
                    Call::Sync => {
                        syncs.fetch_add(1, Ordering::SeqCst);
                    }
                    Call::Read(offset, out) => {
                        if searching.load(Ordering::SeqCst) {
                            paused
                                .get_or_init(|| pause(fs::File::open(&data).unwrap()).unwrap())
                                .read(offset, out)?;
                        }
                        let after = fail_after.load(Ordering::SeqCst);
                        if fail_now.load(Ordering::SeqCst)
                            || (after > 0 && syncs.load(Ordering::SeqCst) >= after)
                        {
                            failed.fetch_add(1, Ordering::SeqCst);
                            return Err(io::Error::other("a read this test makes fail"));
                        }
                    }
                    Call::Write(..) | Call::Resize(_) | Call::Close => {}
                }
                Ok(())
            }
        }));
        // Under a memory limit of 16 MiB the store's page cache is 4 MiB: a quarter of the limit.
        options.memory_limit = Some(16 << 20);
        options
    };
    let db = support::open_with(&folder, options()).expect("open");
    support::declare(&db);
    // A second table twice the size of that 4 MiB page cache, in rows small enough to be
    // kept in the table's own pages and of text that no compressor shortens, so that a row takes
    // the room it is written with. Reading all of it leaves the cache full of its pages, so
    // the pages a commit writes afterwards have to be read back from the file.
    support::run(
        &db,
        "CREATE TABLE padding (id INTEGER PRIMARY KEY, body TEXT)",
    );
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let body: String = (0..800)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state >> 60) as u8)
        })
        .collect();
    for first in (1..=PADDING_ROWS).step_by(2000) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 2000 {
            tx.execute(
                "INSERT INTO padding (id, body) VALUES ($id, $body)",
                &params! { "id" => id, "body" => body.as_str() },
            )
            .expect("insert a row of padding");
        }
        tx.commit().expect("commit padding");
    }
    let padding = "SELECT COUNT(body) FROM padding";
    assert_eq!(support::int(&db, padding), PADDING_ROWS);

    // One commit of one row, to learn how many syncs it makes: the last is its arrival on disk.
    syncs.store(0, Ordering::SeqCst);
    support::insert(&db, 1..=1);
    let on_disk_at = syncs.load(Ordering::SeqCst);
    assert!(on_disk_at > 0, "a commit made no sync");
    assert_eq!(support::int(&db, padding), PADDING_ROWS);

    // The same commit for row 2, with every read failing from the moment it is on disk. The
    // commit happened, so the caller is told it did.
    syncs.store(0, Ordering::SeqCst);
    fail_after.store(on_disk_at, Ordering::SeqCst);
    let [id, name, tag] = <[Value; 3]>::try_from(support::row(2)).expect("a row has three columns");
    let commit = db.execute(
        "INSERT INTO t (id, name, tag) VALUES ($id, $name, $tag)",
        &params! { "id" => id, "name" => name, "tag" => tag },
    );
    assert!(
        commit.is_ok(),
        "a commit that reached the disk was reported as {commit:?}"
    );

    // While reads fail, and after they work again, the handle answers with the committed rows or
    // says that the store could not be read. It never answers from before the commit.
    answers_with_rows_or_store_io(&db, 2, "while reads fail");
    // The padding does not fit the page cache, so reading all of it now has to fail, and by
    // name. That also shows the failure this test makes is one the store meets.
    match db.run(padding, &params! {}) {
        Err(error @ Error::StoreIo { .. }) => {
            assert!(
                !error.to_string().contains("reset"),
                "a read that failed is retryable: {error}"
            )
        }
        other => panic!("a scan that must read the file got {other:?} while reads fail"),
    }
    assert!(
        failed.load(Ordering::SeqCst) > 0,
        "no read was made to fail after the commit"
    );
    fail_after.store(0, Ordering::SeqCst);
    support::assert_rows_are(&db, 1..=2); // a failed read does not end the handle

    // The next write lands on top of the commit, or is refused the same way.
    let [id, name, tag] = <[Value; 3]>::try_from(support::row(3)).expect("a row has three columns");
    let rows = match db.execute(
        "INSERT INTO t (id, name, tag) VALUES ($id, $name, $tag)",
        &params! { "id" => id, "name" => name, "tag" => tag },
    ) {
        Ok(_) => 3,
        Err(Error::StoreIo { .. }) => 2,
        Err(other) => panic!("the write after the commit was refused as {other:?}"),
    };
    answers_with_rows_or_store_io(&db, rows, "after the next write");

    // Closed and opened again with nothing failing, the store holds every commit that returned.
    let _ = db.close();
    support::assert_rows_are(&support::open(&folder), 1..=rows);

    // An open, for writing and for reading, while every read fails is a failure of the storage.
    let failing = || {
        support::failing_storage(|call| match call {
            Call::Read(..) => Err(io::Error::other("a read this test makes fail")),
            _ => Ok(()),
        })
    };
    let mut writing = DatabaseOpenOptions::default();
    writing.storage = Some(failing());
    match support::open_with(&folder, writing) {
        Err(Error::StoreIo { .. }) => {}
        other => panic!("an open for writing while reads fail got {:?}", other.err()),
    }
    let mut reading = support::reader_options(None);
    reading.storage = Some(failing());
    match ReadSession::open_with_options(&folder, reading) {
        Err(Error::StoreIo { .. }) => {}
        other => panic!("an open for reading while reads fail got {:?}", other.err()),
    }
    support::assert_rows_are(&support::open(&folder), 1..=rows);
    for kind in [0, 1, 2] {
        let db = support::open_with(&folder, options()).unwrap();
        let read = db.read_session(Default::default()).unwrap();
        let tx = db.transaction().unwrap();
        let mut live = read.open_cursor(support::SCAN, &params! {}).unwrap();
        let empty = "SELECT * FROM t WHERE id=0";
        let mut ended = read.open_cursor(empty, &params! {}).unwrap();
        assert!(ended.fetch(None).unwrap().rows().is_empty());
        for sql in [support::SCAN, "SHOW MEMORY_LIMIT"] {
            db.execute(sql, &params! {}).unwrap();
        }
        db.table_names().unwrap();
        read.execute("SELECT body FROM padding WHERE id = 5010", &params! {})
            .unwrap();
        let scan_sql = "SELECT body FROM padding WHERE id=5000";
        std::thread::scope(|scope| {
            let scan = (kind == 1).then(|| {
                searching.store(true, Ordering::SeqCst);
                scope.spawn(|| read.execute(scan_sql, &params! {}))
            });
            if let Some(scan) = &scan {
                while reads.load(Ordering::SeqCst) == 0 && !scan.is_finished() {
                    std::thread::yield_now();
                }
                assert!(!scan.is_finished(), "scan missed its storage gate");
            }
            searching.store(false, Ordering::SeqCst);
            let failed_before = failed.load(Ordering::SeqCst);
            fail_now.store(kind == 0, Ordering::SeqCst);
            fail_kind.store(kind, Ordering::SeqCst);
            let purge = if kind == 0 {
                "PURGE FROM padding WHERE id = 1"
            } else {
                "PURGE FROM t"
            };
            let error = db.execute(purge, &params! {}).unwrap_err();
            assert!(matches!(error, Error::StoreIo { .. }));
            fail_kind.store(0, Ordering::SeqCst);
            fail_now.store(false, Ordering::SeqCst);
            assert!(kind != 0 || failed.load(Ordering::SeqCst) > failed_before);
            if let Some(scan) = scan {
                support::say("failed");
                let error = scan.join().unwrap().unwrap_err();
                assert!(matches!(error, Error::StoreIo { .. }));
            }
        });
        for refused in [
            db.execute(support::SCAN, &params! {}).map(drop),
            db.execute("SHOW MEMORY_LIMIT", &params! {}).map(drop),
            db.table_names().map(drop),
            db.declaration("t").map(drop),
            db.committed_watermark().map(drop),
            tx.execute(support::SCAN, &params! {}).map(drop),
            read.execute(support::SCAN, &params! {}).map(drop),
            read.table_names().map(drop),
            read.declaration("t").map(drop),
            live.fetch(None).map(drop),
            ended.fetch(None).map(drop),
        ] {
            let closed = matches!(refused, Err(Error::StoreIo { .. }));
            assert_eq!(refused.is_ok(), kind == 0, "kind {kind}: {refused:?}");
            assert!(kind == 0 || closed, "kind {kind} is refused as StoreIo");
        }
        assert!(db.execute("SELECT FROM", &params! {}).is_err());
        drop((tx, live, ended, read));
        let _ = db.close();
        drop(db);
        let db = support::open(&folder);
        let count = support::int(&db, "SELECT COUNT(*) FROM t");
        assert!([0, rows].contains(&count));
        db.verify().unwrap();
        assert_eq!(support::int(&db, padding), PADDING_ROWS);
        if count == 0 {
            support::insert_together(&db, 1..=rows);
        }
        support::insert(&db, rows + 1..=rows + 1);
        support::run(&db, &format!("DELETE FROM t WHERE id = {}", rows + 1));
        support::assert_rows_are(&db, 1..=rows);
        support::close(db);
    }
}

/// Asserts that the scan and the count of the table give rows 1 to `rows`, or are refused
/// because the store could not be read.
fn answers_with_rows_or_store_io(db: &Database, rows: i64, when: &str) {
    let wanted: Vec<Vec<Value>> = (1..=rows).map(support::row).collect();
    match db.run(support::SCAN, &params! {}) {
        Ok(result) => {
            support::assert_same_rows(result.rows(), &wanted, &format!("the rows {when}"))
        }
        Err(Error::StoreIo { .. }) => {}
        Err(other) => panic!("a scan {when} was refused as {other:?}"),
    }
    match db.run("SELECT COUNT(*) FROM t", &params! {}) {
        Ok(result) => assert_eq!(result.rows(), [[Value::Int64(rows)]], "the count {when}"),
        Err(Error::StoreIo { .. }) => {}
        Err(other) => panic!("a count {when} was refused as {other:?}"),
    }
}

// ---- vectors ----

/// The numbers in a vector of the two tests below.
const DIMENSION: usize = 16;

/// Each row of `docs` has an edge from one node to its own, committed with the row.
fn edge(on: &impl Run, n: i64) {
    let sql = "INSERT INTO edges VALUES ($n, $a, $b, 'E')";
    let (a, b) = (support::context(0), support::context(n as u128));
    on.run(sql, &params! { "n" => n, "a" => a, "b" => b })
        .unwrap();
}

/// A closed store with sealed and pending rows, half of each under `c0` and half under `c1`.
fn store_with_vectors(name: &str, sealed: i64, pending: std::ops::RangeInclusive<i64>) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    support::run(
        &db,
        "CREATE TABLE edges (id INT PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
    );
    for numbers in [1..=sealed, pending] {
        let tx = db.transaction().expect("begin");
        numbers.clone().for_each(|n| edge(&tx, n));
        support::insert_docs(&tx, numbers, DIMENSION, |n| format!("c{}", n % 2));
        tx.commit().expect("commit");
        // The first batch is sealed before the rest arrive.
        if support::int(&db, "SELECT COUNT(*) FROM docs") == sealed {
            assert_eq!(support::seal_all(&db), sealed as u64);
        }
    }
    support::close(db);
    folder
}

/// Asserts that every search of `docs` returns exactly what its rows call for: everything and
/// each context, ten rows and more rows than there are, for three vectors. Returns the rows.
fn searches_agree(db: &Database, state: &str) -> Vec<Vec<Value>> {
    let stored = support::stored(db, "SELECT id, ctx, tag, v FROM docs ORDER BY id");
    let queries = [
        support::vector(98_001, DIMENSION),
        support::vector(98_002, DIMENSION),
        support::vector(150, DIMENSION),
    ];
    for query in &queries {
        for ctx in [None, Some("c0"), Some("c1"), Some("new"), Some("newer")] {
            let filter = ctx.map(|ctx| format!("ctx = '{ctx}'"));
            for k in [10, 1000] {
                let passes = |row: &[Value]| ctx.is_none_or(|ctx| row[1] == support::text(ctx));
                let wanted = stored.nearest(passes, query, k);
                let sql = support::nearest_sql("docs", "v", filter.as_deref(), k);
                support::assert_nearest(db, &sql, query, &wanted, state);
            }
        }
    }
    stored.rows().to_vec()
}

#[test]
fn a_kill_at_any_storage_call_of_a_seal_leaves_every_vector_found() {
    if let Some(role) = support::role() {
        return one_seal(role);
    }
    let test = "a_kill_at_any_storage_call_of_a_seal_leaves_every_vector_found";
    // Learn and replay every cut from identical closed bytes: even a verification open can
    // change the physical metadata that the next sealing cycle has to maintain.
    let store = store_with_vectors("kill-seal-source", 40, 101..=160);
    let rows = searches_agree(&support::open(&store), "before the cycle");
    assert_eq!(rows.len(), 100);
    let folder = support::folder("kill-seal-count");
    fs::copy(store.join("data"), folder.join("data"))
        .expect("copy the closed store to count calls");
    let (begins, calls, lines) = learned(test, "sealer", &folder, Some("sealing"));
    assert!(
        lines.iter().any(|line| line == "sealed 60 0"),
        "one cycle was to seal the 60 pending vectors: {lines:?}"
    );
    assert!(
        calls > begins,
        "a cycle that sealed 60 vectors made no write and no sync"
    );

    for_each_stop(
        test,
        "sealer",
        begins + 1..=calls,
        store,
        |k, folder, before| {
            let returned = before.iter().any(|line| line.starts_with("sealed "));

            // The store opens with nothing to mend, every row is there, and every search finds
            // exactly the vectors of those rows, whether the seal's commit is there or not.
            let db = support::open(folder);
            let state = format!("killed at storage call {k} of a seal");
            db.verify().expect("every vector record has its row");
            assert!(
                searches_agree(&db, &state) == rows,
                "{state}: the rows of docs"
            );
            // The next cycles finish the sealing: all 60 vectors when the killed commit is not
            // there, none when it is. A cycle that had returned is there.
            let sealed = support::seal_all(&db);
            db.verify().expect("every vector record has its row");
            assert!(
                sealed == 0 || sealed == 60 && !returned,
                "{state}: the next cycles sealed {sealed} vectors; the killed cycle had {}returned",
                if returned { "" } else { "not " }
            );
            let report = db.run_maintenance_cycle().expect("one more cycle").vector;
            assert_eq!(
                (
                    count(report.maintenance_vectors_done),
                    count(report.maintenance_vectors_remaining)
                ),
                (0, 0),
                "{state}: a cycle after the sealing was finished"
            );
            assert!(
                searches_agree(&db, &state) == rows,
                "{state}: the rows after sealing"
            );
        },
    );
}

/// Runs one maintenance cycle. Announces `sealing <n>` with the storage calls made so far when
/// the cycle is about to begin, `sealed <done> <remaining>` when it has returned, and
/// `calls <total>` once the store is closed.
fn one_seal(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    support::say(format!("sealing {}", calls.load(Ordering::SeqCst)));
    let report = db.run_maintenance_cycle().expect("a cycle").vector;
    support::say(format!(
        "sealed {} {}",
        count(report.maintenance_vectors_done),
        count(report.maintenance_vectors_remaining)
    ));
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together() {
    if let Some(role) = support::role() {
        if role.name == "expiry" {
            let (db, calls) = open_stopping(&role);
            support::say(format!("committing {}", calls.load(Ordering::SeqCst)));
            assert_eq!(db.run_maintenance_cycle().unwrap().expired_rows, Some(20));
            support::say("committed");
            support::close(db);
            return support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
        }
        return one_commit_of_vectors(role);
    }
    let test = "a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together";
    // The rows before the commit, from a store nothing was done to, and the rows after it,
    // from a run of the commit to its end, which also tells which storage calls are its own.
    let untouched = store_with_vectors("kill-vectors-before", 40, 101..=160);
    let before = searches_agree(&support::open(&untouched), "before the commit");
    let folder = store_with_vectors("kill-vectors-count", 40, 101..=160);
    let (begins, calls, _) = learned(test, "writer", &folder, Some("committing"));
    assert!(
        calls > begins,
        "a commit of thirty vectors made no write and no sync"
    );
    let after = searches_agree(&support::open(&folder), "after the commit");
    assert_eq!((before.len(), after.len()), (100, 128));

    for_each_stop(
        test,
        "writer",
        begins + 1..=calls,
        untouched,
        |k, folder, told| {
            let returned = told.iter().any(|line| line == "committed");

            // The rows are those of before the commit or those of after it, and the searches
            // answer from the vectors of exactly those rows: never rows of one and vectors of the
            // other. A commit that had returned is there.
            let db = support::open(folder);
            let state = format!("killed at storage call {k} of a commit of vectors");
            let rows = searches_agree(&db, &state);
            let walk = "SELECT COUNT(*) FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b) COLUMNS (b.id AS b_id))";
            let orphans = "SELECT COUNT(*) FROM edges WHERE id NOT IN (SELECT id FROM docs)";
            let reach = (support::int(&db, walk), support::int(&db, orphans));
            assert_eq!(reach, (rows.len() as i64, 0), "{state}: edges");
            assert!(
                rows == after || rows == before && !returned,
                "{state}: {} rows are there, of neither state or of the wrong one; the commit had \
             {}returned",
                rows.len(),
                if returned { "" } else { "not " }
            );
            // The store goes on from there: what waits is sealed, and the answers stay.
            support::seal_all(&db);
            db.verify().expect("every vector record has its row");
            assert!(
                searches_agree(&db, &state) == rows,
                "{state}: the rows after sealing"
            );
        },
    );
    let store = |name: &str| {
        let folder = store_with_vectors(name, 10, 150..=159);
        let db = support::open(&folder);
        support::insert_docs(&db, 300..=300, DIMENSION, |_| "c0".to_owned());
        support::run(&db, "ALTER TABLE docs SET RETAIN 10 SECONDS");
        support::run(&db, "ALTER TABLE docs ADD COLUMN expires TIMESTAMP EXPIRES");
        support::run(&db, "ALTER TABLE docs ADD COLUMN body TEXT");
        db.execute(
            "UPDATE docs SET expires = $due, body = $body WHERE id < 300",
            &params! { "due" => Value::Timestamp(-1), "body" => support::letters(7, 4000) },
        )
        .unwrap();
        db.execute(
            "UPDATE docs SET expires = $pin WHERE id = 300",
            &params! { "pin" => Value::Timestamp(i64::MAX) },
        )
        .unwrap();
        support::run(&db, "CREATE INDEX by_body ON docs (body)");
        support::run(&db, "CREATE INDEX by_context ON docs (ctx)");
        support::run(&db, "ALTER TABLE docs SET MAINTAIN COUNT (ctx)");
        support::close(db);
        folder
    };
    let folder = store("kill-expiry-count");
    let (begins, calls, _) = learned(test, "expiry", &folder, Some("committing"));
    assert!(calls > begins, "expiry made no durable commit");
    let body = vec![Value::Text(support::letters(7, 4000))];
    for_each_stop(
        test,
        "expiry",
        begins + 1..=calls,
        store("kill-expiry-source"),
        |k, folder, told| {
            let db = support::open(folder);
            let rows = searches_agree(&db, &format!("expiry kill {k}"));
            let returned = told.iter().any(|line| line == "committed");
            assert!(rows.len() == 1 || rows.len() == 21 && !returned);
            assert_eq!(rows.last().unwrap()[0], Value::Int64(300));
            db.verify()
                .expect("expiry keeps rows, counts, indexes, large values and vectors together");
            let bodies = support::rows(&db, "SELECT body FROM docs WHERE body IS NOT NULL");
            assert_eq!(bodies.len(), rows.len() - 1);
            assert!(bodies.iter().all(|row| row == &body));
        },
    );
}

/// In one transaction adds rows 301 to 330, a third of them under a context that has rows and
/// the rest under two that have none; changes the vector of a sealed row and of a pending row;
/// deletes one of each; and moves a sealed row to one of the new contexts. Announces
/// `committing <n>` with the storage calls made so far when the commit is about to begin,
/// `committed` when it has returned, and `calls <total>` once the store is closed.
fn one_commit_of_vectors(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    let tx = db.transaction().expect("begin");
    (301..=330).for_each(|n| edge(&tx, n));
    support::insert_docs(&tx, 301..=330, DIMENSION, |n| {
        ["c0", "new", "newer"][(n % 3) as usize].to_owned()
    });
    for id in [5_i64, 150] {
        tx.execute(
            "UPDATE docs SET v = $v WHERE id = $id",
            &params! { "id" => id, "v" => support::vector(98_100 + id, DIMENSION) },
        )
        .expect("change a vector");
    }
    support::run(&tx, "DELETE FROM docs WHERE id = 6");
    support::run(&tx, "DELETE FROM docs WHERE id = 151");
    support::run(&tx, "DELETE FROM edges WHERE id IN (6, 151)");
    support::run(&tx, "UPDATE docs SET ctx = 'new' WHERE id = 7");
    support::say(format!("committing {}", calls.load(Ordering::SeqCst)));
    tx.commit().expect("commit");
    support::say("committed");
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

// ---- shared by the tests of this file ----

// ---- a commit of rows that a table's rules tie together ----

/// The body of a child: 3,400 bytes, of which the first 3,000 are the same for every child, so
/// that in the index over `body` the children differ only past what an entry keeps of a value.
fn body(n: i64) -> String {
    format!("{}{}", "m".repeat(3000), support::letters(n, 400))
}

/// The children before the commit of the test below and after it, as (id, parent, body).
fn children(committed: bool) -> Vec<(i64, i64, String)> {
    match committed {
        false => (1..=6).map(|n| (n, n % 2 + 1, body(n))).collect(),
        true => {
            let mut children: Vec<_> = (3..=6).map(|n| (n, n % 2 + 1, body(n))).collect();
            children[0].2 = body(103);
            children[1].1 = 3;
            children.extend((7..=18).map(|n| (n, n % 3 + 1, body(n))));
            children
        }
    }
}

/// A closed store with two parents and six children. A child references its parent, the table
/// keeps a count of the children of each parent, and `body` is indexed.
fn store_with_children(name: &str) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    for sql in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
        "CREATE TABLE children (id INTEGER PRIMARY KEY, \
         parent_id INTEGER NOT NULL REFERENCES parents(id), body TEXT NOT NULL)",
        "CREATE INDEX children_parent ON children (parent_id)",
        "CREATE INDEX children_body ON children (body)",
        "ALTER TABLE children SET MAINTAIN COUNT (parent_id)",
        "INSERT INTO parents (id) VALUES (1), (2)",
    ] {
        support::run(&db, sql);
    }
    let tx = db.transaction().expect("begin");
    for child in children(false) {
        insert_child(&tx, child).expect("insert a child");
    }
    tx.commit().expect("commit");
    support::close(db);
    folder
}

fn insert_child(
    on: &impl Run,
    (id, parent, body): (i64, i64, String),
) -> contextdb::Result<contextdb::QueryResult> {
    on.run(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent, $body)",
        &params! { "id" => id, "parent" => parent, "body" => body },
    )
}

/// Asserts that the store holds exactly these children and that everything kept beside the rows
/// says the same as the rows: the parent of every child is there, the kept count of each parent
/// is the number of its children and reads no row, and every child is found by its body through
/// the index, alone and in the order of the bodies.
fn assert_children_are(db: &Database, wanted: &[(i64, i64, String)], when: &str) {
    let rows: Vec<Vec<Value>> = wanted
        .iter()
        .map(|(id, parent, body)| {
            vec![
                Value::Int64(*id),
                Value::Int64(*parent),
                support::text(body),
            ]
        })
        .collect();
    let found = support::rows(db, "SELECT id, parent_id, body FROM children ORDER BY id");
    support::assert_same_rows(&found, &rows, &format!("{when}: the children"));

    let parents = support::rows(db, "SELECT id FROM parents ORDER BY id");
    for parent in 1..=3_i64 {
        let children = wanted.iter().filter(|child| child.1 == parent).count();
        assert!(
            children == 0 || parents.contains(&vec![Value::Int64(parent)]),
            "{when}: parent {parent} is gone and {children} children name it"
        );
        let count = support::run(
            db,
            &format!("SELECT COUNT(*) FROM children WHERE parent_id = {parent}"),
        );
        assert_eq!(
            count.rows(),
            [[Value::Int64(children as i64)]],
            "{when}: the kept count of parent {parent}"
        );
        assert_eq!(count.rows_examined(), 0, "{when}: rows read by the count");
    }
    for (id, _, body) in wanted {
        let by_body = db
            .execute(
                "SELECT id FROM children WHERE body = $body",
                &params! { "body" => body.as_str() },
            )
            .expect("look a child up by its body");
        assert_eq!(
            by_body.rows(),
            [[Value::Int64(*id)]],
            "{when}: child {id} by its body"
        );
    }
    let mut by_body: Vec<&(i64, i64, String)> = wanted.iter().collect();
    by_body.sort_by(|a, b| a.2.cmp(&b.2));
    let in_order: Vec<[Value; 1]> = by_body
        .iter()
        .map(|child| [Value::Int64(child.0)])
        .collect();
    let found = support::rows(db, "SELECT id FROM children ORDER BY body");
    support::assert_same_rows(&found, &in_order, &format!("{when}: the children by body"));
}

#[test]
fn a_kill_inside_a_commit_keeps_references_counts_and_long_entries_with_their_rows() {
    if let Some(role) = support::role() {
        return one_commit_of_children(role);
    }
    let test = "a_kill_inside_a_commit_keeps_references_counts_and_long_entries_with_their_rows";
    // Run once to the end to learn which storage calls belong to the commit.
    let folder = store_with_children("kill-children-count");
    let (begins, calls, _) = learned(test, "writer", &folder, Some("committing"));
    assert!(calls > begins, "the commit made no write and no sync");
    assert_children_are(
        &support::open(&folder),
        &children(true),
        "after the whole commit",
    );

    let store = store_with_children("kill-children-source");
    for_each_stop(
        test,
        "writer",
        begins + 1..=calls,
        store,
        |k, folder, before| {
            let returned = before.iter().any(|line| line == "committed");

            // The store holds what it held before the commit, or everything the commit brought.
            let db = support::open(folder);
            let when = format!("killed at storage call {k}");
            let committed = match support::int(&db, "SELECT COUNT(*) FROM children") {
                6 if !returned => false,
                16 => true,
                rows => panic!(
                    "{when}: {rows} children are there; the commit had {}returned",
                    if returned { "" } else { "not " }
                ),
            };
            assert_children_are(&db, &children(committed), &when);
            let parents = support::int(&db, "SELECT COUNT(*) FROM parents");
            assert_eq!(
                parents,
                if committed { 3 } else { 2 },
                "{when}: the parents"
            );

            // The store goes on from that state, and its rules with it.
            let mut wanted = children(committed);
            wanted.push((50, 1, body(50)));
            insert_child(&db, (50, 1, body(50))).expect("insert a child after reopening");
            assert_children_are(&db, &wanted, &format!("{when}, and one more child"));
            assert!(matches!(
                insert_child(&db, (51, 404, body(51))),
                Err(Error::ForeignKeyViolation { .. })
            ));
        },
    );
}

/// In one transaction adds a parent and twelve children, removes two children, gives one another
/// body and one another parent. Announces `committing <n>` with the storage calls made so far
/// when the commit is about to begin, `committed` when it has returned, and `calls <total>` once
/// the store is closed.
fn one_commit_of_children(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO parents (id) VALUES (3)");
    for child in children(true).split_off(4) {
        insert_child(&tx, child).expect("insert a child");
    }
    support::run(&tx, "DELETE FROM children WHERE id <= 2");
    tx.execute(
        "UPDATE children SET body = $body WHERE id = 3",
        &params! { "body" => body(103) },
    )
    .expect("change a body");
    support::run(&tx, "UPDATE children SET parent_id = 3 WHERE id = 4");
    support::say(format!("committing {}", calls.load(Ordering::SeqCst)));
    tx.commit().expect("commit");
    support::say("committed");
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_kill_in_a_commit_with_an_event_route_keeps_rows_and_actions_together() {
    if let Some(role) = support::role() {
        // Rows 11 to 310 into an empty table: 300 rows, more than the 256 a cycle delivers.
        return one_large_commit(role, 310);
    }
    let test = "a_kill_in_a_commit_with_an_event_route_keeps_rows_and_actions_together";
    let routed = |name| {
        let folder = store_with_rows(name, 0);
        let db = support::open(&folder);
        let wiring = "CREATE SINK out TYPE callback; CREATE EVENT TYPE added WHEN INSERT ON t; ";
        for sql in wiring.split_terminator("; ") {
            support::run(&db, sql);
        }
        support::run(&db, "CREATE ROUTE notify EVENT added TO out");
        support::close(db);
        folder
    };
    let counted = routed("kill-events-count");
    let (begins, calls, _) = learned(test, "writer", &counted, Some("committing"));
    assert!(calls > begins, "the commit made no write and no sync");
    for_each_stop(
        test,
        "writer",
        begins + 1..=calls,
        routed("kill-events-source"),
        |k, folder, _| {
            let db = support::open(folder);
            let rows = support::int(&db, "SELECT COUNT(*) FROM t");
            let heard = Arc::new(AtomicU64::new(0));
            let sink = heard.clone();
            db.register_sink("out", None, move |_| {
                sink.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
            let queued = db.sink_metrics("out").unwrap().queued as i64;
            let delivered = count(db.run_maintenance_cycle().unwrap().delivered) as i64;
            let all = (queued, delivered, heard.load(Ordering::SeqCst) as i64);
            assert!(rows == 0 || rows == 300, "killed at call {k}: {rows} rows");
            let first = rows.min(256); // a cycle delivers at most 256
            assert_eq!(all, (rows, first, first), "killed at call {k}: one per row");
        },
    );
}

/// A closed store with the table declared and rows 1 to `rows` committed.
fn store_with_rows(name: &str, rows: i64) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=rows);
    support::close(db);
    folder
}

/// Opens the child's store through a door that stops before the call the role names, and gives
/// the store with the door's count of calls.
fn open_stopping(role: &support::Role) -> (Database, Arc<AtomicU64>) {
    let (storage, calls) = faulting_before_call(role.number("stop_at"), true, false);
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    (
        support::open_with(&role.folder, options).expect("open"),
        calls,
    )
}

// ---- a kill inside a build ----

/// A closed store whose `docs` has `rows` pending vectors in a column that is at its size from
/// a third of them on: the cycles seal them and build one IVF of `round(sqrt(rows))`
/// groups, 17 for 300 rows and 3 for 12.
fn store_to_build(name: &str, rows: i64) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, &format!("INDEXING_THRESHOLD {}", rows / 3));
    support::load_docs(&db, 1..=rows, "a", |n| support::gathered(n, DIMENSION));
    support::close(db);
    folder
}

/// Asserts that an exact search returns every one of the `rows` rows once, and that INDEXED
/// answers with true distances or is refused by its own kind.
fn every_vector_is_found(db: &Database, rows: usize, state: &str) {
    let stored = support::stored(db, support::DOCS);
    assert_eq!(stored.rows().len(), rows, "{state}: the rows of docs");
    for seed in [98_101, 98_102] {
        let query = support::gathered(seed, DIMENSION);
        let wanted = stored.nearest(|_| true, &query, 1000);
        let sql = support::by_mode(None, "EXACT", 1000);
        support::assert_nearest(db, &sql, &query, &wanted, state);
        match support::search(db, &support::by_mode(None, "INDEXED", 10), &query) {
            Ok(result) => {
                support::assert_sound(&result, &stored, |_| true, &query, 10, state);
            }
            Err(Error::VectorIndexedRouteUnavailable { .. }) => {}
            Err(other) => panic!("{state}: INDEXED gave {other:?}"),
        }
    }
}

fn groups(db: &Database) -> Value {
    support::field(&support::the_partition(db), "lists").clone()
}

#[test]
fn a_kill_in_any_step_of_a_build_leaves_every_vector_found() {
    if let Some(role) = support::role() {
        return one_build(role);
    }
    let test = "a_kill_in_any_step_of_a_build_leaves_every_vector_found";
    // Twelve rows: a build finds centroids only for more than one group, which takes
    // `round(sqrt(rows))` of 2 or more, and the INDEXED search asks for ten rows.
    const ROWS: i64 = 12;
    // Run once to the end to learn the storage calls of each cycle of the build. Every call of
    // one commit is killed by the first test of this file; here each cycle's commits are
    // killed at their first call, their middle one and their last, three kills a cycle.
    let folder = store_to_build("kill-build-count", ROWS);
    let (begins, _, lines) = learned(test, "builder", &folder, Some("building"));
    assert_eq!(groups(&support::open(&folder)), Value::Int64(3));
    let mut ends = vec![begins];
    ends.extend(
        lines
            .iter()
            .filter_map(|line| line.strip_prefix("called ")?.parse::<u64>().ok()),
    );
    let stops: BTreeSet<u64> = (ends.windows(2).filter(|cycle| cycle[1] > cycle[0]))
        .flat_map(|cycle| [cycle[0] + 1, (cycle[0] + 1 + cycle[1]) / 2, cycle[1]])
        .collect();

    let store = store_to_build("kill-build-source", ROWS);
    for_each_stop(test, "builder", stops, store, |k, folder, _| {
        // The store opens with nothing to mend and every vector is found, whichever step's
        // commit the kill fell in; the next cycles finish the build.
        let db = support::open(folder);
        let state = format!("killed at storage call {k} of a build");
        every_vector_is_found(&db, ROWS as usize, &state);
        support::maintenance(&db);
        assert_eq!(groups(&db), Value::Int64(3), "{state}");
        every_vector_is_found(&db, ROWS as usize, &format!("{state}, then built"));
    });
}

#[test]
fn a_killed_build_goes_on_from_its_record() {
    if let Some(role) = support::role() {
        return one_build(role);
    }
    let test = "a_killed_build_goes_on_from_its_record";
    let folder = store_to_build("killed-build-count", 300);
    let (begins, calls, lines) = learned(test, "builder", &folder, Some("building"));
    let whole = support::last_number(&lines, "cycles").expect("the builder counts its cycles");

    // Killed at the middle storage call of the build's cycles.
    let folder = store_to_build("killed-build-goes-on", 300);
    let stop_at = begins + (calls - begins) / 2;
    let mut builder = Child::start(test, &format!("builder stop_at={stop_at}"), &folder);
    let told = builder.until("stopped");
    builder.kill();
    let returned = support::last_number(&told, "cycles").unwrap_or(0);
    assert!(
        0 < returned && returned + 1 < whole,
        "{returned} of {whole} cycles had returned at storage call {stop_at}"
    );
    // The cycles after reopening are those the build had left: the killed cycle's step is
    // there or it is not, and no step that was committed is worked out again.
    let db = support::open(&folder);
    // Finish recovery without consuming a committed vector-build step.
    let position = support::watermark(&db);
    loop {
        let report = db.run_maintenance_cycle().unwrap();
        assert_eq!(report.vector.maintenance_vectors_done, Some(0));
        assert_eq!(support::watermark(&db), position);
        if count(report.recovery_pages_remaining) == 0 {
            break;
        }
    }
    let (ran, _) = support::maintenance(&db);
    assert!(
        ran == whole - returned || ran == whole - returned - 1,
        "{returned} of {whole} cycles had returned; the build then took {ran} more"
    );
    assert_eq!(groups(&db), Value::Int64(17));
    every_vector_is_found(&db, 300, "built after the kill");
}

/// Runs maintenance cycles until nothing is left. Announces `building <n>` with the storage
/// calls made so far before the first, `cycles <n>` and `called <n>` with the calls made so far
/// as each returns, and `calls <total>` once the store is closed.
fn one_build(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    support::say(format!("building {}", calls.load(Ordering::SeqCst)));
    for cycles in 1_u64.. {
        let report = support::cycle(&db);
        support::say(format!("cycles {cycles}"));
        support::say(format!("called {}", calls.load(Ordering::SeqCst)));
        if count(report.maintenance_vectors_remaining) == 0
            && count(report.remaining_partitions) <= count(report.stalled_partitions)
        {
            break;
        }
    }
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

// A killed open reads the allocator record/log and the last commit, not every stored page: at
// most 4 pages more at 20,000 rows than at 1,000 (section 7, C1). The close of that open
// finishes the rebuild, and a read that fails there is what the close returns (C2).
#[test]
fn a_killed_open_reads_the_log_and_last_commit_pages() {
    if let Some(role) = support::role() {
        let db = support::open(&role.folder);
        support::insert(&db, 0..=0);
        support::wait_to_be_killed("committed");
    }
    let (mut reads, mut readers) = (Vec::new(), Vec::new());
    for rows in [1_000, 20_000] {
        let folder = store_with_rows(&format!("killed-open-{rows}"), rows);
        let mut child = Child::start(
            "a_killed_open_reads_the_log_and_last_commit_pages",
            "writer",
            &folder,
        );
        child.until("committed");
        child.kill();
        let (seen, read) = support::counting_bytes_read();
        let size = || fs::metadata(folder.join("data")).unwrap().len();
        let (before, mut as_reader) = (size(), support::reader_options(None));
        as_reader.storage = Some(seen);
        drop(ReadSession::open_with_options(&folder, as_reader).unwrap());
        readers.push(read.load(Ordering::SeqCst).div_ceil(4096));
        assert_eq!(size(), before, "a read mends nothing");
        let bytes = Arc::new(AtomicU64::new(0));
        let armed = Arc::new(AtomicBool::new(false));
        let (counted, failing) = (bytes.clone(), armed.clone());
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(support::failing_storage(move |call| match call {
            Call::Read(..) if failing.load(Ordering::SeqCst) => {
                Err(io::Error::other("a rebuild read this test makes fail"))
            }
            Call::Read(_, out) => {
                counted.fetch_add(out.len() as u64, Ordering::SeqCst);
                Ok(())
            }
            _ => Ok(()),
        }));
        let db = support::open_with(&folder, options).unwrap();
        reads.push(bytes.load(Ordering::SeqCst).div_ceil(4096));
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), rows + 1);
        armed.store(true, Ordering::SeqCst);
        let closed = db.close();
        assert!(
            matches!(&closed, Err(Error::StoreIo { reason, .. }) if reason.contains("rebuild read")),
            "the close finishes the rebuild, and its failed read is what the last close returns: {closed:?}"
        );
    }
    // The count follows the depth of the trees on the open's path, not the rows (section 7).
    for (opens, what) in [(&reads, "writer"), (&readers, "read session")] {
        let more = opens[1].saturating_sub(opens[0]);
        assert!(
            more <= 4,
            "a {what}'s open after a kill reads at most 4 pages more at 20,000 rows than at 1,000: {opens:?}"
        );
    }
}

/// A call that fails in an INSERT leaves its row whole or absent, and its handle serves the disk.
#[test]
fn a_call_that_fails_anywhere_in_an_insert_leaves_the_row_whole_or_absent() {
    let source = support::folder("read-fault-source");
    let create = "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)";
    support::run(&support::open(&source), create);
    let trial = |stop_at: u64| {
        let folder = support::folder(&format!("read-fault-{stop_at}"));
        fs::copy(source.join("data"), folder.join("data")).unwrap();
        let (storage, calls) = support::faulting_before_call(stop_at, false, true);
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        let (count, mut same) = ("SELECT COUNT(*) FROM t", None);
        let done = support::open_with(&folder, options).and_then(|db| {
            let done = db.execute("INSERT INTO t VALUES (1, 'x')", &params! {});
            same = db.run(count, &params! {}).ok();
            done
        });
        let db = support::open(&folder);
        let rows = support::int(&db, count);
        let whole = rows == 1 || (rows == 0 && done.is_err());
        assert!(whole, "call {stop_at}: {rows} rows, {done:?}");
        if let Some(same) = same {
            assert_eq!(same.rows(), [[Value::Int64(rows)]], "call {stop_at}");
        }
        db.verify().unwrap();
        calls.load(Ordering::SeqCst)
    };
    (1..=trial(0)).for_each(|n| _ = trial(n));
}
#[test]
fn a_cut_sink_call_is_uncertain_and_retried() {
    if let Some(role) = support::role() {
        let db = support::open(&role.folder);
        db.register_sink("outgoing", None, |e| match e.action_id {
            2 => support::wait_to_be_killed("calling 2"),
            _ => Ok(()),
        })
        .unwrap();
        return assert!(db.run_maintenance_cycle().is_ok());
    }
    let folder = support::folder("kill-in-sink-call");
    support::close(support::delivery_queue(&folder, 8, 2));
    let mut child = Child::start("a_cut_sink_call_is_uncertain_and_retried", "sink", &folder);
    child.until("calling 2");
    child.kill();
    // A reader opens at once on what the killed writer last saved: the cut call is queued and
    // uncertain.
    let reader = contextdb::ReadSession::open(&folder, contextdb::ReadLimits::default()).unwrap();
    let status = reader.event_bus_status().unwrap();
    let saved = &status.sinks[0].metrics;
    assert_eq!(
        (saved.queued, saved.uncertain),
        (1, 1),
        "a reader sees the cut call uncertain"
    );
    drop((status, reader));
    let db = support::open(&folder);
    db.register_sink("outgoing", None, |e| {
        assert_eq!(e.action_id, 2, "only the cut one comes again");
        Ok(())
    })
    .unwrap();
    assert_eq!(db.run_maintenance_cycle().unwrap().delivered, Some(1));
    let uncertain = db.sink_metrics("outgoing").unwrap().uncertain;
    assert_eq!(uncertain, 1, "a call begun and never finished is uncertain");
}

// ---- a kill inside the tidy-up ----

/// The body of row `id` of the tidy-up's store: a large value for rows 0 and -1, one page for
/// the rest.
fn tidied(id: i64) -> String {
    support::letters(id + 1000, if id <= 0 { 60_000 } else { 2000 })
}

/// A closed store at its disk limit whose tidy-up moves pages, releases them, and has a whole
/// step refused for room at commit, so it writes its retry into `space`: rows of one page
/// around two large values, packed, with the low large value deleted.
fn store_to_tidy(name: &str) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");
    for id in [-1].into_iter().chain(1..=40).chain([0]).chain(41..=55) {
        let row = params! { "id" => id, "body" => tidied(id) };
        db.execute("INSERT INTO t VALUES ($id, $body)", &row)
            .unwrap();
    }
    while db.compact_now().unwrap().work_remaining != Some(false) {}
    support::run(&db, "DELETE FROM t WHERE id = -1");
    db.set_disk_limit(Some(support::data_file(&folder)))
        .unwrap();
    support::close(db);
    folder
}

/// Asks for tidy-up steps alone until the pass ends with no step refused for room, and closes
/// the store; the data file's length then. After a kill the first steps are the recovery of the
/// free pages it left unknown, so no maintenance cycle is needed before them.
fn tidied_to_the_end(db: Database, folder: &Path, state: &str) -> u64 {
    for step in 0.. {
        assert!(
            step < 64,
            "{state}: the tidy-up of a small store, its recovery first, ends"
        );
        let report = db.compact_now().expect("a step");
        if report.recovery_advanced == Some(true) {
            continue;
        }
        if report.compaction_disk_refused != Some(true) && report.work_remaining == Some(false) {
            support::close(db);
            return support::data_file(folder);
        }
    }
    unreachable!()
}

#[test]
fn a_kill_at_any_storage_call_of_the_tidy_up_keeps_every_row_and_ends_at_the_same_size() {
    if let Some(role) = support::role() {
        return tidying(role);
    }
    let test =
        "a_kill_at_any_storage_call_of_the_tidy_up_keeps_every_row_and_ends_at_the_same_size";
    let rows: Vec<Vec<Value>> = [0]
        .into_iter()
        .chain(1..=55)
        .map(|id| vec![Value::Int64(id), Value::Text(tidied(id))])
        .collect();
    let all = "SELECT id, body FROM t ORDER BY id";
    // Run once to the end: the cycles of a whole run, and where its tidy-up ends.
    let folder = store_to_tidy("kill-tidy-count");
    let (begins, calls, lines) = learned(test, "tidier", &folder, Some("tidying"));
    assert!(
        lines.iter().any(|line| line == "refused") && lines.iter().any(|line| line == "moved"),
        "the cycles were to have a step refused for room and one that moves pages: {lines:?}"
    );
    let whole = tidied_to_the_end(support::open(&folder), &folder, "uninterrupted");

    let store = store_to_tidy("kill-tidy-source");
    for_each_stop(test, "tidier", begins + 1..=calls, store, |k, folder, _| {
        // The store opens whole with nothing to mend, every row is there as written, and the
        // tidy-up goes on from where the kill left it to the length an unbroken run ends at.
        let state = format!("killed at storage call {k} of the tidy-up");
        let db = support::open(folder);
        db.verify().unwrap_or_else(|e| panic!("{state}: {e:?}"));
        assert!(support::rows(&db, all) == rows, "{state}: the rows");
        let ended = tidied_to_the_end(db, folder, &state);
        assert_eq!(
            ended, whole,
            "{state}: the tidy-up ends where an unbroken one does"
        );
        let db = support::open(folder);
        assert!(
            support::rows(&db, all) == rows,
            "{state}: the rows after the tidy-up"
        );
        support::close(db);
    });
}

/// Runs three cycles of the tidy-up. Announces `tidying <n>` with the storage calls made so far
/// when they begin, `refused` or `moved` after each cycle that had its step refused for room or
/// moved pages, and `calls <total>` once the store is closed.
fn tidying(role: support::Role) {
    let (db, calls) = open_stopping(&role);
    support::say(format!("tidying {}", calls.load(Ordering::SeqCst)));
    for _ in 0..3 {
        let report = db.run_maintenance_cycle().expect("a cycle");
        if report.compaction_disk_refused == Some(true) {
            support::say("refused");
        }
        if count(report.compacted_pages) > 0 {
            support::say("moved");
        }
    }
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}
