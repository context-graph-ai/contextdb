//! Kill and reopen: a writer killed at any storage call of a commit leaves the whole commit or
//! none of it, and the store opens again with nothing to mend by hand. That holds for a commit
//! of rows with vectors and for the commit of a maintenance cycle that seals vectors: after
//! either, every search finds exactly the vectors of the rows that are there. Damage in the
//! file is refused as damage, by name.
//!
//! A kill lands at a point the child names. The child's storage door counts the writes and
//! syncs of its data file; before call number `stop_at` it announces `stopped` and waits there,
//! and the parent kills it.

#[path = "../support/mod.rs"]
mod support;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use contextdb::{
    Database, DatabaseOpenOptions, Error, ReadSession, ReadSessionOptions, Value, WrapStorage,
    params,
};

use support::{Call, Child, Run};

#[test]
fn a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing() {
    if let Some(role) = support::role() {
        return five_commits(role);
    }
    let test = "a_kill_at_any_storage_call_of_a_commit_leaves_all_or_nothing";
    // Run once to the end to learn how many storage calls five commits make.
    let lines = Child::start(test, "writer stop_at=0", &store_with_rows("kill-count", 0)).end();
    let calls = support::last_number(&lines, "calls").expect("the writer announces its calls");
    assert!(calls >= 5, "five commits made {calls} writes and syncs");

    for k in 1..=calls {
        let folder = store_with_rows(&format!("kill-at-{k}"), 0);
        let mut writer = Child::start(test, &format!("writer stop_at={k}"), &folder);
        let before = writer.until("stopped");
        writer.kill();
        // Commits that had returned before the kill.
        let committed = support::last_number(&before, "committed").unwrap_or(0) as i64;

        let db = support::open(&folder); // it must open: there is nothing to mend by hand
        let rows = support::int(&db, "SELECT COUNT(*) FROM t");
        assert!(
            rows == committed || rows == committed + 1,
            "killed at storage call {k}: {rows} rows are there, {committed} commits had returned"
        );
        support::assert_rows_are(&db, 1..=rows);
        // The store goes on from the state it reopened at.
        support::insert(&db, rows + 1..=rows + 1);
        support::assert_rows_are(&db, 1..=rows + 1);
    }
}

/// Commits rows 1 to 5, one commit each, announcing `committed <n>` after each one returns, and
/// `calls <total>` once the store is closed.
fn five_commits(role: support::Role) {
    let (storage, calls) = stopping_before_call(role.number("stop_at"));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    let db = support::open_with(&role.folder, options).expect("open");
    for n in 1..=5 {
        support::insert(&db, n..=n);
        support::say(format!("committed {n}"));
    }
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_kill_inside_a_commit_of_many_rows_keeps_rows_index_and_count_together() {
    if let Some(role) = support::role() {
        return one_large_commit(role);
    }
    let test = "a_kill_inside_a_commit_of_many_rows_keeps_rows_index_and_count_together";
    // Run once to the end to learn which storage calls belong to the large commit.
    let lines = Child::start(
        test,
        "writer stop_at=0",
        &store_with_rows("kill-large-count", 10),
    )
    .end();
    let begins = support::last_number(&lines, "committing").expect("the writer announces it");
    let calls = support::last_number(&lines, "calls").expect("the writer announces its calls");
    assert!(
        calls > begins,
        "a commit of a thousand rows made no write and no sync"
    );

    for k in begins + 1..=calls {
        let folder = store_with_rows(&format!("kill-large-at-{k}"), 10);
        let mut writer = Child::start(test, &format!("writer stop_at={k}"), &folder);
        let before = writer.until("stopped");
        writer.kill();
        let returned = before.iter().any(|line| line == "committed");

        // The store holds the rows from before the commit or the rows from after it, and in
        // either case its rows, its index and its kept count say the same.
        let db = support::open(&folder);
        match support::int(&db, "SELECT COUNT(*) FROM t") {
            10 if !returned => support::assert_rows_are(&db, 1..=10),
            1005 => support::assert_rows_are(&db, 6..=1010),
            rows => panic!(
                "killed at storage call {k}: {rows} rows are there; the commit had {}returned",
                if returned { "" } else { "not " }
            ),
        }
    }
}

/// In one transaction removes rows 1 to 5 and adds rows 11 to 1010. Announces `committing <n>`
/// with the storage calls made so far when the commit is about to begin, `committed` when it
/// has returned, and `calls <total>` once the store is closed.
fn one_large_commit(role: support::Role) {
    let (storage, calls) = stopping_before_call(role.number("stop_at"));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    let db = support::open_with(&role.folder, options).expect("open");
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DELETE FROM t WHERE id <= 5");
    support::insert(&tx, 11..=1010);
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
    let refused = damage_each_page_of_a_scan(&wanted, |storage| {
        let mut options = ReadSessionOptions::default();
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
    let refused = damage_each_page_of_a_scan(&wanted, |storage| {
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
    support::assert_same_rows(
        &support::rows(&session, support::SCAN),
        wanted,
        "the undamaged scan",
    );
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
        let answer = open(storage).and_then(|session| session.run(support::SCAN, &params! {}));
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

/// Rows of padding in the test below, 800 bytes each: 32 MiB.
const PADDING_ROWS: i64 = 40_000;

#[test]
fn a_commit_on_disk_is_reported_and_served_though_the_read_after_it_fails() {
    let folder = support::folder("read-fails-after-commit");
    // The storage door counts syncs. Told a number, it fails every read made once that many
    // syncs have been counted: the reads that follow a commit's arrival on disk.
    let syncs = Arc::new(AtomicU64::new(0));
    let fail_after = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::failing_storage({
        let (syncs, fail_after, failed) = (syncs.clone(), fail_after.clone(), failed.clone());
        move |call| {
            match call {
                Call::Sync => {
                    syncs.fetch_add(1, Ordering::SeqCst);
                }
                Call::Read(..) => {
                    let after = fail_after.load(Ordering::SeqCst);
                    if after > 0 && syncs.load(Ordering::SeqCst) >= after {
                        failed.fetch_add(1, Ordering::SeqCst);
                        return Err(io::Error::other("a read this test makes fail"));
                    }
                }
                Call::Write(..) => {}
            }
            Ok(())
        }
    }));
    // Under a memory limit of 32 MiB the store's page cache is 8 MiB: a quarter of the limit.
    options.memory_limit = Some(32 << 20);
    let db = support::open_with(&folder, options).expect("open");
    support::declare(&db);
    // A second table four times the size of that 8 MiB page cache, in rows small enough to be
    // kept in the table's own pages. Reading all of it leaves the cache full of its pages, so
    // the pages a commit writes afterwards have to be read back from the file.
    support::run(
        &db,
        "CREATE TABLE padding (id INTEGER PRIMARY KEY, body TEXT)",
    );
    let body = "x".repeat(800);
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
        Err(Error::StoreIo { .. }) => {}
        other => panic!("a scan that must read the file got {other:?} while reads fail"),
    }
    assert!(
        failed.load(Ordering::SeqCst) > 0,
        "no read was made to fail after the commit"
    );
    fail_after.store(0, Ordering::SeqCst);
    answers_with_rows_or_store_io(&db, 2, "once reads work again");

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

/// A closed store with `docs`, its VECTOR column partitioned by context: rows 1 to 100 are in
/// blocks and rows 101 to 300 on pending lists, half of each under `c0` and half under `c1`.
fn store_with_vectors(name: &str) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    for numbers in [1..=100, 101..=300] {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, numbers, DIMENSION, |n| format!("c{}", n % 2));
        tx.commit().expect("commit");
        // The first hundred are sealed before the rest arrive.
        if support::int(&db, "SELECT COUNT(*) FROM docs") == 100 {
            assert_eq!(support::seal_all(&db), 100);
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
    // Run once to the end to learn which storage calls belong to the sealing commit.
    let folder = store_with_vectors("kill-seal-count");
    let rows = searches_agree(&support::open(&folder), "before the cycle");
    assert_eq!(rows.len(), 300);
    let lines = Child::start(test, "sealer stop_at=0", &folder).end();
    let begins = support::last_number(&lines, "sealing").expect("the sealer announces it");
    let calls = support::last_number(&lines, "calls").expect("the sealer announces its calls");
    assert!(
        lines.iter().any(|line| line == "sealed 200 0"),
        "one cycle was to seal the 200 pending vectors: {lines:?}"
    );
    assert!(
        calls > begins,
        "a cycle that sealed 200 vectors made no write and no sync"
    );

    for k in begins + 1..=calls {
        let folder = store_with_vectors(&format!("kill-seal-at-{k}"));
        let mut sealer = Child::start(test, &format!("sealer stop_at={k}"), &folder);
        let before = sealer.until("stopped");
        sealer.kill();
        let returned = before.iter().any(|line| line.starts_with("sealed "));

        // The store opens with nothing to mend, every row is there, and every search finds
        // exactly the vectors of those rows, whether the seal's commit is there or not.
        let db = support::open(&folder);
        let state = format!("killed at storage call {k} of a seal");
        assert!(
            searches_agree(&db, &state) == rows,
            "{state}: the rows of docs"
        );
        // The next cycles finish the sealing: all 200 vectors when the killed commit is not
        // there, none when it is. A cycle that had returned is there.
        let sealed = support::seal_all(&db);
        assert!(
            sealed == 0 || sealed == 200 && !returned,
            "{state}: the next cycles sealed {sealed} vectors; the killed cycle had {}returned",
            if returned { "" } else { "not " }
        );
        let report = db.run_maintenance_cycle().expect("one more cycle").vector;
        assert_eq!(
            (
                report.maintenance_vectors_done,
                report.maintenance_vectors_remaining
            ),
            (0, 0),
            "{state}: a cycle after the sealing was finished"
        );
        assert!(
            searches_agree(&db, &state) == rows,
            "{state}: the rows after sealing"
        );
    }
}

/// Runs one maintenance cycle. Announces `sealing <n>` with the storage calls made so far when
/// the cycle is about to begin, `sealed <done> <remaining>` when it has returned, and
/// `calls <total>` once the store is closed.
fn one_seal(role: support::Role) {
    let (storage, calls) = stopping_before_call(role.number("stop_at"));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    let db = support::open_with(&role.folder, options).expect("open");
    support::say(format!("sealing {}", calls.load(Ordering::SeqCst)));
    let report = db.run_maintenance_cycle().expect("a cycle").vector;
    support::say(format!(
        "sealed {} {}",
        report.maintenance_vectors_done, report.maintenance_vectors_remaining
    ));
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

#[test]
fn a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together() {
    if let Some(role) = support::role() {
        return one_commit_of_vectors(role);
    }
    let test = "a_kill_inside_a_commit_of_vectors_keeps_rows_and_vectors_together";
    // The rows before the commit, from a store nothing was done to, and the rows after it,
    // from a run of the commit to its end, which also tells which storage calls are its own.
    let untouched = store_with_vectors("kill-vectors-before");
    let before = searches_agree(&support::open(&untouched), "before the commit");
    let folder = store_with_vectors("kill-vectors-count");
    let lines = Child::start(test, "writer stop_at=0", &folder).end();
    let begins = support::last_number(&lines, "committing").expect("the writer announces it");
    let calls = support::last_number(&lines, "calls").expect("the writer announces its calls");
    assert!(
        calls > begins,
        "a commit of sixty vectors made no write and no sync"
    );
    let after = searches_agree(&support::open(&folder), "after the commit");
    assert_eq!((before.len(), after.len()), (300, 358));

    for k in begins + 1..=calls {
        let folder = store_with_vectors(&format!("kill-vectors-at-{k}"));
        let mut writer = Child::start(test, &format!("writer stop_at={k}"), &folder);
        let told = writer.until("stopped");
        writer.kill();
        let returned = told.iter().any(|line| line == "committed");

        // The rows are those of before the commit or those of after it, and the searches
        // answer from the vectors of exactly those rows: never rows of one and vectors of the
        // other. A commit that had returned is there.
        let db = support::open(&folder);
        let state = format!("killed at storage call {k} of a commit of vectors");
        let rows = searches_agree(&db, &state);
        assert!(
            rows == after || rows == before && !returned,
            "{state}: {} rows are there, of neither state or of the wrong one; the commit had \
             {}returned",
            rows.len(),
            if returned { "" } else { "not " }
        );
        // The store goes on from there: what waits is sealed, and the answers stay.
        support::seal_all(&db);
        assert!(
            searches_agree(&db, &state) == rows,
            "{state}: the rows after sealing"
        );
    }
}

/// In one transaction adds rows 301 to 360, a third of them under a context that has rows and
/// the rest under two that have none; changes the vector of a sealed row and of a pending row;
/// deletes one of each; and moves a sealed row to one of the new contexts. Announces
/// `committing <n>` with the storage calls made so far when the commit is about to begin,
/// `committed` when it has returned, and `calls <total>` once the store is closed.
fn one_commit_of_vectors(role: support::Role) {
    let (storage, calls) = stopping_before_call(role.number("stop_at"));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    let db = support::open_with(&role.folder, options).expect("open");
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 301..=360, DIMENSION, |n| {
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
    support::run(&tx, "UPDATE docs SET ctx = 'new' WHERE id = 7");
    support::say(format!("committing {}", calls.load(Ordering::SeqCst)));
    tx.commit().expect("commit");
    support::say("committed");
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

// ---- shared by the tests of this file ----

/// A closed store with the table declared and rows 1 to `rows` committed.
fn store_with_rows(name: &str, rows: i64) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare(&db);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=rows);
    tx.commit().expect("commit");
    support::close(db);
    folder
}

/// A storage door that counts the writes and syncs of the data file. Before call number
/// `stop_at` it announces `stopped` and waits there to be killed, so that call and every later
/// one never reach the file. `stop_at` 0 never stops. Returns the door and its count.
fn stopping_before_call(stop_at: u64) -> (WrapStorage, Arc<AtomicU64>) {
    let calls = Arc::new(AtomicU64::new(0));
    let storage = support::storage({
        let calls = calls.clone();
        move |call| {
            if matches!(call, Call::Write(..) | Call::Sync)
                && calls.fetch_add(1, Ordering::SeqCst) + 1 == stop_at
            {
                support::wait_to_be_killed("stopped");
            }
        }
    });
    (storage, calls)
}

// ---- a kill inside a build ----

/// A closed store whose `docs` has `rows` pending vectors in a column that is at its size from
/// a third of them on: the cycles seal them and build one grouping of `round(sqrt(rows))`
/// groups, 17 for 300 rows and 3 for 12.
fn store_to_build(name: &str, rows: i64) -> PathBuf {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, &format!("AUTO_INDEX_AT {}", rows / 3));
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
fn a_kill_at_any_storage_call_of_a_build_leaves_every_vector_found() {
    if let Some(role) = support::role() {
        return one_build(role);
    }
    let test = "a_kill_at_any_storage_call_of_a_build_leaves_every_vector_found";
    // Twelve rows: a build finds centres only for more than one group, which takes
    // `round(sqrt(rows))` of 2 or more, and the INDEXED search asks for ten rows. The test's
    // length is one store built for every storage call of the sixteen cycles.
    const ROWS: i64 = 12;
    // Run once to the end to learn which storage calls belong to the cycles of the build.
    let folder = store_to_build("kill-build-count", ROWS);
    let lines = Child::start(test, "builder stop_at=0", &folder).end();
    let begins = support::last_number(&lines, "building").expect("the builder announces it");
    let calls = support::last_number(&lines, "calls").expect("the builder announces its calls");
    assert_eq!(support::last_number(&lines, "cycles"), Some(16));
    assert_eq!(groups(&support::open(&folder)), Value::Int64(3));

    for k in begins + 1..=calls {
        let folder = store_to_build(&format!("kill-build-at-{k}"), ROWS);
        let mut builder = Child::start(test, &format!("builder stop_at={k}"), &folder);
        builder.until("stopped");
        builder.kill();
        // The store opens with nothing to mend and every vector is found, whichever step's
        // commit the kill fell in; the next cycles finish the build.
        let db = support::open(&folder);
        let state = format!("killed at storage call {k} of a build");
        every_vector_is_found(&db, ROWS as usize, &state);
        support::upkeep(&db);
        assert_eq!(groups(&db), Value::Int64(3), "{state}");
        every_vector_is_found(&db, ROWS as usize, &format!("{state}, then built"));
    }
}

#[test]
fn a_killed_build_goes_on_from_its_record() {
    if let Some(role) = support::role() {
        return one_build(role);
    }
    let test = "a_killed_build_goes_on_from_its_record";
    let folder = store_to_build("killed-build-count", 300);
    let lines = Child::start(test, "builder stop_at=0", &folder).end();
    let begins = support::last_number(&lines, "building").expect("the builder announces it");
    let calls = support::last_number(&lines, "calls").expect("the builder announces its calls");
    let whole = support::last_number(&lines, "cycles").expect("the builder counts its cycles");
    assert_eq!(whole, 16);

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
    let (ran, _) = support::upkeep(&db);
    assert!(
        ran == whole - returned || ran == whole - returned - 1,
        "{returned} of {whole} cycles had returned; the build then took {ran} more"
    );
    assert_eq!(groups(&db), Value::Int64(17));
    every_vector_is_found(&db, 300, "built after the kill");
}

/// Runs maintenance cycles until nothing is left. Announces `building <n>` with the storage
/// calls made so far before the first, `cycles <n>` as each returns, and `calls <total>` once
/// the store is closed.
fn one_build(role: support::Role) {
    let (storage, calls) = stopping_before_call(role.number("stop_at"));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(storage);
    let db = support::open_with(&role.folder, options).expect("open");
    support::say(format!("building {}", calls.load(Ordering::SeqCst)));
    for cycles in 1_u64.. {
        let report = support::cycle(&db);
        support::say(format!("cycles {cycles}"));
        if report.maintenance_vectors_remaining == 0
            && report.remaining_partitions <= report.stalled_partitions
        {
            break;
        }
    }
    support::close(db);
    support::say(format!("calls {}", calls.load(Ordering::SeqCst)));
}
