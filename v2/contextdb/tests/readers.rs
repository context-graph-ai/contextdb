//! Readers in other processes: they read while the one writer commits, each statement from one
//! moment; they outlive a killed writer and follow the next one; and what a killed reader or a
//! killed writer leaves in the folder is cleared by the next writer without anyone's help.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use contextdb::{DatabaseOpenOptions, Error, ReadSession, Value, params};

use support::Child;

/// Enough rows that one scan of the table reads many pages.
const ROWS: i64 = 2000;

#[test]
fn a_purge_cuts_off_a_reader_holding_an_older_view() {
    if let Some(role) = support::role() {
        let (reader, counting, reads) = support::reader_pausing(&role);
        counting.store(true, Ordering::SeqCst);
        let result = reader.execute("SELECT id, name FROM t ORDER BY id", &params! {});
        counting.store(false, Ordering::SeqCst);
        if role.number("pause_at") == 0 {
            assert_eq!(result.unwrap().rows().len(), ROWS as usize);
            support::say(format!("reads {}", reads.load(Ordering::SeqCst)));
        } else {
            assert_eq!(format!("{:?}", result.unwrap_err()), "AuthoritativeErasure");
            let next = reader.execute("SELECT id FROM t", &params! {});
            assert!(next.unwrap().rows().is_empty());
            support::say("cut off");
        }
        return;
    }
    let test = "a_purge_cuts_off_a_reader_holding_an_older_view";
    for turn in [
        "",
        "PURGE FROM t",
        "SET DISK_LIMIT NONE",
        "commit",
        "CREATE TABLE other(id INT)",
        "maintenance",
        "DELETE FROM t WHERE id IN (SELECT id FROM t)",
        "open",
    ] {
        let folder = support::folder(&format!("purge-held-reader-{turn}"));
        let armed = Arc::new(AtomicBool::new(false));
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(support::storage({
            let (armed, fault) = (armed.clone(), folder.join("readers/fault"));
            move |call| {
                if matches!(call, support::Call::Sync) && armed.swap(false, Ordering::SeqCst) {
                    std::fs::create_dir(&fault).unwrap();
                }
            }
        }));
        let mut db = support::open_with(&folder, options).unwrap();
        support::declare(&db);
        support::insert_together(&db, 1..=ROWS);
        let r = db.read_session(Default::default()).unwrap();
        drop(r.open_cursor("SELECT id FROM t", &params! {}).unwrap()); // records the form
        let idle = support::memory_used(&r);
        let mut cursor = r.open_cursor("SELECT id FROM t", &params! {}).unwrap();
        assert!(
            cursor.has_more(),
            "a cursor that has produced nothing yet has more to give"
        );
        let (_, mut reader) = support::stopped_halfway(test, "reader", &folder);
        let events = db.subscribe().unwrap();
        let primary = db.committed_watermark().unwrap().0 + 1;
        armed.store(!turn.is_empty(), Ordering::SeqCst);
        let result = db.execute("PURGE FROM t", &params! {});
        assert_eq!(result.is_ok(), turn.is_empty());
        assert!(result.is_ok() || matches!(result, Err(Error::StoreIo { .. })));
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
        for expected in ["AuthoritativeErasure", "CursorClosed"] {
            let error = cursor.fetch(None).unwrap_err();
            assert_eq!(format!("{error:?}"), expected);
            if expected == "CursorClosed" {
                assert_eq!(error.to_string(), "the cursor is closed; open a new one");
            }
        }
        let after = support::memory_used(&r);
        assert!(after <= idle, "a cut-off cursor's charge is released");
        let event = events.try_recv().unwrap();
        assert_eq!(event.lsn.0, primary);
        assert_eq!(event.row_count, ROWS as usize);
        assert_eq!(event.tables_changed(), ["t"]);
        if !turn.is_empty() {
            std::fs::rename(folder.join("readers/fault"), folder.join("restored")).unwrap();
        }
        if turn == "open" {
            db = support::reopen(db, &folder);
        }
        match turn {
            "" | "open" => (),
            "commit" => db.transaction().unwrap().commit().map(drop).unwrap(),
            "maintenance" => db.run_maintenance_cycle().map(drop).unwrap(),
            sql => drop(support::run(&db, sql)),
        }
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
        assert!(events.try_iter().all(|event| event.row_count == 0));
        reader.tell("go");
        reader.expect("cut off");
        reader.end();
        drop((cursor, r));
        assert!(support::listing(&folder.join("readers")).is_empty());
    }
}

#[test]
fn a_reader_in_another_process_reads_while_the_writer_commits() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let test = "a_reader_in_another_process_reads_while_the_writer_commits";
    let folder = support::folder("reader-beside-writer");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);

    // Another store has its own lock: its writer and its reader are not held back by this one.
    let other = support::folder("reader-beside-writer-b");
    let b = support::open(&other);
    support::declare(&b);
    support::insert(&b, 1..=3);
    support::assert_rows_are(&support::open_reader(&other, None), 1..=3);

    // A reader opens the store while the writer holds it, and reads.
    let mut reader = Child::start(test, "reader", &folder);
    reader.expect("open");
    reader.ask("read", "rows 3");

    // The writer commits while that session is open, and the session's next statement sees it.
    support::insert(&db, 4..=4);
    reader.ask("read", "rows 4");

    // A reader that starts later sees the whole newer state.
    let mut later = Child::start(test, "reader", &folder);
    later.expect("open");
    later.ask("read", "rows 4");

    // With two sessions open the writer goes on committing, and both follow.
    support::insert(&db, 5..=6);
    for reader in [&mut reader, &mut later] {
        reader.ask("read", "rows 6");
        reader.end();
    }
    support::insert(&db, 7..=7);
    support::assert_rows_are(&db, 1..=7);
}

#[test]
fn a_statement_reads_one_moment_while_commits_land_under_it() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let test = "a_statement_reads_one_moment_while_commits_land_under_it";
    // The writer is there from the start, or no writer exists until the statement is stopped.
    for absent in [false, true] {
        let folder = support::folder(&format!("statement-one-moment-{absent}"));
        let db = support::open(&folder);
        support::declare(&db);
        support::insert_together(&db, 1..=ROWS);
        let db = Some(db).filter(|_| !absent);
        let mut reader = scanner_stopped_inside_its_scan(test, &folder, ROWS);
        let db = db.unwrap_or_else(|| support::open(&folder));

        // Under the stopped statement the writer adds rows, removes half of what the statement
        // has still to read or has read, and commits many times so that freed pages come up for
        // reuse.
        support::insert(&db, ROWS + 1..=ROWS + 100);
        support::run(&db, &format!("DELETE FROM t WHERE id <= {}", ROWS / 2));
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), ROWS / 2 + 100);
        if absent {
            support::close(db);
        }

        // Let go, the statement answers with the rows of the moment it began at, every one
        // whole; the session's next statement sees the store as it is now.
        reader.tell("go");
        reader.expect(&format!("scanned {ROWS}"));
        reader.until(&format!("now {}", ROWS / 2 + 100));
        reader.end();
    }
}

#[test]
fn a_reader_does_the_same_counted_work_early_and_late_while_a_writer_never_stops() {
    let db = support::open(&support::folder("counted-early-late"));
    support::declare_docs(&db, 4, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 4");
    support::load_docs(&db, 1..=200, "a", |n| support::vector(n, 4));
    support::run(&db, "CREATE TABLE other (id INTEGER PRIMARY KEY)");
    let reader = db.read_session(Default::default()).unwrap();
    let sql = support::by_mode(None, "EXACT", 5);
    let q = params! {"q" => support::vector(3, 4)};
    let counted = || {
        let result = reader.execute(&sql, &q).unwrap();
        (result.rows_examined(), result.distance_evaluations())
    };
    let early = counted();
    let (writing, heard) = std::sync::mpsc::channel();
    let stop = AtomicBool::new(false);
    let late = std::thread::scope(|scope| {
        scope.spawn(|| {
            for id in 1.. {
                support::run(&db, &format!("INSERT INTO other VALUES ({id})"));
                if id == 20 {
                    writing.send(()).unwrap();
                }
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
        });
        heard.recv().unwrap();
        let late = counted();
        stop.store(true, Ordering::SeqCst);
        late
    });
    assert_eq!(late, early, "counted work grows with commits");
}

#[test]
fn a_row_its_vector_and_its_edge_changed_together_are_read_at_one_moment() {
    let db = support::open(&support::folder("one-moment-together"));
    support::declare_docs(&db, 4, "PARTITION_KEY (ctx) INDEXING_THRESHOLD 4");
    support::load_docs(&db, 1..=10, "a", |n| support::vector(n, 4));
    support::run(
        &db,
        "CREATE TABLE edges (source_id UUID, target_id UUID, edge_type TEXT)",
    );
    let kept = db.snapshot().unwrap();
    let tx = db.transaction().unwrap();
    let moved = params! {"v" => Value::Vector(support::vector(99, 4))};
    tx.execute("UPDATE docs SET v = $v WHERE id = 1", &moved)
        .unwrap();
    support::run(
        &tx,
        "INSERT INTO edges VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E')",
    );
    tx.commit().unwrap();
    let nearest = support::by_mode(None, "EXACT", 1);
    let walk = "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[e]->(b) COLUMNS (b.id AS b_id))";
    let at = |sql: &str, moment: Option<&contextdb::Snapshot>| {
        let q = params! {"q" => support::vector(1, 4)};
        let read = moment.map_or_else(
            || db.execute(sql, &q),
            |m| db.execute_at_snapshot(sql, &q, m),
        );
        read.unwrap()
    };
    for (moment, kept_row, edges) in [(Some(&kept), true, 0), (None, false, 1)] {
        let first = at(&nearest, moment).rows()[0][0] == Value::Int64(1);
        let seen = (first, at(walk, moment).rows().len());
        assert_eq!(seen, (kept_row, edges), "one version");
    }
    // One statement over the edges and a vector search reads one moment: the kept view has no
    // edge, so no row; the newest has the edge and the moved vector, so its nearest is not row 1.
    let both = "SELECT d.id FROM docs d JOIN edges e ON e.edge_type = 'E' \
                ORDER BY d.v <=> $q USE VECTOR EXACT LIMIT 1";
    assert!(
        at(both, Some(&kept)).rows().is_empty(),
        "no edge at the kept moment"
    );
    assert_ne!(
        at(both, None).rows()[0][0],
        Value::Int64(1),
        "the moved vector, with the edge"
    );
    db.verify().unwrap();
    // The index changes after the kept moment, so the two views of it differ.
    support::maintenance(&db);
    let shown = at("SHOW VECTOR_INDEXES", Some(&kept));
    assert_ne!(
        shown.rows(),
        at("SHOW VECTOR_INDEXES", None).rows(),
        "the build is after the moment"
    );
}

#[test]
fn pages_a_stopped_reader_retains_count_against_the_disk_limit_until_it_ends() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let test = "pages_a_stopped_reader_retains_count_against_the_disk_limit_until_it_ends";
    let folder = support::folder("reader-retains-pages");
    let db = support::open(&folder);
    support::declare_current_only(&db);
    support::insert_together(&db, 1..=ROWS);
    let flip = |n: u32| format!("UPDATE t SET tag = tag {} 1", ["+", "-"][(n % 2) as usize]);
    (0..2).for_each(|n| drop(support::run(&db, &flip(n))));
    db.set_disk_limit(Some(support::data_file(&folder)))
        .unwrap();
    (0..8).for_each(|n| drop(support::run(&db, &flip(n))));
    let mut reader = scanner_stopped_inside_its_scan(test, &folder, ROWS);
    let refused = (0..8).find_map(|n| db.execute(&flip(n), &params! {}).err());
    assert!(
        matches!(refused, Some(Error::DiskLimitExceeded { .. })),
        "pages the stopped reader retains are not free: {refused:?}"
    );
    reader.tell("go");
    reader.expect(&format!("scanned {ROWS}"));
    reader.end();
    support::cycle(&db);
    support::run(&db, &flip(0));
}

#[test]
fn a_reader_keeps_answering_when_the_writer_is_killed_and_follows_the_restarted_writer() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let test =
        "a_reader_keeps_answering_when_the_writer_is_killed_and_follows_the_restarted_writer";
    let folder = support::folder("reader-outlives-writer");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=ROWS);
    support::close(db);

    // A writer in another process, and this process as a reader of its store.
    let mut writer = Child::start(
        test,
        &format!("writer first={} last={} delete_to=0", ROWS + 1, ROWS + 3),
        &folder,
    );
    writer.expect(&format!("committed {}", ROWS + 3));
    let reader = support::open_reader(&folder, None);
    support::assert_rows_are(&reader, 1..=ROWS + 3);
    // A second reader, stopped inside a statement.
    let mut working = scanner_stopped_inside_its_scan(test, &folder, ROWS + 3);

    // The writer is killed. The reader answers from what was last committed.
    writer.kill();
    support::assert_rows_are(&reader, 1..=ROWS + 3);

    // A new writer takes the store over and changes it. The same session follows it.
    let mut writer = Child::start(
        test,
        &format!(
            "writer first={} last={} delete_to={}",
            ROWS + 4,
            ROWS + 10,
            ROWS / 2
        ),
        &folder,
    );
    writer.expect(&format!("committed {}", ROWS + 10));
    support::assert_rows_are(&reader, ROWS / 2 + 1..=ROWS + 10);

    // The statement that was under way through all of it answers from its own moment, and the
    // session it belongs to then follows the new writer too.
    working.tell("go");
    working.expect(&format!("scanned {}", ROWS + 3));
    working.until(&format!("now {}", ROWS / 2 + 10));
    working.end();
    writer.end();
    support::assert_rows_are(&reader, ROWS / 2 + 1..=ROWS + 10);
}

#[test]
fn several_readers_read_at_once() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let folder = support::folder("several-readers");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=5);

    // Eight sessions, all open before any of them reads.
    let mut readers: Vec<Child> = (0..8)
        .map(|_| Child::start("several_readers_read_at_once", "reader", &folder))
        .collect();
    for reader in &mut readers {
        reader.expect("open");
    }
    for reader in &mut readers {
        reader.ask("read", "rows 5");
    }
    // The writer commits beside all eight, and all eight see it.
    support::insert(&db, 6..=6);
    for reader in &mut readers {
        reader.tell("read");
    }
    for reader in &mut readers {
        reader.expect("rows 6");
        reader.end();
    }
}

#[test]
fn a_killed_reader_leaves_one_file_that_the_next_open_clears() {
    if let Some(role) = support::role() {
        if role.name == "worker-fail" {
            support::say("ready");
            assert_eq!(support::hear().as_deref(), Some("open"));
            let result = contextdb::Database::open_with_options(
                &role.folder,
                DatabaseOpenOptions::default(),
            );
            assert!(
                matches!(result, Err(Error::StoreIo { operation: "open", path, .. }) if path == role.folder)
            );
            support::say("refused");
            assert_eq!(support::hear().as_deref(), Some("restore"));
            contextdb::Database::open_with_options(&role.folder, DatabaseOpenOptions::default())
                .unwrap()
                .close()
                .unwrap();
            support::say("opened");
            return;
        }
        return child(role);
    }
    let test = "a_killed_reader_leaves_one_file_that_the_next_open_clears";
    let folder = support::folder("killed-reader");
    let readers = folder.join("readers");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=ROWS);
    support::close(db);

    #[cfg(target_os = "linux")]
    {
        let mut child = Child::start(test, "worker-fail", &folder);
        child.expect("ready");
        let old = child.limit("nproc", "0");
        child.ask("open", "refused");
        child.limit("nproc", &old);
        child.ask("restore", "opened");
        child.end();
    }
    // Two readers, each stopped inside a statement: each holds one file.
    let mut killed = scanner_stopped_inside_its_scan(test, &folder, ROWS);
    let mut living = scanner_stopped_inside_its_scan(test, &folder, ROWS);
    assert_eq!(
        support::listing(&readers).len(),
        2,
        "one file for each reader in a statement"
    );

    // A kill leaves the dead reader's file where it was.
    killed.kill();
    assert_eq!(
        support::listing(&readers).len(),
        2,
        "a killed reader's file stays behind"
    );

    // The next writer clears it on opening, and leaves the living reader's file alone.
    let db = support::open(&folder);
    assert_eq!(
        support::listing(&readers).len(),
        1,
        "the dead reader's file, and only it, goes"
    );
    support::assert_rows_are(&db, 1..=ROWS);

    // The living reader finishes its statement and ends; a reader that ends leaves no file.
    living.tell("go");
    living.expect(&format!("scanned {ROWS}"));
    living.end();
    assert!(
        support::listing(&readers).is_empty(),
        "a reader that ended left its file behind"
    );
    support::run(&db, "PURGE FROM t WHERE id = 1");
    support::cycle(&db);

    // With `readers` replaced by a plain file a reader cannot make its file: it is refused as a
    // failure of the storage, before it has read a page of the store, and no row comes from it.
    std::fs::remove_dir(&readers).expect("the empty readers folder");
    std::fs::write(&readers, b"not a folder").expect("a plain file where readers was");
    let (door, bytes_read) = support::counting_bytes_read();
    let mut options = support::reader_options(None);
    options.storage = Some(door);
    let outcome = ReadSession::open_with_options(&folder, options)
        .and_then(|session| session.execute(support::SCAN, &params! {}));
    assert_eq!(
        bytes_read.load(Ordering::SeqCst),
        0,
        "a reader that cannot leave its file in readers read the store's pages first"
    );
    match outcome {
        Err(Error::StoreIo { .. }) => {}
        Err(other) => panic!("a reader beside a plain file named readers was refused as {other:?}"),
        Ok(result) => panic!(
            "a reader beside a plain file named readers returned {} rows",
            result.rows().len()
        ),
    }
    let rows = support::int(&db, "SELECT COUNT(*) FROM t");
    let purge = db.execute("PURGE FROM t", &params! {}).unwrap_err();
    assert!(
        matches!(&purge, Error::StoreIo { operation: "purge", path, .. } if *path == folder),
        "{purge:?}"
    );
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM t"),
        rows,
        "a refused PURGE changed nothing"
    );
    support::insert(&db, ROWS + 1..=ROWS + 1);
    let compacted = db.run_maintenance_cycle().unwrap().compacted_pages;
    assert_eq!(
        compacted.unwrap_or(0),
        0,
        "files that cannot be read count every commit as held"
    );
    support::close(db);
    let result = contextdb::Database::open_with_options(&folder, DatabaseOpenOptions::default());
    assert!(matches!(result, Err(Error::StoreIo { path, .. }) if path == folder));
    std::fs::remove_file(&readers).unwrap();
    std::fs::create_dir(&readers).unwrap();
    support::close(support::open(&folder));
}

#[test]
fn a_killed_writer_leaves_its_lock_files_and_the_next_writer_starts_at_once() {
    if let Some(role) = support::role() {
        return child(role);
    }
    let folder = support::folder("killed-writer");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);

    let mut writer = Child::start(
        "a_killed_writer_leaves_its_lock_files_and_the_next_writer_starts_at_once",
        "writer first=1 last=3 delete_to=0",
        &folder,
    );
    writer.expect("committed 3");
    let held = support::listing(&folder);
    assert_eq!(
        held,
        ["data", "lock", "readers", "writer"],
        "a writer holds the store through its lock and its mark beside the data"
    );
    writer.kill();
    assert_eq!(
        support::listing(&folder),
        held,
        "a kill removes nothing from the folder"
    );

    // The first open after the kill gets the store, with every commit that had returned.
    let db = match support::open_with(&folder, DatabaseOpenOptions::default()) {
        Ok(db) => db,
        Err(error) => panic!("the first open after a killed writer was refused: {error:?}"),
    };
    support::assert_rows_are(&db, 1..=3);
    support::insert(&db, 4..=4);
    support::assert_rows_are(&db, 1..=4);
}

// ---- what the children do ----

fn child(role: support::Role) {
    match role.name.as_str() {
        "reader" => reader(&role),
        "writer" => writer(&role),
        "scanner" => scanner(&role),
        other => panic!("no role {other:?} in this file"),
    }
}

/// Opens the store for reading and keeps the one session. Each time it is told `read` it checks
/// that the table holds rows 1 to n, each whole, and announces `rows <n>`.
fn reader(role: &support::Role) {
    let reader = support::open_reader(&role.folder, None);
    support::say("open");
    while let Some(word) = support::hear() {
        assert_eq!(word, "read");
        let rows = support::int(&reader, "SELECT COUNT(*) FROM t");
        support::assert_rows_are(&reader, 1..=rows);
        support::say(format!("rows {rows}"));
    }
}

/// Opens the store for writing, commits rows `first` to `last` one commit each, deletes the rows
/// up to `delete_to`, announces `committed <last>` and holds the store until it is ended or
/// killed.
fn writer(role: &support::Role) {
    let db = support::open(&role.folder);
    let (first, last) = (role.number("first") as i64, role.number("last") as i64);
    support::insert(&db, first..=last);
    support::run(
        &db,
        &format!("DELETE FROM t WHERE id <= {}", role.number("delete_to")),
    );
    support::say(format!("committed {last}"));
    support::hear();
}

/// Opens the store for reading and scans the table once. At storage read number `pause_at` of
/// that scan it announces `reading` and waits for a line; with `pause_at=0` it never stops.
/// The scan must return rows 1 to `rows`, each whole. Then it announces `scanned <rows>`, how
/// many storage reads the scan took as `reads <n>`, and what the session's next statement
/// counts as `now <n>`.
fn scanner(role: &support::Role) {
    let rows = role.number("rows") as i64;
    let (reader, scanning, reads) = support::reader_pausing(role);
    scanning.store(true, Ordering::SeqCst);
    let seen = support::rows(&reader, support::SCAN);
    scanning.store(false, Ordering::SeqCst);
    let wanted: Vec<Vec<Value>> = (1..=rows).map(support::row).collect();
    support::assert_same_rows(&seen, &wanted, "the rows of the moment the scan began at");

    support::say(format!("scanned {rows}"));
    support::say(format!("reads {}", reads.load(Ordering::SeqCst)));
    support::say(format!(
        "now {}",
        support::int(&reader, "SELECT COUNT(*) FROM t")
    ));
}

/// A scanner of `rows` rows stopped halfway through the storage reads of its scan.
fn scanner_stopped_inside_its_scan(test: &str, folder: &Path, rows: i64) -> Child {
    support::stopped_halfway(test, &format!("scanner rows={rows}"), folder).1
}

// ---- a reader beside a build ----

const GROUPED: usize = 16;

/// The search of the two tests below, and the vector it asks for.
fn indexed_ten() -> String {
    support::by_mode(None, "INDEXED", 10)
}

fn asked() -> Vec<f32> {
    support::gathered(97_000, GROUPED)
}

/// A search's counted work and answer as one line: its distance evaluations, then each key
/// with the bits of its distance. Two searches of one commit give the same line.
fn told(result: &contextdb::QueryResult) -> String {
    let rows: Vec<String> = support::found(result, "the indexed search")
        .iter()
        .map(|near| format!("{:?}:{:016x}", near.key, near.distance.to_bits()))
        .collect();
    format!("{} {}", result.distance_evaluations(), rows.join(","))
}

#[test]
fn a_reader_sees_one_ivf_whole_while_the_writer_builds_and_switches() {
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "stepper" => stepper(&role),
            _ => searcher(&role),
        };
    }
    let test = "a_reader_sees_one_ivf_whole_while_the_writer_builds_and_switches";
    let (folder, db) = support::built(
        "reader-beside-build",
        GROUPED,
        "INDEXING_THRESHOLD 1000",
        2000,
    );
    let line = |db: &contextdb::Database| told(&support::answer(db, &indexed_ten(), &asked()));
    let first = line(&db);

    // One reader is stopped halfway through the storage reads of a search of the first
    // IVF.
    let (lines, mut held) = support::stopped_halfway(test, "searcher", &folder);
    assert_eq!(lines.first(), Some(&first), "a search nothing stopped");

    // The writer adds a fifth, and builds and switches to the IVF that makes due. The
    // switch removes every record of the IVF the reader is inside.
    support::load_docs(&db, 2001..=2400, "a", |n| support::gathered(n, GROUPED));
    support::maintenance(&db);
    assert_eq!(
        support::field(&support::the_partition(&db), "lists"),
        &Value::Int64(45)
    );
    let second = line(&db);
    assert!(
        second != first,
        "the new IVF changed nothing a search tells"
    );

    // The stopped reader answers from the IVF it began in, whole; its next search, and
    // a reader that starts now, see the new one.
    held.tell("go");
    assert_eq!(
        held.line().as_ref(),
        Some(&first),
        "the search that was stopped"
    );
    assert!(held.line().is_some_and(|line| line.starts_with("reads ")));
    assert_eq!(
        held.line().as_ref(),
        Some(&second),
        "the stopped reader's next search"
    );
    held.end();
    let lines = Child::start(test, "searcher pause_at=0", &folder).end();
    assert_eq!(
        lines.first(),
        Some(&second),
        "a reader that starts after the switch"
    );

    // 500 more rows make a regrouping due. A writer in another process stops at the first
    // write of a build step, and a reader inspects the partition meanwhile: it answers, from
    // the IVF in force.
    support::load_docs(&db, 2401..=2900, "a", |n| support::gathered(n, GROUPED));
    support::close(db);
    let mut writer = Child::start(test, "stepper", &folder);
    writer.expect("writing");
    let inspected = support::the_partition(&support::open_reader(&folder, None));
    assert_eq!(
        support::field(&inspected, "lists"),
        &Value::Int64(45),
        "inside a build step"
    );
    writer.ask("go", "stepped");
    writer.end();
}

/// Runs cycles until a build is under way, then stops at the first storage write of the next
/// cycle, a build step, announcing `writing` until told to go on; announces `stepped` after.
fn stepper(role: &support::Role) {
    let writing = Arc::new(AtomicBool::new(false));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage({
        let writing = writing.clone();
        move |call| {
            if matches!(call, support::Call::Write(..)) && writing.swap(false, Ordering::SeqCst) {
                support::say("writing");
                support::hear();
            }
        }
    }));
    let db = support::open_with(&role.folder, options).expect("open the store for writing");
    for _ in 0..3 {
        if support::word(&support::the_partition(&db), "maintenance_state") == "building" {
            break;
        }
        support::cycle(&db);
    }
    let state = support::word(&support::the_partition(&db), "maintenance_state");
    assert_eq!(
        state, "building",
        "a regrouping due began within three cycles"
    );
    writing.store(true, Ordering::SeqCst);
    support::cycle(&db);
    support::say("stepped");
}

/// Opens the store for reading and searches once. At storage read number `pause_at` of that
/// search it announces `reading` and waits for a line; `pause_at` 0 never stops. Then it
/// announces the answer's line and `reads <n>`, searches once more and announces that line.
fn searcher(role: &support::Role) {
    let (reader, searching, reads) = support::reader_pausing(role);
    searching.store(true, Ordering::SeqCst);
    let result = support::search(&reader, &indexed_ten(), &asked())
        .expect("a reader is not refused because a build ran");
    searching.store(false, Ordering::SeqCst);
    support::say(told(&result));
    support::say(format!("reads {}", reads.load(Ordering::SeqCst)));
    let next = support::search(&reader, &indexed_ten(), &asked()).expect("the next search");
    support::say(told(&next));
}

#[test]
fn a_replaced_grouping_is_freed_when_its_last_reader_goes() {
    // 1,000 vectors of 512 numbers, half a MiB of blocks at a byte a number, are grouped;
    // 500 more make a regrouping due, which replaces them. A reader holds the first IVF
    // through the switch while new searches use the second, and then lets go. The replaced
    // blocks are some twelfth of the file while held, more than the twentieth the bound
    // below allows to stay free, so a file that kept them would fail it.
    const DIMENSION: usize = 512;
    let folder = support::folder("replaced-ivf");
    let db = support::open(&folder);
    // Not about the interval: MIN_INTERVAL 0 keeps a pass starting at once (design 4, S4).
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    support::declare_docs(&db, DIMENSION, "INDEXING_THRESHOLD 1000");
    support::load_docs(&db, 1..=1000, "a", |n| support::gathered(n, DIMENSION));
    support::maintenance(&db);
    let first = support::whole(&support::the_partition(&db), "durable_vector_bytes") as u64;
    let held = db.transaction().expect("a reader of the first IVF");
    support::load_docs(&db, 1001..=1500, "a", |n| support::gathered(n, DIMENSION));
    let mut states = Vec::new();
    while states.len() < 100 && states.last().is_none_or(|state| state != "idle") {
        db.run_maintenance_cycle().expect("a maintenance cycle");
        states.push(support::word(
            &support::the_partition(&db),
            "maintenance_state",
        ));
    }
    assert!(states.contains(&"building".to_owned()), "{states:?}");
    let indexed = support::by_mode(None, "INDEXED", 10);
    let q = support::gathered(97_000, DIMENSION);
    assert_eq!(support::answer(&db, &indexed, &q).rows().len(), 10);
    assert_eq!(support::int(&held, "SELECT COUNT(*) FROM docs"), 1000);
    let while_held = support::data_file(&folder);
    let newer = db.read_session(Default::default()).unwrap();
    drop(held);
    support::maintenance(&db);
    // The replaced IVF's blocks are free once the reader goes, and maintenance gives free
    // pages back until under a twentieth of the file is free (STOP_RATIO 0.05).
    let end = support::data_file(&folder);
    assert!(
        end + first <= while_held + while_held / 20,
        "the file went from {while_held} to {end} bytes after the reader of {first} bytes of \
         replaced blocks let go"
    );
    assert_eq!(support::int(&newer, "SELECT COUNT(*) FROM docs"), 1500);
}

#[test]
fn rows_deleted_under_a_reader_return_their_file_when_it_ends_cycle_after_cycle() {
    let folder = support::folder("freed-under-reader");
    let db = support::open(&folder);
    // Not about the interval: MIN_INTERVAL 0 keeps a pass starting at once (design 4, S4).
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    support::declare_current_only(&db);
    let mut ends = Vec::new();
    for _ in 0..5 {
        support::insert_together(&db, 1..=2000);
        let full = support::data_file(&folder);
        let held = db.transaction().expect("a reader of the full table");
        support::run(&db, "DELETE FROM t");
        drop(held);
        support::maintenance(&db);
        ends.push(support::data_file(&folder));
        assert!(
            ends[ends.len() - 1] < full / 2,
            "deleted rows keep their pages: {ends:?}"
        );
    }
    // 5%, or one page where 5% is under a page, within the section's 10% ceiling.
    let (first, grown) = (ends[0] / 4096, ends[4] / 4096);
    assert!(
        grown <= first + (first / 20).max(1) && grown <= first + first / 10,
        "the file grew {first} to {grown} pages: {ends:?}"
    );
}

#[test]
fn a_new_process_searches_a_built_ivf_at_once() {
    if let Some(role) = support::role() {
        // Opens the store for writing and searches twice: each answer's line, then how far
        // the two searches moved the committed position.
        let db = support::open(&role.folder);
        let before = support::watermark(&db);
        for _ in 0..2 {
            support::say(told(&support::answer(&db, &indexed_ten(), &asked())));
        }
        support::say(format!("moved {}", support::watermark(&db).0 - before.0));
        return;
    }
    let test = "a_new_process_searches_a_built_ivf_at_once";
    let (folder, db) = support::built("first-search", GROUPED, "INDEXING_THRESHOLD 1000", 2000);
    let wanted = told(&support::answer(&db, &indexed_ten(), &asked()));
    support::close(db);
    // The first search of a fresh process does the work of its second and of the process
    // that built the IVF: nothing is built or mended at open.
    let lines = Child::start(test, "first", &folder).end();
    assert_eq!(lines, [wanted.clone(), wanted, "moved 0".to_owned()]);
}
