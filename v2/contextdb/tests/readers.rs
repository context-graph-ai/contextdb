//! Readers in other processes: they read while the one writer commits, each statement from one
//! moment; they outlive a killed writer and follow the next one; and what a killed reader or a
//! killed writer leaves in the folder is cleared by the next writer without anyone's help.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use contextdb::{DatabaseOpenOptions, ReadSession, ReadSessionOptions, Value};

use support::{Call, Child};

/// Enough rows that one scan of the table reads many pages.
const ROWS: i64 = 2000;

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

    // A reader opens the store while the writer holds it, and reads.
    let mut reader = Child::start(test, "reader", &folder);
    reader.expect("open");
    reader.tell("read");
    reader.expect("rows 3");

    // The writer commits while that session is open, and the session's next statement sees it.
    support::insert(&db, 4..=4);
    reader.tell("read");
    reader.expect("rows 4");

    // A reader that starts later sees the whole newer state.
    let mut later = Child::start(test, "reader", &folder);
    later.expect("open");
    later.tell("read");
    later.expect("rows 4");

    // With two sessions open the writer goes on committing, and both follow.
    support::insert(&db, 5..=6);
    for reader in [&mut reader, &mut later] {
        reader.tell("read");
        reader.expect("rows 6");
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
    let folder = support::folder("statement-one-moment");
    let db = support::open(&folder);
    support::declare(&db);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=ROWS);
    tx.commit().expect("commit");

    let mut reader = scanner_stopped_inside_its_scan(test, &folder, ROWS);

    // Under the stopped statement the writer adds rows, removes half of what the statement has
    // still to read or has read, and commits many times so that freed pages come up for reuse.
    support::insert(&db, ROWS + 1..=ROWS + 100);
    support::run(&db, &format!("DELETE FROM t WHERE id <= {}", ROWS / 2));
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), ROWS / 2 + 100);

    // Let go, the statement answers with the rows of the moment it began at, every one whole;
    // the session's next statement sees the store as it is now.
    reader.tell("go");
    reader.expect(&format!("scanned {ROWS}"));
    reader.until(&format!("now {}", ROWS / 2 + 100));
    reader.end();
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
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=ROWS);
    tx.commit().expect("commit");
    support::close(db);

    // A writer in another process, and this process as a reader of its store.
    let mut writer = Child::start(
        test,
        &format!("writer first={} last={} delete_to=0", ROWS + 1, ROWS + 3),
        &folder,
    );
    writer.expect(&format!("committed {}", ROWS + 3));
    let reader = ReadSession::open(&folder).expect("open the store for reading");
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
        reader.tell("read");
        reader.expect("rows 5");
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
        return child(role);
    }
    let test = "a_killed_reader_leaves_one_file_that_the_next_open_clears";
    let folder = support::folder("killed-reader");
    let readers = folder.join("readers");
    let db = support::open(&folder);
    support::declare(&db);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=ROWS);
    tx.commit().expect("commit");
    support::close(db);

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
    assert_eq!(held, ["data", "lock", "readers"]);
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
    let reader = ReadSession::open(&role.folder).expect("open the store for reading");
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
    let pause_at = role.number("pause_at");
    let rows = role.number("rows") as i64;
    let scanning = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let mut options = ReadSessionOptions::default();
    options.storage = Some(support::storage({
        let (scanning, reads) = (scanning.clone(), reads.clone());
        move |call| {
            if matches!(call, Call::Read(..))
                && scanning.load(Ordering::SeqCst)
                && reads.fetch_add(1, Ordering::SeqCst) + 1 == pause_at
            {
                support::say("reading");
                support::hear();
            }
        }
    }));
    let reader =
        ReadSession::open_with_options(&role.folder, options).expect("open the store for reading");

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

/// Starts a scanner that is stopped halfway through the storage reads of its scan, and returns
/// it once it has announced that it is there.
///
/// Where halfway is comes from running the same scan once to its end in a child of its own: the
/// store is the same for both, so both scans make the same reads. Halfway is inside the scan
/// whatever the first reads of a statement are for.
fn scanner_stopped_inside_its_scan(test: &str, folder: &Path, rows: i64) -> Child {
    let lines = Child::start(test, &format!("scanner pause_at=0 rows={rows}"), folder).end();
    let reads = support::last_number(&lines, "reads").expect("the scanner announces its reads");
    assert!(
        reads > 0,
        "a scan of {rows} rows read nothing through the storage door"
    );
    let halfway = reads.div_ceil(2);
    let mut scanner = Child::start(
        test,
        &format!("scanner pause_at={halfway} rows={rows}"),
        folder,
    );
    scanner.expect("reading");
    scanner
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
fn a_reader_sees_one_grouping_whole_while_the_writer_builds_and_switches() {
    if let Some(role) = support::role() {
        return searcher(&role);
    }
    let test = "a_reader_sees_one_grouping_whole_while_the_writer_builds_and_switches";
    let (folder, db) = support::built("reader-beside-build", GROUPED, "AUTO_INDEX_AT 1000", 2000);
    let line = |db: &contextdb::Database| told(&support::answer(db, &indexed_ten(), &asked()));
    let first = line(&db);

    // One reader is stopped halfway through the storage reads of a search of the first
    // grouping; where halfway is comes from a run of the same search to its end.
    let lines = Child::start(test, "searcher pause_at=0", &folder).end();
    assert_eq!(lines.first(), Some(&first), "a search nothing stopped");
    let reads = support::last_number(&lines, "reads").expect("the reader announces its reads");
    assert!(reads > 1, "an indexed search made {reads} storage reads");
    let role = format!("searcher pause_at={}", reads.div_ceil(2));
    let mut held = Child::start(test, &role, &folder);
    held.expect("reading");

    // The writer adds a fifth, and builds and switches to the grouping that makes due. The
    // switch removes every record of the grouping the reader is inside.
    support::load_docs(&db, 2001..=2400, "a", |n| support::gathered(n, GROUPED));
    support::upkeep(&db);
    assert_eq!(
        support::field(&support::the_partition(&db), "lists"),
        &Value::Int64(49)
    );
    let second = line(&db);
    assert!(
        second != first,
        "the new grouping changed nothing a search tells"
    );

    // The stopped reader answers from the grouping it began in, whole; its next search, and
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
}

/// Opens the store for reading and searches once. At storage read number `pause_at` of that
/// search it announces `reading` and waits for a line; `pause_at` 0 never stops. Then it
/// announces the answer's line and `reads <n>`, searches once more and announces that line.
fn searcher(role: &support::Role) {
    let (door, searching, reads) = support::pausing_at_read(role.number("pause_at"));
    let mut options = ReadSessionOptions::default();
    options.storage = Some(door);
    let reader =
        ReadSession::open_with_options(&role.folder, options).expect("open the store for reading");
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
fn a_new_process_searches_a_built_grouping_at_once() {
    if let Some(role) = support::role() {
        // Opens the store for writing and searches twice: each answer's line, then how far
        // the two searches moved the committed position.
        let db = support::open(&role.folder);
        let before = db.committed_watermark();
        for _ in 0..2 {
            support::say(told(&support::answer(&db, &indexed_ten(), &asked())));
        }
        support::say(format!("moved {}", db.committed_watermark().0 - before.0));
        return;
    }
    let test = "a_new_process_searches_a_built_grouping_at_once";
    let (folder, db) = support::built("first-search", GROUPED, "AUTO_INDEX_AT 1000", 2000);
    let wanted = told(&support::answer(&db, &indexed_ten(), &asked()));
    support::close(db);
    // The first search of a fresh process does the work of its second and of the process
    // that built the grouping: nothing is built or mended at open.
    let lines = Child::start(test, "first", &folder).end();
    assert_eq!(lines, [wanted.clone(), wanted, "moved 0".to_owned()]);
}
