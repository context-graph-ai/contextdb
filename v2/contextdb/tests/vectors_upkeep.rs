//! Sealing: vectors move from pending lists into blocks only when a maintenance cycle is
//! called, a bounded number in each cycle; a reader in another process searches while the
//! writer seals and gets the answer of one commit every time; and a small store of fixed
//! content stays within the size that was recorded for it.

mod support;

use std::fs;
use std::sync::atomic::Ordering;

use contextdb::{
    Database, MaintenanceReport, QueryResult, ReadSession, ReadSessionOptions, params,
};

use support::{Child, Run};

/// What one cycle reports: the vectors it sealed and the vectors still waiting.
fn cycle(db: &Database) -> (u64, u64) {
    let report = db
        .run_maintenance_cycle()
        .expect("a maintenance cycle")
        .vector;
    (
        report.maintenance_vectors_done,
        report.maintenance_vectors_remaining,
    )
}

#[test]
fn sealing_advances_only_when_asked_and_in_bounded_cycles() {
    const DIMENSION: usize = 8;
    let folder = support::folder("sealing-in-cycles");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    // Five thousand vectors: three thousand under a, the partition made first, and two
    // thousand under b.
    for first in (1..=5000_i64).step_by(1000) {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, first..first + 1000, DIMENSION, |n| {
            String::from(if n <= 3000 { "a" } else { "b" })
        });
        tx.commit().expect("commit");
    }
    let queries = [
        support::vector(94_001, DIMENSION),
        support::vector(94_002, DIMENSION),
    ];
    // Ten rows of everything, of a and of b: each equals the reference for both vectors.
    // Returns the rows the three searches read for the first vector.
    let examined = |on: &dyn Run, state: &str| -> [u64; 3] {
        let stored = support::stored(&on, support::DOCS);
        let scopes = [
            (None, None),
            (Some("ctx = 'a'"), Some("a")),
            (Some("ctx = 'b'"), Some("b")),
        ];
        scopes.map(|(filter, ctx): (Option<&str>, Option<&str>)| {
            let passes =
                |row: &[contextdb::Value]| ctx.is_none_or(|ctx| row[1] == support::text(ctx));
            let sql = support::nearest_sql("docs", "v", filter, 10);
            let mut read = Vec::new();
            for query in &queries {
                let wanted = stored.nearest(passes, query, 10);
                assert_eq!(wanted.len(), 10, "{state}");
                let result = support::assert_nearest(&on, &sql, query, &wanted, state);
                read.push(result.rows_examined());
            }
            read[0]
        })
    };

    // Before any cycle every vector is pending: a search reads each pending row of the
    // partitions it selects once, and each row it returns once more.
    assert_eq!(examined(&db, "nothing sealed"), [5010, 3010, 2010]);
    // Nothing seals unless a cycle is called: not a search, not a close, not an open.
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(
        examined(&db, "nothing sealed, after reopen"),
        [5010, 3010, 2010]
    );
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    assert_eq!(
        examined(&reader, "nothing sealed, a reader"),
        [5010, 3010, 2010]
    );

    // The first cycle seals 4,096 vectors in one commit: the first in the order of partition
    // and row key, so all of a and the first 1,096 of b. 904 of b stay pending.
    let unsealed = db.committed_watermark();
    assert_eq!(cycle(&db), (4096, 904));
    let after_first = db.committed_watermark();
    assert!(after_first > unsealed, "a cycle that sealed made no commit");
    let [everything, a, b] = examined(&db, "after the first cycle");
    assert!(
        (914..5000).contains(&everything),
        "everything read {everything} rows"
    );
    assert!(
        (10..914).contains(&a),
        "a, which has nothing pending, read {a} rows"
    );
    assert!(
        (914..2010).contains(&b),
        "b, which has 904 pending, read {b} rows"
    );
    assert_eq!(
        examined(&reader, "after the first cycle, a reader"),
        [everything, a, b]
    );

    // The second seals the rest, and the third has nothing to do and makes no commit.
    assert_eq!(cycle(&db), (904, 0));
    let after_second = db.committed_watermark();
    assert!(
        after_second > after_first,
        "a cycle that sealed made no commit"
    );
    assert_eq!(cycle(&db), (0, 0));
    assert_eq!(
        db.committed_watermark(),
        after_second,
        "a cycle with nothing to seal"
    );
    let sealed = examined(&db, "after the last cycle");
    for (read, pending) in sealed.into_iter().zip([5000, 3000, 2000]) {
        assert!(
            (10..pending).contains(&read),
            "a sealed search read {read} rows"
        );
    }
    assert_eq!(examined(&reader, "after the last cycle, a reader"), sealed);

    // A cycle reports exactly the vectors that came to wait since the one before.
    let position = |db: &Database| db.committed_watermark();
    // Ten new rows.
    support::insert_docs(&db, 5001..=5010, DIMENSION, |_| "a".to_owned());
    assert_eq!(cycle(&db), (10, 0));
    // A sealed row whose vector changes, twice, waits once.
    for fresh in [94_101, 94_102] {
        db.execute(
            "UPDATE docs SET v = $v WHERE id = 1",
            &params! { "v" => support::vector(fresh, DIMENSION) },
        )
        .expect("change a vector");
    }
    assert_eq!(cycle(&db), (1, 0));
    // Five new rows, of which two are deleted and one loses its vector before the cycle.
    support::insert_docs(&db, 5011..=5015, DIMENSION, |_| "b".to_owned());
    support::run(&db, "DELETE FROM docs WHERE id IN (5011, 5012)");
    support::run(&db, "UPDATE docs SET v = NULL WHERE id = 5013");
    assert_eq!(cycle(&db), (2, 0));
    // A row with no vector, and a sealed row that is deleted, leave nothing to seal.
    support::insert_doc(&db, 5016, "a", None).expect("a row with no vector");
    support::run(&db, "DELETE FROM docs WHERE id = 2");
    let before = position(&db);
    assert_eq!(cycle(&db), (0, 0));
    assert_eq!(position(&db), before);
    // A sealed row that moves to the other partition waits there.
    support::run(&db, "UPDATE docs SET ctx = 'b' WHERE id = 3");
    assert_eq!(cycle(&db), (1, 0));
    examined(&db, "at the end");

    // A store with no VECTOR column, and a store with no table: zeros, and no commit.
    let folder = support::folder("sealing-nothing");
    let db = support::open(&folder);
    let before = position(&db);
    let report = db
        .run_maintenance_cycle()
        .expect("a cycle of an empty store");
    assert_eq!(report, MaintenanceReport::default());
    assert_eq!(position(&db), before);
    support::declare(&db);
    support::insert(&db, 1..=10);
    let before = position(&db);
    let report = db
        .run_maintenance_cycle()
        .expect("a cycle of a store with no vectors");
    assert_eq!(
        (
            report.vector.maintenance_vectors_done,
            report.vector.maintenance_vectors_remaining
        ),
        (0, 0)
    );
    assert_eq!(position(&db), before);
    support::assert_rows_are(&db, 1..=10);
}

/// The dimension, the vector and the statement of the searches in the next test.
const SEARCHED: usize = 16;

fn searched_for() -> Vec<f32> {
    support::vector(95_000, SEARCHED)
}

fn search_of_ten() -> String {
    support::nearest_sql("docs", "v", None, 10)
}

/// A search's answer and its counted work as one line: the rows it read, then each key with
/// the bits of its distance. Two searches of one commit give the same line.
fn told(result: &QueryResult) -> String {
    let rows: Vec<String> = support::found(result, "the search of ten")
        .iter()
        .map(|near| format!("{:?}:{:016x}", near.key, near.distance.to_bits()))
        .collect();
    format!("{} {}", result.rows_examined(), rows.join(","))
}

#[test]
fn a_reader_in_another_process_searches_while_the_writer_seals() {
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "looping" => looping(&role),
            "stopped" => stopped(&role),
            other => panic!("no role {other:?} in this test"),
        };
    }
    let test = "a_reader_in_another_process_searches_while_the_writer_seals";
    let folder = support::folder("reader-beside-sealing");
    let db = support::open(&folder);
    support::declare_docs(&db, SEARCHED, "PARTITION_KEY (ctx)");
    let insert = |numbers: std::ops::Range<i64>| {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, numbers, SEARCHED, |n| format!("c{}", n % 2));
        tx.commit().expect("commit");
    };
    for first in (1..6001).step_by(1000) {
        insert(first..first + 1000);
    }
    // The line of the newest commit: the search equals the reference there, and this is what
    // it reads and returns.
    let newest = || -> String {
        let stored = support::stored(&db, support::DOCS);
        let wanted = stored.nearest(|_| true, &searched_for(), 10);
        let sql = search_of_ten();
        told(&support::assert_nearest(
            &db,
            &sql,
            &searched_for(),
            &wanted,
            "the writer",
        ))
    };
    let mut commits = vec![newest()];

    // One reader is stopped halfway through the storage reads of a search of the first commit,
    // where every vector is pending. Where halfway is comes from a run of the same search to
    // its end.
    let lines = Child::start(test, "stopped pause_at=0", &folder).end();
    assert_eq!(lines.first(), commits.first(), "a search nothing stopped");
    let reads = support::last_number(&lines, "reads").expect("the reader announces its reads");
    assert!(
        reads > 1,
        "a search of 6,000 pending rows made {reads} storage reads"
    );
    let mut held = Child::start(
        test,
        &format!("stopped pause_at={}", reads.div_ceil(2)),
        &folder,
    );
    held.expect("reading");

    // A second reader searches in a loop, and announces each answer that differs from its
    // last. Every line it announces must be the line of a commit, and it must come to the
    // newest one: it is never refused and never told to try again.
    let mut looping = Child::start(test, "looping", &folder);
    let mut follow = |commits: &[String]| {
        let newest = commits.last().expect("a commit");
        loop {
            let Some(line) = looping.line() else {
                panic!("the looping reader ended; it had still to reach\n{newest}")
            };
            assert!(
                commits.contains(&line),
                "the looping reader announced an answer that is no commit's:\n{line}"
            );
            if line == *newest {
                return;
            }
        }
    };
    follow(&commits);

    // The writer seals cycle by cycle and inserts and changes between. Each step is one
    // commit that changes what a search reads, so the reader can be seen to arrive at it.
    let steps: [&dyn Fn(); 7] = [
        &|| assert_eq!(cycle(&db), (4096, 1904)),
        &|| insert(6001..6501),
        &|| assert_eq!(cycle(&db), (2404, 0)),
        &|| insert(6501..7001),
        &|| assert_eq!(cycle(&db), (500, 0)),
        &|| {
            let tx = db.transaction().expect("begin");
            for id in 1..=300_i64 {
                tx.execute(
                    "UPDATE docs SET v = $v WHERE id = $id",
                    &params! { "id" => id, "v" => support::vector(95_100 + id, SEARCHED) },
                )
                .expect("change a vector");
            }
            tx.commit().expect("commit");
        },
        &|| assert_eq!(cycle(&db), (300, 0)),
    ];
    for step in steps {
        step();
        let line = newest();
        assert!(
            Some(&line) != commits.last(),
            "a step left the search's line as it was"
        );
        commits.push(line);
        follow(&commits);
    }

    // The reader that was stopped inside its search through all of it answers with the rows
    // of the commit it began at, for the work that commit took; its next search is of the
    // newest commit.
    held.tell("go");
    assert_eq!(
        held.line().as_ref(),
        commits.first(),
        "the search that was stopped"
    );
    assert!(held.line().is_some_and(|line| line.starts_with("reads ")));
    assert_eq!(
        held.line().as_ref(),
        commits.last(),
        "the stopped reader's next search"
    );
    held.end();
    looping.kill();
}

/// Opens the store for reading and searches without end, announcing an answer's line each
/// time it differs from the one before. The parent kills it.
fn looping(role: &support::Role) {
    let reader = ReadSession::open(&role.folder).expect("open the store for reading");
    let (sql, query) = (search_of_ten(), searched_for());
    let mut last = String::new();
    loop {
        let result = match support::search(&reader, &sql, &query) {
            Ok(result) => result,
            Err(error) => panic!("a reader was refused while the writer sealed: {error:?}"),
        };
        let line = told(&result);
        if line != last {
            support::say(&line);
            last = line;
        }
    }
}

/// Opens the store for reading and searches once. At storage read number `pause_at` of that
/// search it announces `reading` and waits for a line; `pause_at` 0 never stops. Then it
/// announces the answer's line and `reads <n>`, the storage reads the search made. Then it
/// searches once more and announces that answer's line.
fn stopped(role: &support::Role) {
    let (door, searching, reads) = support::pausing_at_read(role.number("pause_at"));
    let mut options = ReadSessionOptions::default();
    options.storage = Some(door);
    let reader =
        ReadSession::open_with_options(&role.folder, options).expect("open the store for reading");
    let (sql, query) = (search_of_ten(), searched_for());

    searching.store(true, Ordering::SeqCst);
    let result = support::search(&reader, &sql, &query).expect("a reader's search is not refused");
    searching.store(false, Ordering::SeqCst);
    support::say(told(&result));
    support::say(format!("reads {}", reads.load(Ordering::SeqCst)));
    let next = support::search(&reader, &sql, &query).expect("the reader's next search");
    support::say(told(&next));
}

#[test]
fn a_small_fixed_store_stays_within_its_recorded_size() {
    // The bytes put in: 1,500 ids of 8 bytes, 1,500 vectors of 1,536 bytes and 4,800,000
    // bytes of text. And the most bytes the data file may have.
    const PUT_IN: u64 = 7_116_000;
    // The length is of a store that keeps a large text compressed when that saves an eighth.
    // `body` is letters drawn one by one, which do not shrink by an eighth: stored as it is.
    const RECORDED: u64 = 2 * PUT_IN; // set from the first run

    let folder = support::folder("small-fixed-store");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(384))",
    );
    // Nothing in the content, its order or the store depends on a clock, a random number or a
    // thread, so the file has the same length on every machine and every run.
    let mut put_in = 0_u64;
    for first in (1..=1500_i64).step_by(500) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 500 {
            // 200, 1,700, 3,200, 4,700 or 6,200 lower-case letters: one size that stays in
            // its row and four that do not.
            let body = support::letters(id, 200 + 1500 * (id % 5) as usize);
            let embedding = support::vector(id, 384);
            put_in += 8 + body.len() as u64 + 4 * embedding.len() as u64;
            tx.execute(
                "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, $embedding)",
                &params! { "id" => id, "body" => body, "embedding" => embedding },
            )
            .unwrap_or_else(|error| panic!("note {id} was refused: {error:?}"));
        }
        tx.commit().expect("commit five hundred notes");
    }
    assert_eq!(put_in, PUT_IN, "the bytes this test put in");
    assert_eq!(support::seal_all(&db), 1500);
    support::close(db);

    let length = fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len();
    assert!(
        length <= RECORDED,
        "the data file is {length} bytes long; the most recorded for this store is {RECORDED} \
         bytes, and {PUT_IN} bytes were put in"
    );
}
