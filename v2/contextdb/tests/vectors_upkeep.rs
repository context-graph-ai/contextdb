//! Sealing: vectors move from pending lists into blocks only when a maintenance cycle is
//! called, a bounded number in each cycle; a reader in another process searches while the
//! writer seals and gets the answer of one commit every time; and a small store of fixed
//! content stays within the size that was recorded for it.

mod support;

use std::fs;
use std::sync::atomic::Ordering;

use contextdb::{
    Database, DatabaseOpenOptions, MaintenanceReport, QueryResult, ReadSession, ReadSessionOptions,
    Value, params,
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
    // The column declares a size it never reaches, so no partition here is ever grouped.
    support::declare_docs(
        &db,
        DIMENSION,
        "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000000000",
    );
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
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, embedding VECTOR(384) AUTO_INDEX_AT 1000000000)",
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

// ---- groupings ----

/// The dimension of the tests of a grouping, and the clause under which a partition of 2,000
/// rows is cut into groups.
const GROUPED: usize = 16;
const AT_1000: &str = "AUTO_INDEX_AT 1000";

fn gathered(n: i64) -> Vec<f32> {
    support::gathered(n, GROUPED)
}

/// A partition's `maintenance_state` and `maintenance_reason`.
fn state_of(partition: &support::Shown) -> (String, String) {
    (
        support::word(partition, "maintenance_state"),
        support::word(partition, "maintenance_reason"),
    )
}

/// A partition's `lists`: the groups of the grouping in force, `None` with none.
fn lists_of(partition: &support::Shown) -> Option<i64> {
    match support::field(partition, "lists") {
        Value::Null => None,
        _ => Some(support::whole(partition, "lists")),
    }
}

/// What a build reports of itself in a partition's row: the slots it reads, has packed and
/// has left.
fn progress(partition: &support::Shown) -> [i64; 3] {
    ["total", "done", "remaining"]
        .map(|part| support::whole(partition, &format!("maintenance_vectors_{part}")))
}

#[test]
fn a_grouping_is_built_in_bounded_cycles_and_switched_in_one() {
    let folder = support::folder("grouping-in-cycles");
    let db = support::open(&folder);
    // 2,000 rows are this column's size, and half of the 4,000 vectors a partition of it may
    // have outside a grouping and still be searched INDEXED: every search below is served.
    support::declare_docs(&db, GROUPED, "AUTO_INDEX_AT 2000");
    support::load_docs(&db, 1..=2000, "a", gathered);
    let query = gathered(60_000);
    let late = support::vector(60_001, GROUPED);
    let late_sql = support::by_mode(None, "INDEXED", 1);
    let found_late = |state: &str| {
        let stored = support::stored(&db, support::DOCS);
        let result = support::answer(&db, &late_sql, &late);
        let answer = support::assert_sound(&result, &stored, |_| true, &late, 1, state);
        assert_eq!(answer[0].key, Value::Int64(2001), "{state}");
    };

    for number in 1..=17_u64 {
        if number == 6 {
            // A write between two cycles commits and is found.
            support::insert_doc(&db, 2001, "a", Some(late.clone())).expect("a write in a build");
            found_late("written between two cycles");
        }
        let before = db.committed_watermark().0;
        let report = support::cycle(&db);
        let commits = db.committed_watermark().0 - before;
        // The vectors the cycle reports done and left, the partitions left, and its commits:
        // the first cycle seals and begins, ten find centres, one gives every slot its group,
        // one packs, two measure, one switches, and the seventeenth has nothing to do. The
        // sixth also seals the row written before it.
        let wanted = match number {
            1 => (2000, 2000, 1, 2),
            6 => (1, 2000, 1, 2),
            13 => (2000, 0, 1, 1),
            14 | 15 => (0, 0, 1, 1),
            16 => (0, 0, 0, 1),
            17 => (0, 0, 0, 0),
            _ => (0, 2000, 1, 1),
        };
        assert_eq!(
            (
                report.maintenance_vectors_done,
                report.maintenance_vectors_remaining,
                report.remaining_partitions,
                commits
            ),
            wanted,
            "cycle {number}"
        );
        let partition = support::the_partition(&db);
        let probes = support::field(&partition, "probes").clone();
        if number < 16 {
            assert_eq!(state_of(&partition).0, "building", "after cycle {number}");
            assert_eq!((lists_of(&partition), probes), (None, Value::Null));
            let packed = if number < 13 { 0 } else { 2000 };
            assert_eq!(progress(&partition), [2000, packed, 2000 - packed]);
            // Until the switch every mode returns what EXACT returns.
            let wanted = support::stored(&db, support::DOCS).nearest(|_| true, &query, 10);
            for mode in ["EXACT", "AUTO", "INDEXED"] {
                let sql = support::by_mode(None, mode, 10);
                let state = format!("{mode} after cycle {number}");
                support::assert_nearest(&db, &sql, &query, &wanted, &state);
            }
        } else {
            assert_eq!(state_of(&partition).0, "idle", "after cycle {number}");
            assert_eq!(lists_of(&partition), Some(45), "round(sqrt(2000))");
            assert!(
                matches!(probes, Value::Int64(1..=45)),
                "probes after the switch: {probes:?}"
            );
            assert_eq!(progress(&partition), [0, 0, 0]);
        }
    }
    found_late("after the switch");
}

/// The bytes inspection reports for the index of the one partition.
fn index_bytes(db: &Database) -> i64 {
    support::whole(&support::the_partition(db), "durable_index_bytes")
}

#[test]
fn a_grouping_keeps_a_sketch_of_every_slot_and_measures_its_width() {
    let (_, db) = support::built("grouping-index-bytes", GROUPED, AT_1000, 2000);
    let partition = support::the_partition(&db);
    assert_eq!(lists_of(&partition), Some(45));
    // 2,000 slots of 24 bytes with keys of 9 are one block. The index is 45 centres of 16
    // numbers, the record of 45 groups, the block's 2,000 sketches of 4 bytes behind their
    // count, and the mean.
    assert_eq!(
        support::whole(&partition, "durable_vector_bytes"),
        2000 * 24
    );
    assert_eq!(
        index_bytes(&db),
        45 * 4 * 16 + (12 + 8 * 46) + (4 + 2000 * 4) + 4 * 16
    );
}

#[test]
fn sealing_after_a_grouping_keeps_a_sketch_of_every_slot() {
    let (_, db) = support::built("sealing-after-grouping", GROUPED, AT_1000, 2000);
    let before = index_bytes(&db);
    support::load_docs(&db, 2001..=2300, "a", gathered);
    assert_eq!(support::cycle(&db).maintenance_vectors_done, 300);
    // 300 sketches, and the count of the one tail block they are in.
    assert_eq!(index_bytes(&db) - before, 300 * 4 + 4);
    let stored = support::stored(&db, support::DOCS);
    let sql = support::by_mode(None, "INDEXED", 1);
    for id in 2001..=2300 {
        let query = gathered(id);
        let result = support::answer(&db, &sql, &query);
        let state = format!("row {id}, sealed after the grouping");
        let answer = support::assert_sound(&result, &stored, |_| true, &query, 1, &state);
        assert_eq!(answer[0].key, Value::Int64(id), "{state}");
    }
}

#[test]
fn vectors_sealed_while_a_first_build_starts_are_found_after_the_switch() {
    // Vectors of 64 numbers, of which a block takes 3,083: fewer than one cycle seals.
    const WIDE: usize = 64;
    let wide = |n: i64| support::gathered(n, WIDE);
    let folder = support::folder("sealed-while-first-build-starts");
    let db = support::open(&folder);
    // The grouping is never built again, so the vectors sealed after it stay in its tail.
    support::declare_docs(&db, WIDE, "AUTO_INDEX_AT 1000 CONSOLIDATION NONE");
    support::load_docs(&db, 1..=2000, "a", wide);
    // The first cycle seals and begins the partition's first build. The 3,500 rows written
    // then are sealed by the second, before the build's first round: a whole block of them
    // and the start of the next.
    assert_eq!(support::cycle(&db).maintenance_vectors_done, 2000);
    assert_eq!(state_of(&support::the_partition(&db)).0, "building");
    support::load_docs(&db, 2001..=5500, "a", wide);
    assert_eq!(support::cycle(&db).maintenance_vectors_done, 3500);
    support::upkeep(&db);
    let partition = support::the_partition(&db);
    assert_eq!(state_of(&partition).0, "idle");
    assert_eq!(lists_of(&partition), Some(45));

    // Every search under INDEXED answers, and a row sealed in those cycles is the first row
    // of a search for its own vector, from the block that was filled and from the one begun.
    let stored = support::stored(&db, support::DOCS);
    let sql = support::by_mode(None, "INDEXED", 1);
    for id in (2001..=5500).step_by(233) {
        let query = wide(id);
        let result = support::answer(&db, &sql, &query);
        let state = format!("row {id}, sealed while the first build was starting");
        let answer = support::assert_sound(&result, &stored, |_| true, &query, 1, &state);
        assert_eq!(answer[0].key, Value::Int64(id), "{state}");
    }
    let query = wide(60_002);
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
    support::assert_sound(&result, &stored, |_| true, &query, 10, "ten rows of 5,500");
}

#[test]
fn a_grouping_holds_every_live_vector_once_and_no_stale_slot() {
    let clauses = "PARTITION_KEY (ctx) AUTO_INDEX_AT 1000";
    let (_, db) = support::built("grouping-no-stale", GROUPED, clauses, 1000);
    support::load_docs(&db, 1001..=2000, "b", gathered);
    support::upkeep(&db);
    // 150 rows of a get another vector and 150 rows of b are deleted: each leaves a slot.
    let tx = db.transaction().expect("begin");
    for id in 1..=150_i64 {
        tx.execute(
            "UPDATE docs SET v = $v WHERE id = $id",
            &params! { "id" => id, "v" => support::vector(61_000 + id, GROUPED) },
        )
        .expect("change a vector");
    }
    tx.commit().expect("commit");
    support::run(&db, "DELETE FROM docs WHERE id > 1850");
    let stale = |db: &Database| -> Vec<i64> {
        support::shown(db, "SHOW VECTOR_PARTITIONS")
            .iter()
            .map(|partition| support::whole(partition, "tombstones"))
            .collect()
    };
    assert_eq!(stale(&db), [150, 150]);

    support::upkeep(&db);
    assert_eq!(stale(&db), [0, 0], "after the regrouping");
    let stored = support::stored(&db, support::DOCS);
    let query = gathered(60_002);
    for (ctx, rows) in [("a", 1000), ("b", 850)] {
        let wanted = stored.nearest(|row| row[1] == support::text(ctx), &query, 5000);
        assert_eq!(wanted.len(), rows);
        let sql = support::by_mode(Some(&format!("ctx = '{ctx}'")), "EXACT", 5000);
        support::assert_nearest(&db, &sql, &query, &wanted, ctx);
    }
}

#[test]
fn a_partition_is_grouped_again_only_when_its_changes_reach_the_declared_share() {
    let state = |db: &Database| state_of(&support::the_partition(db));
    let lists = |db: &Database| lists_of(&support::the_partition(db));
    let named = |state: &str, reason: &str| (state.to_owned(), reason.to_owned());

    // New vectors: 20 in 100 of the grouping's 2,000 slots when nothing is declared, and the
    // declared 50.
    for (name, clause, share) in [
        ("regroup-default", "", 400_i64),
        (
            "regroup-declared",
            " CONSOLIDATION (CHANGE_PERCENT = 50)",
            1000,
        ),
    ] {
        let clauses = format!("{AT_1000}{clause}");
        let (_, db) = support::built(name, GROUPED, &clauses, 2000);
        support::load_docs(&db, 2001..=1999 + share, "a", gathered);
        support::upkeep(&db);
        assert_eq!(state(&db), named("idle", "none"), "{name}: one below");
        assert_eq!(lists(&db), Some(45), "{name}: one below");
        support::load_docs(&db, 2000 + share..=2000 + share, "a", gathered);
        support::cycle(&db);
        assert_eq!(state(&db), named("building", "changes"), "{name}");
        support::upkeep(&db);
        let groups = ((2000 + share) as f64).sqrt().round() as i64;
        assert_eq!(lists(&db), Some(groups), "{name}: grouped again");
    }

    // Slots rows have left: 10 in 100.
    let (_, db) = support::built("regroup-tombstones", GROUPED, AT_1000, 2000);
    support::run(&db, "DELETE FROM docs WHERE id < 200");
    support::upkeep(&db);
    assert_eq!(state(&db), named("idle", "none"), "199 slots left behind");
    support::run(&db, "DELETE FROM docs WHERE id = 200");
    support::cycle(&db);
    assert_eq!(state(&db), named("building", "tombstones"));

    // Under CONSOLIDATION NONE the first grouping stays, whatever changes.
    let clauses = format!("{AT_1000} CONSOLIDATION NONE");
    let (_, db) = support::built("regroup-none", GROUPED, &clauses, 2000);
    support::load_docs(&db, 2001..=3000, "a", gathered);
    support::run(&db, "DELETE FROM docs WHERE id <= 500");
    support::upkeep(&db);
    assert_eq!(state(&db), named("idle", "none"));
    assert_eq!(lists(&db), Some(45));
    let partition = support::the_partition(&db);
    assert_eq!(support::whole(&partition, "tombstones"), 500);
}

#[test]
fn a_cycle_under_a_tight_limit_records_the_stall_and_the_old_grouping_serves() {
    // A round over 22 centres of 512 numbers takes 135,168 bytes for its sums and centres;
    // the limit leaves 98,304 over what an open sets aside.
    const WIDE: usize = 512;
    const LIMIT: usize = (9 << 20) + (96 << 10);
    let (folder, db) = support::built("stalled-build", WIDE, "AUTO_INDEX_AT 200", 400);
    support::load_docs(&db, 401..=480, "a", |n| support::gathered(n, WIDE));
    // This cycle seals the 80 and begins the regrouping they make due.
    assert_eq!(support::cycle(&db).maintenance_vectors_done, 80);
    let stored = support::stored(&db, support::DOCS);
    support::close(db);

    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(LIMIT);
    let db = support::open_with(&folder, options).expect("open under the limit");
    let report = match db.run_maintenance_cycle() {
        Ok(report) => report.vector,
        Err(error) => panic!("a cycle whose step does not fit was refused: {error:?}"),
    };
    assert_eq!(
        (report.stalled_partitions, report.remaining_partitions),
        (1, 1)
    );
    let partition = support::the_partition(&db);
    assert_eq!(
        state_of(&partition),
        ("stalled".to_owned(), "memory_limit".to_owned())
    );
    assert_eq!(
        support::word(&partition, "recovery_action"),
        "raise the memory limit"
    );
    let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
    assert_eq!(support::word(&index, "maintenance_state"), "stalled");
    assert_eq!(support::whole(&index, "stalled_partitions"), 1);
    assert_eq!(
        support::word(&index, "recovery_action"),
        "raise the memory limit"
    );

    // The grouping in force still serves.
    assert_eq!(lists_of(&partition), Some(20));
    let query = support::gathered(60_003, WIDE);
    let result = support::answer(&db, &support::by_mode(None, "INDEXED", 10), &query);
    support::assert_sound(&result, &stored, |_| true, &query, 10, "while stalled");

    support::run(&db, "SET MEMORY_LIMIT 64M");
    support::upkeep(&db);
    let partition = support::the_partition(&db);
    assert_eq!(state_of(&partition), ("idle".to_owned(), "none".to_owned()));
    assert_eq!(lists_of(&partition), Some(22), "round(sqrt(480))");
    assert_eq!(support::cycle(&db).stalled_partitions, 0);
}
