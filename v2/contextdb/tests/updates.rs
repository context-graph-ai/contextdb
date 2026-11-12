//! What an UPDATE and a DELETE cost, said as bytes written and memory held. An UPDATE leaves a large value it does not assign where it
//! is; an UPDATE or DELETE of many rows holds one batch of them beside what it has staged,
//! never all of them twice; a row a statement has changed is not met by it again. And three
//! rules of writing rows: assigning what a statement finds its rows by, the columns ON CONFLICT
//! may name, and a long indexed value beside an assigned one.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{Database, DatabaseOpenOptions, Error, Value, params};
use support::{Call, Kind};

const MIB: usize = 1 << 20;

#[test]
fn an_update_of_another_column_leaves_a_large_value_where_it_is() {
    // A text and a JSON value that do not shrink, so each is stored in as many bytes as it has.
    const LEN: usize = 2 * MIB;
    let folder = support::folder("update-leaves-large-values");
    let written = Arc::new(AtomicU64::new(0));
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(support::storage({
        let written = written.clone();
        move |call| {
            if let Call::Write(_, bytes) = call {
                written.fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
        }
    }));
    let db = support::open_with(&folder, options).expect("open the store");
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT, meta JSON)",
    );
    support::run(&db, "CREATE INDEX docs_title ON docs (title)");
    let body = |id: i64| support::large_text(Kind::Scattered, id, LEN);
    let meta = |id: i64| support::large_json(false, id, LEN);
    for id in 1..=4_i64 {
        db.execute(
            "INSERT INTO docs (id, title, body, meta) VALUES ($id, $title, $body, $meta)",
            &params! { "id" => id, "title" => format!("title-{id}"), "body" => body(id), "meta" => meta(id) },
        )
        .expect("insert a document");
    }
    // The bytes the data file is written while `change` runs.
    let written_by = |change: &dyn Fn()| {
        let before = written.load(Ordering::SeqCst);
        change();
        written.load(Ordering::SeqCst) - before
    };
    let assert_doc = |id: i64, title: &str| {
        let found = db
            .execute(
                "SELECT title, body, meta FROM docs WHERE id = $id",
                &params! { "id" => id },
            )
            .expect("read a document");
        let [row] = found.rows() else {
            panic!("document {id} came back as {} rows", found.rows().len());
        };
        assert_eq!(row[0], support::text(title), "the title of document {id}");
        support::assert_text_is(&row[1], &body(id), &format!("the body of document {id}"));
        support::assert_json_is(&row[2], &meta(id), &format!("the meta of document {id}"));
    };

    // The door sees a rewrite: assigning the large value of row 4 writes at least its bytes.
    let rewritten = written_by(&|| {
        db.execute(
            "UPDATE docs SET body = $body WHERE id = 4",
            &params! { "body" => body(40) },
        )
        .expect("assign the large value");
    });
    assert!(
        rewritten >= LEN as u64,
        "assigning a text of {LEN} bytes wrote {rewritten} bytes"
    );

    // Assigning another column writes less than one of the row's two large values holds,
    // whether the row is found by its key, by an index or by reading the table, alone or in a
    // transaction with other statements.
    let by_key = written_by(&|| {
        support::run(&db, "UPDATE docs SET title = 'by key' WHERE id = 1");
    });
    assert!(
        by_key < LEN as u64,
        "an UPDATE of the title wrote {by_key} bytes beside two values of {LEN} bytes"
    );
    assert_doc(1, "by key");

    let by_index = written_by(&|| {
        support::run(
            &db,
            "UPDATE docs SET title = 'by index' WHERE title = 'title-2'",
        );
    });
    assert!(by_index < LEN as u64, "found by an index: {by_index} bytes");
    assert_doc(2, "by index");

    let every_row = written_by(&|| {
        let tx = db.transaction().expect("begin");
        let changed = support::run(&tx, "UPDATE docs SET title = 'all' WHERE id <= 3");
        assert_eq!(changed.rows_affected(), 3);
        support::run(
            &tx,
            "UPDATE docs SET title = 'all again' WHERE title = 'all'",
        );
        tx.commit().expect("commit");
    });
    assert!(
        every_row < LEN as u64,
        "two UPDATEs of the titles of three rows wrote {every_row} bytes"
    );
    for id in 1..=3 {
        assert_doc(id, "all again");
    }

    // An UPDATE that assigns one large value leaves the other.
    let one_of_two = written_by(&|| {
        db.execute(
            "UPDATE docs SET meta = $meta WHERE id = 4",
            &params! { "meta" => meta(41) },
        )
        .expect("assign the JSON value");
    });
    assert!(
        (LEN as u64..rewritten + LEN as u64 / 2).contains(&one_of_two),
        "assigning one of two values of {LEN} bytes wrote {one_of_two} bytes, and assigning \
         the other alone wrote {rewritten}"
    );
    let other = support::rows(&db, "SELECT body, meta FROM docs WHERE id = 4");
    support::assert_text_is(&other[0][0], &body(40), "the body of document 4");
    support::assert_json_is(&other[0][1], &meta(41), "the meta of document 4");

    // The values are where they were after reopening too.
    support::close(db);
    let db = support::open(&folder);
    let titles = support::rows(&db, "SELECT title FROM docs WHERE id <= 3 ORDER BY id");
    assert_eq!(titles, vec![[support::text("all again")]; 3]);
    let found = support::rows(&db, "SELECT body FROM docs WHERE id = 2");
    support::assert_text_is(&found[0][0], &body(2), "the body of document 2, reopened");
}

/// Asserts that a statement over many rows was not refused, and gives its result. A refusal by
/// the memory limit says who asked and for how much: that is what holding the rows twice
/// looks like.
fn fits(outcome: contextdb::Result<contextdb::QueryResult>, what: &str) -> contextdb::QueryResult {
    match outcome {
        Ok(result) => result,
        Err(Error::MemoryBudgetExceeded {
            operation,
            requested_bytes,
            available_bytes,
            ..
        }) => panic!(
            "{what} did not fit a limit with room for what it stages and an eighth more: \
             `{operation}` asked for {requested_bytes} bytes with {available_bytes} left"
        ),
        Err(other) => panic!("{what} was refused: {other:?}"),
    }
}

#[test]
fn an_update_and_a_delete_of_40_000_rows_hold_one_batch_beside_what_they_stage() {
    // Forty batches of a thousand rows: an eighth of what the statement stages is room for
    // five batches and for no more than an eighth of a second copy of the matched rows.
    const ROWS: i64 = 40_000;
    const ROOMY: usize = 512 * MIB;
    let folder = support::folder("many-rows-one-batch");
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(ROOMY);
    let db = support::open_with(&folder, options).expect("open the store under a limit");
    support::run(
        &db,
        "CREATE TABLE small (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, tag TEXT NOT NULL)",
    );
    // Row `id` holds `n = id % 1000`.
    for from in (0..ROWS).step_by(1000) {
        let rows: Vec<String> = (from..from + 1000)
            .map(|id| format!("({id}, {}, 'tag-{}')", id % 1000, id % 7))
            .collect();
        let tx = db.transaction().expect("begin");
        support::run(
            &tx,
            &format!("INSERT INTO small (id, n, tag) VALUES {}", rows.join(", ")),
        );
        tx.commit().expect("commit a thousand rows");
    }
    let sum = |on: &Database| support::int(on, "SELECT SUM(n) FROM small");
    let before = sum(&db);
    assert_eq!(before, ROWS / 1000 * (999 * 1000 / 2));

    // What a statement stages is held by its transaction until the commit. This runs the
    // statement in a transaction under the roomy limit, reads what the transaction holds once
    // the statement has returned, and rolls back.
    let staged_by = |sql: &str| -> usize {
        let tx = db.transaction().expect("begin");
        let idle = db.memory_used();
        let changed = fits(tx.execute(sql, &params! {}), sql);
        assert_eq!(changed.rows_affected(), ROWS as u64, "{sql}");
        let staged = db.memory_used() - idle;
        tx.rollback();
        staged
    };
    // Sets a limit with room for what the statement stages and an eighth of that again: one
    // batch of a thousand rows is a fortieth of the rows and fits in the eighth, and a second
    // copy of the 40,000 matched rows does not fit at all.
    let limit_for = |staged: usize| {
        assert!(
            staged > ROWS as usize * 8,
            "a statement over {ROWS} rows staged {staged} bytes"
        );
        let limit = db.memory_used() + staged + staged / 8;
        db.set_memory_limit(Some(limit)).expect("set the limit");
    };

    let update = "UPDATE small SET n = n + 1 WHERE tag <> 'none'";
    limit_for(staged_by(update));
    let changed = fits(db.execute(update, &params! {}), update);
    assert_eq!(changed.rows_affected(), ROWS as u64);
    // Each row was changed, and changed once.
    assert_eq!(sum(&db), before + ROWS);
    assert_eq!(support::int(&db, "SELECT MIN(n) FROM small"), 1);
    assert_eq!(support::int(&db, "SELECT MAX(n) FROM small"), 1000);
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM small"), ROWS);

    db.set_memory_limit(Some(ROOMY)).expect("lift the limit");
    let delete = "DELETE FROM small WHERE n > 0";
    limit_for(staged_by(delete));
    let removed = fits(db.execute(delete, &params! {}), delete);
    assert_eq!(removed.rows_affected(), ROWS as u64);
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM small"), 0);
    assert!(support::rows(&db, "SELECT id FROM small LIMIT 1").is_empty());
}

#[test]
fn an_update_or_delete_over_more_than_a_batch_changes_each_row_once_and_keeps_every_count() {
    // 1,200 rows are two batches (the first reads 1,024): a row the first batch moves lands
    // where the second still has to read, and a statement that met it again would move it again.
    const ROWS: i64 = 1200;
    let db = support::open(&support::folder("update-along-what-it-finds-by"));
    support::run(
        &db,
        "CREATE TABLE moves (k INTEGER PRIMARY KEY, origin INTEGER NOT NULL, \
         place INTEGER NOT NULL, g INTEGER NOT NULL, UNIQUE (g, k)) MAINTAIN COUNT (g)",
    );
    support::run(&db, "CREATE INDEX moves_place ON moves (place)");
    let tx = db.transaction().expect("begin");
    for n in 1..=ROWS {
        tx.execute(
            "INSERT INTO moves (k, origin, place, g) VALUES ($n, $n, $n, $g)",
            &params! { "n" => n, "g" => n % 7 },
        )
        .expect("insert a row");
    }
    tx.commit().expect("commit");
    // The rows the store should hold, as (k, origin, place, g), told apart by `origin`.
    let mut model: Vec<[i64; 4]> = (1..=ROWS).map(|n| [n, n, n, n % 7]).collect();
    // Asserts the rows, and that each count of `g` is the rows of that `g` and read none.
    let agree = |model: &[[i64; 4]], when: &str| {
        let wanted: Vec<Vec<Value>> = model
            .iter()
            .map(|r| vec![Value::Int64(r[1]), Value::Int64(r[0]), Value::Int64(r[2])])
            .collect();
        let found = support::rows(&db, "SELECT origin, k, place FROM moves ORDER BY origin");
        support::assert_same_rows(&found, &wanted, when);
        for g in 0..7_i64 {
            let count = support::run(&db, &format!("SELECT COUNT(*) FROM moves WHERE g = {g}"));
            let rows = model.iter().filter(|r| r[3] == g).count() as i64;
            assert_eq!(
                count.rows(),
                [[Value::Int64(rows)]],
                "{when}: count of g = {g}"
            );
            assert_eq!(
                count.rows_examined(),
                0,
                "{when}: rows read to count g = {g}"
            );
        }
    };

    // Each statement finds its rows by the key or by the index it assigns, and moves them
    // inside the range it reads; `Some(n)` is the delta to the key, `None` a DELETE.
    type Statement = (&'static str, fn(&[i64; 4]) -> bool, Option<(i64, i64)>);
    let statements: [Statement; 6] = [
        (
            "UPDATE moves SET k = k + 1000 WHERE k >= 1 AND k <= 1200",
            |_| true,
            Some((1000, 0)),
        ),
        (
            "UPDATE moves SET place = place + 1000 WHERE place >= 1 AND place <= 1200",
            |_| true,
            Some((0, 1000)),
        ),
        (
            "UPDATE moves SET place = place + 100 WHERE place > 0",
            |_| true,
            Some((0, 100)),
        ),
        (
            "UPDATE moves SET place = place + 300 WHERE place >= 1400 AND place < 2000",
            |r| (1400..2000).contains(&r[2]),
            Some((0, 300)),
        ),
        (
            "UPDATE moves SET place = place - 50 WHERE place > 50",
            |_| true,
            Some((0, -50)),
        ),
        (
            "DELETE FROM moves WHERE place >= 2100",
            |r| r[2] >= 2100,
            None,
        ),
    ];
    for (sql, matches, delta) in statements {
        let hit = model.iter().filter(|r| matches(r)).count() as u64;
        let done = support::run(&db, sql);
        assert_eq!(done.rows_affected(), hit, "rows changed by {sql}");
        match delta {
            Some((k, place)) => {
                for r in model.iter_mut().filter(|r| matches(r)) {
                    r[0] += k;
                    r[2] += place;
                }
            }
            None => model.retain(|r| !matches(r)),
        }
        assert!(hit > 0, "{sql} matched no row");
        agree(&model, sql);
        assert_eq!(
            support::int(&db, "SELECT COUNT(*) FROM moves"),
            model.len() as i64
        );
    }
    // Two statements in one transaction, downwards.
    let tx = db.transaction().expect("begin");
    for _ in 0..2 {
        let done = support::run(&tx, "UPDATE moves SET place = place - 50 WHERE place > 50");
        assert_eq!(done.rows_affected(), model.len() as u64);
        model.iter_mut().for_each(|r| r[2] -= 50);
    }
    tx.commit().expect("commit");
    agree(&model, "after two moves down in a transaction");
}

#[test]
fn paging_one_stream_by_its_two_column_key_reads_a_page_however_long_the_stream_is() {
    let db = support::open(&support::folder("paging-a-stream"));
    support::run(
        &db,
        "CREATE TABLE stream_events (stream TEXT NOT NULL, ordinal INTEGER NOT NULL, body TEXT, \
         PRIMARY KEY (stream, ordinal))",
    );
    // Adds the events `from..=to` to a stream.
    let append = |stream: &str, from: i64, to: i64| {
        let tx = db.transaction().expect("begin");
        for ordinal in from..=to {
            tx.execute(
                "INSERT INTO stream_events (stream, ordinal, body) VALUES ($stream, $ordinal, $body)",
                &params! { "stream" => stream, "ordinal" => ordinal, "body" => format!("{stream}-{ordinal}") },
            )
            .expect("insert an event");
        }
        tx.commit().expect("commit");
    };
    // The page of at most ten events of stream `b` after `committed`.
    let page = |committed: i64| {
        db.execute(
            "SELECT ordinal, body FROM stream_events WHERE stream = $stream AND ordinal > $committed \
             ORDER BY ordinal ASC LIMIT 10",
            &params! { "stream" => "b", "committed" => committed },
        )
        .expect("read a page of the stream")
    };
    let events = |ordinals: std::ops::RangeInclusive<i64>| -> Vec<Vec<Value>> {
        ordinals
            .map(|ordinal| vec![Value::Int64(ordinal), Value::Text(format!("b-{ordinal}"))])
            .collect()
    };
    // Streams on either side of `b` in key order, each with ordinals the pages ask for.
    append("a", 1, 4000);
    append("c", 1, 4000);

    // A thousand events, of which twenty are still to be read: the page is the first ten of
    // them and costs ten rows.
    append("b", 1, 1000);
    let first = page(980);
    support::assert_same_rows(first.rows(), &events(981..=990), "the page after 980");
    assert_eq!(first.rows_examined(), 10);

    // Two thousand more, read up to twenty before the end again: the same ten rows of work.
    append("b", 1001, 3000);
    let second = page(2980);
    support::assert_same_rows(second.rows(), &events(2981..=2990), "the page after 2980");
    assert_eq!(second.rows_examined(), first.rows_examined());

    // A tail shorter than the page ends at the stream's end, not in the next stream.
    let tail = page(2995);
    support::assert_same_rows(tail.rows(), &events(2996..=3000), "the tail of the stream");
    assert_eq!(tail.rows_examined(), 5);
    let end = page(3000);
    assert!(end.rows().is_empty());
    assert_eq!(end.rows_examined(), 0);
}

#[test]
fn an_update_of_one_indexed_column_leaves_a_long_value_of_another_found_by_equality() {
    // Values of 3,000 bytes that differ only in their last ten.
    let long = |id: i64| format!("{}{id:010}", "v".repeat(2990));
    let db = support::open(&support::folder("update-beside-a-long-indexed-value"));
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, label TEXT NOT NULL, body TEXT NOT NULL)",
    );
    support::run(&db, "CREATE INDEX notes_label ON notes (label)");
    support::run(&db, "CREATE INDEX notes_body ON notes (body)");
    for id in 1..=3_i64 {
        db.execute(
            "INSERT INTO notes (id, label, body) VALUES ($id, $label, $body)",
            &params! { "id" => id, "label" => format!("label-{id}"), "body" => long(id) },
        )
        .expect("insert a note with a long indexed value");
    }
    // The ids and labels of the notes whose body equals the long value of `id`.
    let by_body = |id: i64| {
        db.execute(
            "SELECT id, label FROM notes WHERE body = $body",
            &params! { "body" => long(id) },
        )
        .expect("read by the long value")
        .rows()
        .to_vec()
    };
    let note = |id: i64, label: &str| vec![vec![Value::Int64(id), support::text(label)]];
    for id in 1..=3 {
        support::assert_same_rows(
            &by_body(id),
            &note(id, &format!("label-{id}")),
            "as inserted",
        );
    }

    let changed = support::run(&db, "UPDATE notes SET label = 'moved' WHERE id = 2");
    assert_eq!(changed.rows_affected(), 1);
    let changed = support::run(
        &db,
        "UPDATE notes SET label = 'found' WHERE label = 'label-3'",
    );
    assert_eq!(changed.rows_affected(), 1);

    support::assert_same_rows(&by_body(1), &note(1, "label-1"), "the note left alone");
    support::assert_same_rows(&by_body(2), &note(2, "moved"), "the note changed by key");
    support::assert_same_rows(&by_body(3), &note(3, "found"), "the note changed by index");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 3);
    assert_eq!(
        support::int(&db, "SELECT id FROM notes WHERE label = 'moved'"),
        2
    );
    assert!(support::rows(&db, "SELECT id FROM notes WHERE label = 'label-2'").is_empty());
    let body = support::rows(&db, "SELECT body FROM notes WHERE id = 2");
    assert_eq!(body, vec![vec![support::text(&long(2))]]);
}

const KEYED: &str = "SELECT k, v FROM t ORDER BY k";

/// A store with `t`, keyed by `k`: rows 1 `one`, 2 `two` and 3 `three`.
fn store_of_three_keys(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::run(
        &db,
        "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT NOT NULL)",
    );
    support::run(
        &db,
        "INSERT INTO t (k, v) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    );
    db
}

#[test]
fn an_update_that_gives_two_rows_one_key_is_a_unique_violation_and_changes_nothing() {
    let db = store_of_three_keys("two-rows-one-key");
    let before = support::rows(&db, KEYED);
    match support::refusal(&db, "UPDATE t SET k = 7 WHERE k IN (1, 2)") {
        error @ Error::UniqueViolation { .. } => assert_eq!(
            error.to_string(),
            "unique constraint violation: t (k) = (7)",
            "the refusal says which key two rows were given, as Postgres does"
        ),
        other => panic!("an UPDATE that gives two rows the key 7 was refused as {other:?}"),
    }
    assert_eq!(support::rows(&db, KEYED), before);
    assert!(support::rows(&db, "SELECT v FROM t WHERE k = 7").is_empty());
    assert_eq!(
        support::rows(&db, "SELECT v FROM t WHERE k = 2"),
        [[support::text("two")]]
    );
}
