//! Counts with no row read. A table keeps the counts it declares with `MAINTAIN COUNT`, and a
//! `COUNT(*)` whose filter gives a declared list's columns is one stored number. A table that
//! declares none counts the entries of the one index range the filter gives. Either way the
//! count is the number of rows the same filter returns, and the statement reads no row.

mod support;

use contextdb::{Database, Error, Value, params};
use support::Run;

const MIB: usize = 1 << 20;

/// The filters every check asks: each `ctx`, and each `ctx` with each `conn`, values that no
/// row has among them.
fn filters() -> Vec<String> {
    let mut filters = Vec::new();
    for ctx in 0..=4_i64 {
        filters.push(format!("ctx = {ctx}"));
        for conn in ["a", "b", "zz"] {
            filters.push(format!("ctx = {ctx} AND conn = '{conn}'"));
        }
    }
    filters
}

/// Asserts that every count of `events` equals the number of rows its filter returns, and
/// returns the rows each count read, in the order of [`filters`]. The whole table's count is
/// checked the same way.
fn counts_agree(on: &impl Run, when: &str) -> Vec<u64> {
    let mut read = Vec::new();
    for filter in filters() {
        let rows = support::rows(on, &format!("SELECT id FROM events WHERE {filter}"));
        let count = support::run(on, &format!("SELECT COUNT(*) FROM events WHERE {filter}"));
        assert_eq!(
            count.rows(),
            [[Value::Int64(rows.len() as i64)]],
            "{when}: COUNT(*) WHERE {filter}, beside the {} rows the filter returns",
            rows.len()
        );
        read.push(count.rows_examined());
    }
    let rows = support::rows(on, "SELECT id FROM events");
    assert_eq!(
        support::int(on, "SELECT COUNT(*) FROM events"),
        rows.len() as i64,
        "{when}: the count of the whole table"
    );
    read
}

/// Asserts the same, and that no count read a row.
fn counts_agree_and_read_nothing(db: &Database, when: &str) {
    let read = counts_agree(db, when);
    for (filter, rows) in filters().iter().zip(read) {
        assert_eq!(rows, 0, "{when}: rows read by COUNT(*) WHERE {filter}");
    }
}

/// Event `n`: its `ctx` is `n % 4` and its `conn` is `a` or `b`.
fn insert_events(on: &impl Run, numbers: impl IntoIterator<Item = i64>) {
    for n in numbers {
        on.run(
            "INSERT INTO events (id, ctx, conn, note) VALUES ($id, $ctx, $conn, $note)",
            &params! {
                "id" => n, "ctx" => n % 4, "conn" => if n % 3 == 0 { "a" } else { "b" },
                "note" => format!("note-{n}"),
            },
        )
        .expect("insert an event");
    }
}

/// Puts `events` through inserts, updates that move rows between counted groups and updates
/// that do not, deletes, a refused statement, a rolled-back transaction and a transaction that
/// another commit overtook, and asserts after each that every count is the number of rows and
/// read none.
fn counts_follow_every_write(db: &Database) {
    counts_agree_and_read_nothing(db, "on the empty table");
    let tx = db.transaction().expect("begin");
    insert_events(&tx, 1..=600);
    // Inside the transaction that wrote the rows the counts are theirs already.
    counts_agree(&tx, "inside the transaction that inserted");
    tx.commit().expect("commit the events");
    counts_agree_and_read_nothing(db, "after inserts");

    // Rows move from one counted group to another, by either counted column and by both.
    support::run(db, "UPDATE events SET ctx = 0 WHERE id <= 50");
    support::run(
        db,
        "UPDATE events SET conn = 'a' WHERE ctx = 2 AND id > 500",
    );
    support::run(db, "UPDATE events SET ctx = 3, conn = 'b' WHERE id > 550");
    counts_agree_and_read_nothing(db, "after updates of counted columns");
    // A change to a column no count is over, and a change of a row's key.
    support::run(db, "UPDATE events SET note = 'seen' WHERE ctx = 1");
    support::run(db, "UPDATE events SET id = id + 1000 WHERE id <= 20");
    counts_agree_and_read_nothing(db, "after updates of other columns");

    support::run(db, "DELETE FROM events WHERE ctx = 3 AND conn = 'a'");
    support::run(db, "DELETE FROM events WHERE id > 100 AND id <= 130");
    counts_agree_and_read_nothing(db, "after deletes");

    // A statement that is refused at its last row counted none of its rows.
    let refusal = support::refusal(
        db,
        "INSERT INTO events (id, ctx, conn) VALUES (2001, 1, 'a'), (2002, 1, 'a'), (200, 1, 'a')",
    );
    assert!(
        matches!(refusal, Error::UniqueViolation { .. }),
        "{refusal:?}"
    );
    let refusal = support::refusal(db, "UPDATE events SET ctx = NULL WHERE ctx = 1");
    assert!(
        matches!(refusal, Error::ColumnNotNullable { .. }),
        "{refusal:?}"
    );
    counts_agree_and_read_nothing(db, "after refused statements");

    // A transaction that is rolled back counted nothing, whatever it had staged.
    let tx = db.transaction().expect("begin");
    insert_events(&tx, 3001..=3050);
    support::run(&tx, "DELETE FROM events WHERE ctx = 0");
    support::run(&tx, "UPDATE events SET ctx = 0 WHERE ctx = 2");
    counts_agree(&tx, "inside the transaction that is rolled back");
    tx.rollback();
    counts_agree_and_read_nothing(db, "after a rollback");

    // Two transactions add to one group side by side: the group counts both.
    let tx = db.transaction().expect("begin");
    insert_events(&tx, [4001, 4005]);
    support::run(&tx, "DELETE FROM events WHERE id = 201");
    insert_events(db, [4009, 4013]);
    support::run(db, "DELETE FROM events WHERE id = 205");
    tx.commit().expect("commit beside another commit");
    counts_agree_and_read_nothing(db, "after two commits side by side");
}

#[test]
fn a_kept_count_is_the_number_of_rows_after_every_kind_of_write_and_reads_no_row() {
    let folder = support::folder("kept-counts");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, ctx INTEGER NOT NULL, conn TEXT NOT NULL, \
         note TEXT, UNIQUE (ctx, id), UNIQUE (ctx, conn, id)) \
         MAINTAIN COUNT (ctx), (ctx, conn)",
    );
    counts_follow_every_write(&db);

    // The counts are in the store: they are the same after reopening, and still read no row.
    support::close(db);
    let db = support::open(&folder);
    counts_agree_and_read_nothing(&db, "after reopening");
    // The filter that finds the rows reads them; the count beside it does not.
    let rows = support::run(&db, "SELECT id FROM events WHERE ctx = 1");
    assert!(rows.rows().len() > 100);
    assert_eq!(rows.rows_examined(), rows.rows().len() as u64);
}

#[test]
fn a_table_that_declares_no_count_counts_an_index_range_and_reads_no_row() {
    let db = support::open(&support::folder("counted-index-range"));
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, ctx INTEGER NOT NULL, conn TEXT NOT NULL, \
         note TEXT)",
    );
    support::run(&db, "CREATE INDEX events_ctx_conn ON events (ctx, conn)");
    counts_follow_every_write(&db);

    // A count whose filter the index does not pin has to read rows: by the index's second
    // column alone, and with a term over a column the index does not have.
    for filter in ["conn = 'a'", "ctx = 1 AND note = 'seen'"] {
        let rows = support::rows(&db, &format!("SELECT id FROM events WHERE {filter}"));
        let count = support::run(&db, &format!("SELECT COUNT(*) FROM events WHERE {filter}"));
        assert_eq!(
            count.rows(),
            [[Value::Int64(rows.len() as i64)]],
            "{filter}"
        );
        assert!(!rows.is_empty(), "{filter} returns no row");
        assert!(
            count.rows_examined() >= rows.len() as u64,
            "COUNT(*) WHERE {filter} read {} rows and counted {}",
            count.rows_examined(),
            rows.len()
        );
    }
}

/// The declaration of `groups`: an index on `tag`, and no kept count.
fn declare_groups(db: &Database) {
    support::run(
        db,
        "CREATE TABLE groups (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL)",
    );
    support::run(db, "CREATE INDEX groups_tag ON groups (tag)");
}

#[test]
fn a_new_set_of_counts_on_a_table_with_rows_is_installed_whole_or_not_at_all() {
    const ROWS: i64 = 10_000;
    let folder = support::folder("kept-counts-installed");
    // The page cache is sized when the store is opened, so the limit of the second half is
    // set on a store that was opened under a larger one.
    let mut options = contextdb::DatabaseOpenOptions::default();
    options.memory_limit = Some(256 * MIB);
    let db = support::open_with(&folder, options).expect("open the store under a limit");
    declare_groups(&db);
    // Every row is a group of its own, so the new counts are as many as the rows.
    for from in (0..ROWS).step_by(1000) {
        let values: Vec<String> = (from..from + 1000).map(|n| format!("({n}, {n})")).collect();
        support::run(
            &db,
            &format!("INSERT INTO groups (id, tag) VALUES {}", values.join(", ")),
        );
    }
    let declared = db.declaration("groups").expect("the declaration of groups");
    let count_of = |tag: i64| {
        let count = support::run(
            &db,
            &format!("SELECT COUNT(*) FROM groups WHERE tag = {tag}"),
        );
        assert_eq!(count.rows_examined(), 0, "the count of tag {tag}");
        match count.rows() {
            [row] => row[0].clone(),
            rows => panic!("a count came back as {} rows", rows.len()),
        }
    };

    // With 64 KiB to spare, the 10,000 new counts do not fit: the statement is the memory
    // limit's refusal, the declaration is the one the table had, and its counts still answer.
    let limit = db.memory_used() + (64 << 10);
    db.set_memory_limit(Some(limit)).expect("set the limit");
    match support::refusal(&db, "ALTER TABLE groups SET MAINTAIN COUNT (tag)") {
        Error::MemoryBudgetExceeded {
            budget_limit_bytes, ..
        } => assert_eq!(budget_limit_bytes, limit as u64),
        other => panic!("counts that do not fit the limit were refused as {other:?}"),
    }
    assert_eq!(
        db.declaration("groups").expect("the declaration of groups"),
        declared
    );
    assert_eq!(count_of(7), Value::Int64(1));
    assert_eq!(count_of(ROWS), Value::Int64(0));

    // With room, the same statement installs the counts, and they are right at once.
    db.set_memory_limit(Some(256 * MIB))
        .expect("lift the limit");
    support::run(&db, "ALTER TABLE groups SET MAINTAIN COUNT (tag)");
    let installed = db.declaration("groups").expect("the declaration of groups");
    assert!(
        installed.contains("MAINTAIN COUNT (tag)"),
        "the declaration does not say MAINTAIN COUNT (tag):\n{installed}"
    );
    support::run(&db, "UPDATE groups SET tag = 7 WHERE id < 10");
    assert_eq!(count_of(7), Value::Int64(10));
    assert_eq!(count_of(3), Value::Int64(0));
    assert_eq!(count_of(ROWS - 1), Value::Int64(1));

    // Taking the counts away leaves the same answers, and the declaration it had before.
    support::run(&db, "ALTER TABLE groups SET MAINTAIN COUNT DEFAULT");
    assert_eq!(
        db.declaration("groups").expect("the declaration of groups"),
        declared
    );
    assert_eq!(count_of(7), Value::Int64(10));
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM groups WHERE tag = 7"),
        10
    );
}

#[test]
fn a_count_list_that_cannot_stand_is_refused_by_name_and_declares_nothing() {
    let db = support::open(&support::folder("kept-count-refusals"));
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER NOT NULL, \
         c INTEGER NOT NULL, UNIQUE (b, c))",
    );
    support::run(&db, "CREATE INDEX t_by_b ON t (b)");
    let declared = db.declaration("t").expect("the declaration of t");
    let reason_of = |sql: &str| match support::refusal(&db, sql) {
        Error::SchemaInvalid { reason } => reason,
        other => panic!("{sql}\nwas refused as {other:?}"),
    };
    let says = |reason: &str, words: &[&str]| {
        for word in words {
            assert!(
                reason.contains(word),
                "the refusal does not say `{word}`: {reason}"
            );
        }
    };

    // No index begins with the list: the refusal names the list and the indexes it looked at,
    // the engine's own among them.
    let reason = reason_of("ALTER TABLE t SET MAINTAIN COUNT (a)");
    says(&reason, &["(a)", "t_by_b", "__pk_id"]);
    let reason = reason_of("ALTER TABLE t SET MAINTAIN COUNT (b), (c, b)");
    says(&reason, &["(c, b)", "t_by_b"]);
    // A key or a UNIQUE rule already holds the list to one row at most.
    for list in ["(id)", "(b, c)"] {
        let reason = reason_of(&format!("ALTER TABLE t SET MAINTAIN COUNT {list}"));
        says(&reason, &[list]);
    }
    // The same list twice, and one column twice in a list.
    let reason = reason_of("ALTER TABLE t SET MAINTAIN COUNT (b), (b)");
    says(&reason, &["(b)"]);
    let reason = reason_of("ALTER TABLE t SET MAINTAIN COUNT (b, b)");
    says(&reason, &["b"]);
    // A column the table does not have.
    match support::refusal(&db, "ALTER TABLE t SET MAINTAIN COUNT (b, nope)") {
        Error::ColumnNotFound { table, column } => {
            assert_eq!((table.as_str(), column.as_str()), ("t", "nope"));
        }
        other => panic!("a count over a column that is not there was refused as {other:?}"),
    }
    assert_eq!(db.declaration("t").expect("the declaration of t"), declared);
}

/// Asserts that every count of `notes` equals the number of rows its filter returns: each
/// `ctx`, and each `ctx` with no tag, with the tag `long` and with the tag `s`.
fn note_counts_agree(db: &Database, long: &str, when: &str) {
    for ctx in 1..=3_i64 {
        let filters = [
            (format!("ctx = {ctx}"), params! {}),
            (format!("ctx = {ctx} AND tag IS NULL"), params! {}),
            (
                format!("ctx = {ctx} AND tag = $tag"),
                params! { "tag" => long },
            ),
            (
                format!("ctx = {ctx} AND tag = $tag"),
                params! { "tag" => "s" },
            ),
        ];
        for (filter, given) in filters {
            let rows = db
                .run(&format!("SELECT id FROM notes WHERE {filter}"), &given)
                .expect("the rows of a filter");
            let count = db
                .run(
                    &format!("SELECT COUNT(*) FROM notes WHERE {filter}"),
                    &given,
                )
                .expect("the count of a filter");
            assert_eq!(
                count.rows(),
                [[Value::Int64(rows.rows().len() as i64)]],
                "{when}: COUNT(*) WHERE {filter}, tag of {} bytes",
                given.get("tag").map_or(0, |tag| match tag {
                    Value::Text(tag) => tag.len(),
                    _ => 0,
                })
            );
        }
    }
}

#[test]
fn a_count_over_a_null_and_over_a_value_past_1024_bytes_stays_equal_to_the_rows() {
    let db = support::open(&support::folder("kept-count-null-and-long"));
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, ctx INTEGER NOT NULL, tag TEXT)",
    );
    support::run(&db, "CREATE INDEX notes_ctx_tag ON notes (ctx, tag)");
    support::run(
        &db,
        "ALTER TABLE notes SET MAINTAIN COUNT (ctx), (ctx, tag)",
    );
    let long = support::letters(7, 1500);
    // Note `n` is in ctx 1 or 2 and has no tag, the long tag or a short one.
    let insert = |n: i64, tag: Value| {
        db.execute(
            "INSERT INTO notes (id, ctx, tag) VALUES ($id, $ctx, $tag)",
            &params! { "id" => n, "ctx" => n % 2 + 1, "tag" => tag },
        )
    };
    for n in 1..=12_i64 {
        let tag = match n % 3 {
            0 => Value::Null,
            1 => Value::Text(long.clone()),
            _ => Value::Text("s".to_owned()),
        };
        insert(n, tag).expect("insert a note");
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 12);
    assert_eq!(
        support::int(
            &db,
            "SELECT COUNT(*) FROM notes WHERE ctx = 1 AND tag IS NULL"
        ),
        2,
        "notes 6 and 12"
    );
    let of_long = db
        .execute(
            "SELECT COUNT(*) FROM notes WHERE ctx = 2 AND tag = $tag",
            &params! { "tag" => long.as_str() },
        )
        .expect("the count of the long tag");
    assert_eq!(of_long.rows(), [[Value::Int64(2)]], "notes 1 and 7");
    note_counts_agree(&db, &long, "after the inserts");

    // One note of each kind goes, then every note of ctx 1 that has no tag.
    support::run(&db, "DELETE FROM notes WHERE id <= 3");
    note_counts_agree(&db, &long, "after deleting notes 1 to 3");
    support::run(&db, "DELETE FROM notes WHERE ctx = 1 AND tag IS NULL");
    note_counts_agree(&db, &long, "after deleting the notes of ctx 1 with no tag");
    db.execute(
        "DELETE FROM notes WHERE tag = $tag",
        &params! { "tag" => long.as_str() },
    )
    .expect("delete the notes with the long tag");
    note_counts_agree(&db, &long, "after deleting the notes with the long tag");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 4);

    // A count's values identify a group and are kept whole: a tag past the limit is refused
    // by name, and counts nothing.
    match insert(13, Value::Text(support::letters(8, 3000))) {
        Err(Error::KeyTooLong {
            table,
            key,
            bytes,
            limit,
        }) => {
            assert_eq!(table, "notes");
            assert!(
                key.contains("tag"),
                "the count that refused is named: {key}"
            );
            assert!(bytes > limit, "{bytes} bytes against a limit of {limit}");
        }
        other => panic!("a tag of 3,000 bytes in a kept count: {other:?}"),
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 4);
    note_counts_agree(&db, &long, "after the refused insert");
}

#[test]
fn create_index_on_a_table_that_keeps_all_prefixes_fills_the_new_count_from_its_rows() {
    let db = support::open(&support::folder("kept-count-new-index"));
    support::run(
        &db,
        "CREATE TABLE every (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b, id)) \
         MAINTAIN COUNT ALL PREFIXES",
    );
    support::run(
        &db,
        "INSERT INTO every (id, a, b) VALUES (1, 1, 7), (2, 2, 7), (3, 3, 7), (4, 1, 8)",
    );
    // The index gives the table a new beginning, `b`, and so a new count, over rows that
    // are already there.
    support::run(&db, "CREATE INDEX every_b ON every (b)");
    for (b, rows) in [(7, 3), (8, 1), (9, 0)] {
        let count = support::run(&db, &format!("SELECT COUNT(*) FROM every WHERE b = {b}"));
        assert_eq!(count.rows(), [[Value::Int64(rows)]], "rows of b = {b}");
        assert_eq!(count.rows_examined(), 0, "rows read to count b = {b}");
    }
}
