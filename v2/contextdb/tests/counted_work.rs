//! Counted work: how fast a statement is, said as the number of stored rows it reads. Each
//! statement here must read exactly the rows its answer needs, whatever the size of the table.

mod support;

use std::path::PathBuf;

use contextdb::{Database, ReadSession, ReadSessionOptions, Value, params};

/// A store with two tables of a thousand rows each.
///
/// `t` is the usual table, with its index on `tag` and one more on `name`. `events` has an
/// index over two columns, the second one descending: event `n` is
/// `(n, "k<n % 5>", n, "note-<n>")`.
fn store(name: &str) -> (PathBuf, Database) {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::declare(&db);
    support::run(&db, "CREATE INDEX t_name ON t (name)");
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, seq INTEGER NOT NULL, \
         note TEXT)",
    );
    support::run(
        &db,
        "CREATE INDEX events_kind_seq ON events (kind ASC, seq DESC)",
    );
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1000);
    for n in 1..=1000_i64 {
        tx.execute(
            "INSERT INTO events (id, kind, seq, note) VALUES ($id, $kind, $seq, $note)",
            &params! {
                "id" => n, "kind" => format!("k{}", n % 5), "seq" => n, "note" => format!("note-{n}"),
            },
        )
        .expect("insert an event");
    }
    tx.commit().expect("commit");
    (folder, db)
}

/// The id and note of an event, as a statement returns them.
fn event(n: i64) -> [Value; 2] {
    [Value::Int64(n), Value::Text(format!("note-{n}"))]
}

#[test]
fn count_of_a_whole_table_reads_no_rows() {
    let (_, db) = store("count-reads-no-rows");
    let count = support::run(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(count.rows(), [[Value::Int64(1000)]]);
    assert_eq!(count.rows_examined(), 0);

    // The count follows the table, and still reads nothing.
    support::run(&db, "DELETE FROM t WHERE id <= 10");
    let count = support::run(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(count.rows(), [[Value::Int64(990)]]);
    assert_eq!(count.rows_examined(), 0);

    // It is the number a scan finds, and the scan does read every row.
    let scan = support::run(&db, support::SCAN);
    assert_eq!(scan.rows().len(), 990);
    assert_eq!(scan.rows_examined(), 990);
}

#[test]
fn a_lookup_by_primary_key_reads_one_row() {
    let (_, db) = store("key-lookup-reads-one");
    for id in [1_i64, 500, 1000] {
        let found = db
            .execute(
                "SELECT id, name, tag FROM t WHERE id = $id",
                &params! { "id" => id },
            )
            .expect("look a row up by its key");
        assert_eq!(found.rows(), [support::row(id)]);
        assert_eq!(found.rows_examined(), 1, "looking up key {id}");
    }
    // A key written into the statement is found the same way.
    let found = support::run(&db, "SELECT id, name, tag FROM t WHERE id = 500");
    assert_eq!(found.rows(), [support::row(500)]);
    assert_eq!(found.rows_examined(), 1);

    // A key that is not there is known to be missing without reading any row.
    let missing = support::run(&db, "SELECT id, name, tag FROM t WHERE id = 5000");
    assert!(missing.rows().is_empty());
    assert_eq!(missing.rows_examined(), 0);
}

#[test]
fn paging_by_key_reads_one_page_of_rows() {
    let (folder, _db) = store("paging-by-key");
    // Through a reader with a limit of 16 MiB, beside the writer.
    let mut options = ReadSessionOptions::default();
    options.memory_limit = Some(16 << 20);
    let reader =
        ReadSession::open_with_options(&folder, options).expect("open the store for reading");
    let page = "SELECT id, name, tag FROM t WHERE id > $last ORDER BY id LIMIT 100";

    // Every page costs a page of rows, however many pages came before it.
    for last in (0..1000_i64).step_by(100) {
        let rows = reader
            .execute(page, &params! { "last" => last })
            .expect("read a page");
        let wanted: Vec<_> = (last + 1..=last + 100).map(support::row).collect();
        support::assert_same_rows(rows.rows(), &wanted, &format!("the page after key {last}"));
        assert_eq!(rows.rows_examined(), 100, "the page after key {last}");
    }
    // After the last row there is nothing, and finding that out reads nothing.
    let end = reader
        .execute(page, &params! { "last" => 1000_i64 })
        .expect("read past the end");
    assert!(end.rows().is_empty());
    assert_eq!(end.rows_examined(), 0);
}

#[test]
fn an_indexed_filter_reads_only_the_rows_it_returns() {
    let (_, db) = store("indexed-filter");

    // An index on one column: a tenth of the table has the tag, and a tenth is read.
    let tagged = support::run(&db, "SELECT id, name FROM t WHERE tag = 3 ORDER BY id");
    let wanted: Vec<_> = (0..100)
        .map(|n| support::row(n * 10 + 3)[..2].to_vec())
        .collect();
    support::assert_same_rows(tagged.rows(), &wanted, "the rows with tag 3");
    assert_eq!(tagged.rows_examined(), 100);

    // An index on two columns, filtered by the first: a fifth of the table.
    let of_kind = support::run(
        &db,
        "SELECT id, note FROM events WHERE kind = 'k2' ORDER BY id",
    );
    let wanted: Vec<_> = (0..200).map(|n| event(n * 5 + 2)).collect();
    support::assert_same_rows(of_kind.rows(), &wanted, "the events of kind k2");
    assert_eq!(of_kind.rows_examined(), 200);

    // Filtered by both: the one row.
    let one = support::run(
        &db,
        "SELECT id, note FROM events WHERE kind = 'k2' AND seq = 502",
    );
    assert_eq!(one.rows(), [event(502)]);
    assert_eq!(one.rows_examined(), 1);

    // The newest event of a kind is the first entry the index has for that kind.
    let newest = support::run(
        &db,
        "SELECT id, note FROM events WHERE kind = 'k2' ORDER BY seq DESC LIMIT 1",
    );
    assert_eq!(newest.rows(), [event(997)]);
    assert_eq!(newest.rows_examined(), 1);
}

#[test]
fn a_sort_the_index_already_gives_reads_only_the_limit() {
    let (_, db) = store("sort-from-index");
    let first_ten: Vec<_> = (1..=10).map(|n| support::row(n)[..2].to_vec()).collect();

    // In key order, and in the order of an indexed column.
    for sql in [
        "SELECT id, name FROM t ORDER BY id LIMIT 10",
        "SELECT id, name FROM t ORDER BY name LIMIT 10",
    ] {
        let rows = support::run(&db, sql);
        support::assert_same_rows(rows.rows(), &first_ten, sql);
        assert_eq!(rows.rows_examined(), 10, "{sql}");
    }

    // In the order an index of two columns keeps within one value of its first column.
    let newest = support::run(
        &db,
        "SELECT id, note FROM events WHERE kind = 'k2' ORDER BY seq DESC LIMIT 5",
    );
    assert_eq!(
        newest.rows(),
        [event(997), event(992), event(987), event(982), event(977)]
    );
    assert_eq!(newest.rows_examined(), 5);

    // An order that no index gives has to read the whole table to find its first rows.
    let sorted = support::run(&db, "SELECT id, note FROM events ORDER BY seq LIMIT 5");
    assert_eq!(
        sorted.rows(),
        [event(1), event(2), event(3), event(4), event(5)]
    );
    assert_eq!(sorted.rows_examined(), 1000);
}
