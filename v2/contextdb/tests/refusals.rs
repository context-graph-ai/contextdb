//! Refusals: each one comes as its kind with the fields that say what was refused, and leaves
//! the store as it was.

mod support;

use contextdb::serde_json::json;
use contextdb::{ColumnType, Error, ReadSession, Value, params};

#[test]
fn a_unique_refusal_names_the_table_and_every_column_of_the_rule() {
    let folder = support::folder("unique-refusal");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, UNIQUE (a, b, c))",
    );
    support::run(&db, "INSERT INTO t (id, a, b, c) VALUES (1, 1, 'x', 1)");
    // One column of the three differs: not a duplicate.
    support::run(&db, "INSERT INTO t (id, a, b, c) VALUES (2, 1, 'x', 2)");

    match support::refusal(&db, "INSERT INTO t (id, a, b, c) VALUES (3, 1, 'x', 1)") {
        Error::UniqueViolation { table, column, .. } => {
            assert_eq!((table.as_str(), column.as_str()), ("t", "a, b, c"));
        }
        other => panic!("a duplicate under UNIQUE (a, b, c) was refused as {other:?}"),
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn a_second_create_table_names_the_table_and_changes_nothing() {
    let folder = support::folder("second-create-table");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)",
    );
    support::run(
        &db,
        "INSERT INTO ledger (id, entry) VALUES (1, 'first'), (2, 'second')",
    );
    let declared = db
        .declaration("ledger")
        .expect("the declaration of a declared table");
    let entries = "SELECT id, entry FROM ledger ORDER BY id";
    let stored = support::rows(&db, entries);

    // Refused whether the second declaration differs from the first or repeats it.
    for sql in [
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT NOT NULL)",
    ] {
        match support::refusal(&db, sql) {
            Error::SchemaInvalid { reason } => {
                assert!(
                    reason.contains("ledger"),
                    "the refusal does not name the table: {reason}"
                );
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
    assert_eq!(db.table_names(), ["ledger"]);
    assert_eq!(db.declaration("ledger").expect("the declaration"), declared);
    assert_eq!(support::rows(&db, entries), stored);
    assert_eq!(stored.len(), 2);
}

#[test]
fn a_parse_refusal_names_the_offending_text_the_same_for_a_writer_and_a_reader() {
    let folder = support::folder("parse-refusal");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);
    let reader = ReadSession::open(&folder).expect("open the store for reading");

    for (sql, offending) in [("SELEKT id FROM t", "SELEKT"), ("SELECT id FRUM t", "FRUM")] {
        let from_writer = match support::refusal(&db, sql) {
            Error::ParseError(text) => text,
            other => panic!("{sql}\nwas refused by the writer as {other:?}"),
        };
        let from_reader = match support::refusal(&reader, sql) {
            Error::ParseError(text) => text,
            other => panic!("{sql}\nwas refused by the reader as {other:?}"),
        };
        assert!(
            from_writer.contains(offending),
            "{sql}\nwas refused without naming {offending}: {from_writer}"
        );
        assert_eq!(
            from_writer, from_reader,
            "{sql}\nis refused in different words by a reader"
        );
    }
    assert_eq!(db.table_names(), ["t"]);
    support::assert_rows_are(&db, 1..=3);
}

#[test]
fn text_that_is_not_json_is_refused_in_a_json_column() {
    let folder = support::folder("not-json");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON, shape VECTOR(3))",
    );
    support::run(&db, r#"INSERT INTO docs (id, doc) VALUES (1, '{"k": 1}')"#);
    let not_json = "{ this is not json";

    let refusals = [
        // On the way in, written into the statement and bound.
        db.execute(
            &format!("INSERT INTO docs (id, doc) VALUES (2, '{not_json}')"),
            &params! {},
        ),
        db.execute(
            "INSERT INTO docs (id, doc) VALUES ($id, $doc)",
            &params! { "id" => 2_i64, "doc" => not_json },
        ),
        // In a change to a stored row.
        db.execute(
            &format!("UPDATE docs SET doc = '{not_json}' WHERE id = 1"),
            &params! {},
        ),
    ];
    for refusal in refusals {
        match refusal {
            Err(Error::ColumnTypeMismatch { table, column, .. }) => {
                assert_eq!((table.as_str(), column.as_str()), ("docs", "doc"));
            }
            other => panic!("text that is not JSON got {other:?} in a JSON column"),
        }
    }
    // The one row is there and holds what it held.
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM docs"), 1);
    assert_eq!(
        support::rows(&db, "SELECT id, doc FROM docs ORDER BY id"),
        [[Value::Int64(1), Value::Json(json!({ "k": 1 }))]]
    );

    // Neither a JSON nor a VECTOR column takes an index. The refusal says which column and of
    // what type, and the type is one a caller can name.
    let declared = db
        .declaration("docs")
        .expect("the declaration of a declared table");
    for (column, column_type) in [("doc", ColumnType::Json), ("shape", ColumnType::Vector(3))] {
        match support::refusal(
            &db,
            &format!("CREATE INDEX docs_{column} ON docs ({column})"),
        ) {
            Error::ColumnNotIndexable {
                table,
                column: refused,
                column_type: of_type,
            } => {
                assert_eq!((table.as_str(), refused.as_str()), ("docs", column));
                assert_eq!(of_type, column_type);
            }
            other => panic!("an index over the {column} column was refused as {other:?}"),
        }
    }
    assert_eq!(db.declaration("docs").expect("the declaration"), declared);
}

#[test]
fn a_read_session_refuses_a_statement_that_writes() {
    let folder = support::folder("reader-refuses-writes");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);
    // The store keeps a declared memory limit, so declaring one is a write as well.
    let limit = 512 << 20;
    db.set_memory_limit(Some(limit)).expect("declare a limit");
    let reader = ReadSession::open(&folder).expect("open the store for reading");

    for sql in [
        "INSERT INTO t (id, name, tag) VALUES (4, 'row-0000004', 4)",
        "CREATE TABLE made (id INTEGER PRIMARY KEY)",
        "SET MEMORY_LIMIT 2G",
        "SET MEMORY_LIMIT NONE",
    ] {
        let refusal = support::refusal(&reader, sql);
        assert!(
            matches!(refusal, Error::ReadOnlySession),
            "{sql}\nwas refused as {refusal:?}"
        );
    }
    // The session still reads, and the writer sees nothing changed.
    support::assert_rows_are(&reader, 1..=3);
    assert_eq!(db.table_names(), ["t"]);
    support::assert_rows_are(&db, 1..=3);
    // The limit the store keeps is the one the writer declared, now and after reopening.
    assert_eq!(db.memory_limit(), Some(limit));
    support::close(db);
    assert_eq!(support::open(&folder).memory_limit(), Some(limit));
}
