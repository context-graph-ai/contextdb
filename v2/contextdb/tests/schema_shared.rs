//! A refused declaration or removal leaves the schema, the rows and the vector counts as they
//! were, and a change to one table is seen by every check that reads it from another.

mod support;

use contextdb::{Error, Value, params};

fn count(db: &contextdb::Database, table: &str) -> i64 {
    support::int(db, &format!("SELECT COUNT(*) FROM {table}"))
}

#[test]
fn a_declaration_whose_storage_commit_is_refused_leaves_the_schema_as_it_was() {
    let folder = support::folder("declaration-refused");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert(&db, 1..=3);
    let names = support::names(&db);
    let meta = db.table_meta("t").unwrap().columns.len();
    // Under a limit below the file's size, a declaration of two megabytes of text is refused.
    let bytes = std::fs::metadata(folder.join("data")).unwrap().len();
    db.set_disk_limit(Some(bytes / 2)).unwrap();
    let text = support::letters(3, 2 << 20);
    let create = format!("CREATE TABLE big (id INTEGER PRIMARY KEY, note TEXT DEFAULT '{text}')");
    let alter = format!("ALTER TABLE t ADD COLUMN note TEXT DEFAULT '{text}'");
    for (what, sql) in [("a new table", create), ("a new column", alter)] {
        let error = support::refusal(&db, &sql);
        assert!(
            matches!(error, Error::DiskLimitExceeded { .. }),
            "{what}: the declaration was refused as {error:?}, not for the disk limit"
        );
        assert_eq!(
            support::names(&db),
            names,
            "{what}: a refused declaration leaves the tables of the schema as they were"
        );
        assert_eq!(
            db.table_meta("t").unwrap().columns.len(),
            meta,
            "{what}: a refused declaration leaves the columns of a table as they were"
        );
        support::assert_rows_are(&db, 1..=3);
    }
    assert!(
        matches!(
            support::refusal(&db, "SELECT * FROM big"),
            Error::TableNotFound(_)
        ),
        "a table whose declaration was refused does not exist"
    );
    db.set_disk_limit(None).unwrap();
    support::run(&db, "CREATE TABLE big (id INTEGER PRIMARY KEY, note TEXT)");
    support::run(&db, "ALTER TABLE t ADD COLUMN note TEXT");
    support::insert(&db, 4..=4);
    assert_eq!(count(&db, "t"), 4, "a write after the refusal is kept");
    let db = support::reopen(db, &folder);
    assert_eq!(count(&db, "t"), 4);
    assert_eq!(db.table_meta("t").unwrap().columns.len(), meta + 1);
}

#[test]
fn a_check_on_a_child_sees_its_parent_after_the_parent_was_altered() {
    let db = support::open(&support::folder("parent-altered"));
    support::run(
        &db,
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE)",
    );
    support::run(
        &db,
        "CREATE TABLE kids (id INTEGER PRIMARY KEY, code TEXT REFERENCES parents(code))",
    );
    support::run(&db, "INSERT INTO parents VALUES (1, 'a'), (2, 'b')");
    support::run(&db, "INSERT INTO kids VALUES (10, 'a')");
    support::run(&db, "ALTER TABLE parents ADD COLUMN note TEXT");
    support::run(&db, "ALTER TABLE parents RENAME COLUMN code TO label");
    support::run(&db, "INSERT INTO kids VALUES (11, 'b')");
    let missing = support::refusal(&db, "INSERT INTO kids VALUES (12, 'zzz')");
    assert!(
        matches!(missing, Error::ForeignKeyViolation { .. }),
        "a child whose parent value does not exist is refused after the parent's ALTER: {missing:?}"
    );
    let held = support::refusal(&db, "DELETE FROM parents WHERE id = 1");
    assert!(
        matches!(held, Error::ForeignKeyViolation { .. }),
        "a parent row a child refers to is not deleted after the parent's ALTER: {held:?}"
    );
    let dropped = support::refusal(&db, "DROP TABLE parents");
    assert!(
        matches!(dropped, Error::SchemaInvalid { .. }),
        "a table a child refers to is not dropped after its ALTER: {dropped:?}"
    );
    assert_eq!(count(&db, "kids"), 2, "the child rows are untouched");
    support::run(&db, "DROP TABLE kids");
    support::run(&db, "DROP TABLE parents");
    assert!(support::names(&db).is_empty(), "both tables are dropped");
}

/// What `SHOW VECTOR_INDEXES` says of the table's one column: its vectors and its index bytes.
fn indexes(db: &contextdb::Database, table: &str) -> (i64, i64) {
    let shown = support::shown(db, "SHOW VECTOR_INDEXES");
    let row = shown
        .iter()
        .find(|r| support::word(r, "table") == table)
        .unwrap();
    (
        support::whole(row, "vector_count"),
        support::whole(row, "durable_index_bytes"),
    )
}

/// Runs `sql` for each row 1 to 60, with `$id` and the vector `$v` of that row.
fn fill(db: &contextdb::Database, sql: &str) {
    for n in 1..=60 {
        let args = params! {"id" => n, "v" => Value::Vector(support::gathered(n, 8))};
        db.execute(sql, &args).unwrap();
    }
}

#[test]
fn vector_counts_after_a_vector_column_is_dropped_no_longer_count_it() {
    let folder = support::folder("vector-column-dropped");
    let db = support::open(&folder);
    support::declare_docs(&db, 8, "INDEXING_THRESHOLD 100");
    support::run(
        &db,
        "CREATE TABLE late (id INTEGER PRIMARY KEY, w VECTOR(8) INDEXING_THRESHOLD 100)",
    );
    support::load_docs(&db, 1..=60, "a", |n| support::gathered(n, 8));
    fill(&db, "INSERT INTO late VALUES ($id, $v)");
    support::maintenance(&db);
    // `late.w` has the highest VECTOR number; once dropped and reopened, `x` takes it.
    support::run(&db, "ALTER TABLE late DROP COLUMN w");
    let db = support::reopen(db, &folder);
    support::run(
        &db,
        "ALTER TABLE late ADD COLUMN x VECTOR(8) INDEXING_THRESHOLD 100",
    );
    assert_eq!(
        indexes(&db, "late"),
        (0, 0),
        "the column that took the dropped one's number holds none of its vectors"
    );
    assert_eq!(
        indexes(&db, "docs").0,
        60,
        "the other column keeps its own count"
    );
    // Its own 60 rows are under its threshold of 100.
    fill(&db, "UPDATE late SET x = $v WHERE id = $id");
    support::maintenance(&db);
    assert_eq!(
        indexes(&db, "late"),
        (60, 0),
        "60 vectors are under the threshold of 100"
    );
    assert_eq!(indexes(&db, "docs").0, 60, "the other column is unchanged");
}

#[test]
fn a_purge_whose_later_part_fails_leaves_the_schema_and_the_rows_as_they_were() {
    let folder = support::folder("purge-later-part-fails");
    let db = support::open(&folder);
    support::declare_docs(&db, 8, "INDEXING_THRESHOLD 100");
    support::load_docs(&db, 1..=40, "a", |n| support::gathered(n, 8));
    support::run(
        &db,
        "CREATE TABLE other (id INTEGER PRIMARY KEY, n INTEGER)",
    );
    support::run(&db, "INSERT INTO other VALUES (1, 1), (2, 2)");
    let names = support::names(&db);
    let vectors = |db: &contextdb::Database| indexes(db, "docs").0;
    assert_eq!(vectors(&db), 40);
    // The first part removes 20 rows; the second divides by zero on its first row.
    let error = support::refusal(
        &db,
        "PURGE FROM docs WHERE id <= 20, other WHERE 1 / (id - 1) = 1",
    );
    assert!(
        !matches!(error, Error::TableNotFound(_)),
        "the later part must fail while it runs, not when it is named: {error:?}"
    );
    assert_eq!(support::names(&db), names, "the schema is as it was");
    assert_eq!(count(&db, "docs"), 40, "the first part removed nothing");
    assert_eq!(count(&db, "other"), 2, "the later part removed nothing");
    assert_eq!(vectors(&db), 40, "the vector count is as it was");
    // The same purge with a part that succeeds removes the rows of both, and the count follows.
    support::run(&db, "PURGE FROM docs WHERE id <= 20, other WHERE id = 1");
    assert_eq!(count(&db, "docs"), 20);
    assert_eq!(
        count(&db, "other"),
        1,
        "the second table's part removed its row"
    );
    assert_eq!(
        vectors(&db),
        20,
        "the count follows the rows a purge removes"
    );
}
