//! ALTER TABLE and DROP: what a row written before an added column reads in it, what is left
//! in the store after a column or a table is dropped, what each refusal names, and that a
//! refused statement changes nothing. An altered table is printed back as what it now
//! declares, and the printed text declares that table again.

mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::serde_json::json;
use contextdb::{Clock, Database, DatabaseOpenOptions, Error, ReadSession, Value, params};
use support::Call;

/// A clock that always shows the same second.
struct At(u64);

impl Clock for At {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.0)
    }
}

fn open_at(folder: &Path, second: u64) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(At(second)));
    support::open_with(folder, options).expect("open the store with a clock")
}

/// The length of a store's data file, in bytes.
fn data_file(folder: &Path) -> u64 {
    fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len()
}

/// What a table is printed back as.
fn printed(db: &Database, table: &str) -> String {
    db.declaration(table)
        .unwrap_or_else(|error| panic!("the declaration of {table} was refused: {error:?}"))
}

/// The words of a declaration, in lower case, and its quoted texts: what it says, whatever
/// the spacing and the punctuation it says it in.
fn words(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = sql.chars().peekable();
    while let Some(first) = rest.next() {
        if first == '\'' || first == '"' {
            let mut text = String::from(first);
            loop {
                let next = rest.next().expect("a quoted text ends with its quote");
                text.push(next);
                if next == first && rest.next_if_eq(&first).is_none() {
                    break;
                }
            }
            found.push(text);
        } else if first.is_ascii_alphanumeric() || first == '_' {
            let mut word = String::from(first.to_ascii_lowercase());
            while let Some(next) = rest.next_if(|c| c.is_ascii_alphanumeric() || *c == '_') {
                word.push(next.to_ascii_lowercase());
            }
            found.push(word);
        }
    }
    found
}

/// Whether a printed declaration says this clause: the clause's words, one after the other.
fn says(declaration: &str, clause: &str) -> bool {
    let (text, clause) = (words(declaration), words(clause));
    text.windows(clause.len()).any(|window| window == clause)
}

/// The statements of a printed declaration.
fn statements(text: &str) -> Vec<&str> {
    let ended = text.split(";\n");
    ended
        .filter(|statement| !statement.trim().is_empty())
        .collect()
}

#[test]
fn an_added_column_reads_its_default_from_a_row_written_before_it() {
    let folder = support::folder("added-column-default");
    let db = open_at(&folder, 1_700_000_000);
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    );
    support::run(
        &db,
        "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    for column in [
        "plain TEXT",
        "n INTEGER DEFAULT 7",
        "word TEXT NOT NULL DEFAULT 'none'",
        "ratio REAL DEFAULT 0.5",
        "ok BOOLEAN DEFAULT TRUE",
        "doc JSON DEFAULT '[]'",
        "made TIMESTAMP DEFAULT NOW()",
    ] {
        support::run(&db, &format!("ALTER TABLE t ADD COLUMN {column}"));
    }
    // What a row written before the columns reads in them: nothing where the column declares
    // no default, the default where it declares one, and for NOW() the time of the ALTER, the
    // same for every such row.
    let older = |id: i64, name: &str| {
        vec![
            Value::Int64(id),
            support::text(name),
            Value::Null,
            Value::Int64(7),
            support::text("none"),
            Value::Float64(0.5),
            Value::Bool(true),
            Value::Json(json!([])),
            Value::Timestamp(1_700_000_000_000),
        ]
    };
    let all = "SELECT id, name, plain, n, word, ratio, ok, doc, made FROM t ORDER BY id";
    assert_eq!(
        support::rows(&db, all),
        [older(1, "a"), older(2, "b"), older(3, "c")]
    );
    assert_eq!(
        support::rows(&db, "SELECT * FROM t ORDER BY id"),
        support::rows(&db, all)
    );

    // A filter, a sort and a new index read the same defaults.
    let ids = |sql: &str| -> Vec<i64> {
        let rows = support::rows(&db, sql);
        rows.iter()
            .map(|row| match row[0] {
                Value::Int64(id) => id,
                ref other => panic!("{sql}\nreturned {other:?} for an id"),
            })
            .collect()
    };
    assert_eq!(
        ids("SELECT id FROM t WHERE n = 7 AND word = 'none' ORDER BY id"),
        [1, 2, 3]
    );
    assert_eq!(
        ids("SELECT id FROM t WHERE plain IS NULL ORDER BY n, id"),
        [1, 2, 3]
    );
    support::run(&db, "CREATE INDEX t_by_n ON t (n, id)");
    assert_eq!(ids("SELECT id FROM t WHERE n = 7 ORDER BY id"), [1, 2, 3]);
    assert_eq!(ids("SELECT id FROM t WHERE n = 8 ORDER BY id"), [0_i64; 0]);

    // A statement that changes another column of an older row leaves what the row reads in
    // the added ones; one that assigns an added column changes that column alone.
    support::run(&db, "UPDATE t SET name = 'renamed' WHERE id = 1");
    support::run(&db, "UPDATE t SET n = 8 WHERE id = 2");
    let mut second = older(2, "b");
    second[3] = Value::Int64(8);
    assert_eq!(
        support::rows(&db, all),
        [older(1, "renamed"), second.clone(), older(3, "c")]
    );
    assert_eq!(ids("SELECT id FROM t WHERE n = 7 ORDER BY id"), [1, 3]);
    assert_eq!(ids("SELECT id FROM t WHERE n = 8 ORDER BY id"), [2]);
    support::close(db);

    // Under a later clock the older rows read the time they read before, a row written now
    // takes its defaults as any row does, and a NULL written is a NULL.
    let db = open_at(&folder, 1_800_000_000);
    support::run(&db, "INSERT INTO t (id, name) VALUES (4, 'd')");
    support::run(
        &db,
        "INSERT INTO t (id, name, n, ok) VALUES (5, 'e', NULL, NULL)",
    );
    let mut fourth = older(4, "d");
    fourth[8] = Value::Timestamp(1_800_000_000_000);
    let mut fifth = fourth.clone();
    (fifth[0], fifth[1]) = (Value::Int64(5), support::text("e"));
    (fifth[3], fifth[6]) = (Value::Null, Value::Null);
    assert_eq!(
        support::rows(&db, all),
        [older(1, "renamed"), second, older(3, "c"), fourth, fifth]
    );
}

#[test]
fn adding_a_column_writes_no_row_again() {
    // 2,000 rows of 500 letters: a million bytes of rows.
    const ROWS: i64 = 2_000;
    const LETTERS: usize = 500;
    let folder = support::folder("added-column-writes");
    let (written, counting) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let mut options = DatabaseOpenOptions::default();
    let (bytes, on) = (written.clone(), counting.clone());
    options.storage = Some(support::storage(move |call| {
        if let (Call::Write(_, data), true) = (call, on.load(Ordering::SeqCst)) {
            bytes.fetch_add(data.len() as u64, Ordering::SeqCst);
        }
    }));
    let db = support::open_with(&folder, options).expect("open the store through a door");
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    );
    for first in (1..=ROWS).step_by(500) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 500 {
            let body = support::letters(id, LETTERS);
            tx.execute(
                "INSERT INTO t (id, body) VALUES ($id, $body)",
                &params! { "id" => id, "body" => body },
            )
            .unwrap_or_else(|error| panic!("row {id} was refused: {error:?}"));
        }
        tx.commit().expect("commit five hundred rows");
    }

    counting.store(true, Ordering::SeqCst);
    support::run(
        &db,
        "ALTER TABLE t ADD COLUMN tag TEXT NOT NULL DEFAULT 'new'",
    );
    counting.store(false, Ordering::SeqCst);
    let written = written.load(Ordering::SeqCst);
    let rows = ROWS as u64 * LETTERS as u64;
    assert!(
        written > 0 && written < rows / 4,
        "adding a column wrote {written} bytes to the data file of a table whose rows hold \
         {rows} bytes"
    );
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM t WHERE tag = 'new'"),
        ROWS
    );
    assert_eq!(
        support::rows(&db, "SELECT body, tag FROM t WHERE id = 1234"),
        [[
            support::text(&support::letters(1234, LETTERS)),
            support::text("new")
        ]]
    );
}

#[test]
fn an_added_vector_column_is_null_on_older_rows_and_no_search_returns_them() {
    let folder = support::folder("added-vector-column");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, body TEXT)",
    );
    support::run(
        &db,
        "INSERT INTO docs (id, ctx, body) VALUES (1, 'a', 'x'), (2, 'a', 'x'), (3, 'a', 'x'), \
         (4, 'b', 'x'), (5, 'b', 'x'), (6, 'b', 'x')",
    );
    let column = "v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 4 SEARCH_MODE EXACT";
    support::run(&db, &format!("ALTER TABLE docs ADD COLUMN {column}"));
    assert!(
        says(&printed(&db, "docs"), column),
        "{}",
        printed(&db, "docs")
    );

    let vectors = |db: &Database| support::rows(db, "SELECT id, v FROM docs ORDER BY id");
    let none: Vec<Vec<Value>> = (1..=6)
        .map(|id| vec![Value::Int64(id), Value::Null])
        .collect();
    assert_eq!(vectors(&db), none);
    // No search returns a row that has no vector, with a filter or without.
    let query = [1.0_f32, 0.0, 0.0, 0.0];
    let found = |db: &Database, filter: &str| -> Vec<Value> {
        let sql = format!("SELECT id FROM docs {filter} ORDER BY v <=> $q LIMIT 10");
        let result = support::search(db, &sql, &query);
        let result = result.unwrap_or_else(|error| panic!("{sql}\nwas refused: {error:?}"));
        result.rows().iter().map(|row| row[0].clone()).collect()
    };
    assert!(found(&db, "").is_empty());
    assert!(found(&db, "WHERE ctx = 'a'").is_empty());

    // Rows given a vector, and a row written with one, are found; the others are not.
    for (id, vector) in [
        (2_i64, vec![1.0_f32, 0.0, 0.0, 0.0]),
        (5, vec![0.8, 0.6, 0.0, 0.0]),
    ] {
        db.execute(
            "UPDATE docs SET v = $v WHERE id = $id",
            &params! { "id" => id, "v" => vector },
        )
        .expect("give an older row a vector");
    }
    db.execute(
        "INSERT INTO docs (id, ctx, body, v) VALUES (7, 'a', 'y', $v)",
        &params! { "v" => vec![0.0_f32, 1.0, 0.0, 0.0] },
    )
    .expect("a row with a vector");
    let ids = |ids: &[i64]| -> Vec<Value> { ids.iter().map(|id| Value::Int64(*id)).collect() };
    assert_eq!(found(&db, ""), ids(&[2, 5, 7]));
    assert_eq!(found(&db, "WHERE ctx = 'a'"), ids(&[2, 7]));
    assert_eq!(found(&db, "WHERE ctx = 'b'"), ids(&[5]));

    // The same once the vectors are in blocks, and after close and open.
    assert_eq!(support::seal_all(&db), 3);
    assert_eq!(found(&db, ""), ids(&[2, 5, 7]));
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(found(&db, ""), ids(&[2, 5, 7]));
    assert_eq!(found(&db, "WHERE ctx = 'b'"), ids(&[5]));
    let with_vector = vectors(&db)
        .iter()
        .filter(|row| row[1] != Value::Null)
        .count();
    assert_eq!(with_vector, 3);
}

/// How many rows the stores of the two tests below hold, and how many letters each row's
/// large value has: 3,200,000 bytes that do not shrink when stored.
const ROWS: i64 = 400;
const LETTERS: usize = 8_000;
const PUT_IN: u64 = ROWS as u64 * LETTERS as u64;

/// Writes a large text into `column` of rows 1 to 400 of `table`, a hundred rows to a commit.
/// The rows are inserted when `insert` says so, and updated otherwise. The text of a row is a
/// function of its number and `seed`.
fn fill(db: &Database, table: &str, column: &str, insert: bool, seed: i64) {
    let sql = if insert {
        format!("INSERT INTO {table} (id, {column}) VALUES ($id, $text)")
    } else {
        format!("UPDATE {table} SET {column} = $text WHERE id = $id")
    };
    for first in (1..=ROWS).step_by(100) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 100 {
            let text = support::letters(seed + id, LETTERS);
            tx.execute(&sql, &params! { "id" => id, "text" => text })
                .unwrap_or_else(|error| panic!("{sql}\nwas refused for row {id}: {error:?}"));
        }
        tx.commit().expect("commit a hundred rows");
    }
}

#[test]
fn a_dropped_columns_bytes_leave_the_store() {
    // Two stores take the same writes: a large column filled, then a second large column
    // added and filled. One of them drops the first column in between.
    let store = |test: &str, drop: bool| -> u64 {
        let folder = support::folder(test);
        let db = support::open(&folder);
        support::run(
            &db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        );
        fill(&db, "notes", "body", true, 0);
        if drop {
            support::run(&db, "ALTER TABLE notes DROP COLUMN body");
            assert!(matches!(
                support::refusal(&db, "SELECT body FROM notes"),
                Error::ColumnNotFound { .. }
            ));
        }
        support::run(&db, "ALTER TABLE notes ADD COLUMN other TEXT");
        fill(&db, "notes", "other", false, ROWS);
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), ROWS);
        assert_eq!(
            support::rows(&db, "SELECT other FROM notes WHERE id = 77"),
            [[support::text(&support::letters(ROWS + 77, LETTERS))]]
        );
        support::close(db);
        data_file(&folder)
    };
    let kept = store("dropped-column-kept", false);
    let dropped = store("dropped-column-dropped", true);
    // The store that kept the column holds both columns' bytes. The one that dropped it has
    // let the first column's bytes go, so the second column's took their room.
    assert!(
        kept >= 2 * PUT_IN,
        "the store that dropped nothing is {kept} bytes long"
    );
    assert!(
        dropped + PUT_IN / 2 <= kept,
        "after DROP COLUMN of {PUT_IN} bytes the data file is {dropped} bytes long, and the \
         same store with the column kept is {kept}: the dropped column's bytes are still held"
    );
}

#[test]
fn a_dropped_table_leaves_no_record() {
    // As above, with a table dropped where a column was: one table filled, then a second one.
    let store = |test: &str, drop: bool| -> (PathBuf, u64) {
        let folder = support::folder(test);
        let db = support::open(&folder);
        let declare = |table: &str| {
            let columns = "id INTEGER PRIMARY KEY, body TEXT, tag TEXT, v VECTOR(2)";
            support::run(&db, &format!("CREATE TABLE {table} ({columns})"));
            support::run(&db, &format!("CREATE INDEX by_tag ON {table} (tag, id)"));
        };
        declare("one");
        fill(&db, "one", "body", true, 0);
        support::run(
            &db,
            "UPDATE one SET tag = 'old', v = [1.0, 0.0] WHERE id <= 50",
        );
        if drop {
            let dropped = support::run(&db, "DROP TABLE one");
            assert_eq!(dropped.rows_affected(), 0);
            assert!(db.table_names().is_empty());
        }
        declare("two");
        fill(&db, "two", "body", true, ROWS);
        support::close(db);
        let length = data_file(&folder);
        (folder, length)
    };
    let (_, kept) = store("dropped-table-kept", false);
    let (folder, dropped) = store("dropped-table-dropped", true);
    assert!(
        kept >= 2 * PUT_IN,
        "the store that dropped nothing is {kept} bytes long"
    );
    assert!(
        dropped + PUT_IN / 2 <= kept,
        "after DROP TABLE of {PUT_IN} bytes the data file is {dropped} bytes long, and the \
         same store with the table kept is {kept}: the dropped table's bytes are still held"
    );

    // Nothing answers for the dropped table: not the writer, not a reader, not after close
    // and open; and a table declared under its name starts empty, with none of its indexes.
    let db = support::open(&folder);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    assert_eq!(db.table_names(), ["two"]);
    assert_eq!(reader.table_names().expect("a reader's tables"), ["two"]);
    let missing = |error: Error, what: &str| {
        assert!(
            matches!(&error, Error::TableNotFound(name) if name == "one"),
            "{what} gave {error:?}"
        );
    };
    missing(
        db.declaration("one").expect_err("a declaration"),
        "the declaration",
    );
    missing(
        reader.declaration("one").expect_err("a declaration"),
        "a reader's declaration",
    );
    for sql in [
        "SELECT id FROM one",
        "INSERT INTO one (id) VALUES (1)",
        "UPDATE one SET tag = 'x'",
        "DELETE FROM one",
        "DROP TABLE one",
        "DROP INDEX by_tag ON one",
        "ALTER TABLE one ADD COLUMN c TEXT",
        "CREATE INDEX again ON one (tag)",
        "SELECT id FROM one ORDER BY v <=> [1.0, 0.0] LIMIT 1",
    ] {
        missing(support::refusal(&db, sql), sql);
    }
    support::run(
        &db,
        "CREATE TABLE one (id INTEGER PRIMARY KEY, tag TEXT, v VECTOR(2))",
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM one"), 0);
    assert!(support::rows(&db, "SELECT id FROM one WHERE tag = 'old'").is_empty());
    assert!(support::rows(&db, "SELECT id FROM one ORDER BY v <=> [1.0, 0.0] LIMIT 5").is_empty());
    support::run(
        &db,
        "INSERT INTO one (id, tag, v) VALUES (1, 'new', [0.0, 1.0])",
    );
    support::run(&db, "CREATE INDEX by_tag ON one (tag)");
    assert_eq!(
        support::rows(&db, "SELECT id FROM one ORDER BY v <=> [1.0, 0.0] LIMIT 5"),
        [[Value::Int64(1)]]
    );
    // The other table was not touched.
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM two"), ROWS);
}

/// A store with a table for each thing a drop or an ALTER may depend on, and rows in them.
fn store_with_dependents(test: &str) -> Database {
    let db = support::open(&support::folder(test));
    for sql in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, kind TEXT NOT NULL IMMUTABLE, \
         tenant TEXT NOT NULL, parent_id INTEGER REFERENCES parents(id), \
         v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 4)",
        "CREATE INDEX t_first ON t (b, a)",
        "CREATE INDEX t_second ON t (a)",
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('NEEDS')",
        "CREATE TABLE task_results (id UUID PRIMARY KEY, task_id UUID NOT NULL, \
         success BOOLEAN NOT NULL)",
        "CREATE INDEX task_results_task_id_idx ON task_results (task_id)",
        "CREATE TABLE ranked (id UUID PRIMARY KEY, confidence REAL, \
         embedding VECTOR(4) RANK_POLICY (JOIN task_results ON task_id, \
         FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)', \
         SORT_KEY effective_confidence))",
        "INSERT INTO parents (id, name) VALUES (1, 'p')",
        "INSERT INTO t (id, a, b, kind, tenant, parent_id, v) VALUES \
         (1, 'x', 10, 'k', 'north', 1, [1.0, 0.0]), (2, 'y', 20, 'k', 'south', 1, [0.0, 1.0])",
    ] {
        support::run(&db, sql);
    }
    db
}

/// What [`state`] reads: the declarations, the rows of `t` read two ways, and the newest commit.
type State = (
    Vec<String>,
    Vec<Vec<Value>>,
    Vec<Vec<Value>>,
    contextdb::TxId,
);

/// Everything a refused statement must leave as it was: each table's declaration, the rows,
/// and the number of the newest commit.
fn state(db: &Database) -> State {
    let names = db.table_names();
    let declared = names.iter().map(|table| printed(db, table)).collect();
    let rows = support::rows(
        db,
        "SELECT id, a, b, kind, tenant, parent_id, v FROM t ORDER BY id",
    );
    let parents = support::rows(db, "SELECT id, name FROM parents ORDER BY id");
    (declared, rows, parents, db.committed_watermark())
}

/// What a refused ALTER or DROP must come as.
enum Refused {
    /// `ColumnInIndex` for this column of `t` and this index.
    InIndex(&'static str, &'static str),
    /// `VectorPartitionKeyInUse` for the column `tenant` of `t` and the VECTOR column `v`.
    InPartitionKey,
    /// `ImmutableColumn` for the column `kind` of `t`.
    Immutable,
    /// `SchemaInvalid`.
    Invalid,
    /// `DropBlockedByRankPolicy` for this table and the policy of `ranked`, and for this
    /// column where the statement names one.
    Ranked(&'static str, Option<&'static str>),
    /// `TableNotFound` for the table `nope`.
    NoTable,
}

impl Refused {
    fn is(&self, error: &Error) -> bool {
        match (self, error) {
            (
                Refused::InIndex(of, by),
                Error::ColumnInIndex {
                    table,
                    column,
                    index,
                },
            ) => table == "t" && column == of && index == by,
            (
                Refused::InPartitionKey,
                Error::VectorPartitionKeyInUse {
                    table,
                    column,
                    index,
                },
            ) => table == "t" && column == "tenant" && index == "v",
            (Refused::Immutable, Error::ImmutableColumn { table, column }) => {
                table == "t" && column == "kind"
            }
            (Refused::Invalid, Error::SchemaInvalid { reason }) => !reason.is_empty(),
            (
                Refused::Ranked(of, named),
                Error::DropBlockedByRankPolicy {
                    table,
                    column,
                    policy,
                },
            ) => {
                table == of
                    && policy == "effective_confidence"
                    && named.is_none_or(|named| column == named)
            }
            (Refused::NoTable, Error::TableNotFound(name)) => name == "nope",
            _ => false,
        }
    }
}

#[test]
fn each_dependent_refusal_names_what_depends_and_changes_nothing() {
    let db = store_with_dependents("dependent-refusals");
    // A table that keeps its rows to itself: SYNC SAFE cannot stand on it.
    support::run(&db, "ALTER TABLE t SET SYNC OFF");
    let before = state(&db);
    let cases = [
        // The first index, in the order they were made, that names the column.
        (
            "ALTER TABLE t DROP COLUMN a",
            Refused::InIndex("a", "t_first"),
        ),
        (
            "ALTER TABLE t DROP COLUMN a RESTRICT",
            Refused::InIndex("a", "t_first"),
        ),
        ("ALTER TABLE t DROP b", Refused::InIndex("b", "t_first")),
        ("ALTER TABLE t DROP COLUMN tenant", Refused::InPartitionKey),
        (
            "ALTER TABLE t DROP COLUMN tenant CASCADE",
            Refused::InPartitionKey,
        ),
        (
            "ALTER TABLE t RENAME COLUMN tenant TO owner",
            Refused::InPartitionKey,
        ),
        ("ALTER TABLE t DROP COLUMN kind", Refused::Immutable),
        ("ALTER TABLE t DROP COLUMN kind CASCADE", Refused::Immutable),
        (
            "ALTER TABLE t RENAME COLUMN kind TO sort",
            Refused::Immutable,
        ),
        // A column of the key, a table another references, and a column that makes a DAG
        // table a table of edges.
        ("ALTER TABLE t DROP COLUMN id", Refused::Invalid),
        ("ALTER TABLE t RENAME COLUMN id TO key", Refused::Invalid),
        (
            "ALTER TABLE parents DROP COLUMN id CASCADE",
            Refused::Invalid,
        ),
        ("DROP TABLE parents", Refused::Invalid),
        ("DROP TABLE IF EXISTS parents", Refused::Invalid),
        ("ALTER TABLE edges DROP COLUMN source_id", Refused::Invalid),
        (
            "ALTER TABLE edges DROP COLUMN target_id CASCADE",
            Refused::Invalid,
        ),
        (
            "ALTER TABLE edges RENAME COLUMN edge_type TO kind",
            Refused::Invalid,
        ),
        // What the rank policy of `ranked` reads.
        (
            "DROP TABLE task_results",
            Refused::Ranked("task_results", None),
        ),
        (
            "DROP INDEX task_results_task_id_idx ON task_results",
            Refused::Ranked("task_results", None),
        ),
        (
            "ALTER TABLE task_results DROP COLUMN success",
            Refused::Ranked("task_results", Some("success")),
        ),
        (
            "ALTER TABLE task_results DROP COLUMN task_id CASCADE",
            Refused::Ranked("task_results", Some("task_id")),
        ),
        (
            "ALTER TABLE ranked DROP COLUMN confidence",
            Refused::Ranked("ranked", Some("confidence")),
        ),
        // A column that may hold no NULL, with no default, on a table that has rows; a name
        // that is taken; a second key.
        (
            "ALTER TABLE t ADD COLUMN must TEXT NOT NULL",
            Refused::Invalid,
        ),
        ("ALTER TABLE t ADD COLUMN a INTEGER", Refused::Invalid),
        (
            "ALTER TABLE t ADD COLUMN k INTEGER PRIMARY KEY",
            Refused::Invalid,
        ),
        ("ALTER TABLE t RENAME COLUMN a TO b", Refused::Invalid),
        // Options that cannot stand.
        (
            "ALTER TABLE t SET RETAIN 7 DAYS SYNC SAFE",
            Refused::Invalid,
        ),
        ("ALTER TABLE t SET MAINTAIN COUNT (kind)", Refused::Invalid),
        ("ALTER TABLE t SET MAINTAIN COUNT (id)", Refused::Invalid),
        // What is not there.
        ("DROP TABLE nope", Refused::NoTable),
        ("ALTER TABLE nope ADD COLUMN c TEXT", Refused::NoTable),
        ("ALTER TABLE nope DROP COLUMN c", Refused::NoTable),
        ("DROP INDEX t_first ON nope", Refused::NoTable),
    ];
    for (sql, refused) in &cases {
        let error = support::refusal(&db, sql);
        assert!(refused.is(&error), "{sql}\nwas refused as {error:?}");
        assert!(
            state(&db) == before,
            "{sql}\nwas refused and changed the store"
        );
    }
    // Keeping only current versions is refused on a table that sends its rows out and keeps
    // the first write.
    support::run(&db, "ALTER TABLE t SET SYNC TWO WAY");
    let before = state(&db);
    let error = support::refusal(&db, "ALTER TABLE t SET HISTORY CURRENT ONLY");
    assert!(Refused::Invalid.is(&error), "{error:?}");
    assert!(state(&db) == before);

    // An index and a column that are not there, by name.
    match support::refusal(&db, "DROP INDEX nope ON t") {
        Error::IndexNotFound { table, index } => {
            assert_eq!((table.as_str(), index.as_str()), ("t", "nope"));
        }
        other => panic!("dropping an index that is not there was refused as {other:?}"),
    }
    for sql in [
        "ALTER TABLE t DROP COLUMN nope",
        "ALTER TABLE t RENAME COLUMN nope TO c",
    ] {
        match support::refusal(&db, sql) {
            Error::ColumnNotFound { table, column } => {
                assert_eq!((table.as_str(), column.as_str()), ("t", "nope"));
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
    // A limit on partitions below the partitions the column has. No document says which of
    // the two kinds this is.
    let error = support::refusal(&db, "ALTER TABLE t ALTER COLUMN v SET MAX_PARTITIONS 1");
    assert!(
        matches!(
            &error,
            Error::VectorPartitionLimitExceeded { index, .. }
            | Error::InvalidVectorPartitionDeclaration { index, .. }
                if index.table == "t" && index.column == "v"
        ),
        "a limit of one partition on a column that has two was refused as {error:?}"
    );
    assert!(state(&db) == before);
    // With IF EXISTS, a table or an index that is not there is nothing to do, and changes
    // no declaration and no row.
    for sql in [
        "DROP TABLE IF EXISTS nope",
        "DROP INDEX IF EXISTS nope ON t",
    ] {
        let result = support::run(&db, sql);
        assert_eq!(result.rows_affected(), 0, "{sql}");
        assert!(result.rows().is_empty(), "{sql}");
    }
    let after = state(&db);
    assert!(after.0 == before.0 && after.1 == before.1 && after.2 == before.2);
}

#[test]
fn an_altered_table_is_printed_as_what_it_now_declares() {
    let folder = support::folder("altered-printed");
    let db = support::open(&folder);
    for sql in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, tenant TEXT NOT NULL, \
         parent TEXT REFERENCES parents(code), \
         v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 2 SEARCH_MODE AUTO, \
         UNIQUE (a, tenant))",
        "CREATE INDEX t_by_a ON t (a DESC, id)",
        "INSERT INTO parents (id, code) VALUES (1, 'p')",
        "INSERT INTO t (id, a, tenant, parent, v) VALUES (1, 'x', 'north', 'p', [1.0, 0.0])",
    ] {
        support::run(&db, sql);
    }
    // Each ALTER, what the table's declaration says after it, and what it no longer says.
    let steps: &[(&str, &[&str], &[&str])] = &[
        (
            "ALTER TABLE t ADD COLUMN extra INTEGER NOT NULL DEFAULT 5",
            &["extra INTEGER NOT NULL DEFAULT 5"],
            &[],
        ),
        (
            "ALTER TABLE t RENAME COLUMN a TO title",
            &[
                "title TEXT",
                "UNIQUE (title, tenant)",
                "ON t (title DESC, id)",
            ],
            &["a TEXT", "UNIQUE (a", "ON t (a"],
        ),
        (
            "ALTER TABLE parents RENAME COLUMN code TO label",
            &["parent TEXT REFERENCES parents (label)"],
            &["parents (code)"],
        ),
        (
            "ALTER TABLE t ALTER COLUMN v SET SEARCH_MODE EXACT",
            &["PARTITION_KEY (tenant) MAX_PARTITIONS 2 SEARCH_MODE EXACT"],
            &["SEARCH_MODE AUTO"],
        ),
        (
            "ALTER TABLE t ALTER COLUMN v SET MAX_PARTITIONS 9",
            &["PARTITION_KEY (tenant) MAX_PARTITIONS 9 SEARCH_MODE EXACT"],
            &["MAX_PARTITIONS 2"],
        ),
        (
            "ALTER TABLE t SET RETAIN 24 HOURS SYNC SAFE",
            &["RETAIN 24 HOURS SYNC SAFE"],
            &[],
        ),
        (
            "ALTER TABLE t SET RETAIN 7 DAYS",
            &["RETAIN 7 DAYS"],
            &["HOURS", "SYNC SAFE"],
        ),
        ("ALTER TABLE t DROP RETAIN", &[], &["RETAIN"]),
        ("ALTER TABLE t SET SYNC PUSH ONLY", &["SYNC PUSH ONLY"], &[]),
        (
            "ALTER TABLE t SET SYNC PULL ONLY",
            &["SYNC PULL ONLY"],
            &["PUSH ONLY"],
        ),
        ("ALTER TABLE t SET SYNC OFF", &["SYNC OFF"], &["PULL ONLY"]),
        (
            "ALTER TABLE t SET SYNC TWO WAY",
            &["SYNC TWO WAY"],
            &["SYNC OFF"],
        ),
        (
            "ALTER TABLE t SET SYNC CONFLICT KEEP LATEST",
            &["SYNC CONFLICT KEEP LATEST", "SYNC TWO WAY"],
            &[],
        ),
        (
            "ALTER TABLE t SET HISTORY CURRENT ONLY",
            &["HISTORY CURRENT ONLY"],
            &[],
        ),
        (
            "ALTER TABLE t SET HISTORY ALL",
            &["SYNC CONFLICT KEEP LATEST"],
            &["CURRENT ONLY"],
        ),
        (
            "ALTER TABLE t SET SYNC CONFLICT KEEP FIRST",
            &["SYNC CONFLICT KEEP FIRST"],
            &["KEEP LATEST"],
        ),
        (
            "ALTER TABLE t SET MAINTAIN COUNT (title)",
            &["MAINTAIN COUNT (title)"],
            &[],
        ),
        (
            "ALTER TABLE t SET MAINTAIN COUNT ALL PREFIXES",
            &["MAINTAIN COUNT ALL PREFIXES"],
            &["MAINTAIN COUNT (title)"],
        ),
        (
            "ALTER TABLE t SET MAINTAIN COUNT NONE",
            &["MAINTAIN COUNT NONE"],
            &["ALL PREFIXES"],
        ),
        (
            "ALTER TABLE t SET MAINTAIN COUNT DEFAULT",
            &["SYNC TWO WAY"],
            &["MAINTAIN COUNT"],
        ),
        (
            "DROP INDEX t_by_a ON t",
            &["UNIQUE (title, tenant)"],
            &["t_by_a"],
        ),
        (
            "CREATE INDEX t_by_extra ON t (extra, id)",
            &["t_by_extra"],
            &[],
        ),
        (
            "ALTER TABLE t DROP COLUMN extra CASCADE",
            &["title TEXT"],
            &["extra", "t_by_extra"],
        ),
        (
            "ALTER TABLE t ADD COLUMN w VECTOR(3) SEARCH_MODE INDEXED",
            &[
                "w VECTOR(3) SEARCH_MODE INDEXED",
                "v VECTOR(2) PARTITION_KEY (tenant)",
            ],
            &[],
        ),
        (
            "ALTER TABLE t DROP COLUMN v",
            &["w VECTOR(3)"],
            &["VECTOR(2)", "PARTITION_KEY"],
        ),
    ];
    assert_eq!(steps.len(), 25);
    let mut db = db;
    for (number, (sql, now, no_longer)) in steps.iter().enumerate() {
        support::run(&db, sql);
        let text = printed(&db, "t");
        for clause in *now {
            assert!(
                says(&text, clause),
                "after {sql}\nno `{clause}` in:\n{text}"
            );
        }
        for clause in *no_longer {
            assert!(
                !says(&text, clause),
                "after {sql}\nstill `{clause}` in:\n{text}"
            );
        }
        // The printed texts declare the same two tables in an empty store.
        let again = support::open(&support::folder(&format!("altered-printed-again-{number}")));
        for table in ["parents", "t"] {
            let declared = printed(&db, table);
            for statement in statements(&declared) {
                support::run(&again, statement);
            }
        }
        assert_eq!(printed(&again, "t"), text, "after {sql}, declared again");
        assert_eq!(
            printed(&again, "parents"),
            printed(&db, "parents"),
            "after {sql}"
        );
        // And the store says the same after close and open.
        support::close(db);
        db = support::open(&folder);
        assert_eq!(printed(&db, "t"), text, "after {sql}, closed and opened");
    }
    // The rows came through every one of them.
    assert_eq!(
        support::rows(&db, "SELECT id, title, tenant, parent FROM t"),
        [[
            Value::Int64(1),
            support::text("x"),
            support::text("north"),
            support::text("p")
        ]]
    );
}

#[test]
fn a_renamed_vector_column_keeps_its_declaration_and_its_vectors() {
    let folder = support::folder("renamed-vector-column");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, \
         v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 3 SEARCH_MODE EXACT)",
    );
    let insert_under = |db: &Database, column: &str, id: i64, ctx: &str| {
        db.execute(
            &format!("INSERT INTO docs (id, ctx, {column}) VALUES ($id, $ctx, $v)"),
            &params! { "id" => id, "ctx" => ctx, "v" => support::vector(id, 4) },
        )
    };
    let insert = |db: &Database, id: i64, ctx: &str| insert_under(db, "v", id, ctx);
    // Twenty vectors in blocks and five still waiting, in two partitions.
    for id in 1..=20 {
        insert(&db, id, if id % 2 == 0 { "a" } else { "b" }).expect("a row with a vector");
    }
    assert_eq!(support::seal_all(&db), 20);
    for id in 21..=25 {
        insert(&db, id, "a").expect("a row with a vector");
    }
    let query = support::vector(1000, 4);
    let nearest = |db: &Database, column: &str, filter: &str| {
        let sql = format!(
            "SELECT id, {column} <=> $q AS distance FROM docs {filter} ORDER BY distance LIMIT 10"
        );
        support::search(db, &sql, &query).map(|result| result.rows().to_vec())
    };
    let whole = nearest(&db, "v", "").expect("a search");
    let of_a = nearest(&db, "v", "WHERE ctx = 'a'").expect("a search");
    assert_eq!((whole.len(), of_a.len()), (10, 10));

    support::run(&db, "ALTER TABLE docs RENAME COLUMN v TO embedding");
    // The same answers under the new name, from the blocks and from the waiting vectors, and
    // none under the old one.
    assert_eq!(nearest(&db, "embedding", "").expect("a search"), whole);
    assert_eq!(
        nearest(&db, "embedding", "WHERE ctx = 'a'").expect("a search"),
        of_a
    );
    assert!(nearest(&db, "v", "").is_err());
    let text = printed(&db, "docs");
    let clauses = "embedding VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 3 SEARCH_MODE EXACT";
    assert!(
        says(&text, clauses) && !says(&text, "v VECTOR(4)"),
        "{text}"
    );
    // The waiting vectors are the same five: the rename sealed nothing and lost nothing.
    assert_eq!(support::seal_all(&db), 5);
    assert_eq!(nearest(&db, "embedding", "").expect("a search"), whole);
    // The limit on partitions still counts the two the column had.
    insert_under(&db, "embedding", 26, "c").expect("a third partition");
    match insert_under(&db, "embedding", 27, "d") {
        Err(Error::VectorPartitionLimitExceeded {
            index,
            max_partitions: 3,
        }) => {
            assert_eq!(
                (index.table.as_str(), index.column.as_str()),
                ("docs", "embedding")
            );
        }
        other => panic!("a fourth partition gave {other:?}"),
    }
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(
        nearest(&db, "embedding", "WHERE ctx = 'a'").expect("a search"),
        of_a
    );
}

#[test]
fn no_statement_drops_history_and_set_history_all_is_the_way_back() {
    let db = support::open(&support::folder("no-drop-history"));
    support::run(
        &db,
        "CREATE TABLE gauges (id TEXT PRIMARY KEY, state TEXT NOT NULL) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
    );
    let before = printed(&db, "gauges");
    assert!(says(&before, "HISTORY CURRENT ONLY"), "{before}");
    for sql in [
        "ALTER TABLE gauges DROP HISTORY",
        "ALTER TABLE gauges SET HISTORY NONE",
    ] {
        if let Ok(result) = db.execute(sql, &params! {}) {
            panic!("{sql}\nwas accepted: {result:?}");
        }
        assert_eq!(printed(&db, "gauges"), before, "after {sql}");
    }
    support::run(&db, "ALTER TABLE gauges SET HISTORY ALL");
    assert!(!says(&printed(&db, "gauges"), "CURRENT ONLY"));
    // What HISTORY CURRENT ONLY needed may go once every version is kept.
    support::run(&db, "ALTER TABLE gauges SET SYNC CONFLICT KEEP FIRST");
}

#[test]
fn an_alter_or_a_drop_in_a_transaction_takes_hold_when_it_commits() {
    let db = support::open(&support::folder("alter-in-transaction"));
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)");
    support::run(&db, "CREATE TABLE gone (id INTEGER PRIMARY KEY)");
    support::run(&db, "INSERT INTO t (id, a) VALUES (1, 'x')");
    let before = printed(&db, "t");

    // Rolled back, it leaves the table as it was.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'new'");
    support::run(&tx, "DROP TABLE gone");
    tx.rollback();
    assert_eq!(printed(&db, "t"), before);
    assert_eq!(db.table_names(), ["gone", "t"]);
    assert!(matches!(
        support::refusal(&db, "SELECT c FROM t"),
        Error::ColumnNotFound { .. }
    ));

    // A transaction either declares or writes rows, with ALTER and DROP as with CREATE.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'new'");
    assert!(matches!(
        support::refusal(&tx, "INSERT INTO t (id, a) VALUES (2, 'y')"),
        Error::MixedSchemaAndRowWrite
    ));
    support::run(&tx, "DROP TABLE gone");
    // Until it commits, nobody else sees either.
    assert_eq!(db.table_names(), ["gone", "t"]);
    assert_eq!(printed(&db, "t"), before);
    tx.commit().expect("commit the two declarations");
    assert_eq!(db.table_names(), ["t"]);
    assert_eq!(
        support::rows(&db, "SELECT id, a, c FROM t"),
        [[Value::Int64(1), support::text("x"), support::text("new")]]
    );
    let tx = db.transaction().expect("begin");
    support::run(&tx, "INSERT INTO t (id, a) VALUES (2, 'y')");
    for sql in [
        "ALTER TABLE t DROP COLUMN c",
        "DROP TABLE t",
        "ALTER TABLE t SET SYNC OFF",
    ] {
        assert!(
            matches!(support::refusal(&tx, sql), Error::MixedSchemaAndRowWrite),
            "{sql}"
        );
    }
    tx.commit().expect("commit the row");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 2);
}

#[test]
fn a_vector_column_far_larger_than_the_memory_limit_is_dropped_under_it() {
    const DIMENSION: usize = 64;
    const VECTORS: i64 = 20_000;
    const HELD: u64 = VECTORS as u64 * DIMENSION as u64 * 4;
    const KIB: usize = 1 << 10;
    const MIB: usize = 1 << 20;
    // Two stores take the same writes: 20,000 vectors of 64 numbers, 5,120,000 bytes of them,
    // in four partitions, all sealed, and then four tables of large text, 12,800,000 bytes.
    // One of them drops the vector column in between, under a memory limit.
    let store = |test: &str, drop: bool| -> u64 {
        let folder = support::folder(test);
        let db = support::open(&folder);
        support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
        for first in (1..=VECTORS).step_by(1000) {
            let tx = db.transaction().expect("begin");
            support::insert_docs(&tx, first..first + 1000, DIMENSION, |n| {
                format!("p{}", n % 4)
            });
            tx.commit().expect("commit a thousand vectors");
        }
        assert_eq!(support::seal_all(&db), VECTORS as u64);
        support::close(db);
        if drop {
            // 9 MiB is what a store needs to open at all. The 384 KiB above it are less than
            // a tenth of the vectors, which is 512,000 bytes.
            let mut options = DatabaseOpenOptions::default();
            options.memory_limit = Some(9 * MIB + 384 * KIB);
            let db = support::open_with(&folder, options).expect("open under the limit");
            let sql = "ALTER TABLE docs DROP COLUMN v";
            if let Err(error) = db.execute(sql, &params! {}) {
                panic!("{sql}\nof {VECTORS} vectors was refused under the limit: {error:?}");
            }
            // The column is gone, its rows are not, and a search on it is refused by name.
            assert!(!says(&printed(&db, "docs"), "v VECTOR"));
            let sql = support::nearest_sql("docs", "v", None, 10);
            match support::search(&db, &sql, &support::vector(1, DIMENSION)) {
                Err(Error::ColumnNotFound { table, column }) => {
                    assert_eq!((table.as_str(), column.as_str()), ("docs", "v"));
                }
                other => panic!("a search on the dropped column answered {other:?}"),
            }
            support::close(db);
        }
        let db = support::open(&folder);
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM docs"), VECTORS);
        for (table, seed) in [("n0", 0), ("n1", ROWS), ("n2", 2 * ROWS), ("n3", 3 * ROWS)] {
            support::run(
                &db,
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT)"),
            );
            fill(&db, table, "body", true, seed);
        }
        support::close(db);
        data_file(&folder)
    };
    let kept = store("drop-vector-column-kept", false);
    let dropped = store("drop-vector-column-dropped", true);
    // The store that dropped the column has let its vectors, blocks and waiting entries go,
    // so the text written afterwards took their room.
    assert!(
        dropped + HELD / 2 <= kept,
        "after DROP COLUMN of {HELD} bytes of vectors the data file is {dropped} bytes long, \
         and the same store with the column kept is {kept}: the column's bytes are still held"
    );
}

#[test]
fn a_column_added_and_dropped_in_one_transaction_leaves_nothing_stored() {
    // Two stores hold the same 400 rows. One of them adds a column whose default is 8,000
    // letters, 3,200,000 bytes over its rows, and drops it again in the same transaction.
    let store = |test: &str, churn: bool| -> u64 {
        let folder = support::folder(test);
        let db = support::open(&folder);
        support::run(
            &db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        );
        fill(&db, "notes", "body", true, 0);
        if churn {
            let before = printed(&db, "notes");
            let default = support::letters(7, LETTERS);
            let tx = db.transaction().expect("begin");
            support::run(
                &tx,
                &format!("ALTER TABLE notes ADD COLUMN extra TEXT DEFAULT '{default}'"),
            );
            support::run(&tx, "ALTER TABLE notes DROP COLUMN extra");
            tx.commit().expect("commit the column added and dropped");
            // The table declares what it declared before, and has no such column.
            assert_eq!(printed(&db, "notes"), before);
            assert!(matches!(
                support::refusal(&db, "SELECT extra FROM notes"),
                Error::ColumnNotFound { .. }
            ));
            // A column of that name added now finds nothing kept for it in any row.
            support::run(&db, "ALTER TABLE notes ADD COLUMN extra TEXT");
            assert_eq!(
                support::int(&db, "SELECT COUNT(*) FROM notes WHERE extra IS NULL"),
                ROWS
            );
            support::run(&db, "ALTER TABLE notes DROP COLUMN extra");
        }
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), ROWS);
        assert_eq!(
            support::rows(&db, "SELECT body FROM notes WHERE id = 77"),
            [[support::text(&support::letters(77, LETTERS))]]
        );
        support::close(db);
        data_file(&folder)
    };
    let plain = store("added-and-dropped-plain", false);
    let churned = store("added-and-dropped-churned", true);
    assert!(
        churned <= plain + PUT_IN / 4,
        "the store whose column was added and dropped in one transaction is {churned} bytes \
         long and the same store without it is {plain}: something is still held for the column"
    );
}

#[test]
fn a_table_dropped_and_declared_again_in_one_transaction_reads_none_of_the_old_rows() {
    let folder = support::folder("dropped-and-declared-again");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)");
    support::run(
        &db,
        "INSERT INTO t (id, a) VALUES (1, 'x'), (2, 'y'), (3, 'z')",
    );
    let tx = db.transaction().expect("begin");
    support::run(&tx, "DROP TABLE t");
    support::run(
        &tx,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, b TEXT)",
    );
    // Inside the transaction the new table is empty: no row of the old one is counted or read.
    assert_eq!(support::int(&tx, "SELECT COUNT(*) FROM t"), 0);
    // Until it commits, everyone else reads the old table.
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 3);
    tx.commit()
        .expect("commit the table dropped and declared again");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
    assert!(says(&printed(&db, "t"), "n INTEGER"));
    assert!(matches!(
        support::refusal(&db, "SELECT a FROM t"),
        Error::ColumnNotFound { .. }
    ));
    // A row written under an old row's key is the only row there is.
    support::run(&db, "INSERT INTO t (id, n, b) VALUES (1, 5, 'new')");
    assert_eq!(
        support::rows(&db, "SELECT id, n, b FROM t"),
        [[Value::Int64(1), Value::Int64(5), support::text("new")]]
    );
}

#[test]
fn dropping_a_column_never_shifts_what_the_other_columns_hold() {
    let db = support::open(&support::folder("alter-drop-no-shift"));
    support::run(&db, "CREATE TABLE p (c INTEGER PRIMARY KEY)");
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, b INTEGER, c INTEGER REFERENCES p(c), \
         UNIQUE (c, b))",
    );
    support::run(&db, "INSERT INTO p (c) VALUES (200)");
    support::run(&db, "INSERT INTO t (id, b, c) VALUES (1, 100, 200)");

    // Inside the transaction that ran the ALTER, the column still holds 200.
    let tx = db.transaction().expect("begin");
    support::run(&tx, "ALTER TABLE t DROP COLUMN b CASCADE");
    assert_eq!(
        support::rows(&tx, "SELECT c FROM t"),
        [[Value::Int64(200)]],
        "inside the transaction that dropped b"
    );
    tx.commit().expect("commit the drop");

    // After the commit, c still holds 200 (the reference to p(200) refuses its parent's removal).
    assert!(
        matches!(
            support::refusal(&db, "DELETE FROM p WHERE c = 200"),
            Error::ForeignKeyViolation { .. }
        ),
        "t still references p(200) after b was dropped"
    );

    // Rows shorter than the table, drops in the middle, adds after: each cell is its column's.
    support::run(
        &db,
        "CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    );
    support::run(&db, "INSERT INTO u (id, a, b) VALUES (1, 10, 20)");
    let tx = db.transaction().expect("begin");
    for change in [
        "ADD COLUMN c INTEGER DEFAULT 30",
        "DROP COLUMN a",
        "ADD COLUMN d INTEGER DEFAULT 40",
        "DROP COLUMN c",
    ] {
        support::run(&tx, &format!("ALTER TABLE u {change}"));
    }
    let wanted = [[Value::Int64(1), Value::Int64(20), Value::Int64(40)]];
    assert_eq!(
        support::rows(&tx, "SELECT id, b, d FROM u"),
        wanted,
        "inside the transaction"
    );
    tx.commit().expect("commit the changes");
    support::run(&db, "INSERT INTO u (id, b, d) VALUES (2, 21, 41)");
    assert_eq!(
        support::rows(&db, "SELECT id, b, d FROM u ORDER BY id"),
        [
            wanted[0].clone(),
            [Value::Int64(2), Value::Int64(21), Value::Int64(41)]
        ]
    );
}
