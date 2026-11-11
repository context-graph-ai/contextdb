//! What a statement may say about a VECTOR column, through every door: the search mode a
//! request runs in, the refusal of a search that asks for an index, every other refusal of a
//! similarity search by its kind, and the three clauses of a VECTOR column's declaration.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{
    Database, Error, ReadSession, ReadSessionOptions, Value, VectorIndexRef,
    VectorPartitionDeclarationIssue, VectorPolicyDeclarationIssue, params,
};

use support::{Call, Near, Run};

const DIMENSION: usize = 8;

/// How the refusal of a statement that only a later version runs ends.
const LATER_VERSION: &str = "is not supported in this version of the library";

fn column(table: &str, column: &str) -> VectorIndexRef {
    VectorIndexRef {
        table: table.to_owned(),
        column: column.to_owned(),
    }
}

/// Declares a table like `docs` under another name, with these clauses after `VECTOR(8)`.
fn declare(db: &Database, table: &str, clauses: &str) {
    support::run(
        db,
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, \
             tag INTEGER NOT NULL, v VECTOR({DIMENSION}) {clauses})"
        ),
    );
}

/// Inserts rows with these numbers: row `n` under the context `c<n % 3>`.
fn fill(on: &impl Run, table: &str, numbers: std::ops::RangeInclusive<i64>) {
    for n in numbers {
        let inserted = on.run(
            &format!("INSERT INTO {table} (id, ctx, tag, v) VALUES ($id, $ctx, $tag, $v)"),
            &params! {
                "id" => n, "ctx" => format!("c{}", n % 3), "tag" => n % 50,
                "v" => support::vector(n, DIMENSION),
            },
        );
        if let Err(error) = inserted {
            panic!("row {n} of {table} was refused: {error:?}");
        }
    }
}

/// A search of `table` with this filter and this clause between ORDER BY and LIMIT.
fn search_sql(table: &str, filter: Option<&str>, clause: &str, k: usize) -> String {
    let filter = filter.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
    format!(
        "SELECT id, v <=> $q AS distance FROM {table}{filter} ORDER BY distance {clause} LIMIT {k}"
    )
}

/// Asserts that a search was refused because it asked for an index, naming this column.
fn refused_as_indexed(outcome: contextdb::Result<contextdb::QueryResult>, table: &str, sql: &str) {
    match outcome {
        Err(Error::VectorIndexedRouteUnavailable { index }) => {
            assert_eq!(index, column(table, "v"), "the column named for {sql}");
        }
        other => panic!("{sql}\nasked for an index and got {other:?}"),
    }
}

/// True for a sentence that names both the function `ROW_VECTOR`, in any letter case, and the
/// `<=>` it was written beside. Every test of that refusal holds it to both.
fn names_row_vector_beside_a_distance(sentence: &str) -> bool {
    sentence.to_uppercase().contains("ROW_VECTOR") && sentence.contains("<=>")
}

#[test]
fn vector_search_mode_routes_each_request() {
    let folder = support::folder("search-mode-routes");
    let db = support::open(&folder);
    // Four tables with the same rows: no SEARCH_MODE, and each of the three.
    let tables = [
        ("plain", "PARTITION_KEY (ctx)"),
        ("auto", "PARTITION_KEY (ctx) SEARCH_MODE AUTO"),
        ("exact", "PARTITION_KEY (ctx) SEARCH_MODE EXACT"),
        ("indexed", "PARTITION_KEY (ctx) SEARCH_MODE INDEXED"),
    ];
    for (table, clauses) in tables {
        declare(&db, table, clauses);
    }
    // Rows 1 to 80 in blocks, rows 81 to 120 on pending lists.
    for (table, _) in tables {
        let tx = db.transaction().expect("begin");
        fill(&tx, table, 1..=80);
        tx.commit().expect("commit");
    }
    support::seal_all(&db);
    for (table, _) in tables {
        let tx = db.transaction().expect("begin");
        fill(&tx, table, 81..=120);
        tx.commit().expect("commit");
    }
    let query = support::vector(90_000, DIMENSION);
    type Scope = (Option<&'static str>, fn(&[Value]) -> bool);
    let scopes: [Scope; 3] = [
        (None, |_| true),
        (Some("ctx = 'c1'"), |row| row[1] == support::text("c1")),
        (Some("ctx IN ('c0', 'c2')"), |row| {
            row[1] != support::text("c1")
        }),
    ];

    // Through one door: for every table and scope, what each way of writing the mode gives.
    let through = |on: &dyn Run, door: &str| {
        for (table, _) in tables {
            let stored = support::stored(&on, &format!("SELECT id, ctx, tag, v FROM {table}"));
            for (filter, passes) in scopes {
                let wanted = stored.nearest(passes, &query, 10);
                let answer = |clause: &str| -> u64 {
                    let sql = search_sql(table, filter, clause, 10);
                    let what = format!("through {door}");
                    let result = support::assert_nearest(&on, &sql, &query, &wanted, &what);
                    result.rows_examined()
                };
                let refused = |clause: &str| {
                    let sql = search_sql(table, filter, clause, 10);
                    refused_as_indexed(support::search(&on, &sql, &query), table, &sql);
                };
                // USE VECTOR overrides the column's mode both ways: EXACT and AUTO answer on
                // every table, the one declared INDEXED too, and INDEXED is refused on every
                // table, the one declared EXACT too. AUTO and EXACT are one search: the same
                // rows for the same counted work.
                let exact = answer("USE VECTOR EXACT");
                assert_eq!(
                    answer("USE VECTOR AUTO"),
                    exact,
                    "AUTO and EXACT on {table}"
                );
                assert_eq!(answer("use vector exact"), exact, "{table}");
                refused("USE VECTOR INDEXED");
                // With no clause the column's mode is in force, and no mode is AUTO.
                if table == "indexed" {
                    refused("");
                } else {
                    assert_eq!(answer(""), exact, "no clause on {table}");
                }
            }
        }
    };
    through(&db, "the writer");
    let tx = db.transaction().expect("begin");
    through(&tx, "a transaction");
    tx.rollback();
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    through(&reader, "a read session");

    // A table of one row and a column declared SEARCH_MODE EXACT, with nothing else set.
    support::run(
        &db,
        "CREATE TABLE two (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE EXACT)",
    );
    support::run(&db, "INSERT INTO two (id, v) VALUES (1, [1.0, 0.0])");
    let found = db
        .execute(
            "SELECT id, v <=> $q AS distance FROM two ORDER BY distance LIMIT 5",
            &params! { "q" => vec![1.0_f32, 0.0] },
        )
        .expect("search the table of one row");
    let wanted = Near {
        key: Value::Int64(1),
        distance: support::distance(&[1.0, 0.0], &[1.0, 0.0]),
    };
    assert_eq!(support::found(&found, "the table of one row"), [wanted]);
}

#[test]
fn indexed_search_is_refused_at_every_door() {
    let folder = support::folder("indexed-refused");
    let db = support::open(&folder);
    declare(&db, "plain", "PARTITION_KEY (ctx)");
    declare(&db, "indexed", "PARTITION_KEY (ctx) SEARCH_MODE INDEXED");
    declare(&db, "empty", "SEARCH_MODE INDEXED");
    // Two thousand pending rows in each: a search that read rows would read them all.
    for table in ["plain", "indexed"] {
        let tx = db.transaction().expect("begin");
        fill(&tx, table, 1..=2000);
        tx.commit().expect("commit");
    }
    let query = support::vector(91_000, DIMENSION);
    // Every way a search asks for an index: in the statement, by the column's declaration,
    // both, on a table with no row, and for a partition no row is in.
    let asking: Vec<(&str, String)> = vec![
        ("plain", search_sql("plain", None, "USE VECTOR INDEXED", 5)),
        (
            "plain",
            search_sql("plain", Some("ctx = 'c1'"), "USE VECTOR INDEXED", 5),
        ),
        (
            "plain",
            search_sql("plain", Some("ctx = 'zz'"), "USE VECTOR INDEXED", 5),
        ),
        ("indexed", search_sql("indexed", None, "", 5)),
        ("indexed", search_sql("indexed", Some("ctx = 'zz'"), "", 5)),
        (
            "indexed",
            search_sql("indexed", None, "USE VECTOR INDEXED", 5),
        ),
        (
            "indexed",
            "SELECT * FROM indexed ORDER BY v <=> $q LIMIT 1".to_owned(),
        ),
        ("empty", search_sql("empty", None, "", 5)),
        ("empty", search_sql("empty", None, "USE VECTOR INDEXED", 5)),
    ];
    let before = (
        db.committed_watermark(),
        support::int(&db, "SELECT COUNT(*) FROM indexed"),
    );
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let tx = db.transaction().expect("begin");
    for (table, sql) in &asking {
        // Never an answer, through the writer, a transaction and a reader alike.
        refused_as_indexed(support::search(&db, sql, &query), table, sql);
        refused_as_indexed(support::search(&tx, sql, &query), table, sql);
        refused_as_indexed(support::search(&reader, sql, &query), table, sql);
    }
    // The refused transaction is as usable as before, and nothing changed.
    fill(&tx, "indexed", 2001..=2001);
    tx.rollback();
    assert_eq!(
        (
            db.committed_watermark(),
            support::int(&db, "SELECT COUNT(*) FROM indexed")
        ),
        before
    );
    // The same column answers when the statement asks for the exact search.
    let stored = support::stored(&db, "SELECT id, ctx, tag, v FROM indexed");
    let wanted = stored.nearest(|_| true, &query, 5);
    let sql = search_sql("indexed", None, "USE VECTOR EXACT", 5);
    support::assert_nearest(&reader, &sql, &query, &wanted, "EXACT on an INDEXED column");
    support::close(db);

    // At once, with no row read. Each statement runs first in a session of its own, whose
    // storage door counts the reads of the data file. The refused search reads no more of the
    // file than a count of the whole table, which reads no row; a search that answers reads
    // more than that count, so the comparison can tell.
    let reads_of = |sql: &str| -> (u64, contextdb::Result<contextdb::QueryResult>) {
        let reads = Arc::new(AtomicU64::new(0));
        let mut options = ReadSessionOptions::default();
        options.storage = Some(support::storage({
            let reads = reads.clone();
            move |call| {
                if matches!(call, Call::Read(..)) {
                    reads.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
        let session = ReadSession::open_with_options(&folder, options).expect("open for reading");
        let opened = reads.load(Ordering::SeqCst);
        let outcome = support::search(&session, sql, &query);
        (reads.load(Ordering::SeqCst) - opened, outcome)
    };
    let (count_reads, count) = reads_of("SELECT COUNT(*) FROM indexed");
    let count = count.expect("count the table");
    assert_eq!(count.rows(), [[Value::Int64(2000)]]);
    assert_eq!(count.rows_examined(), 0);
    for (table, sql) in asking.iter().filter(|(table, _)| *table == "indexed") {
        let (reads, outcome) = reads_of(sql);
        refused_as_indexed(outcome, table, sql);
        assert!(
            reads <= count_reads,
            "{sql}\nwas refused after {reads} storage reads; a count that reads no row made \
             {count_reads}"
        );
    }
    let (exact_reads, exact) = reads_of(&sql);
    assert_eq!(exact.expect("the exact search").rows().len(), 5);
    assert!(
        exact_reads > count_reads,
        "an exact search of 2,000 pending rows made {exact_reads} storage reads, a count \
         {count_reads}"
    );
}

#[test]
fn vector_query_errors_are_typed() {
    let folder = support::folder("vector-query-errors");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, r REAL, v VECTOR(4), \
         w VECTOR(4))",
    );
    for n in 1..=20_i64 {
        db.execute(
            "INSERT INTO docs (id, ctx, r, v, w) VALUES ($id, 'a', 0.5, $v, $w)",
            &params! {
                "id" => n, "v" => support::vector(n, 4), "w" => support::vector(100 + n, 4),
            },
        )
        .expect("insert a row");
    }
    // Half of them sealed.
    support::seal_all(&db);
    support::run(&db, "UPDATE docs SET r = 1.5 WHERE id > 10");
    for n in 21..=30_i64 {
        db.execute(
            "INSERT INTO docs (id, ctx, v, w) VALUES ($id, 'a', $v, $w)",
            &params! {
                "id" => n, "v" => support::vector(n, 4), "w" => support::vector(100 + n, 4),
            },
        )
        .expect("insert a row");
    }
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let state = |db: &Database| {
        let rows = support::rows(db, "SELECT id, ctx, r, v, w FROM docs ORDER BY id");
        (rows, db.committed_watermark())
    };
    let before = state(&db);

    let good = support::vector(92_000, 4);
    let with = |index: usize, number: f32| -> Vec<f32> {
        let mut vector = good.clone();
        vector[index] = number;
        vector
    };
    // The refusal of a statement run with `$q` and `$v` bound to these vectors. A SELECT is
    // run through the writer, a transaction and a reader, and must be refused alike.
    let refusal = |sql: &str, q: &[f32], v: &[f32]| -> Error {
        let bound = params! { "q" => q.to_vec(), "v" => v.to_vec(), "p" => vec![0.5_f32; 4] };
        let kind = |error: &Error| format!("{error:?}");
        let from_writer = match db.execute(sql, &bound) {
            Err(error) => error,
            Ok(result) => panic!("{sql}\nwas not refused: {result:?}"),
        };
        if sql.starts_with("SELECT") {
            let tx = db.transaction().expect("begin");
            for from in [tx.execute(sql, &bound), reader.execute(sql, &bound)] {
                match from {
                    Err(error) => assert_eq!(kind(&error), kind(&from_writer), "{sql}"),
                    Ok(result) => panic!("{sql}\nwas not refused at every door: {result:?}"),
                }
            }
        }
        from_writer
    };
    let plan_error = |sql: &str, names: &[&str]| match refusal(sql, &good, &good) {
        Error::PlanError(text) => {
            for name in names {
                assert!(
                    text.contains(name),
                    "{sql}\nwas refused without naming {name}: {text}"
                );
            }
        }
        other => panic!("{sql}\nwas refused as {other:?}, not as a plan error"),
    };

    // A search with no LIMIT has no kind of its own: it is refused as every statement of a
    // later version is, in a sentence that names it.
    for sql in [
        "SELECT id FROM docs ORDER BY v <=> $q",
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance",
        "SELECT id FROM docs WHERE ctx = 'a' ORDER BY v <=> $q USE VECTOR EXACT",
    ] {
        match refusal(sql, &good, &good) {
            Error::PlanError(text) => assert!(
                text.contains("vector search without LIMIT") && text.ends_with(LATER_VERSION),
                "{sql}\nwas refused in the sentence: {text}"
            ),
            other => panic!("{sql}\nwas refused as {other:?}, not as a plan error"),
        }
    }
    // USE VECTOR in a SELECT that does not order by a distance.
    for sql in [
        "SELECT id FROM docs ORDER BY id USE VECTOR EXACT LIMIT 3",
        "SELECT id FROM docs USE VECTOR AUTO LIMIT 3",
        "SELECT id FROM docs WHERE ctx = 'a' ORDER BY r, id USE VECTOR INDEXED LIMIT 3",
    ] {
        let refused = refusal(sql, &good, &good);
        assert!(
            matches!(refused, Error::UseVectorRequiresVectorOrder),
            "{sql}\ngot {refused:?}"
        );
    }
    // A distance from a column that is not a VECTOR column.
    for (sql, not_a_vector) in [
        ("SELECT id FROM docs ORDER BY ctx <=> $q LIMIT 3", "ctx"),
        (
            "SELECT id, r <=> $q AS distance FROM docs ORDER BY distance LIMIT 3",
            "r",
        ),
    ] {
        match refusal(sql, &good, &good) {
            Error::UnknownVectorIndex { index } => assert_eq!(index, column("docs", not_a_vector)),
            other => panic!("{sql}\ngot {other:?}"),
        }
    }
    // A vector of another length than the column's.
    let search = "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance LIMIT 3";
    for length in [1_usize, 3, 5, 768] {
        match refusal(search, &vec![0.5; length], &good) {
            Error::VectorIndexDimensionMismatch {
                index,
                expected,
                actual,
            } => {
                assert_eq!(index, column("docs", "v"));
                assert_eq!((expected, actual), (4, length));
            }
            other => panic!("a search with a vector of {length} numbers got {other:?}"),
        }
    }
    // A number that is not finite, in the vector of a search and in a vector that is written.
    let not_finite = [
        (0, f32::NAN),
        (2, f32::NAN),
        (3, f32::INFINITY),
        (1, f32::NEG_INFINITY),
    ];
    for (position, number) in not_finite {
        let bad = with(position, number);
        let cases = [
            (search, &bad, &good, "v"),
            (
                "SELECT id FROM docs ORDER BY w <=> $q LIMIT 3",
                &bad,
                &good,
                "w",
            ),
            (
                "INSERT INTO docs (id, ctx, v) VALUES (99, 'a', $v)",
                &good,
                &bad,
                "v",
            ),
            (
                "INSERT INTO docs (id, ctx, v, w) VALUES (99, 'a', $q, $v)",
                &good,
                &bad,
                "w",
            ),
            ("UPDATE docs SET v = $v WHERE id = 1", &good, &bad, "v"),
            ("UPDATE docs SET w = $v WHERE id >= 1", &good, &bad, "w"),
        ];
        for (sql, q, v, named) in cases {
            match refusal(sql, q, v) {
                Error::InvalidVectorValue {
                    index,
                    position: at,
                } => {
                    assert_eq!(index, column("docs", named), "{sql}");
                    assert_eq!(at, position, "{sql}: where {number} stands");
                }
                other => panic!("{sql}\nwith {number} at {position} got {other:?}"),
            }
        }
    }

    // ROW_VECTOR is a function this version does not have: the refusal names the function and
    // the `<=>` it stands beside.
    let sql = "SELECT id FROM docs ORDER BY v <=> ROW_VECTOR('docs', 'v', 1) LIMIT 3";
    match refusal(sql, &good, &good) {
        Error::PlanError(text) => assert!(
            names_row_vector_beside_a_distance(&text),
            "{sql}\nwas refused without naming both ROW_VECTOR and <=>: {text}"
        ),
        other => panic!("{sql}\ngot {other:?}"),
    }
    // A distance anywhere but as the one ORDER BY key of a search, ascending, and in that
    // search's select list.
    for sql in [
        // In a WHERE.
        "SELECT id FROM docs WHERE v <=> $q < 0.5",
        "SELECT id FROM docs WHERE v <=> $q < 0.5 ORDER BY id LIMIT 3",
        "SELECT id FROM docs WHERE v <=> $q < 0.5 ORDER BY v <=> $q LIMIT 3",
        "DELETE FROM docs WHERE v <=> $q < 0.5",
        // In a select list with no ORDER BY on it.
        "SELECT id, v <=> $q AS distance FROM docs",
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY id LIMIT 3",
        // With DESC.
        "SELECT id FROM docs ORDER BY v <=> $q DESC LIMIT 3",
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance DESC LIMIT 3",
        // Beside a second ORDER BY item.
        "SELECT id FROM docs ORDER BY v <=> $q, id LIMIT 3",
        "SELECT id FROM docs ORDER BY id, v <=> $q LIMIT 3",
        // With a left side that is not a column.
        "SELECT id FROM docs ORDER BY $q <=> v LIMIT 3",
        // In a write.
        "UPDATE docs SET r = v <=> $q WHERE id = 1",
        "INSERT INTO docs (id, ctx, r) VALUES (99, 'a', $v <=> $q)",
    ] {
        plan_error(sql, &["<=>"]);
    }
    // A second distance that is not the ORDER BY's own: of another column, or to another
    // vector. The refusal names `<=>` and the column, as `column <name>`: the letter alone is
    // in nearly every sentence.
    plan_error(
        "SELECT id, w <=> $q AS other FROM docs ORDER BY v <=> $q LIMIT 3",
        &["<=>", "column w"],
    );
    plan_error(
        "SELECT id, v <=> $p AS other FROM docs ORDER BY v <=> $q LIMIT 3",
        &["<=>", "column v"],
    );
    plan_error(
        "SELECT id, v <=> $q AS distance, 1 - (w <=> $q) AS other FROM docs \
         ORDER BY distance LIMIT 3",
        &["<=>", "column w"],
    );

    // There is no score column: on a table with no such column it is a name like any other.
    for sql in [
        "SELECT id, score FROM docs ORDER BY v <=> $q LIMIT 3",
        "SELECT id, v <=> $q AS distance, score FROM docs ORDER BY distance LIMIT 3",
    ] {
        match refusal(sql, &good, &good) {
            Error::ColumnNotFound { table, column } => {
                assert_eq!(
                    (table.as_str(), column.as_str()),
                    ("docs", "score"),
                    "{sql}"
                );
            }
            other => panic!("{sql}\ngot {other:?}"),
        }
    }

    // Each of them left the store as it was, and the column still answers.
    assert!(state(&db) == before);
    let stored = support::stored(&db, "SELECT id, ctx, r, v FROM docs");
    let wanted = stored.nearest(|_| true, &good, 3);
    support::assert_nearest(&db, search, &good, &wanted, "after every refusal");
}

#[test]
fn what_the_parser_reads_and_this_version_does_not_run_is_refused_when_planned() {
    let folder = support::folder("refused-when-planned");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE pairs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v VECTOR(3), \
         w VECTOR(3))",
    );
    support::run(
        &db,
        "INSERT INTO pairs (id, a, b, v, w) VALUES (1, 1, 2, [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]), \
         (2, 3, 4, [0.0, 1.0, 0.0], [1.0, 0.0, 0.0])",
    );
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let state = |db: &Database| {
        let rows = support::rows(db, "SELECT id, a, b, v, w FROM pairs ORDER BY id");
        (rows, db.committed_watermark())
    };
    let before = state(&db);

    // The sentence a statement is refused in. It is a plan error, never a syntax error, and
    // the same through the writer, a transaction and a reader.
    let refused_when_planned = |sql: &str| -> String {
        let tx = db.transaction().expect("begin");
        let none = params! {};
        let doors = [
            db.execute(sql, &none),
            tx.execute(sql, &none),
            reader.execute(sql, &none),
        ];
        let sentences = doors.map(|outcome| match outcome {
            Err(Error::PlanError(text)) => text,
            Err(other) => panic!("{sql}\nwas refused as {other:?}, not as a plan error"),
            Ok(result) => panic!("{sql}\nwas not refused: {result:?}"),
        });
        assert!(
            sentences[0] == sentences[1] && sentences[0] == sentences[2],
            "{sql}\nwas refused in three sentences: {sentences:?}"
        );
        let [sentence, ..] = sentences;
        sentence
    };

    // A row value is read wherever an expression is, and refused by name anywhere but beside
    // IN: compared with `=`, and in a select list.
    for sql in [
        "SELECT id FROM pairs WHERE (a, b) = (1, 2)",
        "SELECT (a, b) FROM pairs",
    ] {
        let sentence = refused_when_planned(sql);
        assert!(
            sentence.to_lowercase().contains("row value"),
            "{sql}\nwas refused without naming the row value: {sentence}"
        );
    }
    // Anything is read on the right of `<=>`, and anything but a vector literal, its quoted
    // text or a bound name is refused naming `<=>`: a column, and ROW_VECTOR(...), whose
    // refusal names the function too.
    let sql = "SELECT id FROM pairs ORDER BY v <=> w LIMIT 3";
    let sentence = refused_when_planned(sql);
    assert!(
        sentence.contains("<=>"),
        "{sql}\nwas refused without naming <=>: {sentence}"
    );
    let sql = "SELECT id FROM pairs ORDER BY v <=> ROW_VECTOR('pairs', 'v', 1) LIMIT 3";
    let sentence = refused_when_planned(sql);
    assert!(
        names_row_vector_beside_a_distance(&sentence),
        "{sql}\nwas refused without naming both ROW_VECTOR and <=>: {sentence}"
    );
    // The two statements that describe an index and its partitions, and a search with no
    // LIMIT, are refused as every statement of a later version is, each by its name.
    for (sql, name) in [
        ("SHOW VECTOR_INDEXES", "VECTOR_INDEXES"),
        ("SHOW VECTOR_PARTITIONS", "VECTOR_PARTITIONS"),
        (
            "SELECT id FROM pairs ORDER BY v <=> [1.0, 0.0, 0.0]",
            "vector search without LIMIT",
        ),
    ] {
        let sentence = refused_when_planned(sql);
        assert!(
            sentence.contains(name) && sentence.ends_with(LATER_VERSION),
            "{sql}\nwas refused in the sentence: {sentence}"
        );
    }

    // Each of them left the store as it was, and the column still answers.
    assert!(state(&db) == before);
    let stored = support::stored(&db, "SELECT id, a, b, v FROM pairs");
    let query = [1.0_f32, 0.0, 0.0];
    let wanted = stored.nearest(|_| true, &query, 2);
    let sql = support::nearest_sql("pairs", "v", None, 2);
    support::assert_nearest(&db, &sql, &query, &wanted, "after every refusal");
}

/// The words of a declaration's line for one column: what stands between spaces, commas and
/// parentheses.
fn words(declaration: &str, column: &str) -> Vec<String> {
    let line = declaration
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{column} ")))
        .unwrap_or_else(|| panic!("no line for the column {column} in\n{declaration}"));
    line.split(|c: char| c.is_whitespace() || "(),".contains(c))
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

#[test]
fn vector_column_declarations_are_checked_stored_and_printed_back() {
    let folder = support::folder("vector-declarations");
    let db = support::open(&folder);
    let fresh = db.committed_watermark();

    // Each clause that cannot stand, by its kind, naming the column; and no table declared.
    use VectorPartitionDeclarationIssue as Issue;
    let refused: [(&str, Issue); 14] = [
        (
            "v VECTOR(4) PARTITION_KEY (nowhere)",
            Issue::UnknownPartitionKeyColumn,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx, nowhere)",
            Issue::UnknownPartitionKeyColumn,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (loose)",
            Issue::NullablePartitionKeyColumn,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx, loose)",
            Issue::NullablePartitionKeyColumn,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (r)",
            Issue::UnsupportedPartitionKeyColumnType,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (doc)",
            Issue::UnsupportedPartitionKeyColumnType,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (other)",
            Issue::UnsupportedPartitionKeyColumnType,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx, ctx)",
            Issue::DuplicatePartitionKeyColumn,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (v) NOT NULL",
            Issue::VectorColumnInPartitionKey,
        ),
        (
            "v VECTOR(4) MAX_PARTITIONS 4",
            Issue::MaxPartitionsWithoutPartitionKey,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 0",
            Issue::MaxPartitionsNotPositive,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS -1",
            Issue::MaxPartitionsNotPositive,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 4294967296",
            Issue::MaxPartitionsOutOfRange,
        ),
        (
            "v VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 99999999999999999999999999999999",
            Issue::MaxPartitionsOutOfRange,
        ),
    ];
    let table = |vector_column: &str| {
        format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, kind INTEGER NOT NULL, \
             loose TEXT, r REAL NOT NULL, doc JSON NOT NULL, other VECTOR(4) NOT NULL, \
             {vector_column})"
        )
    };
    for (vector_column, wanted) in refused {
        let sql = table(vector_column);
        match support::refusal(&db, &sql) {
            Error::InvalidVectorPartitionDeclaration { index, issue } => {
                assert_eq!(index, column("t", "v"), "{sql}");
                assert_eq!(issue, wanted, "{sql}");
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
        assert!(db.table_names().is_empty(), "{sql}\ndeclared a table");
    }
    for vector_column in [
        "v VECTOR(0)",
        "v VECTOR(0) PARTITION_KEY (ctx) MAX_PARTITIONS 2",
    ] {
        let sql = table(vector_column);
        match support::refusal(&db, &sql) {
            Error::InvalidVectorPolicyDeclaration { index, issue } => {
                assert_eq!(index, column("t", "v"), "{sql}");
                assert_eq!(
                    issue,
                    VectorPolicyDeclarationIssue::DimensionsNotPositive,
                    "{sql}"
                );
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
        assert!(db.table_names().is_empty(), "{sql}\ndeclared a table");
    }
    assert_eq!(
        db.committed_watermark(),
        fresh,
        "a refused declaration committed"
    );

    // What stands: each column with the words its declaration prints back for it. The three
    // clauses come in one order; MAX_PARTITIONS is printed for every column with a
    // PARTITION_KEY, as 256 when none was written; SEARCH_MODE only when it was written.
    let stands: [(&str, &str, &[&str]); 7] = [
        ("bare", "VECTOR(4)", &["bare", "VECTOR", "4"]),
        (
            "keyed",
            "VECTOR(4) PARTITION_KEY (ctx)",
            &[
                "keyed",
                "VECTOR",
                "4",
                "PARTITION_KEY",
                "ctx",
                "MAX_PARTITIONS",
                "256",
            ],
        ),
        (
            "two_keys",
            "VECTOR(4) PARTITION_KEY (kind, ctx) MAX_PARTITIONS 4294967295",
            &[
                "two_keys",
                "VECTOR",
                "4",
                "PARTITION_KEY",
                "kind",
                "ctx",
                "MAX_PARTITIONS",
                "4294967295",
            ],
        ),
        (
            "capped",
            "VECTOR(4) PARTITION_KEY (ctx) MAX_PARTITIONS 2 SEARCH_MODE AUTO",
            &[
                "capped",
                "VECTOR",
                "4",
                "PARTITION_KEY",
                "ctx",
                "MAX_PARTITIONS",
                "2",
                "SEARCH_MODE",
                "AUTO",
            ],
        ),
        (
            "strict",
            "VECTOR(768) partition_key (ctx) search_mode exact NOT NULL",
            &[
                "strict",
                "VECTOR",
                "768",
                "PARTITION_KEY",
                "ctx",
                "MAX_PARTITIONS",
                "256",
                "SEARCH_MODE",
                "EXACT",
                "NOT",
                "NULL",
            ],
        ),
        (
            "by_index",
            "VECTOR(3) SEARCH_MODE INDEXED",
            &["by_index", "VECTOR", "3", "SEARCH_MODE", "INDEXED"],
        ),
        ("one", "VECTOR(1) MAX_PARTITIONS 1 PARTITION_KEY (ctx)", &[]),
    ];
    // The last of them writes two clauses the wrong way round, which is not read.
    let (out_of_order, standing) = stands.split_last().expect("seven columns");
    let wrong = format!(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, {} {})",
        out_of_order.0, out_of_order.1
    );
    assert!(
        matches!(
            support::refusal(&db, &wrong),
            Error::ParseError(_) | Error::PlanError(_)
        ),
        "{wrong}"
    );
    assert!(db.table_names().is_empty());

    let columns: Vec<String> = standing
        .iter()
        .map(|(name, clauses, _)| format!("{name} {clauses}"))
        .collect();
    support::run(
        &db,
        &format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, kind INTEGER NOT NULL, {})",
            columns.join(", ")
        ),
    );
    let printed = db.declaration("t").expect("the declaration of t");
    let check = |declaration: &str, state: &str| {
        for (name, _, wanted) in standing {
            assert_eq!(
                words(declaration, name),
                *wanted,
                "{state}: the line of {name}"
            );
        }
    };
    check(&printed, "as declared");

    // The limit that was printed is the limit that holds: `capped` takes two partitions.
    let insert = |db: &Database, id: i64, ctx: &str| {
        db.execute(
            "INSERT INTO t (id, ctx, kind, capped, strict) VALUES ($id, $ctx, 0, $v, $e)",
            &params! {
                "id" => id, "ctx" => ctx, "v" => support::vector(id, 4),
                "e" => support::vector(id, 768),
            },
        )
    };
    let holds = |db: &Database, state: &str| {
        for (id, ctx) in [(1, "a"), (2, "b")] {
            if let Err(error) = insert(db, id, ctx) {
                panic!("{state}: partition {ctx} of two was refused: {error:?}");
            }
        }
        match insert(db, 3, "c") {
            Err(Error::VectorPartitionLimitExceeded {
                index,
                max_partitions,
            }) => {
                assert_eq!(index, column("t", "capped"), "{state}");
                assert_eq!(max_partitions, 2, "{state}");
            }
            other => panic!("{state}: a third partition of `capped` got {other:?}"),
        }
        // And the mode that was printed is the mode in force.
        let sql = "SELECT id FROM t ORDER BY by_index <=> $q LIMIT 1";
        match db.execute(sql, &params! { "q" => vec![0.5_f32; 3] }) {
            Err(Error::VectorIndexedRouteUnavailable { index }) => {
                assert_eq!(index, column("t", "by_index"), "{state}");
            }
            other => panic!("{state}: {sql}\ngot {other:?}"),
        }
    };
    holds(&db, "as declared");

    // After reopen the same text, through the writer and through a reader.
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(
        db.declaration("t").expect("the declaration"),
        printed,
        "after reopen"
    );
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    assert_eq!(
        reader.declaration("t").expect("a reader's declaration"),
        printed
    );

    // The printed text declares the same table in a new store.
    let folder = support::folder("vector-declarations-again");
    let again = support::open(&folder);
    for statement in printed.split_inclusive(";\n") {
        support::run(&again, statement);
    }
    assert_eq!(
        again.declaration("t").expect("the declaration"),
        printed,
        "declared again"
    );
    check(
        &again.declaration("t").expect("the declaration"),
        "declared again",
    );
    holds(&again, "declared again");
}

/// How a statement was refused through the writer, a transaction and a reader, with `$q` bound
/// to this vector: it must be refused at all three, and alike. The transaction is rolled back.
fn refused_at_every_door(db: &Database, reader: &ReadSession, sql: &str, query: &[f32]) -> Error {
    let bound = params! { "q" => query.to_vec() };
    let tx = db.transaction().expect("begin");
    let doors = [
        ("the writer", db.execute(sql, &bound)),
        ("a transaction", tx.execute(sql, &bound)),
        ("a reader", reader.execute(sql, &bound)),
    ];
    let refusals = doors.map(|(door, outcome)| match outcome {
        Err(error) => error,
        Ok(result) => panic!("{sql}\nwas not refused through {door}: {result:?}"),
    });
    let kinds: Vec<String> = refusals.iter().map(|error| format!("{error:?}")).collect();
    assert!(
        kinds[0] == kinds[1] && kinds[0] == kinds[2],
        "{sql}\nwas refused in three ways: {kinds:?}"
    );
    let [from_writer, ..] = refusals;
    from_writer
}

#[test]
fn a_second_sort_key_after_a_distance_is_refused_as_a_later_form_whose_order_by_is_there() {
    let folder = support::folder("second-sort-key");
    let db = support::open(&folder);
    declare(&db, "docs", "");
    fill(&db, "docs", 1..=5);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let query = support::vector(93_000, DIMENSION);
    let before = (
        support::rows(&db, "SELECT id, ctx, tag, v FROM docs ORDER BY id"),
        db.committed_watermark(),
    );

    // What a statement is told whose distance is in no ORDER BY at all: there the ORDER BY is
    // what is missing, and the sentence may say so.
    let no_order_by = "SELECT id, v <=> $q AS distance FROM docs LIMIT 3";
    let missing = match refused_at_every_door(&db, &reader, no_order_by, &query) {
        Error::PlanError(text) => text,
        other => panic!("{no_order_by}\nwas refused as {other:?}"),
    };

    // A search with a second sort key after its distance has its ORDER BY. It is a form of a
    // later version: refused when planned, as every such form is, naming `<=>`, and not in
    // the sentence of a statement that lacks the ORDER BY. That holds with USE VECTOR too,
    // which is not told that it "requires ORDER BY ... <=>" by a statement that wrote one.
    let mut wrong = Vec::new();
    for sql in [
        "SELECT id FROM docs ORDER BY v <=> $q, id LIMIT 3",
        "SELECT id, v <=> $q AS distance FROM docs ORDER BY distance, id LIMIT 3",
        "SELECT id FROM docs ORDER BY v <=> $q, id USE VECTOR EXACT LIMIT 3",
    ] {
        match refused_at_every_door(&db, &reader, sql, &query) {
            Error::PlanError(text) => {
                if !(text.contains("<=>") && text.ends_with(LATER_VERSION)) {
                    wrong.push(format!(
                        "{sql}\nwas not refused as a form of a later version that names <=>: {text}"
                    ));
                }
                if text == missing {
                    wrong.push(format!(
                        "{sql}\nwas told what a statement with no ORDER BY on its distance is \
                         told: {text}"
                    ));
                }
            }
            other => wrong.push(format!(
                "{sql}\nwas refused as {other:?} ({other}), not as a plan error"
            )),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n\n"));

    // Each of them left the store as it was, and the search without the second key answers.
    let after = (
        support::rows(&db, "SELECT id, ctx, tag, v FROM docs ORDER BY id"),
        db.committed_watermark(),
    );
    assert!(after == before, "a refused statement changed the store");
    let stored = support::stored(&db, "SELECT id, ctx, tag, v FROM docs");
    let wanted = stored.nearest(|_| true, &query, 3);
    let sql = support::nearest_sql("docs", "v", None, 3);
    support::assert_nearest(&db, &sql, &query, &wanted, "after every refusal");
}

#[test]
fn a_distance_on_a_column_of_another_table_is_a_column_that_is_not_found() {
    let folder = support::folder("distance-wrongly-qualified");
    let db = support::open(&folder);
    declare(&db, "docs", "");
    declare(&db, "other", "");
    fill(&db, "docs", 1..=5);
    fill(&db, "other", 1..=5);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let query = support::vector(94_000, DIMENSION);

    // Under the name of a table that is declared and is not the statement's, and under a name
    // no table has.
    let mut wrong = Vec::new();
    for qualifier in ["other", "nowhere"] {
        // What any column written under that name is refused as: not found, naming the
        // statement's table and the column.
        let plain = format!("SELECT {qualifier}.v FROM docs LIMIT 3");
        let not_found = refused_at_every_door(&db, &reader, &plain, &query);
        match &not_found {
            Error::ColumnNotFound { table, column } => {
                assert_eq!((table.as_str(), column.as_str()), ("docs", "v"), "{plain}");
            }
            other => panic!("{plain}\nwas refused as {other:?}"),
        }
        // The same column on the left of `<=>` is refused the same way: as the distance the
        // search is ordered by, under its select-list name or written in the ORDER BY, and as
        // a second distance beside it.
        for sql in [
            format!(
                "SELECT id, {qualifier}.v <=> $q AS distance FROM docs ORDER BY distance LIMIT 3"
            ),
            format!("SELECT id FROM docs ORDER BY {qualifier}.v <=> $q LIMIT 3"),
            format!("SELECT id, {qualifier}.v <=> $q AS far FROM docs ORDER BY v <=> $q LIMIT 3"),
        ] {
            let refused = refused_at_every_door(&db, &reader, &sql, &query);
            if format!("{refused:?}") != format!("{not_found:?}") {
                wrong.push(format!(
                    "{sql}\nwas refused as {refused:?}, not as {not_found:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n\n"));

    // Under its own table's name the column is the column, and the search answers.
    let stored = support::stored(&db, "SELECT id, ctx, tag, v FROM docs");
    let wanted = stored.nearest(|_| true, &query, 3);
    for sql in [
        "SELECT id, docs.v <=> $q AS distance FROM docs ORDER BY distance LIMIT 3",
        "SELECT id, docs.v <=> $q AS distance FROM docs ORDER BY docs.v <=> $q LIMIT 3",
    ] {
        support::assert_nearest(
            &db,
            sql,
            &query,
            &wanted,
            "the column under its table's name",
        );
        support::assert_nearest(&reader, sql, &query, &wanted, "through a reader");
    }
}

#[test]
fn a_distance_on_a_table_that_does_not_exist_is_a_table_that_is_not_found() {
    let folder = support::folder("distance-no-such-table");
    let db = support::open(&folder);
    declare(&db, "other", "");
    fill(&db, "other", 1..=5);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    let query = support::vector(94_100, DIMENSION);

    // What any statement on a table that is not there is told.
    let plain = "SELECT other.v FROM missing LIMIT 3";
    match refused_at_every_door(&db, &reader, plain, &query) {
        Error::TableNotFound(table) => assert_eq!(table, "missing", "{plain}"),
        other => panic!("{plain}\nwas refused as {other:?}"),
    }
    // A distance on a column of a table that is there changes nothing about that: the table
    // of the statement is still the first thing that is wrong with it.
    let mut wrong = Vec::new();
    for sql in [
        "SELECT id FROM missing ORDER BY other.v <=> $q LIMIT 3",
        "SELECT other.v <=> $q AS d FROM missing ORDER BY d LIMIT 3",
    ] {
        match refused_at_every_door(&db, &reader, sql, &query) {
            Error::TableNotFound(table) if table == "missing" => {}
            other => wrong.push(format!(
                "{sql}\nwas refused as {other:?}, not as the table missing not found"
            )),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n\n"));
}

#[test]
fn a_distance_in_the_where_of_a_write_is_refused_on_a_table_with_no_row() {
    let folder = support::folder("distance-in-a-write-no-row");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, r REAL, v VECTOR(4))",
    );
    let bound = params! { "q" => vec![0.5_f32; 4] };
    let writes = [
        "UPDATE docs SET r = 1.5 WHERE v <=> $q < 0.5",
        "DELETE FROM docs WHERE v <=> $q < 0.5",
    ];

    // With no row in the table and with one: the refusal of a statement does not depend on
    // what its table holds. Through the writer and inside a transaction, which stays usable.
    let mut wrong = Vec::new();
    for rows in [0_i64, 1] {
        if rows == 1 {
            support::run(
                &db,
                "INSERT INTO docs (id, r, v) VALUES (1, 0.25, [1.0, 0.0, 0.0, 0.0])",
            );
        }
        let position = db.committed_watermark();
        let tx = db.transaction().expect("begin");
        for sql in writes {
            let doors = [
                ("the writer", db.execute(sql, &bound)),
                ("a transaction", tx.execute(sql, &bound)),
            ];
            for (door, outcome) in doors {
                match outcome {
                    Err(Error::PlanError(text)) if text.contains("<=>") => {}
                    Err(other) => wrong.push(format!(
                        "{sql}\nwith {rows} rows, through {door}, was refused as {other:?}, not \
                         as a plan error that names <=>"
                    )),
                    Ok(result) => wrong.push(format!(
                        "{sql}\nwith {rows} rows, through {door}, was answered with {} rows \
                         affected, not refused",
                        result.rows_affected()
                    )),
                }
            }
        }
        // The transaction is as usable as before, and nothing was written.
        assert_eq!(support::int(&tx, "SELECT COUNT(*) FROM docs"), rows);
        tx.rollback();
        assert_eq!(db.committed_watermark(), position, "with {rows} rows");
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM docs"), rows);
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n\n"));
    assert_eq!(
        support::rows(&db, "SELECT id, r FROM docs"),
        [[Value::Int64(1), Value::Float64(0.25)]]
    );
}
