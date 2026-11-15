//! Counted work of the statements that read more than one table or more than one row for an
//! answer: each reads the stored rows its answer needs and holds only what it says it holds,
//! whatever the size of the tables.

mod support;

use std::collections::BTreeSet;

use contextdb::{
    ContextId, Database, DatabaseOpenOptions, Error, Uuid, Value, params, serde_json::json,
};

const MIB: usize = 1 << 20;

/// A store with the usual table `t` of a thousand rows and `owners`, one row for each row of
/// `t`, reached from it through the index on `t_id`: owner `n` is `(5000 + n, n, "note-<n>")`.
fn owned(name: &str) -> Database {
    let db = support::open(&support::folder(name));
    support::declare(&db);
    support::run(
        &db,
        "CREATE TABLE owners (id INTEGER PRIMARY KEY, t_id INTEGER NOT NULL, note TEXT)",
    );
    support::run(&db, "CREATE INDEX owners_t ON owners (t_id)");
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1000);
    for n in 1..=1000_i64 {
        tx.execute(
            "INSERT INTO owners (id, t_id, note) VALUES ($id, $t, $note)",
            &params! { "id" => 5000 + n, "t" => n, "note" => format!("note-{n}") },
        )
        .expect("insert an owner");
    }
    tx.commit().expect("commit");
    db
}

/// A store under a limit of 16 MiB with these tables, each of 48 rows whose `body` is half a
/// mebibyte of text that begins with the table's name and the row's number: 24 MiB a table,
/// which the limit cannot hold, and every body different from every other.
fn large(name: &str, tables: &[&str]) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(16 * MIB);
    let db = support::open_with(&support::folder(name), options).expect("open under a limit");
    for table in tables {
        support::run(
            &db,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT)"),
        );
        for n in 1..=48_i64 {
            db.execute(
                &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
                &params! { "id" => n, "body" => format!("{table}-{n:02}-{}", "x".repeat(MIB / 2)) },
            )
            .expect("insert a large row");
        }
    }
    db
}

/// The first bytes of the one text a statement returns.
fn head(db: &Database, sql: &str) -> String {
    match support::rows(db, sql).as_slice() {
        [row] => match row.as_slice() {
            [Value::Text(text)] => text[..6].to_owned(),
            other => panic!("{sql}\nreturned {other:?}, not one text"),
        },
        other => panic!("{sql}\nreturned {} rows, not one", other.len()),
    }
}

#[test]
fn a_join_reads_one_right_row_for_each_left_row_through_an_index() {
    let db = owned("join-through-an-index");
    let joined = support::run(
        &db,
        "SELECT x.id, o.note FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id <= 10",
    );
    let wanted: Vec<Vec<Value>> = (1..=10_i64)
        .map(|n| vec![Value::Int64(n), Value::Text(format!("note-{n}"))])
        .collect();
    support::assert_same_rows(joined.rows(), &wanted, "the joined rows");
    // Ten rows of `t` and the one owner of each. A scan of `owners` reads a thousand.
    assert_eq!(joined.rows_examined(), 20);
    let sql = "SELECT x.id, o.note FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id <= 8 \
               AND o.id IN (SELECT id FROM owners WHERE id <= 5007)";
    let listed = support::run(&db, sql);
    assert_eq!(listed.rows().len(), 7);
    assert_eq!(listed.rows_examined(), 22, "the list is read once");
    // Another indexed column of `owners` named by a list leaves the join on its index: the three
    // names, the one row of `t`, and the one owner of it.
    support::run(&db, "CREATE INDEX owners_note ON owners (note)");
    let by_note = "SELECT x.id FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id = 1 \
                   AND o.note IN (SELECT note FROM owners WHERE id <= 5003)";
    assert_eq!(support::run(&db, by_note).rows_examined(), 5);

    // A left 7.0 is no key of an INTEGER column and a left TEXT of 300 bytes cuts an index: the
    // join still reads the other index (`jr_b`, or `jr_btag` past tag 6), one left row and the
    // rows of b = 3. Row 100 is under Context 2, which a handle of Context 1 does not see.
    for sql in [
        "CREATE TABLE jl (id INTEGER PRIMARY KEY, a REAL, b INTEGER, txt TEXT)",
        "CREATE TABLE jr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, txt TEXT, tag INTEGER, \
         context_id UUID NOT NULL CONTEXT_ID)",
        "CREATE INDEX jr_ab ON jr (a, b)",
        "CREATE INDEX jr_b ON jr (b)",
        "CREATE INDEX jr_txt ON jr (txt, tag)",
        "CREATE INDEX jr_btag ON jr (b, tag)",
    ] {
        support::run(&db, sql);
    }
    let (long, ctx) = ("x".repeat(300), Uuid::from_u128);
    support::run(&db, &format!("INSERT INTO jl VALUES (1, 7.0, 3, '{long}')"));
    for (id, a, b, owner) in (1..=12_i64)
        .map(|n| (n, n, n % 4, 1))
        .chain([(100, 7, 3, 2)])
    {
        let row = format!("({id}, {a}, {b}, '{long}', {id}, '{}')", ctx(owner));
        support::run(&db, &format!("INSERT INTO jr VALUES {row}"));
    }
    let seven = |n: i64| [Value::Int64(n)];
    let narrowed = db.scoped_with_contexts(BTreeSet::from([ContextId::new(ctx(1))]));
    let by_a = "SELECT jr.id FROM jl JOIN jr ON jr.a = jl.a AND jr.b = jl.b";
    let found = support::run(&db, by_a);
    support::assert_same_rows(found.rows(), &[seven(7), seven(100)], "7.0 equals 7");
    assert_eq!(found.rows_examined(), 1 + 4, "the left row, the 4 of b = 3");
    assert_eq!(
        support::rows(&narrowed, by_a),
        [seven(7)],
        "hidden is unmatched"
    );
    let by_txt = "SELECT jr.id FROM jl JOIN jr ON jr.txt = jl.txt AND jr.b = jl.b WHERE jr.tag > 6";
    let found = support::run(&db, by_txt);
    support::assert_same_rows(
        found.rows(),
        &[seven(7), seven(11), seven(100)],
        "b = 3, tag > 6",
    );
    assert_eq!(found.rows_examined(), 1 + 3, "only the entries above tag 6");
    // The reader's shape: 7.0 is no key of INTEGER, so the join walks `r_ab`, not `r_b`.
    for sql in [
        "CREATE TABLE rr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        "CREATE INDEX r_ab ON rr (a, b)",
        "CREATE INDEX r_b ON rr (b)",
        "CREATE TABLE ll (id REAL, a INTEGER, b INTEGER)",
        "INSERT INTO ll VALUES (7.0, 3, 5)",
    ] {
        support::run(&db, sql);
    }
    for id in 1..=501_i64 {
        let a = if id == 7 { 3 } else { 0 };
        support::run(&db, &format!("INSERT INTO rr VALUES ({id}, {a}, 5)"));
    }
    let shape = support::run(
        &db,
        "SELECT ll.a, rr.id FROM ll JOIN rr ON rr.id = ll.id AND rr.a = ll.a AND rr.b = ll.b \
         WHERE rr.b = 5",
    );
    support::assert_same_rows(shape.rows(), &[[Value::Int64(3), Value::Int64(7)]], "row 7");
    assert_eq!(
        shape.rows_examined(),
        1 + 1,
        "the left row and the row of (3, 5)"
    );
    // By the whole key of `jr`, the rows of `b = 1` that pass the filter are not read: the left
    // row and the one right row it names.
    let by_key = support::run(
        &db,
        "SELECT jr.id FROM jl JOIN jr ON jr.id = jl.id WHERE jr.b = 1",
    );
    support::assert_same_rows(by_key.rows(), &[seven(1)], "the key names row 1");
    assert_eq!(by_key.rows_examined(), 1 + 1, "not the 3 rows of b = 1");
}

#[test]
fn a_cross_join_reads_its_right_side_once_as_an_inner_join_that_always_holds_does() {
    let db = owned("cross-join-reads-once");
    for join in ["CROSS JOIN owners o", "INNER JOIN owners o ON 1 = 1"] {
        let counted = support::run(
            &db,
            &format!("SELECT COUNT(*) AS n FROM t x {join} WHERE x.id <= 3"),
        );
        assert_eq!(counted.rows(), [[Value::Int64(3000)]], "{join}");
        // Three rows of `t`, and `owners` once: not once for each of the three.
        assert_eq!(counted.rows_examined(), 1003, "{join}");
    }
}

#[test]
fn rows_that_pass_through_a_statement_are_not_held_by_it() {
    // Two tables of 24 MiB under a limit of 16 MiB: each answer is right only if the bodies
    // that pass through a WITH name, a UNION ALL, a sort or a grouping are not all held.
    let db = large("rows-pass-through", &["big", "more"]);
    for (sql, wanted) in [
        (
            "WITH named AS (SELECT id, body FROM big) SELECT MAX(body) AS last FROM named",
            "big-48",
        ),
        (
            "WITH whole AS (SELECT body FROM big UNION ALL SELECT body FROM more) \
             SELECT MAX(body) AS last FROM whole",
            "more-4",
        ),
        (
            "SELECT MAX(body) AS last FROM big GROUP BY id / 100",
            "big-48",
        ),
    ] {
        assert_eq!(head(&db, sql), wanted, "{sql}");
    }
    let rows = support::int(
        &db,
        "WITH whole AS (SELECT body FROM big UNION ALL SELECT body FROM more) \
         SELECT COUNT(body) AS n FROM whole",
    );
    assert_eq!(rows, 96, "both tables pass through");
    // A subquery's text is copied into every row it is shown in: 48 rows of half a mebibyte.
    assert_refused_by_the_memory_limit(
        &db,
        "SELECT id, (SELECT body FROM big WHERE id = 1) AS b FROM more",
    );
    // A name read twice is read once and its rows kept: 48 ids fit.
    let twice =
        "WITH n AS (SELECT id FROM big) SELECT COUNT(*) AS c FROM n a JOIN n b ON a.id = b.id";
    assert_eq!(
        support::int(&db, twice),
        48,
        "a name read twice under the limit"
    );
    // A sort keeps the rows that can still leave: LIMIT plus OFFSET of the 48 bodies.
    let page = support::rows(
        &db,
        "SELECT id FROM big ORDER BY body DESC LIMIT 2 OFFSET 3",
    );
    assert_eq!(page, [[Value::Int64(45)], [Value::Int64(44)]]);
    // Forty-five bodies are 22.5 MiB, which the limit cannot hold.
    assert_refused_by_the_memory_limit(
        &db,
        "SELECT id FROM big ORDER BY body DESC LIMIT 40 OFFSET 5",
    );
}

#[test]
fn the_values_of_rows_that_offset_skips_are_not_held() {
    // (rows, bytes of a value, limit): the skipped rows are over what a statement is left.
    for (rows, len, limit) in [
        (20_100_i64, 1_usize << 10, 16 * MIB),
        (2005, 64 << 10, 13 * MIB),
    ] {
        let db = loaded("offset-skips-values", limit, "body", rows, |n| {
            padded(n, len)
        });
        let skip = rows - 5;
        let page = support::rows(
            &db,
            &format!("SELECT id, body FROM t ORDER BY id LIMIT 5 OFFSET {skip}"),
        );
        assert_eq!(page.len(), 5, "five rows after the {skip} skipped");
        for (row, id) in page.iter().zip(skip + 1..) {
            match row.as_slice() {
                [Value::Int64(found), Value::Text(body)] => {
                    assert_eq!(*found, id);
                    assert_eq!(body, &padded(id, len), "the body of row {id}");
                }
                other => panic!("a row of {} values, not a number and a body", other.len()),
            }
        }
    }
}

#[test]
fn row_number_reads_each_partition_to_its_bound_and_one_more() {
    let db = support::open(&support::folder("row-number-reads-to-the-bound"));
    support::run(
        &db,
        "CREATE TABLE sessions (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, at INTEGER NOT NULL)",
    );
    support::run(
        &db,
        "CREATE INDEX sessions_newest ON sessions (ctx ASC, at DESC, id ASC)",
    );
    // Session `n` is in the context `c<n % 3>` at the time `n`: three hundred in each.
    let tx = db.transaction().expect("begin");
    for n in 1..=900_i64 {
        tx.execute(
            "INSERT INTO sessions (id, ctx, at) VALUES ($id, $ctx, $at)",
            &params! { "id" => n, "ctx" => format!("c{}", n % 3), "at" => n },
        )
        .expect("insert a session");
    }
    tx.commit().expect("commit");
    let newest = support::run(
        &db,
        "WITH ranked AS ( \
           SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r \
           FROM sessions WHERE ctx IN ('c0', 'c1')) \
         SELECT ctx, id, r FROM ranked \
         WHERE (ctx = 'c0' AND r <= 2) OR (ctx = 'c1' AND r <= 1) ORDER BY ctx ASC, r ASC",
    );
    let row =
        |ctx: &str, id: i64, r: i64| vec![support::text(ctx), Value::Int64(id), Value::Int64(r)];
    assert_eq!(
        newest.rows(),
        [row("c0", 900, 1), row("c0", 897, 2), row("c1", 898, 1)]
    );
    // The three rows returned, and at most one more in each of the two partitions. Sorting the
    // input reads the six hundred rows of the two contexts.
    assert!(
        (3..=5).contains(&newest.rows_examined()),
        "{} rows were read",
        newest.rows_examined()
    );
}

#[test]
fn limit_with_offset_over_an_ordered_index_reads_the_rows_it_skips_and_returns() {
    let db = support::open(&support::folder("limit-offset-over-an-index"));
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, seq INTEGER NOT NULL)",
    );
    support::run(&db, support::EVENTS_INDEX);
    // Event `n` is `(n, "k<n % 5>", n)`.
    let tx = db.transaction().expect("begin");
    for n in 1..=1000_i64 {
        tx.execute(
            "INSERT INTO events (id, kind, seq) VALUES ($id, $kind, $seq)",
            &params! { "id" => n, "kind" => format!("k{}", n % 5), "seq" => n },
        )
        .expect("insert an event");
    }
    tx.commit().expect("commit");
    let page = support::run(
        &db,
        "SELECT id FROM events WHERE kind = 'k1' ORDER BY seq DESC LIMIT 5 OFFSET 10",
    );
    // The newest of the kind is 996; ten are skipped.
    let wanted = [946_i64, 941, 936, 931, 926].map(|n| [Value::Int64(n)]);
    assert_eq!(page.rows(), wanted);
    assert_eq!(page.rows_examined(), 15);
}

#[test]
fn a_lookup_by_an_indexed_json_path_reads_the_rows_it_returns() {
    let db = support::open(&support::folder("json-path-lookup"));
    support::run(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON)");
    support::run(&db, "CREATE INDEX docs_kind ON docs ((doc -> 'kind'))");
    // Document `n` is `{"kind": "k<n % 100>", "n": n}`.
    let tx = db.transaction().expect("begin");
    for n in 1..=1000_i64 {
        tx.execute(
            "INSERT INTO docs (id, doc) VALUES ($id, $doc)",
            &params! { "id" => n, "doc" => json!({ "kind": format!("k{}", n % 100), "n": n }) },
        )
        .expect("insert a document");
    }
    tx.commit().expect("commit");
    let found = support::run(&db, "SELECT id FROM docs WHERE doc -> 'kind' = '\"k7\"'");
    let wanted: Vec<Vec<Value>> = (0..10_i64)
        .map(|n| vec![Value::Int64(n * 100 + 7)])
        .collect();
    support::assert_same_rows(found.rows(), &wanted, "the documents of the kind");
    assert_eq!(found.rows_examined(), 10);
}

#[test]
fn a_grouped_count_over_a_kept_count_reads_no_rows_and_gives_the_groups_the_rows_give() {
    let db = support::open(&support::folder("grouped-count-over-a-kept-count"));
    support::run(
        &db,
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT NOT NULL)",
    );
    support::run(
        &db,
        "CREATE TABLE kept (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT NOT NULL, \
         UNIQUE (ctx, conn, id)) MAINTAIN COUNT (ctx), (ctx, conn)",
    );
    // Row `n` of each is `(n, "c<n % 3>", "k<n % 4>")`: fifty rows for each pair.
    let tx = db.transaction().expect("begin");
    for n in 1..=600_i64 {
        for table in ["plain", "kept"] {
            tx.execute(
                &format!("INSERT INTO {table} (id, ctx, conn) VALUES ($id, $ctx, $conn)"),
                &params! { "id" => n, "ctx" => format!("c{}", n % 3), "conn" => format!("k{}", n % 4) },
            )
            .expect("insert a row");
        }
    }
    tx.commit().expect("commit");
    let group = |name: String, rows: i64| vec![Value::Text(name), Value::Int64(rows)];
    for (grouped, wanted) in [
        (
            "SELECT ctx, COUNT(*) FROM {} GROUP BY ctx",
            (0..3)
                .map(|n| group(format!("c{n}"), 200))
                .collect::<Vec<_>>(),
        ),
        (
            "SELECT conn, COUNT(*) FROM {} WHERE ctx = 'c1' GROUP BY conn",
            (0..4)
                .map(|n| group(format!("k{n}"), 50))
                .collect::<Vec<_>>(),
        ),
    ] {
        let from_counts = support::run(&db, &grouped.replace("{}", "kept"));
        let from_rows = support::run(&db, &grouped.replace("{}", "plain"));
        // The same groups in the same order, the order of the grouping values.
        assert_eq!(from_counts.rows(), wanted, "{grouped}");
        assert_eq!(from_rows.rows(), wanted, "{grouped}");
        // One kept record for each group and no row; without one, every row of the table.
        assert_eq!(from_counts.rows_examined(), 0, "{grouped}");
        assert_eq!(from_rows.rows_examined(), 600, "{grouped}");
    }
}

/// A store whose table `t` is `(id INTEGER PRIMARY KEY, <column> TEXT)` with a row for each
/// number from 1 to `rows`, its text given by `text`. The rows are loaded with no limit, about
/// a mebibyte to a commit, and the store is then opened again under `limit`: of a limit, 9 MiB
/// are set aside by the open and the rest is the room a statement has.
fn loaded(
    name: &str,
    limit: usize,
    column: &str,
    rows: i64,
    text: impl Fn(i64) -> String,
) -> Database {
    let folder = support::folder(name);
    let db = support::open(&folder);
    support::run(
        &db,
        &format!("CREATE TABLE t (id INTEGER PRIMARY KEY, {column} TEXT)"),
    );
    let insert = format!("INSERT INTO t (id, {column}) VALUES ($id, $text)");
    let mut n = 1;
    while n <= rows {
        let tx = db.transaction().expect("begin");
        let mut bytes = 0;
        while n <= rows && bytes < MIB {
            let text = text(n);
            bytes += text.len();
            tx.execute(&insert, &params! { "id" => n, "text" => text })
                .expect("insert a row");
            n += 1;
        }
        tx.commit().expect("commit");
    }
    support::close(db);
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    support::open_with(&folder, options).expect("open under a limit")
}

/// Exactly `len` bytes of text that begin with this number, in six digits.
fn padded(number: i64, len: usize) -> String {
    format!("{number:06}-{}", "x".repeat(len - 7))
}

/// Asserts that the statement is refused, and by the memory limit.
fn assert_refused_by_the_memory_limit(db: &Database, sql: &str) {
    let refusal = support::refusal(db, sql);
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "{sql}\nwas refused by {refusal:?}, not by the memory limit"
    );
}

#[test]
fn distinct_holds_one_entry_for_each_different_value() {
    // 384 rows of 64 KiB are 24 MiB in each store, under a limit of 16 MiB.
    const ROWS: i64 = 384;
    const LEN: usize = 64 << 10;
    let few = loaded("distinct-of-ten-values", 16 * MIB, "c", ROWS, |n| {
        padded(n % 10, LEN)
    });
    let found = support::rows(&few, "SELECT DISTINCT c FROM t");
    let mut heads: Vec<String> = found
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Text(text)] if text.len() == LEN => text[..6].to_owned(),
            other => panic!(
                "a row of {} values, not one text of {LEN} bytes",
                other.len()
            ),
        })
        .collect();
    heads.sort();
    let wanted: Vec<String> = (0..10).map(|n| format!("{n:06}")).collect();
    assert_eq!(heads, wanted, "each of the ten values once");

    // Every value different: one entry for each is the whole 24 MiB.
    let all = loaded("distinct-of-every-value", 16 * MIB, "c", ROWS, |n| {
        padded(n, LEN)
    });
    assert_refused_by_the_memory_limit(&all, "SELECT DISTINCT c FROM t");
}

#[test]
fn bound_limit_and_offset_answer_as_the_literals_and_refuse_what_is_not_a_count() {
    let db = support::open(&support::folder("bound-limit-and-offset"));
    support::declare(&db);
    support::insert_together(&db, 1..=100);

    let bound = "SELECT id FROM t ORDER BY id LIMIT $n OFFSET $m";
    let found = db
        .execute(bound, &params! { "n" => 5_i64, "m" => 10_i64 })
        .expect("LIMIT and OFFSET take bound whole numbers");
    let written = support::rows(&db, "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10");
    assert_eq!(found.rows(), written);
    assert_eq!(written, [11_i64, 12, 13, 14, 15].map(|n| [Value::Int64(n)]));

    let not_counts = [
        Value::Int64(-1),
        Value::Float64(1.5),
        Value::Text("5".to_owned()),
        Value::Null,
    ];
    for bad in not_counts {
        for (n, m) in [
            (bad.clone(), Value::Int64(0)),
            (Value::Int64(5), bad.clone()),
        ] {
            match db.execute(bound, &params! { "n" => n, "m" => m }) {
                Err(Error::PlanError(_)) => {}
                other => panic!("with {bad:?} bound, not refused as a plan error: {other:?}"),
            }
        }
    }
}

/// Runs `check` on a thread of 2 MiB, less than the main thread of a program has, over a store
/// with the usual table `t`: a statement that nests must be refused by its depth and never
/// overflow the stack (CDB-250). `check` returns what went wrong.
fn on_a_small_stack(name: &'static str, check: fn(&Database) -> Vec<String>) {
    let outcome = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(2 * MIB)
            .spawn_scoped(scope, move || {
                let db = support::open(&support::folder(name));
                support::declare(&db);
                support::insert(&db, 1..=3);
                check(&db)
            })
            .expect("start the thread")
            .join()
    });
    match outcome {
        Ok(told) => assert!(told.is_empty(), "{told:#?}"),
        Err(_) => panic!("a long statement panicked on a stack of 2 MiB"),
    }
}

fn is_refused_by_depth(result: contextdb::Result<contextdb::QueryResult>) -> bool {
    match result {
        Err(error) => error.to_string().contains("deeper than the limit of 64"),
        Ok(_) => false,
    }
}

/// What went wrong, if anything, with 64 levels of an expression that nests by `open` and
/// `close` (read) and with 65 (refused by depth).
fn nested_to_the_limit(db: &Database, open: &str, close: &str) -> Vec<String> {
    let ran = |levels: usize| {
        let sql = format!(
            "SELECT id FROM t WHERE {}id{}",
            open.repeat(levels),
            close.repeat(levels)
        );
        db.execute(&sql, &params! {})
    };
    let mut told = Vec::new();
    if let Err(error) = ran(64) {
        told.push(format!("64 levels of `{open}` were refused: {error:?}"));
    }
    if !is_refused_by_depth(ran(65)) {
        told.push(format!("65 levels of `{open}` were not refused by depth"));
    }
    told
}

#[test]
fn parentheses_and_not_nested_to_the_limit_are_read_and_one_past_it_is_refused_on_a_small_stack() {
    on_a_small_stack("long-parentheses", |db| {
        let mut told = nested_to_the_limit(db, "(", ")");
        told.extend(nested_to_the_limit(db, "NOT ", ""));
        told
    });
}

#[test]
fn a_function_call_nested_to_the_limit_is_read_and_one_past_it_is_refused_on_a_small_stack() {
    on_a_small_stack("long-calls", |db| {
        nested_to_the_limit(db, "COALESCE(", ", 1)")
    });
}

#[test]
fn a_chain_of_operators_is_refused_by_depth_and_quoted_parentheses_and_ands_are_not_levels() {
    on_a_small_stack("long-chains", |db| {
        let ran = |sql: &str| db.execute(sql, &params! {});
        let mut told = Vec::new();
        // A chain of operators nests in the tree it builds, not in the text.
        let chain = format!("SELECT id FROM t WHERE id = 1{}", " + 1".repeat(200));
        if !is_refused_by_depth(ran(&chain)) {
            told.push("a chain of 200 operators was not refused by depth".to_owned());
        }
        // Parentheses in quoted text or a comment are not levels; a long run of ANDs is not
        // a chain.
        let quoted = format!(
            "SELECT '{}' /* {} */ FROM t",
            "(".repeat(200),
            "(".repeat(200)
        );
        let run = format!(
            "SELECT id FROM t WHERE id = 1{}",
            " AND id = 1".repeat(2000)
        );
        for sql in [quoted, run] {
            if let Err(error) = ran(&sql) {
                told.push(format!("{}... was refused: {error:?}", &sql[..30]));
            }
        }
        let ors = format!("SELECT id FROM t WHERE {}id = 3", "id = 9 OR ".repeat(1023));
        if !ran(&ors).is_ok_and(|result| result.rows() == [[Value::Int64(3)]]) {
            told.push("a run of 1024 ORs did not give its row".to_owned());
        }
        // SELECTs nested in one another count the same way, the outermost one as the first level.
        let selects = |levels: usize| {
            (1..levels).fold("SELECT id FROM t".to_owned(), |inner, _| {
                format!("SELECT id FROM t WHERE id IN ({inner})")
            })
        };
        if let Err(error) = ran(&selects(64)) {
            told.push(format!("64 nested SELECTs were refused: {error:?}"));
        }
        if !is_refused_by_depth(ran(&selects(65))) {
            told.push("65 nested SELECTs were not refused by depth".to_owned());
        }
        told
    });
}

#[test]
fn union_arms_and_with_names_in_a_long_statement_are_read_or_refused_on_a_small_stack() {
    on_a_small_stack("long-unions-and-names", |db| {
        let ran = |sql: &str| db.execute(sql, &params! {});
        let mut told = Vec::new();
        // Arms of a UNION are one list read in turn, whatever their number.
        let arms = vec!["SELECT id FROM t"; 3000].join(" UNION ALL ");
        match ran(&arms) {
            Ok(result) if result.rows().len() == 9000 => {}
            other => told.push(format!(
                "3000 UNION ALL arms of 3 rows gave {:?}",
                other.map(|result| result.rows().len())
            )),
        }
        // Names of a WITH that read the name before them nest: a few are read, and a chain
        // far past the limit is refused by depth.
        let with = |names: usize| {
            let mut text = String::from("WITH n0 AS (SELECT id FROM t)");
            for n in 1..names {
                text.push_str(&format!(", n{n} AS (SELECT id FROM n{})", n - 1));
            }
            format!("{text} SELECT id FROM n{}", names - 1)
        };
        if let Err(error) = ran(&with(16)) {
            told.push(format!("a chain of 16 WITH names was refused: {error:?}"));
        }
        if !is_refused_by_depth(ran(&with(300))) {
            told.push("a chain of 300 WITH names was not refused by depth".to_owned());
        }
        told
    });
}
