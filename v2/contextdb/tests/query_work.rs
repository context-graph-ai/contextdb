//! Work of the statements that read more than one table or more than one row for an answer:
//! each holds only what it says it holds, whatever the size of the tables. The rows each reads
//! are counted in `tests/sql/query_work.sql`.

mod support;

use contextdb::{Database, DatabaseOpenOptions, Error, Value, params};

const MIB: usize = 1 << 20;

/// A store under a limit that leaves a statement 7 MiB, with these tables, each of 48 rows whose
/// `body` is half a mebibyte of text that begins with the table's name and the row's number:
/// 24 MiB a table, which the limit cannot hold, and every body different from every other.
fn large(name: &str, tables: &[&str]) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(support::opening::open_floor() + 7 * MIB);
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
fn rows_that_pass_through_a_statement_are_not_held_by_it() {
    // Two tables of 24 MiB under that limit: each answer is right only if the bodies
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
    // A value the key cannot take does not make the join hold the whole right side's bodies.
    let joined = "WITH l AS (SELECT 1.5 AS k UNION ALL SELECT id FROM more) \
                  SELECT MAX(big.body) AS last FROM l JOIN big ON big.id = l.k";
    assert_eq!(
        head(&db, joined),
        "big-48",
        "each of more's ids finds its row of big"
    );
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

/// A store whose table `t` is `(id INTEGER PRIMARY KEY, <column> TEXT)` with a row for each
/// number from 1 to `rows`, its text given by `text`. The rows are loaded with no limit, about
/// a mebibyte to a commit, and the store is then opened again under `limit`: of a limit,
/// `support::opening::open_floor()` is set aside by the open and the rest is the room a statement has.
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
    // The offset skips rows of the answer and not the entries the dedup has to hold.
    assert_refused_by_the_memory_limit(&all, "SELECT DISTINCT c FROM t OFFSET 383");
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

#[test]
fn a_parameter_nothing_binds_is_a_plan_error_naming_it_before_any_row_is_read() {
    let db = support::open(&support::folder("unbound-parameter"));
    support::declare(&db);
    support::insert_together(&db, 1..=100);
    let before = db.work_done().unwrap().rows_read;
    match db.execute("SELECT id FROM t WHERE id > $floor", &params! {}) {
        Err(Error::PlanError(text)) => assert!(
            text.contains("floor"),
            "the error must name the parameter: {text}"
        ),
        other => panic!("an unbound $floor was not a plan error: {other:?}"),
    }
    assert_eq!(
        db.work_done().unwrap().rows_read,
        before,
        "refused before any row is read"
    );
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

/// What went wrong, if anything, with `read` of `what` (read) and with `refused` (refused by
/// depth), each run by `ran`.
fn read_and_refused(
    what: &str,
    [read, refused]: [usize; 2],
    ran: impl Fn(usize) -> contextdb::Result<contextdb::QueryResult>,
) -> Vec<String> {
    let mut told = Vec::new();
    if let Err(error) = ran(read) {
        told.push(format!("{read} {what} were refused: {error:?}"));
    }
    if !is_refused_by_depth(ran(refused)) {
        told.push(format!("{refused} {what} were not refused by depth"));
    }
    told
}

/// What went wrong, if anything, with 64 levels of an expression that nests by `open` and
/// `close` (read) and with 65 (refused by depth).
fn nested_to_the_limit(db: &Database, open: &str, close: &str) -> Vec<String> {
    read_and_refused(&format!("levels of `{open}`"), [64, 65], |levels| {
        let (open, close) = (open.repeat(levels), close.repeat(levels));
        db.execute(
            &format!("SELECT id FROM t WHERE {open}id{close}"),
            &params! {},
        )
    })
}

#[test]
fn a_grouped_nearest_search_is_answered_or_refused_on_a_small_stack() {
    on_a_small_stack("grouped-nearest", |db| {
        support::run(
            db,
            "CREATE TABLE p (id INTEGER, session INTEGER, v VECTOR(2))",
        );
        support::run(
            db,
            "INSERT INTO p VALUES (1, 1, [1.0, 0.0]), (2, 1, [0.0, 1.0]), (3, 2, [0.9, 0.1]), (4, 3, [-1.0, 0.0])",
        );
        let sql = "SELECT session FROM p GROUP BY session ORDER BY MIN(v <=> [1.0, 0.0]) LIMIT 2";
        // Answered in the order of the nearest, or refused by a typed error; never a panic.
        match db.execute(sql, &params! {}) {
            Ok(answer) if answer.rows() == [[Value::Int64(1)], [Value::Int64(2)]] => vec![],
            Ok(answer) => vec![format!("the nearest sessions were {:?}", answer.rows())],
            Err(_) => vec![],
        }
    });
}

#[test]
fn parentheses_and_not_nested_to_the_limit_are_read_and_one_past_it_is_refused_on_a_small_stack() {
    on_a_small_stack("long-parentheses", |db| {
        for sql in [
            "CREATE TABLE nodes (id UUID PRIMARY KEY)",
            "CREATE TABLE edges (id INT PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            "INSERT INTO nodes VALUES ('00000000-0000-0000-0000-000000000001'),('00000000-0000-0000-0000-000000000002'),('00000000-0000-0000-0000-000000000003'),('00000000-0000-0000-0000-000000000004'),('00000000-0000-0000-0000-000000000005')",
            "INSERT INTO edges VALUES (1,'00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001','BASED_ON'),(2,'00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000001','ABOUT'),(3,'00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000004','SERVES'),(4,'00000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000005','SERVES')",
        ] {
            support::run(db, sql);
        }
        let graph = "WITH based AS (SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:BASED_ON]-(d) WHERE e.id = '00000000-0000-0000-0000-000000000001' COLUMNS (d.id AS d_id))), about AS (SELECT d_id FROM GRAPH_TABLE (edges MATCH (e)<-[:ABOUT]-(d) WHERE e.id = '00000000-0000-0000-0000-000000000001' COLUMNS (d.id AS d_id))), served_by_based AS (SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM based) COLUMNS (i.id AS i_id))), served_by_about AS (SELECT i_id FROM GRAPH_TABLE (edges MATCH (d)-[:SERVES]->(i) WHERE d.id IN (SELECT d_id FROM about) COLUMNS (i.id AS i_id))) SELECT id FROM nodes WHERE id IN (SELECT i_id FROM served_by_based) OR id IN (SELECT i_id FROM served_by_about) ORDER BY id";
        let expected: Vec<_> = [4, 5]
            .map(|n| vec![Value::Uuid(contextdb::Uuid::from_u128(n))])
            .into();
        assert_eq!(support::rows(db, graph), expected);
        let ranked = "WITH ranked_sessions AS (SELECT id, tag, name, ROW_NUMBER() OVER (PARTITION BY tag ORDER BY id DESC, name ASC) AS session_rank FROM t WHERE tag IN (1,2) AND id IS NOT NULL) SELECT id FROM ranked_sessions WHERE (tag = 1 AND session_rank <= 2) OR (tag = 2 AND session_rank <= 3) ORDER BY tag, session_rank";
        assert_eq!(support::ids(db, ranked), [1, 2]);
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
        told.extend(read_and_refused("nested SELECTs", [64, 65], |n| {
            ran(&selects(n))
        }));
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
        told.extend(read_and_refused("chained WITH names", [16, 300], |n| {
            ran(&with(n))
        }));
        told
    });
}
