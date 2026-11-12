//! How long a key may be, and what a long value in an index costs.
//!
//! The values that identify a row are kept whole and may take 2,704 bytes as the store writes
//! them; one byte more is refused by name. A value in an index has no limit: a text too long for
//! its room in an index entry (256 bytes) is kept there as its first bytes and a digest of the
//! whole. That must never show: equality finds exactly the rows that hold the value, an order and a range
//! are by the values themselves, a UNIQUE rule compares the values, and the index stays small.

mod support;

use std::fs;

use contextdb::{Database, Error, QueryResult, Uuid, Value, params};
use support::{Kind, Run};

/// The most bytes a key may take as the store writes it.
const KEY_LIMIT: u64 = 2704;
/// The most bytes a TEXT column takes in an index entry (the engine's `ENTRY_TEXT`, which tests
/// cannot name).
const ENTRY_TEXT: usize = 256;
/// The longest text an index keeps whole: a cut text gives 19 of its room to its tag, its mark and
/// its digest. A longer one is kept as this many bytes and a digest.
const WHOLE: usize = ENTRY_TEXT - 19;

/// The ids a statement returned, in the order it returned them.
fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows()
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("an id came back as {other:?}"),
        })
        .collect()
}

/// A text of `len` bytes: `shared` times the letter `family`, and then letters fixed by `seed`.
/// Two texts of one family and one `shared` begin with the same `shared` bytes.
fn long(family: char, shared: usize, seed: i64, len: usize) -> String {
    let mut text = family.to_string().repeat(shared);
    text.push_str(&support::letters(seed, len - shared));
    text
}

fn key_too_long(outcome: contextdb::Result<QueryResult>, table: &str, bytes: u64, when: &str) {
    match outcome {
        Err(Error::KeyTooLong {
            table: said_table,
            key,
            bytes: said_bytes,
            limit,
        }) => assert_eq!(
            (said_table.as_str(), key.as_str(), said_bytes, limit),
            (table, "primary key", bytes, KEY_LIMIT),
            "{when}"
        ),
        other => panic!("{when}: {other:?}"),
    }
}

#[test]
fn a_key_of_2704_bytes_is_stored_and_one_of_2705_is_refused_by_name() {
    let db = support::open(&support::folder("key-length"));
    support::run(&db, "CREATE TABLE names (name TEXT PRIMARY KEY, n INTEGER)");
    let insert = |on: &dyn Run, name: &str, n: i64| {
        on.run(
            "INSERT INTO names (name, n) VALUES ($name, $n)",
            &params! { "name" => name, "n" => n },
        )
    };
    // A text takes its bytes and three more, and one more for each zero byte in it.
    let longest = "k".repeat(2701);
    let with_zero = format!("{}\0{}", "z".repeat(1000), "z".repeat(1699));
    insert(&db, &longest, 1).expect("a key of 2,704 bytes");
    insert(&db, &with_zero, 2).expect("a key of 2,704 bytes, one of them for a zero byte");
    key_too_long(
        insert(&db, &"k".repeat(2702), 3),
        "names",
        2705,
        "a key of 2,705 bytes",
    );
    key_too_long(
        insert(&db, &format!("{with_zero}z"), 3),
        "names",
        2705,
        "a key of 2,705 bytes, one of them for a zero byte",
    );
    key_too_long(
        insert(&db, &"k".repeat(60_000), 3),
        "names",
        60_003,
        "a key of 60,003 bytes",
    );
    // Inside a transaction the refusal leaves the statements around it standing.
    let tx = db.transaction().expect("begin");
    insert(&tx, "short", 4).expect("a short key");
    key_too_long(
        insert(&tx, &"q".repeat(2702), 5),
        "names",
        2705,
        "a key of 2,705 bytes inside a transaction",
    );
    // A change of a key is held to the same length.
    key_too_long(
        tx.execute(
            "UPDATE names SET name = $name WHERE n = 4",
            &params! { "name" => "s".repeat(2702) },
        ),
        "names",
        2705,
        "an UPDATE to a key of 2,705 bytes",
    );
    tx.commit().expect("commit");
    assert_eq!(
        support::rows(&db, "SELECT n FROM names ORDER BY n"),
        [1_i64, 2, 4].map(|n| [Value::Int64(n)])
    );

    // The longest key is found by its value and reads one row. A value longer than any key
    // can be names no row: that is an empty answer, found without reading anything.
    let find = |name: &str| {
        db.execute(
            "SELECT n FROM names WHERE name = $name",
            &params! { "name" => name },
        )
        .expect("look a row up by its key")
    };
    let found = find(&longest);
    assert_eq!(found.rows(), [[Value::Int64(1)]]);
    assert_eq!(found.rows_examined(), 1);
    assert_eq!(find(&with_zero).rows(), [[Value::Int64(2)]]);
    for missing in ["k".repeat(2702), "k".repeat(60_000)] {
        let found = find(&missing);
        assert!(found.rows().is_empty());
        assert_eq!(found.rows_examined(), 0);
    }

    // A key of several columns is measured as a whole: a UUID takes 17 bytes.
    support::run(
        &db,
        "CREATE TABLE labels (owner UUID NOT NULL, label TEXT NOT NULL, PRIMARY KEY (owner, label))",
    );
    let label = |len: usize| {
        db.execute(
            "INSERT INTO labels (owner, label) VALUES ($owner, $label)",
            &params! { "owner" => Uuid::from_u128(1), "label" => "l".repeat(len) },
        )
    };
    label(2684).expect("a key of 17 and 2,687 bytes");
    key_too_long(label(2685), "labels", 2705, "a key of 17 and 2,688 bytes");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM labels"), 1);
}

/// The table of the tests below: a long value in a plain index (`body`), under a UNIQUE rule
/// (`sole`), and twice in an index of four columns after two short ones (`value` and
/// `identity_value`).
fn declare_anchors(db: &Database) {
    support::run(
        db,
        "CREATE TABLE anchors (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, name TEXT NOT NULL, \
         value TEXT NOT NULL, identity_value TEXT NOT NULL, body TEXT, sole TEXT UNIQUE)",
    );
    support::run(db, "CREATE INDEX anchors_body ON anchors (body)");
    support::run(
        db,
        "CREATE INDEX anchors_exact ON anchors (context_id, name, value, identity_value)",
    );
}

/// Writes anchor `id`, whose every long column holds `text`.
fn insert_anchor(on: &impl Run, id: i64, text: &str) -> contextdb::Result<QueryResult> {
    on.run(
        "INSERT INTO anchors (id, context_id, name, value, identity_value, body, sole) \
         VALUES ($id, $context, 'command', $text, $text, $text, $text)",
        &params! { "id" => id, "context" => Uuid::from_u128(7), "text" => text },
    )
}

/// The three statements that find an anchor by a long value: through the plain index, through
/// the UNIQUE rule's index, and through the index of four columns.
const BY_VALUE: [&str; 3] = [
    "SELECT id FROM anchors WHERE body = $text",
    "SELECT id FROM anchors WHERE sole = $text",
    "SELECT id FROM anchors WHERE context_id = $context AND name = 'command' AND value = $text \
     AND identity_value = $text",
];

/// Asserts that each of the three statements, asked for `text`, returns exactly these rows and
/// read no other row to do it.
fn assert_found_by_value(db: &Database, text: &str, wanted: &[i64], what: &str) {
    for sql in BY_VALUE {
        let mut values = params! { "text" => text };
        if sql.contains("$context") {
            values.insert("context".to_owned(), Value::Uuid(Uuid::from_u128(7)));
        }
        let found = db
            .execute(sql, &values)
            .expect("look rows up by a long value");
        assert_eq!(ids(&found), wanted, "{what}: {sql}");
        assert_eq!(
            found.rows_examined(),
            wanted.len() as u64,
            "{what}: rows read by {sql}"
        );
    }
}

#[test]
fn a_row_whose_indexed_value_is_60_000_bytes_is_stored_and_found_reading_one_row() {
    const LEN: usize = 60_000;
    let folder = support::folder("long-indexed-values");
    let db = support::open(&folder);
    declare_anchors(&db);
    // Ten values of 60,000 bytes that differ from their first byte on, and short ones beside.
    let text = |id: i64| support::large_text(Kind::Scattered, id, LEN);
    for id in 1..=10 {
        insert_anchor(&db, id, &text(id))
            .expect("a row with a value of 60,000 bytes in three indexes");
        insert_anchor(&db, 100 + id, &format!("short-{id}")).expect("a short row");
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM anchors"), 20);

    let check = |db: &Database| {
        for id in 1..=10 {
            assert_found_by_value(db, &text(id), &[id], &format!("the value of row {id}"));
            assert_found_by_value(db, &format!("short-{id}"), &[100 + id], "a short value");
        }
        // A value of the same length that no row holds finds nothing and reads nothing.
        assert_found_by_value(db, &text(11), &[], "a value no row holds");
        // The value comes back byte for byte.
        let stored = support::rows(db, "SELECT body, value FROM anchors WHERE id = 3");
        support::assert_text_is(&stored[0][0], &text(3), "the body of row 3");
        support::assert_text_is(&stored[0][1], &text(3), "the value of row 3");
    };
    check(&db);

    // An UPDATE moves the row to its new value in every index, and a DELETE takes it out.
    db.execute(
        "UPDATE anchors SET body = $text, sole = $text, value = $text, identity_value = $text WHERE id = 1",
        &params! { "text" => text(11) },
    )
    .expect("change a long value");
    assert_found_by_value(&db, &text(1), &[], "the value row 1 held");
    assert_found_by_value(&db, &text(11), &[1], "the value row 1 holds now");
    db.execute(
        "UPDATE anchors SET body = $text, sole = $text, value = $text, identity_value = $text WHERE id = 1",
        &params! { "text" => text(1) },
    )
    .expect("change it back");
    support::run(&db, "DELETE FROM anchors WHERE id = 10");
    assert_found_by_value(&db, &text(10), &[], "the value of the deleted row");
    insert_anchor(&db, 10, &text(10)).expect("insert the row again");

    // An index declared over rows that are already there holds them the same way.
    support::run(&db, "CREATE INDEX anchors_value ON anchors (value, id)");
    let found = db
        .execute(
            "SELECT id FROM anchors WHERE value = $text",
            &params! { "text" => text(5) },
        )
        .expect("look a row up through the new index");
    assert_eq!((ids(&found), found.rows_examined()), (vec![5], 1));

    support::close(db);
    check(&support::open(&folder));
}

#[test]
fn long_values_that_begin_alike_are_each_found_by_their_own_equality_and_by_no_other() {
    const LEN: usize = 60_000;
    let db = support::open(&support::folder("long-values-alike"));
    declare_anchors(&db);
    // Rows 1 and 2 share their first 100 bytes, less than any index keeps of a value: they
    // differ in the bytes kept. Rows 3, 4 and 5 share their first 3,000 bytes, which is more
    // than any index keeps: in every index they differ only by their digest.
    // Row 6 is the first 3,000 bytes alone, and row 7 is row 3's value without its last byte.
    let mut values = vec![
        long('p', 100, 1, LEN),
        long('p', 100, 2, LEN),
        long('q', 3000, 3, LEN),
        long('q', 3000, 4, LEN),
        long('q', 3000, 5, LEN),
        "q".repeat(3000),
    ];
    values.push(values[2][..LEN - 1].to_owned());
    assert_eq!(values[0][..100], values[1][..100]);
    assert_ne!(values[0][..WHOLE], values[1][..WHOLE]);
    assert_eq!(values[2][..3000], values[4][..3000]);
    for (id, value) in (1..).zip(&values) {
        insert_anchor(&db, id, value).expect("a row whose long values begin like another's");
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM anchors"), 7);

    for (id, value) in (1..).zip(&values) {
        assert_found_by_value(&db, value, &[id], &format!("the value of row {id}"));
    }
    // Values that begin the same and that no row holds find nothing, and no row is read for
    // them: one more of each family, and a stored value with one byte more.
    for missing in [
        long('p', 100, 8, LEN),
        long('q', 3000, 9, LEN),
        format!("{}x", values[3]),
        "q".repeat(3001),
    ] {
        assert_found_by_value(&db, &missing, &[], "a value that begins like stored ones");
    }
}

/// The rows of the ordering tests, as (id, body): short values, long values that differ early,
/// and thirteen long values that begin with the same 3,000 bytes, which is more than any index
/// keeps of a value. Among those thirteen, the order the ids are in, the order of the values
/// and the order of their digests are three different orders. Row 30 has no body.
fn lines() -> Vec<(i64, Option<String>)> {
    let mut lines = vec![
        (1, Some("a".to_owned())),
        (2, Some("m".to_owned())),
        (3, Some("zz".to_owned())),
        (4, Some("b".repeat(5000))),
        (5, Some("y".repeat(5000))),
        // The longest value an index of one column keeps whole, and the shortest it cuts:
        // the second begins the thirteen.
        (6, Some("m".repeat(WHOLE))),
        (7, Some("m".repeat(3000))),
        (30, None),
    ];
    for n in 0..12_i64 {
        // Ids rise while the values neither rise nor fall.
        lines.push((10 + n, Some(long('m', 3000, (n * 7) % 12, 3400))));
    }
    lines
}

/// The thirteen bodies that begin alike, in id order.
fn run_of_lines() -> Vec<String> {
    lines()
        .into_iter()
        .filter(|(id, _)| *id == 7 || (10..22).contains(id))
        .filter_map(|(_, body)| body)
        .collect()
}

/// The ids of the lines in the order of their bodies: rising, NULL last; or falling, NULL first.
fn in_order(descending: bool) -> Vec<i64> {
    let mut lines = lines();
    // A body that is there sorts before one that is not; bodies compare as their bytes do.
    lines.sort_by(|(_, a), (_, b)| match (a, b) {
        (Some(a), Some(b)) => a.as_bytes().cmp(b.as_bytes()),
        (a, b) => b.is_some().cmp(&a.is_some()),
    });
    if descending {
        lines.reverse();
    }
    lines.into_iter().map(|(id, _)| id).collect()
}

/// Declares `table` (id, kind, body) with this index, and writes the lines under kind `k` and
/// three rows of another kind.
fn store_of_lines(db: &Database, table: &str, index_columns: &str) {
    support::run(
        db,
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, body TEXT)"),
    );
    support::run(
        db,
        &format!("CREATE INDEX {table}_index ON {table} ({index_columns})"),
    );
    let insert = format!("INSERT INTO {table} (id, kind, body) VALUES ($id, $kind, $body)");
    let tx = db.transaction().expect("begin");
    for (id, body) in lines() {
        let body = body.map_or(Value::Null, Value::Text);
        tx.execute(
            &insert,
            &params! { "id" => id, "kind" => "k", "body" => body },
        )
        .expect("insert a line");
    }
    for (id, body) in [(101_i64, "n"), (102, "c"), (103, "x")] {
        tx.execute(
            &insert,
            &params! { "id" => 1000 + id, "kind" => "j", "body" => body.repeat(4000) },
        )
        .expect("insert a line of another kind");
    }
    tx.commit().expect("commit the lines");
}

/// Asserts that `sql` returns the lines in this order, whole and with every `LIMIT`. With
/// `run` it also asserts the rows read: a statement with `LIMIT n` reads `n` rows, and when
/// its last row is one of the thirteen that begin alike (they are at these places in the
/// order) it reads all thirteen, because only their values say which of them come first.
fn assert_order(db: &Database, sql: &str, wanted: &[i64], run: Option<std::ops::Range<usize>>) {
    let whole = support::run(db, sql);
    assert_eq!(ids(&whole), wanted, "{sql}");
    for limit in 1..=wanted.len() {
        let limited = format!("{sql} LIMIT {limit}");
        let found = support::run(db, &limited);
        assert_eq!(ids(&found), wanted[..limit], "{limited}");
        if let Some(run) = &run {
            let read = match run.contains(&(limit - 1)) {
                true => run.end,
                false => limit,
            };
            assert_eq!(found.rows_examined(), read as u64, "rows read by {limited}");
        }
    }
}

#[test]
fn an_order_by_through_an_index_returns_long_values_in_value_order_with_a_limit_too() {
    let db = support::open(&support::folder("long-values-ordered"));
    store_of_lines(&db, "lines", "body");
    // The kind `j` rows sort among the others by their bodies.
    let mut wanted = in_order(false);
    let place = |wanted: &[i64], id: i64| wanted.iter().position(|found| *found == id).unwrap();
    // c… after b…, n… after every m…, x… before y….
    let at = place(&wanted, 4) + 1;
    wanted.insert(at, 1102);
    let at = place(&wanted, 5);
    wanted.insert(at, 1103);
    let at = place(&wanted, 1103);
    wanted.insert(at, 1101);
    assert_eq!(wanted.len(), 23);
    let first = place(&wanted, 7);
    assert_eq!(
        wanted[first - 1],
        6,
        "the longest whole value comes just before the thirteen"
    );

    assert_order(
        &db,
        "SELECT id FROM lines ORDER BY body",
        &wanted,
        Some(first..first + 13),
    );
    // The same order with the index read backwards, and for one kind through a filter the
    // index does not serve.
    let falling: Vec<i64> = wanted.iter().rev().copied().collect();
    assert_order(
        &db,
        "SELECT id FROM lines ORDER BY body DESC",
        &falling,
        None,
    );
    assert_order(
        &db,
        "SELECT id FROM lines WHERE kind = 'k' ORDER BY body",
        &in_order(false),
        None,
    );
}

#[test]
fn an_order_by_through_a_descending_index_returns_long_values_descending_with_a_limit_too() {
    let db = support::open(&support::folder("long-values-ordered-descending"));
    // An index whose one column is declared DESC.
    store_of_lines(&db, "lines", "body DESC");
    let kind_k = "SELECT id FROM lines WHERE kind = 'k' ORDER BY body DESC";
    let falling = in_order(true);
    assert_eq!(
        falling[0], 30,
        "the row with no body comes first going down"
    );
    let run_at = falling.iter().position(|id| *id == 7).unwrap() - 12;
    assert_eq!(
        falling[run_at + 13],
        6,
        "the longest whole value comes just after the thirteen"
    );
    // The thirteen come back falling, never rising and never as their digests fall.
    let rising_run: Vec<i64> = in_order(false)
        .into_iter()
        .filter(|id| *id == 7 || (10..22).contains(id))
        .collect();
    let found = ids(&support::run(&db, kind_k));
    let found_run: Vec<i64> = found[run_at..run_at + 13].to_vec();
    assert_ne!(
        found_run, rising_run,
        "the run of long values came back ascending"
    );
    assert_eq!(found, falling, "{kind_k}");

    // Every row of the table, in the index's own order, with the rows read for each LIMIT. The
    // rows of kind `j` sort among the others by their bodies: x… after y…, n… before every m…,
    // c… before b….
    let mut wanted = falling.clone();
    let place = |wanted: &[i64], id: i64| wanted.iter().position(|found| *found == id).unwrap();
    let at = place(&wanted, 5) + 1;
    wanted.insert(at, 1103);
    let at = place(&wanted, 1103) + 1;
    wanted.insert(at, 1101);
    let at = place(&wanted, 4);
    wanted.insert(at, 1102);
    let first = place(&wanted, 7) - 12;
    assert_order(
        &db,
        "SELECT id FROM lines ORDER BY body DESC",
        &wanted,
        Some(first..first + 13),
    );
    // The same index read backwards gives the rising order.
    let rising: Vec<i64> = wanted.iter().rev().copied().collect();
    assert_order(&db, "SELECT id FROM lines ORDER BY body", &rising, None);

    // An index of two columns, the second declared DESC, with the first given by the filter:
    // the long values are cut in the column after the one the filter fixed.
    store_of_lines(&db, "kinds", "kind ASC, body DESC");
    let through_two = "SELECT id FROM kinds WHERE kind = 'k' ORDER BY body DESC";
    assert_order(&db, through_two, &falling, None);
    let found = ids(&support::run(
        &db,
        &format!("{through_two} LIMIT {}", run_at + 13),
    ));
    assert_ne!(
        found[run_at..].to_vec(),
        rising_run,
        "the run of long values came back ascending through the index of two columns"
    );
    // With a LIMIT the index gives the order: far fewer rows are read than the kind has.
    let few = support::run(&db, &format!("{through_two} LIMIT 2"));
    assert_eq!(ids(&few), falling[..2]);
    assert_eq!(few.rows_examined(), 2);
}

/// Whether a body is on the kept side of a bound.
type Keeps = fn(&[u8], &[u8]) -> bool;

#[test]
fn a_range_bounded_by_a_long_value_returns_exactly_the_rows_in_range() {
    let db = support::open(&support::folder("long-values-ranged"));
    store_of_lines(&db, "lines", "body");
    let lines = lines();
    let run = run_of_lines();
    let total = support::int(&db, "SELECT COUNT(*) FROM lines WHERE kind = 'k'") as usize;
    // Bounds that are stored values, short and long, inside the thirteen and outside them, and
    // bounds that no row holds: one that begins like the thirteen, and one longer than all.
    let bounds = [
        "m".to_owned(),
        "m".repeat(WHOLE),
        "m".repeat(3000),
        run[4].clone(),
        run[9].clone(),
        "b".repeat(5000),
        format!("{}zzz", "m".repeat(3000)),
        "m".repeat(2000),
        "y".repeat(5001),
    ];
    for (number, bound) in bounds.iter().enumerate() {
        let operators: [(&str, Keeps); 4] = [
            (">", |body, bound| body > bound),
            (">=", |body, bound| body >= bound),
            ("<", |body, bound| body < bound),
            ("<=", |body, bound| body <= bound),
        ];
        for (operator, keeps) in operators {
            let sql = format!(
                "SELECT id FROM lines WHERE kind = 'k' AND body {operator} $bound ORDER BY id"
            );
            let found = db
                .execute(&sql, &params! { "bound" => bound.as_str() })
                .expect("read a range bounded by a long value");
            let mut wanted: Vec<i64> = lines
                .iter()
                .filter(|(_, body)| {
                    body.as_ref()
                        .is_some_and(|body| keeps(body.as_bytes(), bound.as_bytes()))
                })
                .map(|(id, _)| *id)
                .collect();
            wanted.sort_unstable();
            assert_eq!(
                ids(&found),
                wanted,
                "bound {number}: body {operator} a value"
            );
            // The rows read are the rows of the range, of either kind, and at most the rest of
            // the thirteen that begin like the bound: never the table for a range of a few rows.
            let most = (wanted.len() + 3 + 13).min(total + 3) as u64;
            assert!(
                found.rows_examined() <= most,
                "bound {number}: body {operator} a value read {} rows for a range of {}",
                found.rows_examined(),
                wanted.len()
            );
        }
    }

    // Between two of the thirteen, either end taken in or left out.
    let (low, high) = match run[4] < run[9] {
        true => (&run[4], &run[9]),
        false => (&run[9], &run[4]),
    };
    for (lower, upper) in [(">", "<"), (">=", "<="), (">", "<="), (">=", "<")] {
        let sql = format!(
            "SELECT id FROM lines WHERE body {lower} $low AND body {upper} $high ORDER BY id"
        );
        let found = db
            .execute(
                &sql,
                &params! { "low" => low.as_str(), "high" => high.as_str() },
            )
            .expect("read a range between two long values");
        let mut wanted: Vec<i64> = lines
            .iter()
            .filter(|(_, body)| {
                body.as_ref().is_some_and(|body| {
                    let above = if lower == ">" {
                        body > low
                    } else {
                        body >= low
                    };
                    let below = if upper == "<" {
                        body < high
                    } else {
                        body <= high
                    };
                    above && below
                })
            })
            .map(|(id, _)| *id)
            .collect();
        wanted.sort_unstable();
        assert_eq!(ids(&found), wanted, "{sql}");
        assert!(
            found.rows_examined() <= 13,
            "a range inside the thirteen read {} rows",
            found.rows_examined()
        );
    }
}

#[test]
fn a_unique_rule_over_long_values_refuses_the_same_value_and_takes_one_that_differs_in_a_byte() {
    const LEN: usize = 60_000;
    let db = support::open(&support::folder("long-values-unique"));
    support::run(
        &db,
        "CREATE TABLE commands (id INTEGER PRIMARY KEY, command TEXT UNIQUE, a TEXT, b TEXT, UNIQUE (a, b))",
    );
    let insert = |on: &dyn Run, id: i64, text: &str, a: &str, b: &str| {
        on.run(
            "INSERT INTO commands (id, command, a, b) VALUES ($id, $text, $a, $b)",
            &params! { "id" => id, "text" => text, "a" => a, "b" => b },
        )
    };
    let refused = |outcome: contextdb::Result<QueryResult>, columns: &str, when: &str| match outcome
    {
        Err(Error::UniqueViolation { table, column, .. }) => {
            assert_eq!(
                (table.as_str(), column.as_str()),
                ("commands", columns),
                "{when}"
            );
        }
        other => panic!("{when}: {other:?}"),
    };
    let value = long('c', 3000, 1, LEN);
    // The same value but for its last byte, and but for its first byte past what an index keeps.
    let last_differs = format!("{}#", &value[..LEN - 1]);
    let early_differs = format!("{}#{}", &value[..3000], &value[3001..]);
    let (pa, pb) = (long('a', 3000, 2, LEN), long('b', 3000, 3, LEN));
    let pb_differs = format!("{}#", &pb[..LEN - 1]);

    insert(&db, 1, &value, &pa, &pb).expect("the first row");
    let again = insert(&db, 2, &value, "x", "y");
    // The refusal of a long value carries its first 64 bytes and its length, not the value.
    let said = again
        .as_ref()
        .err()
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        said.contains(&value[..64]) && !said.contains(&value[..65]) && said.contains("60000"),
        "the refusal must carry the first 64 bytes of the value and its length: {said}"
    );
    assert!(
        said.len() < 200,
        "a refusal of a 60,000-byte value is {} bytes",
        said.len()
    );
    refused(again, "command", "the same long value a second time");
    refused(
        insert(&db, 2, "t", &pa, &pb),
        "a, b",
        "the same pair of long values a second time",
    );
    insert(&db, 2, &last_differs, &pa, &pb_differs).expect("values that differ in their last byte");
    insert(&db, 3, &early_differs, &pb, &pa).expect("values that differ further in");
    // A change to a value another row holds is refused; a change to one no row holds is taken.
    refused(
        db.execute(
            "UPDATE commands SET command = $text WHERE id = 3",
            &params! { "text" => last_differs.as_str() },
        ),
        "command",
        "an UPDATE to a long value another row holds",
    );
    refused(
        db.execute(
            "UPDATE commands SET b = $b WHERE id = 2",
            &params! { "b" => pb.as_str() },
        ),
        "a, b",
        "an UPDATE to a pair of long values another row holds",
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM commands"), 3);
    for (id, text) in [(1_i64, &value), (2, &last_differs), (3, &early_differs)] {
        let found = db
            .execute(
                "SELECT id FROM commands WHERE command = $text",
                &params! { "text" => text.as_str() },
            )
            .expect("look a row up by its long value");
        assert_eq!((ids(&found), found.rows_examined()), (vec![id], 1));
    }

    // A child table that references the long UNIQUE column finds a stored value, and finds none
    // for a value that differs in its last byte.
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, command TEXT REFERENCES commands(command))",
    );
    let note = |id: i64, text: &str| {
        db.execute(
            "INSERT INTO notes (id, command) VALUES ($id, $text)",
            &params! { "id" => id, "text" => text },
        )
    };
    note(1, &value).expect("a child naming a stored long value");
    let missing = format!("{}#", &early_differs[..LEN - 1]);
    match note(2, &missing) {
        Err(Error::ForeignKeyViolation { table, .. }) => assert_eq!(table, "notes"),
        other => panic!("a child naming a long value no row holds gave {other:?}"),
    }
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM notes"), 1);

    // Two transactions write one long value side by side: the second to commit is refused.
    // One that wrote a value that only begins the same commits.
    let taken = long('c', 3000, 7, LEN);
    let beside = long('c', 3000, 8, LEN);
    let losing = db.transaction().expect("begin");
    let winning = db.transaction().expect("begin");
    insert(&losing, 10, &taken, "l", "l").expect("stage the value");
    insert(&winning, 11, &beside, "w", "w").expect("stage a value that begins the same");
    insert(&db, 12, &taken, "d", "d").expect("commit the value meanwhile");
    winning
        .commit()
        .expect("commit a value that only begins like the others");
    match losing.commit() {
        Err(Error::UniqueViolation { table, column, .. }) => {
            assert_eq!((table.as_str(), column.as_str()), ("commands", "command"));
        }
        other => panic!("a commit of a long value another commit took gave {other:?}"),
    }
    assert_eq!(
        support::rows(&db, "SELECT id FROM commands ORDER BY id"),
        [1_i64, 2, 3, 11, 12].map(|id| [Value::Int64(id)])
    );
}

#[test]
fn two_hundred_rows_of_50_000_byte_indexed_values_make_a_store_smaller_than_twice_their_bytes() {
    const ROWS: i64 = 200;
    const LEN: usize = 50_000;
    let folder = support::folder("long-values-store-size");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    );
    support::run(&db, "CREATE INDEX notes_body ON notes (body)");
    // Text that does not shrink, so each value is stored once in as many bytes as it has: an
    // index that held the values too would hold as many again.
    let body = |id: i64| support::large_text(Kind::Scattered, id, LEN);
    for first in (1..=ROWS).step_by(10) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 10 {
            tx.execute(
                "INSERT INTO notes (id, body) VALUES ($id, $body)",
                &params! { "id" => id, "body" => body(id) },
            )
            .expect("insert a note");
        }
        tx.commit().expect("commit ten notes");
    }
    let found = db
        .execute(
            "SELECT id FROM notes WHERE body = $body",
            &params! { "body" => body(137) },
        )
        .expect("look a note up by its body");
    assert_eq!((ids(&found), found.rows_examined()), (vec![137], 1));
    support::close(db);

    let values = ROWS as u64 * LEN as u64;
    let stored = fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len();
    assert!(
        stored >= values,
        "the store is {stored} bytes and holds {values} bytes of values that do not shrink"
    );
    assert!(
        stored < 2 * values,
        "the store is {stored} bytes for {values} bytes of values: an index entry holds a whole value"
    );
}

#[test]
fn a_store_with_twelve_indexes_stays_under_twice_its_bytes_and_under_its_recorded_size() {
    const RECORDED: u64 = 6_422_528; // today's size, rounded up to the next 64 KiB
    let folder = support::folder("many-indexes-store-size");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, host TEXT NOT NULL, path TEXT NOT NULL, \
         kind TEXT NOT NULL, seq INTEGER NOT NULL, at INTEGER NOT NULL, note TEXT NOT NULL)",
    );
    let indexes = "host;path;kind;host,path;host,kind;path,kind;kind,host;host,path,kind;\
                   path,host;kind,path;host,kind,path;path,host,kind";
    for (n, columns) in indexes.split(';').enumerate() {
        support::run(&db, &format!("CREATE INDEX i{n} ON events ({columns})"));
    }
    let (mut put_in, tx) = (0_u64, db.transaction().expect("begin"));
    for id in 0..4_000_i64 {
        let (seq, at, note) = (id * 3, 1_700_000_000 + id * 61, support::letters(id, 800));
        let (host, kind) = (
            format!("host-{}.build.cluster.example.internal.corp", id % 7),
            ["created", "changed", "removed", "moved", "read"][id as usize % 5],
        );
        let path = format!("/srv/data/projects/{}/releases/{}", id % 13, id % 101);
        put_in += (8 + host.len() + path.len() + kind.len() + 8 + 8 + note.len()) as u64;
        let sql = "INSERT INTO events VALUES ($id, $host, $path, $kind, $seq, $at, $note)";
        let row = params! { "id" => id, "host" => host, "path" => path, "kind" => kind, "seq" => seq, "at" => at, "note" => note };
        tx.execute(sql, &row).expect("insert");
    }
    tx.commit().expect("commit the events");
    support::close(db);
    let stored = fs::metadata(folder.join("data")).expect("data file").len();
    assert!(
        stored <= 2 * put_in,
        "the store is {stored} bytes for {put_in} bytes put in, over twice: a store with many indexes got larger than the promise allows; find the change that made index pages bigger and undo it"
    );
    assert!(
        stored <= RECORDED,
        "the store is {stored} bytes, over the recorded {RECORDED}: the store grew; a larger store needs the owner's word, not a larger number"
    );
}
