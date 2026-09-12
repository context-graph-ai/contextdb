//! A bounded read reports how many rows IT examined, for the statement it
//! was asked.
//!
//! Rows examined is the number an operator reads to tell a lookup that
//! touched three rows from one that walked a table. It is scoped to the
//! statement: the statement that scanned three rows says three, and a
//! statement that touched no rows at all says none. A door that reports zero
//! for a scan that really examined three rows does not report a smaller
//! number, it reports NO number -- every query looks free, and the field
//! stops being usable for the one thing it exists for.
//!
//! Three is not an incidental fixture size here: the table is seeded with
//! exactly three rows, so both doors are held to the same literal rather than
//! to whatever either of them happens to produce.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Ceilings far above anything this fixture needs, so a ceiling can never be
/// what makes the two doors disagree.
fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

const SEEDED_ROWS: u64 = 3;

fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE traced (id INTEGER PRIMARY KEY, body TEXT)",
            &empty(),
        )
        .expect("create the fixture table");
    for id in 0..SEEDED_ROWS {
        database
            .execute(
                &format!("INSERT INTO traced (id, body) VALUES ({id}, 'row-{id}')"),
                &empty(),
            )
            .expect("seed a row");
    }
    database
}

fn eager(database: &Database, sql: &str) -> QueryResult {
    database
        .execute(sql, &empty())
        .unwrap_or_else(|error| panic!("the executor answers {sql}: {error}"))
}

fn bounded(database: &Database, sql: &str) -> QueryResult {
    database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, &empty())
        .unwrap_or_else(|error| panic!("a bounded read answers {sql}: {error}"))
}

const SCAN: &str = "SELECT id FROM traced";

#[test]
fn a_bounded_scan_reports_the_rows_it_examined() {
    let database = seeded();

    let eager_scan = eager(&database, SCAN);
    let bounded_scan = bounded(&database, SCAN);
    println!(
        "OBSERVED scan: executor examined {} | bounded examined {}",
        eager_scan.trace.rows_examined, bounded_scan.trace.rows_examined
    );

    assert_eq!(
        eager_scan.trace.rows_examined, SEEDED_ROWS,
        "the fixture holds exactly {SEEDED_ROWS} rows and the executor scanned all of them"
    );
    assert_eq!(
        bounded_scan.trace.rows_examined, SEEDED_ROWS,
        "a bounded scan of the same {SEEDED_ROWS} rows examined all of them too, and says so"
    );
}

#[test]
fn a_bounded_scan_still_reports_its_own_rows_after_unrelated_schema_work() {
    let database = seeded();

    let before = bounded(&database, SCAN).trace.rows_examined;
    let schema_work = eager(
        &database,
        "CREATE TABLE unrelated_to_the_scan (id INTEGER PRIMARY KEY)",
    );
    let after = bounded(&database, SCAN).trace.rows_examined;
    println!(
        "OBSERVED across schema work: bounded examined {before}, then the schema statement \
         examined {}, then bounded examined {after}",
        schema_work.trace.rows_examined
    );

    assert_eq!(
        schema_work.trace.rows_examined, 0,
        "a statement that creates a table examines no rows"
    );
    assert_eq!(
        before, SEEDED_ROWS,
        "a bounded scan of {SEEDED_ROWS} rows examines all of them"
    );
    assert_eq!(
        after, SEEDED_ROWS,
        "and still does after unrelated schema work: the figure belongs to the statement, not to \
         everything the store has done since"
    );
}

#[test]
fn a_bounded_read_examines_what_the_executor_examines_for_every_shape() {
    let database = seeded();
    database
        .execute("CREATE INDEX traced_body ON traced (body)", &empty())
        .expect("index the body column");

    let mut disagreements = Vec::new();
    for (what, sql) in [
        ("a scan of the whole table", SCAN),
        (
            "a lookup by primary key",
            "SELECT body FROM traced WHERE id = 1",
        ),
        (
            "a lookup an index can answer",
            "SELECT id FROM traced WHERE body = 'row-1'",
        ),
        (
            "a predicate that matches nothing",
            "SELECT id FROM traced WHERE body = 'row-nobody-wrote'",
        ),
        (
            "a count over the whole table",
            "SELECT COUNT(*) FROM traced",
        ),
    ] {
        let eager_examined = eager(&database, sql).trace.rows_examined;
        let bounded_examined = bounded(&database, sql).trace.rows_examined;
        println!(
            "OBSERVED {what}: executor examined {eager_examined} | bounded examined \
             {bounded_examined}"
        );
        if bounded_examined != eager_examined {
            disagreements.push(format!(
                "{what}: the executor examined {eager_examined} rows and a bounded read reports \
                 {bounded_examined}"
            ));
        }
    }

    assert!(
        disagreements.is_empty(),
        "rows examined is what an operator reads to tell a lookup that touched a few rows from \
         one that walked the table, so a door that reports a different figure for the same \
         statement reports no usable figure at all:\n{}",
        disagreements.join("\n")
    );
}
