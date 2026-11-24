//! Extends readers_limits; isolated until the designed tables_read call exists.
mod support;
use contextdb::{ReadSession, ReadSessionOptions, Result};
use std::path::Path;
#[test]
fn tables_read_names_the_tables_inside_the_statement_plan() {
    let db = support::open(&support::folder("tables-read"));
    support::declare(&db);
    let session = db.read_session(Default::default()).unwrap();
    let sql = "WITH x AS (SELECT id FROM t) SELECT id FROM x UNION SELECT id FROM t";
    assert_eq!(session.tables_read(sql).unwrap().to_vec(), ["t"]);
    let write = "DELETE FROM t WHERE id = 1";
    assert_eq!(session.tables_read(write).unwrap().to_vec(), ["t"]);
    // A pattern's COLUMNS read the node table that carries the column, beside the edges.
    support::run(
        &db,
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL)",
    );
    support::run(&db, "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)");
    let walk = "SELECT name FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b) COLUMNS (b.name AS name))";
    let session = db.read_session(Default::default()).unwrap();
    assert_eq!(
        session.tables_read(walk).unwrap().to_vec(),
        ["edges", "nodes"]
    );
    for sql in ["SELEC * FROM t", "SELECT id FROM absent"] {
        assert!(session.tables_read(sql).is_err(), "{sql}");
    }
}

/// An answer from a reader is charged to the reader by what it holds: refused when it does not
/// fit, charged exactly what it is asked for, held while it is held and given back when dropped.
fn charged<T>(
    folder: &Path,
    text: usize,
    tight: bool,
    what: &str,
    ask: impl Fn(&ReadSession) -> Result<T>,
) {
    let reader = |memory: usize| {
        let mut options = ReadSessionOptions::default();
        options.limits.memory = memory as u64;
        let reader = ReadSession::open_with_options(folder, options).unwrap();
        drop(reader.table_names().unwrap());
        reader
    };
    let roomy = reader(64 << 20);
    let base = support::memory_used(&roomy);
    let answer = ask(&roomy).unwrap_or_else(|e| panic!("{what}: a roomy reader refused: {e:?}"));
    let held = support::memory_used(&roomy) - base;
    assert!(
        held >= text,
        "{what}: {text} bytes of names were charged only {held}"
    );
    drop(answer);
    assert_eq!(
        support::memory_used(&roomy),
        base,
        "{what}: dropping the answer did not give it back"
    );
    if !tight {
        return; // planning the statement needs more than its answer holds, so a fit is not exact
    }
    // Room is what a reader has left. Holding answers uses it up until less than one answer's
    // charge is left, and a reader given one more byte than that must then admit the next.
    let ready = support::opening::reader_ready_limit(folder, 0);
    // A reader this tight may not even parse SHOW, whose refusal names the room it had.
    let room = |s: &ReadSession| match s.execute("SHOW MEMORY_LIMIT", &contextdb::params! {}) {
        Err(contextdb::Error::MemoryBudgetExceeded {
            available_bytes, ..
        }) => available_bytes as usize,
        Ok(report) => {
            let at = report
                .columns()
                .iter()
                .position(|name| name == "available")
                .unwrap();
            match report.rows()[0][at] {
                contextdb::Value::Int64(bytes) => bytes as usize,
                ref other => panic!("{what}: available is {other:?}"),
            }
        }
        Err(other) => panic!("{what}: SHOW was refused as {other:?}"),
    };
    let first = reader(ready);
    let spare = room(&first);
    let (whole, left) = (spare / held, spare % held);
    let short = held - 1 - left;
    let attempt = |limit: usize| {
        let tight = reader(limit);
        let kept: Vec<_> = (0..whole)
            .map(|_| ask(&tight).expect("room for what was held"))
            .collect();
        let outcome = ask(&tight);
        drop(kept);
        outcome.is_ok()
    };
    assert!(
        !attempt(ready + short),
        "{what}: an answer one byte over the budget was admitted"
    );
    assert!(
        attempt(ready + short + 1),
        "{what}: an answer was asked for more than the {held} bytes it holds"
    );
}

#[test]
fn reader_metadata_answers_are_charged_by_entry_held_and_given_back() {
    let folder = support::folder("reader-metadata-charge");
    let db = support::open(&folder);
    let columns: Vec<String> = (0..1500)
        .map(|n| format!("column_with_a_long_name_{n:05} INTEGER"))
        .collect();
    support::run(
        &db,
        &format!(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY, {})",
            columns.join(", ")
        ),
    );
    let tables: Vec<String> = (0..300)
        .map(|n| format!("table_with_a_long_name_{n:05}"))
        .collect();
    for name in &tables {
        support::run(
            &db,
            &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY)"),
        );
    }
    support::close(db);
    let names: usize = columns.iter().map(|c| c.len() - " INTEGER".len()).sum();
    charged(&folder, names, true, "table_meta", |s| s.table_meta("wide"));
    let union = tables
        .iter()
        .map(|t| format!("SELECT id FROM {t}"))
        .collect::<Vec<_>>()
        .join(" UNION ");
    charged(
        &folder,
        tables.iter().map(String::len).sum(),
        false,
        "tables_read",
        |s| s.tables_read(&union),
    );
}
