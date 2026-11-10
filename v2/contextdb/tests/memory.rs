//! The memory limit: the host declares it for a writer and for a reader, a statement that would
//! cross it is refused by name with committed work intact, memory in use goes back when it is
//! released, and the store remembers a declared limit.

mod support;

use std::path::Path;

use contextdb::serde_json::{self, json};
use contextdb::{
    Database, DatabaseOpenOptions, Error, ReadSession, ReadSessionOptions, Value, params,
};

use support::Run;

const MIB: usize = 1 << 20;

#[test]
fn a_statement_that_would_cross_the_limit_is_refused_by_name_and_committed_rows_stay() {
    let folder = support::folder("memory-limit-refuses");
    let limit = 16 * MIB;
    let db = open_limited(&folder, limit);
    assert_eq!(db.memory_limit(), Some(limit));
    support::declare(&db);
    support::insert(&db, 1..=10);
    support::run(&db, "CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)");
    let insert = "INSERT INTO big (id, body) VALUES ($id, $body)";

    // One statement that needs more than the whole limit.
    let refusal = db
        .execute(
            insert,
            &params! { "id" => 0_i64, "body" => "x".repeat(limit + MIB) },
        )
        .expect_err("a value larger than the limit was taken");
    over_the_limit(refusal, limit);

    // A transaction holds its rows in memory until it commits. At one megabyte a statement, one
    // of the first sixteen would cross a limit of sixteen megabytes.
    let megabyte = "x".repeat(MIB);
    let tx = db.transaction().expect("begin");
    let mut taken = 0_i64;
    let refusal = loop {
        assert!(
            taken < 16,
            "sixteen rows of 1 MiB were taken under a limit of 16 MiB"
        );
        match tx.execute(
            insert,
            &params! { "id" => taken + 1, "body" => megabyte.as_str() },
        ) {
            Ok(_) => taken += 1,
            Err(refusal) => break refusal,
        }
    };
    over_the_limit(refusal, limit);
    // The refused statement left the transaction as it was.
    assert_eq!(support::int(&tx, "SELECT COUNT(*) FROM big"), taken);
    tx.rollback();

    // What was committed is intact, and the store takes the next write without being reopened.
    support::assert_rows_are(&db, 1..=10);
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM big"), 0);
    support::insert(&db, 11..=11);
    support::close(db);
    support::assert_rows_are(&support::open(&folder), 1..=11);
}

#[test]
fn a_reader_has_its_own_limit() {
    let folder = support::folder("reader-limit");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)");
    let megabyte = "x".repeat(MIB);
    for id in 1..=40_i64 {
        db.execute(
            "INSERT INTO big (id, body) VALUES ($id, $body)",
            &params! { "id" => id, "body" => megabyte.as_str() },
        )
        .expect("insert under no limit");
    }
    // Forty megabytes in one answer.
    let all = "SELECT id, body FROM big ORDER BY id";

    // A reader with a limit of 16 MiB is refused, and the refusal names the reader's limit.
    let narrow = read_limited(&folder, 16 * MIB);
    over_the_limit(support::refusal(&narrow, all), 16 * MIB);
    // The session is as good as before: it reads what fits.
    assert_eq!(support::int(&narrow, "SELECT COUNT(*) FROM big"), 40);
    assert_eq!(
        support::rows(&narrow, "SELECT id, body FROM big WHERE id = 7"),
        [[Value::Int64(7), Value::Text(megabyte.clone())]]
    );

    // A reader with no limit, and the writer that has none, answer the same statement.
    let wide = ReadSession::open(&folder).expect("open the store for reading");
    assert_eq!(support::rows(&wide, all).len(), 40);
    assert_eq!(support::rows(&db, all).len(), 40);

    // A writer with a limit of 32 MiB is refused by its own limit, while the two readers go on
    // by theirs.
    support::close(db);
    let db = open_limited(&folder, 32 * MIB);
    over_the_limit(support::refusal(&db, all), 32 * MIB);
    over_the_limit(support::refusal(&narrow, all), 16 * MIB);
    assert_eq!(support::rows(&wide, all).len(), 40);
}

#[test]
fn a_value_too_large_for_the_limit_is_refused_while_its_row_is_still_counted() {
    let folder = support::folder("value-over-limit");
    let limit = 16 * MIB;
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, doc JSON)",
    );
    let large = serde_json::Value::String("x".repeat(limit));
    for (id, doc) in [
        (1_i64, json!({ "k": 1 })),
        (2, large),
        (3, json!({ "k": 3 })),
    ] {
        db.execute(
            "INSERT INTO docs (id, title, doc) VALUES ($id, $title, $doc)",
            &params! { "id" => id, "title" => format!("doc-{id}"), "doc" => doc },
        )
        .expect("insert under no limit");
    }
    support::close(db);

    // Through a reader with the limit, then through a writer with it. Neither open is refused:
    // nothing of the large value is read until a statement selects it.
    the_large_value_is_refused_and_its_row_counted(&read_limited(&folder, limit), limit);
    the_large_value_is_refused_and_its_row_counted(&open_limited(&folder, limit), limit);
}

fn the_large_value_is_refused_and_its_row_counted(on: &impl Run, limit: usize) {
    let title = |id: i64| [Value::Int64(id), Value::Text(format!("doc-{id}"))];
    let select_large = "SELECT doc FROM docs WHERE id = 2";

    // Statements that do not select the large value count and list its row.
    assert_eq!(support::int(on, "SELECT COUNT(id) FROM docs"), 3);
    assert_eq!(
        support::rows(on, "SELECT id, title FROM docs ORDER BY id"),
        [title(1), title(2), title(3)]
    );

    // The statement that selects it is refused for the bytes the value needs. Asked twice, the
    // refusal is the same to the byte: the first one left nothing charged.
    let first = over_the_limit(support::refusal(on, select_large), limit);
    assert!(
        first.0 >= limit as u64,
        "the refusal asked for {} bytes for a 16 MiB value",
        first.0
    );
    let second = over_the_limit(support::refusal(on, select_large), limit);
    assert_eq!(first, second, "a refused read left memory charged");

    // Everything else is as readable as before.
    assert_eq!(
        support::rows(on, "SELECT doc FROM docs WHERE id = 1"),
        [[Value::Json(json!({ "k": 1 }))]]
    );
    assert_eq!(support::int(on, "SELECT COUNT(id) FROM docs"), 3);
}

#[test]
fn memory_in_use_goes_back_when_a_result_is_dropped() {
    let folder = support::folder("memory-goes-back");
    let db = support::open(&folder);
    support::declare(&db);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1000);
    tx.commit().expect("commit");
    // Run the scan once before measuring, so that anything the first statement sets up for good
    // is already there.
    let _ = support::run(&db, support::SCAN);
    let baseline = db.memory_used();

    // A result is counted for as long as it is held, at least for the text it holds.
    {
        let result = support::run(&db, support::SCAN);
        let text: usize = result
            .rows()
            .iter()
            .map(|row| {
                if let Value::Text(name) = &row[1] {
                    name.len()
                } else {
                    0
                }
            })
            .sum();
        let held = db.memory_used();
        assert!(
            held >= baseline + text,
            "a held result with {text} bytes of text: {baseline} bytes in use before it, {held} with it"
        );
        // The result is dropped here.
    }
    assert_eq!(
        db.memory_used(),
        baseline,
        "memory in use after the result was dropped"
    );

    // The same for the rows a transaction holds, when it is rolled back.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1001..=1100);
    assert!(
        db.memory_used() > baseline,
        "the rows a transaction holds are not counted"
    );
    tx.rollback();
    assert_eq!(
        db.memory_used(),
        baseline,
        "memory in use after the transaction was rolled back"
    );

    // And a refused statement leaves nothing counted.
    support::refusal(&db, "INSERT INTO t (id, name, tag) VALUES (1, 'again', 0)");
    assert_eq!(
        db.memory_used(),
        baseline,
        "memory in use after a refused statement"
    );
}

#[test]
fn a_declared_limit_survives_reopen_however_it_was_set() {
    let folder = support::folder("limit-survives-reopen");
    let db = support::open(&folder);
    assert_eq!(db.memory_limit(), None, "a fresh store has no limit");

    // Declared through the library.
    db.set_memory_limit(Some(512 * MIB))
        .expect("declare a limit");
    assert_eq!(db.memory_limit(), Some(512 * MIB));
    assert_eq!(shown(&db, "limit"), Value::Int64(536_870_912));
    let db = reopen(db, &folder);
    assert_eq!(db.memory_limit(), Some(512 * MIB));

    // Declared in SQL, in the sizes SQL takes.
    support::run(&db, "SET MEMORY_LIMIT 2G");
    assert_eq!(db.memory_limit(), Some(2_147_483_648));
    let db = reopen(db, &folder);
    assert_eq!(db.memory_limit(), Some(2_147_483_648));

    // An open that names a limit has that limit for as long as it is open. The store goes on
    // remembering the one that was declared.
    support::close(db);
    let db = open_limited(&folder, 64 * MIB);
    assert_eq!(db.memory_limit(), Some(64 * MIB));
    let db = reopen(db, &folder);
    assert_eq!(db.memory_limit(), Some(2_147_483_648));

    // Declared as none, either way, it is none after reopening.
    support::run(&db, "SET MEMORY_LIMIT NONE");
    assert_eq!(db.memory_limit(), None);
    let db = reopen(db, &folder);
    assert_eq!(db.memory_limit(), None);
    support::run(&db, "SET MEMORY_LIMIT 512M");
    assert_eq!(db.memory_limit(), Some(512 * MIB));
    db.set_memory_limit(None).expect("declare no limit");
    let db = reopen(db, &folder);
    assert_eq!(db.memory_limit(), None);

    // A reader reports a limit too, and it is the reader's own, not the one the store keeps.
    support::run(&db, "SET MEMORY_LIMIT 2G");
    let columns = [
        "limit",
        "used",
        "available",
        "startup_ceiling",
        "ceiling_refresh_failures",
    ];
    let reader = read_limited(&folder, 64 * MIB);
    assert_eq!(
        support::run(&reader, "SHOW MEMORY_LIMIT").columns(),
        columns
    );
    assert_eq!(shown(&reader, "limit"), Value::Int64(64 * MIB as i64));
    match (shown(&reader, "used"), shown(&reader, "available")) {
        (Value::Int64(used), Value::Int64(available)) => {
            assert_eq!(
                used + available,
                64 * MIB as i64,
                "what a reader has used and has left"
            );
        }
        other => panic!("a reader with a limit reports used and available as {other:?}"),
    }
    // A reader that was given no limit has none, whatever the store keeps for its writer.
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    assert_eq!(
        support::run(&reader, "SHOW MEMORY_LIMIT").columns(),
        columns
    );
    assert_eq!(shown(&reader, "limit"), Value::Text("none".to_owned()));
    assert_eq!(shown(&db, "limit"), Value::Int64(2_147_483_648));
}

#[test]
fn a_limit_set_inside_a_transaction_holds_at_once_and_a_rollback_does_not_undo_it() {
    let folder = support::folder("limit-set-in-transaction");
    let db = support::open(&folder);
    support::declare(&db);
    assert_eq!(db.memory_limit(), None);

    // Set inside a transaction that also writes a row, the limit holds at once: the
    // transaction and the handle both report it.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1);
    support::run(&tx, "SET MEMORY_LIMIT 512M");
    let limit = Value::Int64(536_870_912);
    assert_eq!(shown(&tx, "limit"), limit);
    assert_eq!(shown(&db, "limit"), limit);
    assert_eq!(db.memory_limit(), Some(512 * MIB));

    // The rollback takes the row back and leaves the limit, in this open and in the store.
    tx.rollback();
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
    assert_eq!(shown(&db, "limit"), limit);
    assert_eq!(db.memory_limit(), Some(512 * MIB));
    let db = reopen(db, &folder);
    assert_eq!(shown(&db, "limit"), limit);
    assert_eq!(db.memory_limit(), Some(512 * MIB));

    // The same when the transaction is dropped instead.
    {
        let tx = db.transaction().expect("begin");
        support::run(&tx, "SET MEMORY_LIMIT 2G");
        // The transaction is dropped here, uncommitted.
    }
    assert_eq!(db.memory_limit(), Some(2_147_483_648));
    let db = reopen(db, &folder);
    assert_eq!(shown(&db, "limit"), Value::Int64(2_147_483_648));
    assert_eq!(db.memory_limit(), Some(2_147_483_648));
}

// ---- shared by the tests of this file ----

fn open_limited(folder: &Path, limit: usize) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    support::open_with(folder, options).expect("open the store for writing under a limit")
}

fn read_limited(folder: &Path, limit: usize) -> ReadSession {
    let mut options = ReadSessionOptions::default();
    options.memory_limit = Some(limit);
    ReadSession::open_with_options(folder, options)
        .expect("open the store for reading under a limit")
}

fn reopen(db: Database, folder: &Path) -> Database {
    support::close(db);
    support::open(folder)
}

/// Asserts that a refusal is the memory limit's and names this limit. Returns the bytes that
/// were asked for and the bytes that were left.
fn over_the_limit(refusal: Error, limit: usize) -> (u64, u64) {
    match refusal {
        Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        } => {
            assert_eq!(
                budget_limit_bytes, limit as u64,
                "the refusal names the limit it reached"
            );
            assert!(
                requested_bytes > available_bytes,
                "refused {requested_bytes} bytes with {available_bytes} left"
            );
            (requested_bytes, available_bytes)
        }
        other => panic!("crossing the memory limit was refused as {other:?}"),
    }
}

/// One column of what `SHOW MEMORY_LIMIT` reports.
fn shown(on: &impl Run, column: &str) -> Value {
    let report = support::run(on, "SHOW MEMORY_LIMIT");
    let at = report
        .columns()
        .iter()
        .position(|name| name == column)
        .unwrap_or_else(|| panic!("SHOW MEMORY_LIMIT has no column {column}"));
    match report.rows() {
        [row] => row[at].clone(),
        rows => panic!("SHOW MEMORY_LIMIT returned {} rows", rows.len()),
    }
}
