//! The memory limit: the host declares it for a writer and for a reader, a statement that would
//! cross it is refused by name with committed work intact, memory in use goes back when it is
//! released, and the store remembers a declared limit.

mod support;

use contextdb::serde_json::{self, json};
use contextdb::{Error, ReadLimits, ReadSession, Value, params};

use support::Run;

const MIB: usize = 1 << 20;

#[test]
fn a_statement_that_would_cross_the_limit_is_refused_by_name_and_committed_rows_stay() {
    let folder = support::folder("memory-limit-refuses");
    let limit = 16 * MIB;
    let db = support::open_limited(&folder, limit);
    assert_eq!(support::limit(&db), Some(limit));
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
    let narrow = support::open_reader(&folder, Some(16 * MIB));
    over_the_limit(support::refusal(&narrow, all), 16 * MIB);
    // The session is as good as before: it reads what fits.
    assert_eq!(support::int(&narrow, "SELECT COUNT(*) FROM big"), 40);
    assert_eq!(
        support::rows(&narrow, "SELECT id, body FROM big WHERE id = 7"),
        [[Value::Int64(7), Value::Text(megabyte.clone())]]
    );

    // A reader under 512 MiB, and the writer under the default, answer the same statement.
    let wide = support::open_reader(&folder, None);
    assert_eq!(support::rows(&wide, all).len(), 40);
    assert_eq!(support::rows(&db, all).len(), 40);

    // A writer with a limit of 32 MiB is refused by its own limit, while the two readers go on
    // by theirs.
    support::close(db);
    let db = support::open_limited(&folder, 32 * MIB);
    over_the_limit(support::refusal(&db, all), 32 * MIB);
    over_the_limit(support::refusal(&narrow, all), 16 * MIB);
    assert_eq!(support::rows(&wide, all).len(), 40);

    // A reader under 16 MiB answers a find on Context Graph's schema: its cache and its
    // catalog of 87 tables and 25 indexes fit.
    let folder = support::folder("reader-limit-consumer-schema");
    let db = support::open(&folder);
    let schema = include_str!("fixtures/consumer_schema.sql").split_once("\n-- Second texts");
    for sql in schema.expect("the first part").0.split(";\n") {
        let sql: Vec<&str> = sql.lines().filter(|line| !line.starts_with("--")).collect();
        if !sql.concat().trim().is_empty() {
            support::run(&db, &sql.join("\n"));
        }
    }
    support::close(db);
    let (context, near) = (support::context(1), vec!["0.5"; 768].join(","));
    let find = format!(
        "SELECT id FROM event_search_passages WHERE context_id = '{context}' AND \
         generation_id = '{context}' ORDER BY vector_text <=> '[{near}]' LIMIT 5"
    );
    let reader = support::open_reader(&folder, Some(16 * MIB));
    assert_eq!(support::rows(&reader, &find), Vec::<Vec<Value>>::new());
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
    the_large_value_is_refused_and_its_row_counted(
        &support::open_reader(&folder, Some(limit)),
        limit,
    );
    the_large_value_is_refused_and_its_row_counted(&support::open_limited(&folder, limit), limit);

    // Projection already admits a field's figure: evaluation must not admit it again.
    let folder = support::folder("json-field-one-cover");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE fields(id INTEGER PRIMARY KEY, d JSON)");
    let field = "x".repeat(64 << 10);
    for id in 1..=2_i64 {
        db.execute(
            "INSERT INTO fields VALUES ($id, $d)",
            &params! { "id" => id, "d" => json!({"k": field.repeat(id as usize)}) },
        )
        .unwrap();
    }
    let sql = "SELECT d ->> 'k' FROM fields WHERE id = 1";
    drop(support::run(&db, sql));
    // The JSON row plus one 18-times field figure fits; two figures cannot fit.
    let room = 22 * field.len() + support::statement_text_bytes(sql);
    let limit = support::used(&db) + room;
    db.set_memory_limit(Some(limit)).unwrap();
    let answer = db.execute(sql, &params! {}).expect("one field figure fits");
    eprintln!(
        "field bytes={}, spare={room}, peak={:?}",
        field.len(),
        answer.peak_bytes()
    );
    assert_eq!(answer.rows(), [[Value::Text(field)]]);
    drop(answer);
    assert!(
        matches!(
            db.execute("SELECT d ->> 'k' FROM fields WHERE id = 2", &params! {}),
            Err(Error::MemoryBudgetExceeded { .. })
        ),
        "the larger field must exceed the same limit"
    );
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
    // The first run of the form adds its line to the record, once; the refusals compared are after it.
    let _ = support::refusal(on, select_large);
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
    let join = "SELECT a.id FROM docs a JOIN docs b ON a.id = b.id \
                WHERE a.id IN (SELECT id FROM docs WHERE id = 2) LIMIT 1";
    assert_eq!(support::rows(on, join), [[Value::Int64(2)]]);
    // A window reads only the columns it names: the value no column of it names is never loaded.
    let window = "WITH r AS (SELECT id, title, ROW_NUMBER() OVER (PARTITION BY title ORDER BY id) \
                  AS n FROM docs) SELECT id FROM r WHERE title = 'doc-2' AND n <= 3";
    assert_eq!(support::rows(on, window), [[Value::Int64(2)]]);
}

#[test]
fn memory_in_use_goes_back_when_a_result_is_dropped() {
    let folder = support::folder("memory-goes-back");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=1000);
    // Run the scan once before measuring, so that anything the first statement sets up for good
    // is already there.
    let _ = support::run(&db, support::SCAN);
    let baseline = support::used(&db);

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
        let held = support::used(&db);
        assert!(
            held >= baseline + text,
            "a held result with {text} bytes of text: {baseline} bytes in use before it, {held} with it"
        );
        // The result is dropped here.
    }
    assert_eq!(
        support::used(&db),
        baseline,
        "memory in use after the result was dropped"
    );

    // What a subquery's list took goes back when the part reading it is done.
    let held = |filter: &str| {
        let arm = format!("SELECT COUNT(*) FROM t WHERE {filter}");
        let _result = support::run(&db, &format!("{arm} UNION ALL {arm}"));
        support::used(&db)
    };
    let _ = held("id >= 1");
    // Each form's first run adds its line to the record, once.
    let _ = held("id IN (SELECT id FROM t)");
    let plain = held("id >= 1");
    assert_eq!(held("id IN (SELECT id FROM t)"), plain, "with a subquery");
    // The record holds a line for each form run since the baseline; the rows are what is judged.
    let baseline = support::used(&db);
    // The same for the rows a transaction holds, when it is rolled back.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1001..=1100);
    assert!(
        support::used(&db) > baseline,
        "the rows a transaction holds are not counted"
    );
    tx.rollback();
    assert_eq!(
        support::used(&db),
        baseline,
        "memory in use after the transaction was rolled back"
    );

    // And a refused statement leaves nothing counted.
    support::refusal(&db, "INSERT INTO t (id, name, tag) VALUES (1, 'again', 0)");
    assert_eq!(
        support::used(&db),
        baseline + support::form_charge("INSERT INTO t (id, name, tag) VALUES (?, ?, ?)"),
        "memory in use after a refused statement: the record's line for its form, and no more"
    );
    support::run(&db, "ALTER TABLE t SET RETAIN 10 SECONDS");
    support::run(&db, "ALTER TABLE t ADD COLUMN expires TIMESTAMP EXPIRES");
    db.execute(
        "UPDATE t SET expires = $due",
        &params! { "due" => Value::Timestamp(-1) },
    )
    .unwrap();
    let before_expiry = support::used(&db);
    assert_eq!(db.run_maintenance_cycle().unwrap().expired_rows, Some(1000));
    assert_eq!(
        support::used(&db),
        before_expiry,
        "expiry releases its charge exactly once"
    );
    assert_eq!(db.run_maintenance_cycle().unwrap().expired_rows, Some(0));
    assert_eq!(
        support::used(&db),
        before_expiry,
        "empty expiry credits nothing again"
    );

    support::run(
        &db,
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('E')",
    );
    support::run(
        &db,
        "INSERT INTO edges VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E')",
    );
    let before_delete = support::used(&db);
    let tx = db.transaction().unwrap();
    support::run(&tx, "DELETE FROM edges WHERE id = 1");
    assert!(
        support::used(&db) > before_delete,
        "the staged edge deletion is charged"
    );
    tx.commit().unwrap();
    assert_eq!(
        support::used(&db),
        before_delete + support::form_charge("DELETE FROM edges WHERE id = ?"),
        "the deleted edge leaves no resident charge but the record's line for its form"
    );
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM edges"), 0);
}

/// A read that keeps nothing per row needs no more room over a join than over a table. A list
/// of 10,000 values from a join of four ten-row tables is answered under the smallest room the
/// same list from a table is answered in, plus a thousand rows: room for what a cross join
/// keeps of its right sides, thirty rows, and a tenth of the 10,000 joined rows.
#[test]
fn an_in_list_over_a_join_needs_the_room_of_the_same_list_over_a_table() {
    let db = support::open(&support::folder("in-list-over-a-join"));
    support::declare(&db);
    support::insert_together(&db, 1..=10_000);
    support::run(&db, "CREATE TABLE d (n INTEGER PRIMARY KEY)");
    support::run(
        &db,
        "INSERT INTO d (n) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)",
    );
    let used = support::used(&db);
    let answered = |sql: &str, room: usize| {
        db.set_memory_limit(Some(used + support::limit_work_bytes() + room))
            .expect("a limit above what is in use");
        match db.execute(sql, &params! {}) {
            Ok(_) => true,
            Err(Error::MemoryBudgetExceeded { .. }) => false,
            Err(other) => panic!("{sql}\nwith {room} bytes of room was refused as {other:?}"),
        }
    };
    let over_a_table = "SELECT 3 IN (SELECT id FROM t) AS found";
    let over_a_join = "SELECT 3 IN (SELECT a.n * 1000 + b.n * 100 + c.n * 10 + e.n \
                       FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e) AS found";
    // The smallest room the list over a table is answered in, to the byte.
    let (mut refused, mut enough) = (0, 8 * MIB);
    assert!(answered(over_a_table, enough), "{over_a_table}\nin 8 MiB");
    while enough - refused > 1 {
        let room = (refused + enough) / 2;
        if answered(over_a_table, room) {
            enough = room;
        } else {
            refused = room;
        }
    }
    // A joined row of four values takes at least its four values and their vector.
    let row = size_of::<Vec<Value>>() + 4 * size_of::<Value>();
    assert!(
        answered(over_a_join, enough + 1000 * row),
        "{over_a_join}\nwas refused with {} bytes of room, a thousand rows of four values more \
         than the {enough} the same list over a table needs: the join's rows were held",
        enough + 1000 * row
    );
}

/// A list is charged as it grows and gives its bytes back when its SELECT is done. One MiB
/// beyond what an open holds takes a list of 10,000 values, one at a time, so each SELECT of
/// the UNION builds one, and not a list of 20,000; 15,000 rows of a WITH name read twice would
/// fit, but not with their list.
#[test]
fn a_list_is_charged_as_it_grows_and_given_back_when_its_select_is_done() {
    let folder = support::folder("memory-lists");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=20_000);
    let limit = support::used(&db) + MIB;
    db.set_memory_limit(Some(limit))
        .expect("1 MiB above what is in use");
    let list = |most: i64| format!("SELECT 3 IN (SELECT id FROM t WHERE id <= {most}) AS found");
    let kept = "WITH w AS (SELECT id FROM t WHERE id <= 15000) SELECT COUNT(*) FROM w";
    for (sql, answers) in [
        (format!("{} UNION ALL {}", list(10_000), list(10_000)), true),
        (list(20_000), false),
        (kept.to_owned(), true),
        (format!("{kept} UNION ALL SELECT COUNT(*) FROM w"), false),
    ] {
        match db.execute(&sql, &params! {}) {
            Ok(_) if answers => {}
            Err(refusal) if !answers => {
                over_the_limit(refusal, limit);
            }
            outcome => panic!("{sql}\nunder a limit of {limit} bytes gave {outcome:?}"),
        }
    }

    // Under the default limit a list of 40,000 values is read whole.
    db.set_memory_limit(None)
        .expect("NONE is accepted where the store was opened with no limit (README:182)");
    support::insert_together(&db, 20_001..=40_000);
    db.set_memory_limit(Some(16 * MIB)).unwrap();
    let wide = db.execute(&list(40_000), &params! {});
    assert!(wide.is_ok(), "a 40,000-value list fits 16 MiB: {wide:?}");
    // The script's list/CTE/join cases use the same dynamically measured opening charge.
    db.set_memory_limit(Some(256 * MIB)).unwrap();
    support::run(&db, "CREATE TABLE digits(n INTEGER PRIMARY KEY)");
    support::run(
        &db,
        "INSERT INTO digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)",
    );
    // The first run of each form adds its line to the statement record, once: they run here
    // first so that the figures below are of the work, not of the record's lines.
    for sql in [
        "SELECT COUNT(*) FROM digits WHERE n IN (SELECT a.n FROM digits a CROSS JOIN digits b)",
        "WITH w AS (SELECT a.n FROM digits a CROSS JOIN digits b) SELECT COUNT(*) FROM w",
        "SELECT COUNT(*) FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e",
        "SELECT COUNT(*) FROM digits WHERE n IN (SELECT a.n FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e)",
    ] {
        let _ = db.execute(sql, &params! {});
    }
    let baseline = support::used(&db);
    db.set_memory_limit(Some(baseline + MIB)).unwrap();
    for (sql, count) in [
        (
            "SELECT COUNT(*) FROM digits WHERE n IN (SELECT a.n FROM digits a CROSS JOIN digits b)",
            10,
        ),
        (
            "WITH w AS (SELECT a.n FROM digits a CROSS JOIN digits b) SELECT COUNT(*) FROM w",
            100,
        ),
        (
            "SELECT COUNT(*) FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e",
            100000,
        ),
    ] {
        assert_eq!(support::int(&db, sql), count);
        assert_eq!(
            support::used(&db),
            baseline,
            "statement scratch was released"
        );
    }
    assert!(matches!(db.execute("SELECT COUNT(*) FROM digits WHERE n IN (SELECT a.n FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e)", &params! {}), Err(Error::MemoryBudgetExceeded { .. })));
    assert_eq!(
        support::used(&db),
        baseline,
        "refused list scratch was released"
    );
    assert_eq!(
        support::memory_shown(&db, "limit"),
        Value::Int64((baseline + MIB) as i64)
    );
    assert_eq!(
        support::memory_shown(&db, "available"),
        Value::Int64(MIB as i64)
    );

    // Fund parsing and planning, but leave the subquery no room for its own list.
    db.set_memory_limit(Some(256 * MIB))
        .expect("room to declare the empty table");
    support::run(&db, "CREATE TABLE nothing (n INTEGER PRIMARY KEY)");
    let plain = "SELECT n FROM nothing";
    let nested = "SELECT n FROM nothing WHERE n IN (SELECT n FROM nothing)";
    let text = [plain, nested]
        .map(support::statement_text_bytes)
        .into_iter()
        .max()
        .unwrap();
    // Opening under the cap needs no limit-setting commit or its freed-page workspace.
    support::close(db);
    let db = support::open_limited(
        &folder,
        support::opening::opening_charge(&folder, None) + text,
    );
    assert!(support::run(&db, plain).rows().is_empty());
    assert!(matches!(
        db.execute(nested, &params! {}),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
}

#[test]
fn a_declared_limit_survives_reopen_however_it_was_set() {
    let folder = support::folder("limit-survives-reopen");
    let db = support::open(&folder);
    support::assert_default_limit(support::limit(&db), "a fresh store");
    let settings = [
        ("STORAGE_COMPACTION START_RATIO", "0.2"),
        ("STORAGE_COMPACTION STOP_RATIO", "0.1"),
        ("MAINTENANCE_POLL_INTERVAL", "2 SECONDS"),
        ("UNINDEXED_VECTOR_LIMIT", "400"),
    ];
    let mut kept = Vec::new();
    for (name, value) in settings {
        let sql = format!("SHOW {name}");
        let before = support::rows(&db, &sql);
        let tx = db.transaction().expect("begin setting declaration");
        support::run(&tx, &format!("SET {name} {value}"));
        let rows = support::rows(&db, &sql);
        assert_ne!(rows, before, "{name} did not change");
        assert_eq!(rows.len(), 1, "{name}");
        assert_eq!(support::rows(&tx, &sql), rows, "{name} holds at once");
        tx.rollback();
        kept.push((sql, rows));
    }
    let db = support::reopen(db, &folder);
    for (sql, rows) in kept {
        assert_eq!(support::rows(&db, &sql), rows);
    }
    support::assert_default_limit(support::limit(&db), "a store never given a limit, reopened");

    // Declared through the library.
    db.set_memory_limit(Some(512 * MIB))
        .expect("declare a limit");
    assert_eq!(support::limit(&db), Some(512 * MIB));
    assert_eq!(
        support::memory_shown(&db, "limit"),
        Value::Int64(536_870_912)
    );
    let db = support::reopen(db, &folder);
    assert_eq!(support::limit(&db), Some(512 * MIB));

    // Declared in SQL, in the sizes SQL takes.
    support::run(&db, "SET MEMORY_LIMIT 1G");
    assert_eq!(support::limit(&db), Some(1_073_741_824));
    let db = support::reopen(db, &folder);
    assert_eq!(support::limit(&db), Some(1_073_741_824));
    support::run(&db, "SET MEMORY_LIMIT DEFAULT");
    support::assert_default_limit(support::limit(&db), "after SET MEMORY_LIMIT DEFAULT");
    support::run(&db, "SET MEMORY_LIMIT 1G");

    // An open that names a limit has that limit for as long as it is open. The store goes on
    // remembering the one that was declared.
    support::close(db);
    let db = support::open_limited(&folder, 64 * MIB);
    assert_eq!(support::limit(&db), Some(64 * MIB));
    let db = support::reopen(db, &folder);
    assert_eq!(support::limit(&db), Some(1_073_741_824));

    // M4: with no opening option the session has no starting ceiling, takes NONE and any size
    // at or above its floor, and NONE survives reopen.
    assert_eq!(support::memory_shown(&db, "startup_ceiling"), Value::Null);
    support::run(&db, "SET MEMORY_LIMIT 4096G");
    assert_eq!(
        support::limit(&db),
        Some(1 << 42),
        "a limit above the machine's memory"
    );
    support::run(&db, "SET MEMORY_LIMIT NONE");
    let db = support::reopen(db, &folder);
    assert_eq!(support::limit(&db), None, "NONE survives reopen");
    // An opening option is the ceiling: a value above it is refused naming it.
    let opened = support::open_limited(&support::folder("limit-above-option"), 64 * MIB);
    let ceiling = support::memory_shown(&opened, "startup_ceiling");
    assert_eq!(
        ceiling,
        Value::Int64(64 << 20),
        "the option is the starting ceiling"
    );
    for past in ["SET MEMORY_LIMIT 65M", "SET MEMORY_LIMIT NONE"] {
        let refused = support::refusal(&opened, past).to_string();
        assert!(
            refused.contains("67108864"),
            "{past} names the option: {refused}"
        );
    }

    // A reader reports a limit too, and it is the reader's own, not the one the store keeps.
    support::run(&db, "SET MEMORY_LIMIT 1G");
    let columns = [
        "limit",
        "used",
        "available",
        "startup_ceiling",
        "ceiling_refresh_failures",
    ];
    let reader = support::open_reader(&folder, Some(64 * MIB));
    assert_eq!(
        support::run(&reader, "SHOW MEMORY_LIMIT").columns(),
        columns
    );
    assert_eq!(
        support::memory_shown(&reader, "limit"),
        Value::Int64(64 * MIB as i64)
    );
    match (
        support::memory_shown(&reader, "used"),
        support::memory_shown(&reader, "available"),
    ) {
        (Value::Int64(used), Value::Int64(available)) => {
            assert_eq!(
                used + available,
                64 * MIB as i64,
                "what a reader has used and has left"
            );
        }
        other => panic!("a reader with a limit reports used and available as {other:?}"),
    }
    // A default reader has its own 16 MiB limit, whatever the store keeps for its writer.
    let reader = ReadSession::open(&folder, ReadLimits::default()).expect("default reader");
    assert_eq!(
        support::run(&reader, "SHOW MEMORY_LIMIT").columns(),
        columns
    );
    assert_eq!(
        support::memory_shown(&reader, "limit"),
        Value::Int64(16 * MIB as i64)
    );
    assert_eq!(
        support::memory_shown(&db, "limit"),
        Value::Int64(1_073_741_824)
    );
}

#[test]
fn a_limit_set_inside_a_transaction_holds_at_once_and_a_rollback_does_not_undo_it() {
    let folder = support::folder("limit-set-in-transaction");
    let db = support::open(&folder);
    support::declare(&db);
    support::assert_default_limit(support::limit(&db), "a fresh store");

    // Set inside a transaction that also writes a row, the limit holds at once: the
    // transaction and the handle both report it.
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1);
    support::run(&tx, "SET MEMORY_LIMIT 512M");
    let limit = Value::Int64(536_870_912);
    assert_eq!(support::memory_shown(&tx, "limit"), limit);
    assert_eq!(support::memory_shown(&db, "limit"), limit);
    assert_eq!(support::limit(&db), Some(512 * MIB));

    // The rollback takes the row back and leaves the limit, in this open and in the store.
    tx.rollback();
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), 0);
    assert_eq!(support::memory_shown(&db, "limit"), limit);
    assert_eq!(support::limit(&db), Some(512 * MIB));
    let db = support::reopen(db, &folder);
    assert_eq!(support::memory_shown(&db, "limit"), limit);
    assert_eq!(support::limit(&db), Some(512 * MIB));

    // The same when the transaction is dropped instead.
    {
        let tx = db.transaction().expect("begin");
        support::run(&tx, "SET MEMORY_LIMIT 1G");
        // The transaction is dropped here, uncommitted.
    }
    assert_eq!(support::limit(&db), Some(1_073_741_824));
    let db = support::reopen(db, &folder);
    assert_eq!(
        support::memory_shown(&db, "limit"),
        Value::Int64(1_073_741_824)
    );
    assert_eq!(support::limit(&db), Some(1_073_741_824));
}

#[test]
fn a_compaction_step_with_too_little_memory_is_refused_before_it_reads_a_page() {
    use std::sync::atomic::{AtomicU64, Ordering};
    // The step reserves one fixed workspace, whatever the size of the file: at 2,000 and at
    // 6,000 notes the refusal asks for the same bytes.
    let mut requested = Vec::new();
    for notes in [2000_i64, 6000] {
        // Deleting the first 1,000 of 2,000 notes of 1,700 letters frees far more than a tenth of
        // the file, so a compaction step is due.
        let folder = support::folder(&format!("compaction-under-a-small-limit-{notes}"));
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let opened = |limit: Option<usize>| {
            let mut options = contextdb::DatabaseOpenOptions::default();
            options.memory_limit = limit;
            options.storage = Some(support::storage({
                let reads = reads.clone();
                move |call| {
                    if matches!(call, support::Call::Read(..)) {
                        reads.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
            support::open_with(&folder, options).expect("open through the counting door")
        };
        let db = opened(None);
        support::run(
            &db,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        );
        // Not about the interval: MIN_INTERVAL 0 keeps a pass starting at once (design 4, S4).
        support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
        let tx = db.transaction().expect("begin");
        for id in 1..=notes {
            let body = support::letters(id, 1700);
            let insert = "INSERT INTO notes (id, body) VALUES ($id, $body)";
            tx.execute(insert, &params! { "id" => id, "body" => body })
                .expect("insert a note");
        }
        tx.commit().expect("commit the notes");
        support::run(&db, &format!("DELETE FROM notes WHERE id <= {}", notes / 2));
        support::close(db);
        // What a fresh open uses, and 32 KiB of room over it: less than the step's allowance of
        // 256 pages, the pages it may read and write (S3), which it reserves before reading one.
        let db = opened(None);
        let limit = support::used(&db) + 32 * 1024;
        support::close(db);
        let db = opened(Some(limit));
        let before = (
            reads.load(Ordering::SeqCst),
            support::data_file(&folder),
            support::watermark(&db),
        );
        let refusal = db
            .run_maintenance_cycle()
            .expect_err("the step needs its page list");
        requested.push(over_the_limit(refusal, limit).0);
        let after = (
            reads.load(Ordering::SeqCst),
            support::data_file(&folder),
            support::watermark(&db),
        );
        assert_eq!(
            after, before,
            "a refused step read no page and changed no commit or file"
        );
        support::close(db);
        // With the memory the pass goes on a step a cycle, and a step moves a page (S3, S4).
        let db = opened(None);
        let moved = (0..16).any(|_| {
            let report = db.run_maintenance_cycle().expect("a cycle with memory");
            support::count(report.compacted_pages) > 0
        });
        assert!(moved, "the refused step was due: with memory it moves");
        // A window's sort reserves its whole input: refused with room for little more than a
        // limit's change, memory given back; under the ordinary limit it answered.
        let window = "WITH r AS (SELECT ROW_NUMBER() OVER (ORDER BY body) AS n FROM notes) SELECT COUNT(*) FROM r WHERE n = 1";
        assert_eq!(support::int(&db, window), 1);
        let used = support::used(&db);
        db.set_memory_limit(Some(used + support::limit_work_bytes() + 64 * 1024))
            .unwrap();
        assert!(matches!(
            support::refusal(&db, window),
            Error::MemoryBudgetExceeded { .. }
        ));
        assert_eq!(
            support::used(&db),
            used,
            "memory in use after the refused window"
        );
    }
    assert_eq!(
        requested[0], requested[1],
        "the step's workspace does not grow with the file"
    );
}

// ---- shared by the tests of this file ----

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

#[test]
fn only_two_methods_change_the_limits() {
    let source = include_str!("../src/database/mod.rs");
    let set = |l: &&str| l.trim().starts_with("pub fn set_") && l.contains("limit");
    assert_eq!(
        source.lines().filter(set).count(),
        2,
        "set_memory_limit, set_disk_limit"
    );
}
